"""Opt-in, owner-bound profile previews. Limits bound results, not source scan size."""

from __future__ import annotations

import math
import re
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from .config import validate_config, validate_identifier
from .profile_builder import ProfileBuilder


class ProfilePreviewOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: Literal["validate", "targeted", "full"] = "validate"
    attributes: list[str] = Field(default_factory=list, max_length=50)
    grouped_outputs: list[str] = Field(default_factory=list, max_length=20)
    max_clusters: int = Field(default=25, ge=1, le=1000)
    max_members: int = Field(default=10000, ge=1, le=1000000000)
    timeout_seconds: int = Field(default=15, ge=1, le=120)
    max_bytes_billed: int | None = Field(default=None, ge=1048576)
    allow_unknown_scan_cost: bool = False
    allow_full_population: bool = False


def validate_profiles(config):
    validate_config(config)
    sources = config.get("sources", [])
    fields = []
    for rule in config.get("survivorship", []):
        contributing = [
            s
            for s in sources
            if any(a["name"] == rule["attribute"] for a in s.get("attributes", []))
        ]
        if not contributing:
            raise ValueError(
                f"Survivorship field {rule['attribute']} has no source attribute mapping"
            )
        fields.append(
            {
                "attribute": rule["attribute"],
                "strategy": rule["strategy"],
                "sources": [s["id"] for s in contributing],
                "source_deduplication": {s["id"]: bool(s.get("deduplicate")) for s in contributing},
            }
        )
    return {
        "mode": "validate",
        "status": "validated",
        "data_scanned": False,
        "attributes": fields,
        "grouped_outputs": [
            {"id": d["id"], "dimensions": d["dimensions"], "fields": d["fields"]}
            for d in config.get("grouped_outputs", [])
        ],
        "source_scan_cost": "unknown",
        "limitations": [
            "Metadata validation does not establish business-key uniqueness, source overlap, currency compatibility or identity accuracy."
        ],
    }


@contextmanager
def query_budget(adapter, seconds, max_bytes_billed=None):
    # Delegate to the adapter's native cancellation / timeout / scan-cap implementation.
    # Never time out another user's queries.
    if not hasattr(adapter, "query_budget"):
        raise ValueError(
            "Data profile previews require an adapter with an enforced query budget. "
            "Metadata validation remains available."
        )
    with adapter.query_budget(seconds, max_bytes_billed=max_bytes_billed) as check:
        yield check


class BudgetAdapter:
    def __init__(self, backend, check):
        self.backend, self.check = backend, check

    def __getattr__(self, name):
        value = getattr(self.backend, name)
        if name not in {"execute", "query", "query_one", "get_table_columns", "table_exists"}:
            return value

        def bounded(*args, **kwargs):
            self.check()
            result = value(*args, **kwargs)
            self.check()
            return result

        return bounded


def _json(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def preview_profiles(service, config, run_id, fingerprint, lens, options):
    validation = validate_profiles(config)
    if options.mode == "validate":
        return validation
    if not options.attributes and not options.grouped_outputs:
        raise ValueError("Select at least one profile attribute or grouped output")
    has_scan_cap = bool(
        getattr(service.adapter, "supports_scan_cost_cap", False)
        and options.max_bytes_billed is not None
    )
    if not options.allow_unknown_scan_cost and not has_scan_cap:
        raise ValueError(
            "Source scan cost is unknown. Explicitly allow source scans (or set max_bytes_billed on BigQuery) before requesting a data preview; result limits do not bound scan size."
        )
    if options.mode == "full" and not options.allow_full_population:
        raise ValueError("A full-population profile preview requires explicit confirmation")
    from .ai_config import draft_hash
    from .ai_review import PreviewAdapter, StaleReview, _literal

    if draft_hash(config) != fingerprint:
        raise StaleReview("Draft changed; run a fresh graph preview")
    report = service.get(run_id, fingerprint, lens)
    row = service._row(run_id)
    if (
        not row["scratch_token"]
        or row["input_expires_at"] <= datetime.now(timezone.utc).isoformat()
    ):
        raise StaleReview("Preview inputs expired; run a fresh graph preview")
    scoped = PreviewAdapter(service.adapter, row["scratch_token"])
    requested = set(options.attributes)
    if not set(options.grouped_outputs) <= {d["id"] for d in config.get("grouped_outputs", [])}:
        raise ValueError("Requested grouped outputs are not configured")
    rules = scoped.query(
        "SELECT * FROM idr_meta.survivorship_rule WHERE is_active=TRUE ORDER BY attribute_name"
    )
    known = {r["attribute_name"] for r in rules}
    if not requested <= known:
        raise ValueError("Requested profile attributes are not configured in this draft")
    # Keep all dependencies of a shared row winner even when previewing one field.
    groups = {
        r.get("row_group") for r in rules if r["attribute_name"] in requested and r.get("row_group")
    }
    rules = [r for r in rules if r["attribute_name"] in requested or r.get("row_group") in groups]
    builder = ProfileBuilder(scoped)
    with query_budget(
        service.adapter, options.timeout_seconds, max_bytes_billed=options.max_bytes_billed
    ) as check:
        backend = BudgetAdapter(service.adapter, check)
        scoped = PreviewAdapter(backend, row["scratch_token"])
        builder = ProfileBuilder(scoped)
        if options.mode == "full":
            if report["telemetry"]["total_entities"] > options.max_members:
                raise ValueError(
                    "Full preview exceeds max_members; raise the explicit budget or use targeted mode"
                )
            scoped.execute(
                "CREATE OR REPLACE TABLE idr_work.profile_preview_members AS SELECT entity_key, label AS resolved_id FROM idr_work.ai_review_membership"
            )
        else:
            candidates = scoped.query(f"""SELECT label, COUNT(*) AS members FROM idr_work.ai_review_membership
                GROUP BY label HAVING COUNT(*) <= {options.max_members} ORDER BY label LIMIT {options.max_clusters}""")
            selected, members = [], 0
            for cluster in candidates:
                if members + int(cluster["members"]) <= options.max_members:
                    selected.append(_literal(cluster["label"], scoped.dialect))
                    members += int(cluster["members"])
            predicate = "label IN (" + ",".join(selected) + ")" if selected else "1=0"
            scoped.execute(
                "CREATE OR REPLACE TABLE idr_work.profile_preview_members AS SELECT entity_key, label AS resolved_id FROM idr_work.ai_review_membership WHERE "
                + predicate
            )
        counts = scoped.query(
            "SELECT COUNT(*) AS members, COUNT(DISTINCT resolved_id) AS clusters FROM idr_work.profile_preview_members"
        )[0]
        if not counts["members"]:
            return {
                "status": "no_complete_clusters_within_budget",
                "mode": options.mode,
                "selected_clusters": 0,
                "selected_members": 0,
                "profile_count": 0,
                "attributes": [],
                "grouped_outputs": {},
                "limitations": [
                    "No complete cluster fits the member budget, or the evaluated population is empty. No source profile values were scanned."
                ],
            }
        sources = scoped.query("SELECT * FROM idr_meta.source_table WHERE is_active=TRUE")
        mappings = scoped.query("SELECT * FROM idr_meta.entity_attribute_mapping")
        fields = []
        profile_count = 0
        grouped_results = {}
        if requested:
            staging = builder._build_staging_cte(
                rules, mappings, sources, membership_table="idr_work.profile_preview_members"
            )
            if not staging:
                raise ValueError("No profile attribute sources are configured")
            scoped.execute("CREATE OR REPLACE TABLE idr_work.profile_preview_values AS " + staging)
            lookup = {
                key: s["table_id"]
                for s in sources
                for key in (s["table_id"], s["table_fqn"], s["table_fqn"].replace(".", "_"))
            }
            ranked = builder._rank_frequency("SELECT * FROM idr_work.profile_preview_values", rules)
            ranked = builder._rank_row_groups(ranked, rules, lookup)
            expressions = [
                builder._build_column_selection(
                    {**r, "_prepared_frequency": r["strategy"] == "FREQUENCY"}, lookup
                )
                for r in rules
            ]
            scoped.execute(
                "CREATE OR REPLACE TABLE idr_work.profile_preview_result AS SELECT resolved_id, "
                + ",".join(expressions)
                + " FROM ("
                + ranked
                + ") ranked GROUP BY resolved_id"
            )
            types = {
                c["name"]: c["type"]
                for c in scoped.get_table_columns("idr_work.profile_preview_result")
            }
            baseline = backend._rewrite_table_fqn("idr_out.golden_profiles")
            baseline_types = (
                {c["name"]: c["type"] for c in backend.get_table_columns(baseline)}
                if backend.table_exists(baseline) and lens == "default"
                else {}
            )
            baseline_columns = set(baseline_types)
            members_table = scoped._rewrite_table_fqn("idr_work.profile_preview_members")
            after = scoped._rewrite_table_fqn("idr_work.profile_preview_result")
            for attr in sorted(requested):
                validate_identifier(attr, "profile attribute")
                numeric = bool(
                    re.search(r"INT|DOUBLE|FLOAT|DECIMAL|NUMERIC|REAL", types[attr.lower()], re.I)
                )
                stats = scoped.query(
                    f"SELECT COUNT(*) AS profiles, COUNT({attr}) AS non_null"
                    + (f",SUM({attr}) AS sample_sum" if numeric else "")
                    + " FROM idr_work.profile_preview_result"
                )[0]
                field = {
                    "attribute": attr,
                    "after": {k: _json(v) for k, v in stats.items()},
                    "baseline_available": attr.lower() in baseline_columns,
                }
                if attr.lower() in baseline_columns:
                    before = backend.query(
                        f"SELECT COUNT(*) AS profiles, COUNT(b.{attr}) AS non_null"
                        + (
                            f",SUM(b.{attr}) AS sample_sum"
                            if numeric
                            and re.search(
                                r"INT|DOUBLE|FLOAT|DECIMAL|NUMERIC|REAL",
                                baseline_types[attr.lower()],
                                re.I,
                            )
                            else ""
                        )
                        + f" FROM {baseline} b WHERE b.resolved_id IN (SELECT DISTINCT resolved_id FROM {members_table})"
                    )[0]
                    from .adapters.base import get_dialect_config

                    str_type = get_dialect_config(scoped.dialect)["string_type"]
                    changed = backend.query(f"""SELECT COUNT(*) AS paired_profiles,
                        COALESCE(SUM(CASE WHEN (a.{attr} IS NULL AND b.{attr} IS NOT NULL) OR (a.{attr} IS NOT NULL AND b.{attr} IS NULL)
                        OR CAST(a.{attr} AS {str_type}) <> CAST(b.{attr} AS {str_type}) THEN 1 ELSE 0 END),0) AS changed_profiles
                        FROM {after} a JOIN {baseline} b ON a.resolved_id=b.resolved_id""")[0]
                    field.update(before={k: _json(v) for k, v in before.items()}, **changed)
                fields.append(field)
            profile_count = int(
                scoped.query_one("SELECT COUNT(*) FROM idr_work.profile_preview_result") or 0
            )
        if options.grouped_outputs:
            from .grouped_outputs import materialize_grouped_outputs

            materialize_grouped_outputs(
                scoped,
                run_id,
                config=config,
                membership_table="idr_work.profile_preview_members",
                output_ids=options.grouped_outputs,
            )
            for output_id in options.grouped_outputs:
                stats = scoped.query(
                    f"SELECT COUNT(*) AS rows, COALESCE(SUM(CASE WHEN _idr_conflict THEN 1 ELSE 0 END),0) AS conflicts FROM idr_out.grouped_{validate_identifier(output_id)}"
                )[0]
                grouped_results[output_id] = {k: int(v or 0) for k, v in stats.items()}
        return {
            "status": "complete",
            "mode": options.mode,
            "run_id": run_id,
            "draft_hash": fingerprint,
            "lens": lens,
            "selected_clusters": int(counts["clusters"]),
            "selected_members": int(counts["members"]),
            "profile_count": profile_count,
            "grouped_outputs": grouped_results,
            "attributes": fields,
            "source_scan_cost": "bounded_by_max_bytes_billed" if has_scan_cap else "unknown",
            "max_bytes_billed": options.max_bytes_billed if has_scan_cap else None,
            "timeout_seconds": options.timeout_seconds,
            "sample_scope": "full_evaluated_population"
            if options.mode == "full"
            else "deterministic_complete_clusters_not_representative",
            "input_scope": "frozen_graph_membership_with_current_source_values",
            "limitations": [
                "Sums describe selected profiles only; do not extrapolate to the population.",
                "Targeted subsets are deterministic, not representative; clusters larger than the member budget are omitted.",
                "Baseline comparisons cover identical resolved IDs only and use currently saved profiles.",
                "Result limits do not bound source scan size. No warehouse data was sent to AI.",
            ],
        }
