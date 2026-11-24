"""Run-bound review and explicit comparisons. Record-level artifacts stay in warehouse.

Each preview gets isolated logical schemas. The adapter facade never changes the
connected adapter's mappings, so cancellation and interleaved users are isolated.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import os
import re
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any

from .adapters.base import IDRAdapter, get_dialect_config
from .ai_config import AIConfigContext, candidate_errors, draft_hash, policy_errors, safe_name
from .config import (
    apply_config_to_warehouse,
    compute_dry_run_summary,
    validate_config,
    validate_identifier,
)
from .runner import IDRRunner, RunConfig
from .schema_defs import SYSTEM_TABLES
from .schema_manager import SchemaManager
from .stages.base import StageContext
from .stages.graph import GraphStage


class StaleReview(ValueError):
    pass


class PreviewValidationError(ValueError):
    """User-authored configuration errors, safe to show locally, never sent to AI."""


def _literal(value: Any, dialect: str) -> str:
    text = str(value)
    if dialect in ("bigquery", "databricks"):
        text = text.replace("\\", "\\\\")
    return "'" + text.replace("'", "''") + "'"


class PreviewAdapter:
    """Connection-preserving facade; all canonical internal schemas are isolated."""

    def __init__(self, adapter: Any, token: str):
        validate_identifier(token, "preview token")
        self.backend = adapter
        self.token = token
        project = getattr(adapter, "project", None) if getattr(adapter, "dialect", "") == "bigquery" else None
        prefix = f"`{str(project).strip('`')}`." if project else ""
        self.schema_mapping = {
            s: f"{prefix}ai_{token}_{s}" for s in ("idr_meta", "idr_work", "idr_out")
        }

    def __getattr__(self, name):
        return getattr(self.backend, name)

    def _rewrite_schemas(self, sql):
        return IDRAdapter._rewrite_schemas(self, sql)

    def _rewrite_table_fqn(self, fqn):
        return IDRAdapter._rewrite_table_fqn(self, fqn)

    def execute(self, sql):
        return self.backend.execute(self._rewrite_schemas(sql))

    def execute_script(self, sql):
        return self.backend.execute_script(self._rewrite_schemas(sql))

    def query(self, sql, params=None):
        return self.backend.query(self._rewrite_schemas(sql), params=params)

    def query_one(self, sql, params=None):
        return self.backend.query_one(self._rewrite_schemas(sql), params=params)

    def table_exists(self, table):
        return self.backend.table_exists(self._rewrite_table_fqn(table))

    def get_table_columns(self, table):
        return self.backend.get_table_columns(self._rewrite_table_fqn(table))

    def close(self):
        # This facade does not own its connection.
        pass


def _physical(adapter, table):
    if getattr(adapter, "dialect", "") == "bigquery" and "." in table:
        parts = table.replace("`", "").split(".")
        if len(parts) == 2:
            schema, name = parts
            dataset = getattr(adapter, "dataset_mapping", {}).get(schema, schema)
            project = getattr(adapter, "project", None)
            return f"`{str(project).strip('`')}`.{dataset}.{name}" if project else f"{dataset}.{name}"
    return IDRAdapter._rewrite_table_fqn(adapter, table)


class AIReviewService:
    def __init__(self, adapter, owner: str = "cli"):
        self.adapter = adapter
        identity = {
            k: str(getattr(adapter, k, "")) for k in ("db_path", "project", "database", "catalog")
        }
        identity["schemas"] = getattr(adapter, "schema_mapping", {})
        self.owner = hashlib.sha256(
            json.dumps([owner, identity], sort_keys=True).encode()
        ).hexdigest()
        self.report_days = max(1, int(os.environ.get("IDR_AI_REPORT_DAYS", "30")))
        self.input_hours = max(1, int(os.environ.get("IDR_AI_INPUT_HOURS", "24")))

    def _ensure_store(self):
        a = self.adapter
        a.execute("CREATE SCHEMA IF NOT EXISTS idr_out")
        table = next(t for t in SYSTEM_TABLES if t.fqn == "idr_out.ai_review_report")
        SchemaManager(a)._create_table(table)

    def _store(self, report, token):
        self._decorate_recommendations(report)
        self._ensure_store()
        now = datetime.now(timezone.utc)
        vals = [
            report["run_id"],
            self.owner,
            json.dumps(report),
            token,
            (now + timedelta(hours=self.input_hours)).isoformat(),
            (now + timedelta(days=self.report_days)).isoformat(),
        ]
        self.adapter.execute(
            "INSERT INTO idr_out.ai_review_report "
            "(run_id, owner_key, report_json, scratch_token, input_expires_at, expires_at) VALUES ("
            + ",".join(_literal(v, self.adapter.dialect) for v in vals)
            + ")"
        )

    def _row(self, run_id):
        if not self.adapter.table_exists("idr_out.ai_review_report"):
            raise StaleReview("No saved reviews for this connection; run a preview.")
        rows = self.adapter.query(
            "SELECT * FROM idr_out.ai_review_report WHERE run_id = ? AND owner_key = ?",
            params=[run_id, self.owner],
        )
        if not rows:
            raise StaleReview("Review not found for this connection and owner; run a preview.")
        row = rows[0]
        if row["expires_at"] <= datetime.now(timezone.utc).isoformat():
            raise StaleReview("Review expired; run a new preview.")
        return row

    def get(self, run_id, fingerprint=None, lens=None):
        report = json.loads(self._row(run_id)["report_json"])
        if fingerprint and report["draft_hash"] != fingerprint:
            raise StaleReview("Draft changed; run a new preview.")
        if lens is not None and report["lens"] != lens:
            raise StaleReview("Review belongs to a different lens; preview the selected lens.")
        self._decorate_recommendations(report)
        return report

    @staticmethod
    def _decorate_recommendations(report):
        """Keep numbers, units and next steps authoritative, including old reports."""
        telemetry = report["telemetry"]
        clusters, entities = telemetry["total_clusters"], telemetry["total_entities"]
        singles = telemetry["singletons"]
        telemetry["singleton_cluster_pct"] = telemetry["singleton_pct"]
        telemetry["singleton_entity_pct"] = round(100 * singles / max(1, entities), 2)
        priority = {"high": 0, "medium": 1, "info": 2}
        report["findings"].sort(key=lambda finding: priority.get(finding["severity"], 3))
        for index, finding in enumerate(report["findings"]):
            finding["id"] = f"finding_{index + 1}"
            patch = finding.get("patch")
            finding.setdefault(
                "recommendation",
                (
                    f"Compare this draft change: {patch['label']}."
                    if patch
                    else "Keep the current settings while inspecting the affected groups locally; do not change matching based on this diagnostic alone."
                ),
            )
            finding.setdefault(
                "expected_effect",
                "The comparison measures the effect on this same extracted population; no improvement is assumed.",
            )
            finding.setdefault(
                "tradeoff",
                patch["tradeoff"]
                if patch
                else "Graph metrics alone cannot distinguish legitimate shared identifiers from incorrect matches.",
            )
        next_step = (
            report["findings"][0]["recommendation"]
            if report["findings"]
            else (
                "Run a FULL preview to obtain evidence."
                if not entities
                else "Keep this draft for review; use labeled evaluation to assess identity accuracy before deciding it is ready."
            )
        )
        report["headline"] = (
            f"{entities:,} entities resolve into {clusters:,} clusters. "
            f"{singles:,} singleton clusters are {telemetry['singleton_pct']}% of clusters "
            f"and {telemetry['singleton_entity_pct']}% of entities. Next step: {next_step}"
        )
        report["recommendation_version"] = 1

    def _skipped_findings(self, config, options, skipped):
        """Offer bounded, unmeasured trials; never silently relax a global limit."""
        findings = []
        for stat in skipped:
            kind = stat["identifier_type"].lower()
            rules = [
                rule
                for rule in config.get("rules", [])
                if kind in [key.lower() for key in rule["match_keys"]]
            ]
            for rule in rules:
                configured = int(rule.get("max_group_size", 10000))
                effective = min(configured, options.get("max_group_size") or configured)
                smallest = stat.get("min_group_size_seen")
                largest = stat["max_group_size_seen"]
                recommendation = f"Keep {rule['id']}'s group limit at {configured}; inspect the skipped {kind} groups locally before allowing broader matching."
                if effective < configured:
                    recommendation = f"Keep {rule['id']}'s limit unchanged: the preview's global cap {effective} is lower than its rule cap {configured}. Raising this rule alone would not admit these groups; inspect them locally first."
                patch = None
                if (
                    len(rules) == 1
                    and len(rule["match_keys"]) == 1
                    and smallest
                    and effective == configured
                    and configured < smallest <= min(configured * 2, 100000)
                    and smallest <= (options.get("max_group_size") or 100000)
                ):
                    patch = self._patch(config, "set_group_limit", str(rule["id"]), smallest)
                    patch["label"] = f"Compare {rule['id']} group limit {configured} → {smallest}"
                    patch["tradeoff"] = (
                        "Admitting additional shared values can join different people. Compare first and inspect affected groups locally."
                    )
                    recommendation = f"Compare {rule['id']} with max_group_size {configured} → {smallest}, the smallest skipped group size. Keep the current draft until the effect is reviewed."
                if patch:
                    try:
                        self.patch(config, patch)
                    except ValueError:
                        patch = None
                        recommendation = f"Keep {rule['id']}'s locked group limit at {configured}; review the skipped {kind} groups locally."
                findings.append(
                    {
                        "severity": "medium",
                        "title": f"Review {rule['id']}: {stat['skipped_groups']} skipped {kind} group(s)",
                        "detail": f"These groups exceeded the effective cap {effective}. Their sizes range from {smallest if smallest is not None else 'unknown'} to {largest}. Repetition alone does not prove a placeholder.",
                        "metric": f"rule limit={configured}; effective cap={effective}; skipped groups={stat['skipped_groups']}; largest={largest}",
                        "recommendation": recommendation,
                        "current_value": configured,
                        "proposed_value": patch["value"] if patch else None,
                        "expected_effect": (
                            f"Admits groups of size {smallest} through this rule's cap; actual links and membership changes must be measured."
                            if patch
                            else "Preserves the existing guardrail; no additional shared-value links are admitted."
                        ),
                        "tradeoff": patch["tradeoff"]
                        if patch
                        else "Keeping the cap may miss legitimate high-fanout matches, but avoids an unverified increase in shared-value linking.",
                        "patch": patch,
                    }
                )
            if not rules:
                findings.append(
                    {
                        "severity": "medium",
                        "title": f"Inspect skipped {kind} groups locally",
                        "detail": "This identifier could not be attributed to one active rule in the supplied draft.",
                        "metric": f"skipped groups={stat['skipped_groups']}",
                        "recommendation": f"Keep current limits and check the {kind} mappings against this preview before changing rules.",
                        "patch": None,
                    }
                )
        return findings

    def purge(self, all_owned=False, limit=10):
        self._ensure_store()
        rows = self.adapter.query(
            "SELECT run_id, scratch_token, input_expires_at, expires_at FROM idr_out.ai_review_report WHERE owner_key = ?",
            params=[self.owner],
        )
        now = datetime.now(timezone.utc).isoformat()
        count = 0
        for r in rows:
            if count >= limit:
                break
            if all_owned or r["input_expires_at"] <= now:
                if not r["scratch_token"] and not all_owned and r["expires_at"] > now:
                    continue
                if r["scratch_token"]:
                    self._drop(r["scratch_token"])
                count += 1
                self.adapter.execute(
                    "UPDATE idr_out.ai_review_report SET scratch_token = '' WHERE run_id = "
                    + _literal(r["run_id"], self.adapter.dialect)
                    + " AND owner_key = "
                    + _literal(self.owner, self.adapter.dialect)
                )
                if all_owned or r["expires_at"] <= now:
                    self.adapter.execute(
                        "DELETE FROM idr_out.ai_review_report WHERE run_id = "
                        + _literal(r["run_id"], self.adapter.dialect)
                        + " AND owner_key = "
                        + _literal(self.owner, self.adapter.dialect)
                    )
        return count

    def _drop(self, token):
        if not re.fullmatch(r"p[a-f0-9]{24}", token):
            raise ValueError("Invalid preview workspace")
        for schema in PreviewAdapter(self.adapter, token).schema_mapping.values():
            self.adapter.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")

    def _initialize(self, scoped, config):
        SchemaManager(scoped).initialize()
        apply_config_to_warehouse(scoped, config, record_snapshot=False)
        # Seed only engine baseline outputs, locally in warehouse. Never copy active
        # metadata, locks, watermarks or record-level data into a model request.
        for t in SYSTEM_TABLES:
            if t.schema != "idr_out" or not t.name.endswith("_current"):
                continue
            if self.adapter.table_exists(t.fqn):
                dest = scoped._rewrite_table_fqn(t.fqn)
                source = _physical(self.adapter, t.fqn)
                self.adapter.execute(f"DELETE FROM {dest}")
                available = {c["name"].lower() for c in self.adapter.get_table_columns(source)}
                shared = ",".join(c.name for c in t.columns if c.name.lower() in available)
                self.adapter.execute(f"INSERT INTO {dest} ({shared}) SELECT {shared} FROM {source}")
        if self.adapter.table_exists("idr_out.run_history"):
            dest = scoped._rewrite_table_fqn("idr_out.run_history")
            source = _physical(self.adapter, "idr_out.run_history")
            available = {c["name"].lower() for c in self.adapter.get_table_columns(source)}
            table = next(t for t in SYSTEM_TABLES if t.fqn == "idr_out.run_history")
            shared = ",".join(c.name for c in table.columns if c.name.lower() in available)
            self.adapter.execute(
                f"INSERT INTO {dest} ({shared}) SELECT {shared} FROM {source} WHERE status LIKE 'SUCCESS%'"
            )
        # INCR previews need their own copy of extraction state.
        if self.adapter.table_exists("idr_meta.run_state"):
            dest = scoped._rewrite_table_fqn("idr_meta.run_state")
            source = _physical(self.adapter, "idr_meta.run_state")
            available = {c["name"].lower() for c in self.adapter.get_table_columns(source)}
            table = next(t for t in SYSTEM_TABLES if t.fqn == "idr_meta.run_state")
            shared = ",".join(c.name for c in table.columns if c.name.lower() in available)
            self.adapter.execute(
                f"INSERT INTO {dest} ({shared}) SELECT {shared} FROM {source} WHERE table_id != '__idr_run_lock__'"
            )

    def preview(self, config, options=None, progress=None, ground_truth=None):
        try:
            validate_config(config)
        except ValueError as exc:
            raise PreviewValidationError(f"Invalid draft configuration: {exc}") from exc
        if not config.get("sources") or not (config.get("rules") or config.get("fuzzy_rules")):
            raise PreviewValidationError("Preview requires usable sources and matching rules")
        if ground_truth is None:
            ground_truth = config.get("evaluation")
        evaluation_options = None
        if ground_truth:
            from .evaluation_config import EvaluationOptions

            evaluation_options = EvaluationOptions.model_validate(ground_truth)
        self.purge()
        token = "p" + uuid.uuid4().hex[:24]
        scoped = PreviewAdapter(self.adapter, token)
        options = dict(options or {})
        options["dry_run"] = True
        options["skip_lenses"] = False
        options["lens"] = options.get("lens") or "default"
        if options["lens"] in ("none", "__none__"):
            options["lens"] = "default"
        rc = RunConfig(**options)
        options["run_mode"] = rc.run_mode
        try:
            self._initialize(scoped, config)
            if evaluation_options:
                from .evaluation_config import materialize_labels

                ground_truth = materialize_labels(
                    self.adapter,
                    config,
                    evaluation_options,
                    scoped._rewrite_table_fqn("idr_work.ai_ground_truth"),
                )
                ground_truth["_options"] = evaluation_options.model_dump(mode="json")
            self._save_input(scoped, config, options, ground_truth)
            result = IDRRunner(scoped, progress_callback=progress).run(rc)
            if result.status == "SUCCESS_NO_CHANGES":
                from .stages.output import OutputStage

                string = get_dialect_config(scoped.dialect)["string_type"]
                scoped.execute(
                    f"CREATE TABLE idr_work.lp_labels (entity_key {string}, label {string})"
                )
                scoped.execute(f"CREATE TABLE idr_work.lp_edges_lens (src {string}, dst {string})")
                scoped.execute(
                    f"CREATE TABLE idr_work.edges_new (left_entity_key {string}, right_entity_key {string}, identifier_type {string})"
                )
                ctx = StageContext(
                    scoped,
                    get_dialect_config(scoped.dialect),
                    result.run_id,
                    logging.getLogger(__name__),
                    [],
                    dry_run=True,
                )
                OutputStage(ctx, config=rc).generate_dry_run_output()
                result.status = "DRY_RUN_COMPLETE"
            if result.status != "DRY_RUN_COMPLETE":
                if result.error_code in {"GRAPH_NOT_CONVERGED", "FULL_REBUILD_REQUIRED"}:
                    raise PreviewValidationError(result.error)
                raise ValueError("Preview failed; inspect local run diagnostics before retrying")
            summary = compute_dry_run_summary(
                scoped, result.run_id, rc.run_mode, result.edges_created, result.clusters_impacted
            )
            if options["lens"] != "default":
                lid = _literal(options["lens"], scoped.dialect)
                baseline_clusters = int(
                    scoped.query_one(
                        f"SELECT COUNT(*) FROM idr_out.identity_lens_clusters_current WHERE lens_id={lid}"
                    )
                    or 0
                )
                summary.update(
                    current_clusters=baseline_clusters,
                    clusters_delta=result.clusters_impacted - baseline_clusters,
                    current_edges=None,
                    edges_delta=None,
                    edges_delta_by_type={},
                    shadow_preserved_entities=0,
                    corroborating_identifier_types=[],
                )
                summary["edge_baseline_available"] = False
            report = self._report(scoped, config, result.run_id, options)
            from .profile_preview import validate_profiles

            report["profile_validation"] = validate_profiles(config)
            if ground_truth:
                self._evaluate(scoped, report, ground_truth)
            self._evaluation_findings(report)
            self._store(report, token)
            logging.getLogger(__name__).info(
                "AI preview outcome=%s mode=%s duration=%.3f",
                report["status"],
                rc.run_mode,
                result.duration_seconds,
            )
            return {
                **asdict(result),
                "dry_run_summary": summary,
                "draft_hash": report["draft_hash"],
                "profile_validation": report["profile_validation"],
                "review": report,
            }
        except BaseException:
            self._drop(token)
            raise

    @staticmethod
    def _evaluate(scoped, report, ground_truth):
        from .evaluation_config import EvaluationOptions, acceptance
        from .evaluator import IDREvaluator

        scoped.execute(
            "CREATE OR REPLACE TABLE idr_work.ai_evaluation_membership AS SELECT entity_key, label AS resolved_id FROM idr_work.ai_review_membership"
        )
        kwargs = {
            k: v
            for k, v in ground_truth.items()
            if k in {"ground_truth_table", "entity_key_col", "truth_id_col", "top_n_errors"}
        }
        evaluated = IDREvaluator(scoped).evaluate_ground_truth(
            **kwargs,
            membership_table=scoped._rewrite_table_fqn("idr_work.ai_evaluation_membership"),
        )
        report["evaluation"] = {
            k: v for k, v in evaluated.items() if v is None or isinstance(v, (int, float))
        }
        options = EvaluationOptions.model_validate(ground_truth.get("_options") or kwargs)
        report["evaluation_acceptance"] = acceptance(evaluated, options)
        if report["evaluation_acceptance"]["status"] != "passed":
            report["limitations"].append(
                "Labeled evaluation: " + "; ".join(report["evaluation_acceptance"]["reasons"])
            )
            if report["status"] == "no_detected_risks":
                report["status"] = report["health"] = "needs_review"

    def _save_input(self, scoped, config, options, ground_truth=None):
        string = get_dialect_config(scoped.dialect)["string_type"]
        scoped.execute(f"CREATE TABLE idr_work.ai_preview_input (payload {string})")
        payload = json.dumps({"config": config, "options": options, "ground_truth": ground_truth})
        scoped.execute(
            "INSERT INTO idr_work.ai_preview_input VALUES ("
            + _literal(payload, scoped.dialect)
            + ")"
        )

    def _report(self, scoped, config, run_id, options):
        # Every required query is explicit. A missing metric fails review rather
        # than being converted into zero and labeled healthy.
        if (
            options.get("lens", "default") == "default"
            and not options.get("strict", False)
            and scoped.table_exists("idr_work.fuzzy_results")
        ):
            scoped.execute("""CREATE OR REPLACE TABLE idr_work.ai_review_membership AS
                SELECT l.entity_key, COALESCE(f.super_cluster_id,l.label) AS label
                FROM idr_work.lp_labels l LEFT JOIN idr_work.fuzzy_results f ON l.label=f.resolved_id""")
        else:
            scoped.execute(
                "CREATE OR REPLACE TABLE idr_work.ai_review_membership AS SELECT * FROM idr_work.lp_labels"
            )
        sizes = scoped.query("""WITH c AS (SELECT label, COUNT(*) n FROM idr_work.ai_review_membership GROUP BY label)
            SELECT COUNT(*) total_clusters, COALESCE(SUM(n),0) total_entities,
            COALESCE(MAX(n),0) max_cluster_size, COALESCE(SUM(CASE WHEN n=1 THEN 1 ELSE 0 END),0) singletons,
            COALESCE(SUM(CASE WHEN n>100 THEN 1 ELSE 0 END),0) large_clusters FROM c""")[0]
        tel = {k: int(v or 0) for k, v in sizes.items()}
        tel["singleton_pct"] = round(100 * tel["singletons"] / max(1, tel["total_clusters"]), 2)
        if 0 < tel["singletons"] < tel["total_clusters"] and tel["singleton_pct"] >= 100:
            tel["singleton_pct"] = 99.99
        tel["edges_by_type"] = {
            r["identifier_type"].lower(): int(r["n"])
            for r in scoped.query(
                "SELECT identifier_type, COUNT(*) n FROM idr_work.edges_new GROUP BY identifier_type"
            )
        }
        # Named lens telemetry must count the actual filtered edges, not superset.
        evaluated_types = None
        if options.get("lens", "default") != "default":
            definition = next(
                lens_def for lens_def in config["lenses"] if lens_def["id"] == options["lens"]
            )
            include = {k.lower() for k in definition.get("include_types", [])}
            exclude = {k.lower() for k in definition.get("exclude_types", [])}
            evaluated_types = {
                k.lower()
                for r in config.get("rules", [])
                for k in r["match_keys"]
                if (not include or k.lower() in include) and k.lower() not in exclude
            }
            selected_types = evaluated_types | {"manual_override"}
            type_list = ",".join(_literal(k, scoped.dialect) for k in selected_types)
            tel["edges_by_type"] = {
                r["identifier_type"].lower(): int(r["n"])
                for r in scoped.query(f"""
                SELECT e.identifier_type, COUNT(*) n FROM idr_work.edges_new e
                JOIN idr_work.lp_edges_lens l ON l.src=e.left_entity_key AND l.dst=e.right_entity_key
                WHERE LOWER(e.identifier_type) IN ({type_list}) GROUP BY e.identifier_type""")
            }
        source_expr = (
            "SPLIT(entity_key, ':')[OFFSET(0)]"
            if scoped.dialect == "bigquery"
            else "SPLIT_PART(entity_key, ':', 1)"
        )
        tel["source_coverage"] = (
            scoped.query(f"""WITH sizes AS (SELECT label, COUNT(*) n FROM idr_work.ai_review_membership GROUP BY label)
            SELECT {source_expr} source_id, COUNT(*) total_entities,
            SUM(CASE WHEN s.n=1 THEN 1 ELSE 0 END) singleton_entities
            FROM idr_work.ai_review_membership m JOIN sizes s ON m.label=s.label GROUP BY 1""")
        )
        tel["rule_contributions"] = [
            {
                "rule_id": str(rule["id"]),
                "edge_count": sum(
                    tel["edges_by_type"].get(key.lower(), 0) for key in rule["match_keys"]
                ),
                "causal_effect_measured": False,
            }
            for rule in config.get("rules", [])
            if evaluated_types is None
            or any(key.lower() in evaluated_types for key in rule["match_keys"])
        ]
        tel["zero_edge_rules"] = [
            {"rule_id": str(rule["id"]), "identifier_type": key.lower()}
            for rule in config.get("rules", [])
            for key in rule["match_keys"]
            if (evaluated_types is None or key.lower() in evaluated_types)
            and not tel["edges_by_type"].get(key.lower(), 0)
        ]
        findings = []
        for rule in config.get("fuzzy_rules", []):
            if options.get("strict") or options.get("lens", "default") != "default":
                continue
            threshold = float(rule.get("threshold", 0.85))
            if threshold < 0.9:
                findings.append(
                    {
                        "severity": "info",
                        "title": f"Fuzzy rule {rule['id']} uses threshold {threshold}",
                        "detail": "A stricter threshold may separate legitimate matches as well as false positives. Compare its effect; the threshold alone does not establish error.",
                        "metric": str(threshold),
                        "patch": self._patch(config, "raise_fuzzy_threshold", str(rule["id"]), 0.9),
                    }
                )
        for r in config.get("rules", []):
            if evaluated_types is not None and not any(
                k.lower() in evaluated_types for k in r["match_keys"]
            ):
                continue
            n = sum(tel["edges_by_type"].get(k.lower(), 0) for k in r.get("match_keys", []))
            if n == 0:
                findings.append(
                    {
                        "severity": "info",
                        "title": f"Rule {r['id']} added no edges in this view",
                        "detail": "This may be correct for the evaluated population or lens. Compare before removing it.",
                        "metric": "0 edges",
                        "patch": self._patch(config, "remove_rule", str(r["id"]), None),
                    }
                )
        tel["mega_cluster_contributors"] = []
        if tel["large_clusters"]:
            counts = scoped.query("""WITH large AS (SELECT label FROM idr_work.ai_review_membership GROUP BY label HAVING COUNT(*)>100)
                SELECT e.identifier_type, COUNT(*) edge_count FROM idr_work.edges_new e
                JOIN idr_work.ai_review_membership m ON e.left_entity_key=m.entity_key
                JOIN large l ON m.label=l.label GROUP BY e.identifier_type ORDER BY edge_count DESC""")
            for count in counts:
                kind = count["identifier_type"].lower()
                if evaluated_types is not None and kind not in evaluated_types:
                    continue
                tel["mega_cluster_contributors"].append(
                    {"identifier_type": kind, "edge_count": int(count["edge_count"])}
                )
                rules = [
                    rule
                    for rule in config.get("rules", [])
                    if kind in [k.lower() for k in rule["match_keys"]]
                ]
                patch = None
                if len(rules) == 1:
                    if len(config.get("rules", [])) > 1:
                        patch = self._patch(config, "remove_rule", str(rules[0]["id"]), None)
                    elif int(rules[0].get("max_group_size", 10000)) > 2:
                        patch = self._patch(
                            config,
                            "set_group_limit",
                            str(rules[0]["id"]),
                            max(2, min(100, int(rules[0].get("max_group_size", 10000)) // 2)),
                        )
                findings.append(
                    {
                        "severity": "high",
                        "title": f"Large clusters contain {kind} links",
                        "detail": f"{count['edge_count']} {kind} edges occur inside components above 100 records. Edge volume is an association, not a proven cause. Compare one change to measure its effect.",
                        "metric": str(count["edge_count"]),
                        "patch": patch,
                    }
                )
            if not counts:
                findings.append(
                    {
                        "severity": "high",
                        "title": "Large clusters need review",
                        "detail": "Graph size alone does not prove identity errors.",
                        "metric": str(tel["max_cluster_size"]),
                        "patch": None,
                    }
                )
        skipped = scoped.query(
            "SELECT identifier_type, COUNT(*) n, MIN(group_size) min_group, MAX(group_size) max_group FROM idr_out.skipped_identifier_groups WHERE run_id = ? GROUP BY identifier_type",
            params=[run_id],
        )
        skipped = [
            r
            for r in skipped
            if evaluated_types is None or r["identifier_type"].lower() in evaluated_types
        ]
        tel["skipped_supernodes"] = [
            {
                "identifier_type": r["identifier_type"].lower(),
                "skipped_groups": int(r["n"]),
                "max_group_size_seen": int(r["max_group"]),
                "min_group_size_seen": int(r["min_group"]),
            }
            for r in skipped
        ]
        findings.extend(self._skipped_findings(config, options, tel["skipped_supernodes"]))
        for finding in findings:
            if finding.get("patch"):
                try:
                    self.patch(config, finding["patch"])
                except ValueError:
                    finding["patch"] = None
        if not tel["total_entities"]:
            findings = []
        status = (
            "insufficient_evidence"
            if not tel["total_entities"]
            else "high_risk"
            if tel["large_clusters"]
            else "needs_review"
            if findings
            else "no_detected_risks"
        )
        return {
            "version": 1,
            "run_id": run_id,
            "draft_hash": draft_hash(config),
            "lens": options.get("lens", "default"),
            "run_mode": options.get("run_mode", "FULL"),
            "execution_options": options,
            "status": status,
            "health": status,
            "headline": "Review of this exact draft and graph view; graph diagnostics do not establish identity accuracy.",
            "provider": "deterministic",
            "model": "graph_analyzer",
            "pii_masked": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "telemetry": tel,
            "findings": findings,
            "evaluation": None,
            "limitations": (
                ["Incremental population only"] if options.get("run_mode") == "INCR" else []
            )
            + (
                [
                    "Named lens evaluates deterministic lens memberships; primary fuzzy refinement is separate"
                ]
                if options.get("lens", "default") != "default"
                else []
            ),
        }

    @staticmethod
    def _evaluation_findings(report):
        metrics = report.get("evaluation") or {}
        fp, fn = metrics.get("fp_pairs", 0), metrics.get("fn_pairs", 0)
        if fp or fn:
            report["status"] = "high_risk" if fp else "needs_review"
            report["health"] = report["status"]
            report["findings"].append(
                {
                    "severity": "high" if fp else "medium",
                    "title": "Labeled evaluation found incorrect identity pairs",
                    "detail": "Measured against the supplied labels in this evaluated population. Detailed identities remain local.",
                    "metric": f"false positives={fp}; false negatives={fn}",
                    "patch": None,
                }
            )

    @staticmethod
    def _patch(config, action, target, value):
        return {
            "action": action,
            "target": target,
            "value": value,
            "base_draft_hash": draft_hash(config),
            "label": f"{action.replace('_', ' ')}: {target}",
            "measured": False,
            "tradeoff": "May change legitimate links; compare before accepting.",
        }

    def patch(self, config, patch):
        if patch.get("base_draft_hash") != draft_hash(config):
            raise StaleReview("Patch is stale; review the current draft")
        candidate = copy.deepcopy(config)
        action, target, value = patch["action"], str(patch["target"]), patch.get("value")
        if action in ("remove_rule", "set_group_limit"):
            matches = [r for r in candidate.get("rules", []) if str(r["id"]) == target]
            if len(matches) != 1:
                raise ValueError("Patch must identify one exact rule")
            if action == "remove_rule":
                candidate["rules"] = [r for r in candidate["rules"] if str(r["id"]) != target]
            else:
                if (
                    isinstance(value, bool)
                    or not isinstance(value, int)
                    or not 2 <= value <= 100000
                ):
                    raise ValueError("Group limit must be an integer from 2 to 100000")
                matches[0]["max_group_size"] = value
        elif action == "raise_fuzzy_threshold":
            matches = [r for r in candidate.get("fuzzy_rules", []) if str(r["id"]) == target]
            if (
                len(matches) != 1
                or isinstance(value, bool)
                or not isinstance(value, (float, int))
                or not float(matches[0]["threshold"]) < value <= 0.99
            ):
                raise ValueError("Choose a stricter fuzzy threshold, up to 0.99")
            matches[0]["threshold"] = value
        elif action == "set_lens_types":
            matches = [
                lens_def for lens_def in candidate.get("lenses", []) if lens_def["id"] == target
            ]
            if (
                len(matches) != 1
                or not isinstance(value, list)
                or not all(isinstance(x, str) for x in value)
            ):
                raise ValueError("Choose a lens and a list of mapped identifier types")
            mapped = {
                i["type"].upper() for s in candidate["sources"] for i in s.get("identifiers", [])
            }
            if not set(x.upper() for x in value) <= mapped:
                raise ValueError("Unknown lens identifier types")
            matches[0]["include_types"] = value
        else:
            raise ValueError("Unsupported patch action")
        context = AIConfigContext.model_validate(config.get("ai_assistance") or {})
        errors = candidate_errors(candidate) + policy_errors(candidate, context, config)
        validate_config(candidate)
        if not (candidate.get("rules") or candidate.get("fuzzy_rules")):
            errors.append("Cannot remove the last matching rule")
        if errors:
            raise ValueError("; ".join(errors))
        return candidate

    def compare(self, run_id, config, patch, lens="default", progress=None):
        baseline = self.get(run_id, draft_hash(config), lens)
        row = self._row(run_id)
        if row["input_expires_at"] <= datetime.now(timezone.utc).isoformat():
            raise StaleReview("Comparison inputs expired; run a fresh preview")
        candidate = self.patch(config, patch)
        if patch["action"] in ("remove_rule", "set_group_limit"):
            selected = next(r for r in config["rules"] if str(r["id"]) == str(patch["target"]))
            selected_types = {k.lower() for k in selected["match_keys"]}
            if any(
                str(r["id"]) != str(selected["id"])
                and selected_types & {k.lower() for k in r["match_keys"]}
                for r in config["rules"]
            ):
                raise ValueError(
                    "Shared identifier rules require a fresh preview; extracted evidence cannot distinguish their normalization"
                )
        source = PreviewAdapter(self.adapter, row["scratch_token"])
        token = "p" + uuid.uuid4().hex[:24]
        scoped = PreviewAdapter(self.adapter, token)
        inputs = json.loads(source.query_one("SELECT payload FROM idr_work.ai_preview_input"))
        options = inputs["options"]
        ground_truth = inputs.get("ground_truth")
        try:
            self._initialize(scoped, candidate)
            # Replace copied live baselines with the exact baseline of the original preview.
            for table in SYSTEM_TABLES:
                if (
                    table.schema == "idr_out"
                    and table.name.endswith("_current")
                    and source.table_exists(table.fqn)
                ):
                    dest, src = (
                        scoped._rewrite_table_fqn(table.fqn),
                        source._rewrite_table_fqn(table.fqn),
                    )
                    self.adapter.execute(f"CREATE OR REPLACE TABLE {dest} AS SELECT * FROM {src}")
            # Copy the evaluated extraction, never re-read mutable source tables.
            for name in (
                "entities_delta",
                "identifiers_all",
                "identifiers_delta",
                "entity_attributes",
                "reextracted_entities",
            ):
                fqn = "idr_work." + name
                if source.table_exists(fqn):
                    dest, src = scoped._rewrite_table_fqn(fqn), source._rewrite_table_fqn(fqn)
                    self.adapter.execute(f"CREATE OR REPLACE TABLE {dest} AS SELECT * FROM {src}")
            if patch["action"] in ("remove_rule", "set_group_limit"):
                changed = next(r for r in config["rules"] if str(r["id"]) == str(patch["target"]))
                types = ",".join(
                    _literal(k.lower(), self.adapter.dialect) for k in changed["match_keys"]
                )
                if patch["action"] == "remove_rule":
                    scoped.execute(
                        f"DELETE FROM idr_work.identifiers_all WHERE LOWER(identifier_type) IN ({types})"
                    )
                else:
                    scoped.execute(
                        f"UPDATE idr_work.identifiers_all SET max_group_size = {int(patch['value'])} WHERE LOWER(identifier_type) IN ({types})"
                    )
            trial_id = "dry_trial_" + uuid.uuid4().hex[:12]
            original_id = baseline["run_id"]
            scoped.execute(
                f"UPDATE idr_work.entities_delta SET run_id = '{trial_id}' WHERE run_id = '{original_id}'"
            )
            ctx = StageContext(
                scoped,
                get_dialect_config(scoped.dialect),
                trial_id,
                logging.getLogger(__name__),
                [],
                dry_run=True,
                progress_callback=progress,
            )
            graph = GraphStage(ctx, RunConfig(**options))
            if progress:
                progress({"event": "stage_start", "stage": "comparison", "run_id": trial_id})
            graph.build_edges()
            graph.label_propagation(options.get("max_iters", 30))
            if not options.get("strict", False):
                graph.run_fuzzy_matching(options.get("max_iters", 30))
            self._selected_lens(scoped, graph, candidate, options)
            report = self._report(scoped, candidate, trial_id, options)
            # Partition changes are independent of arbitrary resolved IDs. Count
            # records in baseline components that split or trial components that merge.
            b, c = (
                source._rewrite_table_fqn("idr_work.ai_review_membership"),
                scoped._rewrite_table_fqn("idr_work.ai_review_membership"),
            )
            changes = self.adapter.query(f"""WITH pairs AS (SELECT b.entity_key, b.label old_label, c.label new_label
                FROM {b} b JOIN {c} c ON b.entity_key=c.entity_key),
                splits AS (SELECT old_label FROM pairs GROUP BY old_label HAVING COUNT(DISTINCT new_label)>1),
                merges AS (SELECT new_label FROM pairs GROUP BY new_label HAVING COUNT(DISTINCT old_label)>1)
                SELECT COUNT(*) compared_entities,
                COALESCE(SUM(CASE WHEN old_label IN (SELECT old_label FROM splits) THEN 1 ELSE 0 END),0) split_affected_entities,
                COALESCE(SUM(CASE WHEN new_label IN (SELECT new_label FROM merges) THEN 1 ELSE 0 END),0) merge_affected_entities FROM pairs""")[
                0
            ]
            report["comparison"] = {
                "baseline_run_id": run_id,
                "changes": {k: int(v or 0) for k, v in changes.items()},
                "baseline_telemetry": baseline["telemetry"],
                "same_extraction": True,
                "patch": {**patch, "measured": True},
            }
            if ground_truth:
                gt = dict(ground_truth)
                gt["ground_truth_table"] = source._rewrite_table_fqn("idr_work.ai_ground_truth")
                self._evaluate(scoped, report, gt)
                dest, src = (
                    scoped._rewrite_table_fqn("idr_work.ai_ground_truth"),
                    source._rewrite_table_fqn("idr_work.ai_ground_truth"),
                )
                self.adapter.execute(f"CREATE TABLE {dest} AS SELECT * FROM {src}")
            if ground_truth:
                ground_truth = {
                    **ground_truth,
                    "ground_truth_table": scoped._rewrite_table_fqn("idr_work.ai_ground_truth"),
                }
            self._save_input(scoped, candidate, options, ground_truth)
            self._evaluation_findings(report)
            self._store(report, token)
            logging.getLogger(__name__).info("AI comparison outcome=%s", report["status"])
            return report
        except BaseException:
            self._drop(token)
            raise

    @staticmethod
    def _selected_lens(scoped, graph, config, options):
        lens = options.get("lens", "default")
        if lens == "default":
            return
        definition = next(
            (lens_def for lens_def in config.get("lenses", []) if lens_def["id"] == lens), None
        )
        if not definition:
            raise ValueError("Selected lens is not defined in this draft")
        graph.resolve_lens({**definition, "lens_id": lens}, options.get("max_iters", 30))
        scoped.execute(
            "CREATE OR REPLACE TABLE idr_work.lp_labels AS SELECT * FROM idr_work.lp_labels_lens"
        )

    def explain(self, run_id, fingerprint, lens, client=None, draft_config=None):
        report = self.get(run_id, fingerprint, lens)
        # Upgrade older aggregate reports without re-running extraction or graph work.
        if draft_config is not None:
            if draft_hash(draft_config) != fingerprint:
                raise StaleReview("Draft changed; run a fresh preview")
            generic = "Some shared identifiers exceeded group limits"
            if any(f["title"] == generic for f in report["findings"]):
                report["findings"] = [f for f in report["findings"] if f["title"] != generic]
                report["findings"].extend(
                    self._skipped_findings(
                        draft_config,
                        report.get("execution_options", {}),
                        report["telemetry"].get("skipped_supernodes", []),
                    )
                )
                self._decorate_recommendations(report)
        if client and client.config.is_enabled and report["findings"]:
            tel = report["telemetry"]
            # AI may prioritize validated actions, but cannot invent metrics or changes.
            # Never send config, SQL, record values, hashes, or free-text prose.
            payload = {
                k: tel[k]
                for k in (
                    "total_entities",
                    "total_clusters",
                    "max_cluster_size",
                    "singletons",
                    "singleton_cluster_pct",
                    "singleton_entity_pct",
                    "large_clusters",
                )
            }
            payload["candidates"] = [
                {
                    "id": f["id"],
                    "severity": f["severity"],
                    "action": f["patch"]["action"] if f.get("patch") else "inspect_locally",
                    "target": safe_name(f["patch"]["target"]) if f.get("patch") else None,
                    "value": f["patch"]["value"]
                    if f.get("patch") and isinstance(f["patch"]["value"], (int, float))
                    else None,
                }
                for f in report["findings"]
            ]
            ids = [f["id"] for f in report["findings"]]
            schema = {
                "type": "object",
                "properties": {
                    "recommendation_ids": {
                        "type": "array",
                        "items": {"type": "string", "enum": ids},
                        "maxItems": len(ids),
                    }
                },
                "required": ["recommendation_ids"],
                "additionalProperties": False,
            }
            try:
                result = client.chat_structured(
                    [
                        {
                            "role": "system",
                            "content": "Prioritize the supplied diagnostic actions for human review. Return only their IDs, highest priority first. Graph metrics do not prove identity accuracy.",
                        },
                        {"role": "user", "content": json.dumps(payload)},
                    ],
                    schema,
                    "review_priorities",
                )
                selected = result.get("recommendation_ids", [])
                if not isinstance(selected, list):
                    raise ValueError("Invalid priorities")
                rank = {
                    key: index
                    for index, key in enumerate(
                        dict.fromkeys(
                            key for key in selected if isinstance(key, str) and key in ids
                        )
                    )
                }
                report["findings"].sort(key=lambda f: rank.get(f["id"], len(rank)))
                report["provider"] = client.config.provider
                report["model"] = client.config.resolved_model()
                # Preserve severity ordering and authoritative summary wording.
                self._decorate_recommendations(report)
            except Exception:
                pass
        return report
