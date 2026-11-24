"""Contracts and local policy enforcement for PII-safe configuration assistance.

Only ``model_evidence`` and ``model_goal`` may cross the model boundary. Configs,
SQL, values, catalog prose, user goals, and diagnostic exceptions stay local.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class AIConfigContext(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal[1] = 1
    grain: Literal["individual", "household", "account"] = "individual"
    event_policy: Literal["bridge", "include", "exclude"] = "bridge"
    approved_event_aggregates: dict[str, dict[str, Literal["AGG_SUM", "AGG_MIN", "AGG_MAX"]]] = (
        Field(default_factory=dict)
    )
    excluded_tables: list[str] = Field(default_factory=list)
    system_of_record: str | None = None
    allowed_matching_identifiers: list[str] | None = None
    locked_fields: list[str] = Field(default_factory=list)
    table_roles: dict[
        str, Literal["MASTER_ENTITY", "SUB_ENTITY", "EVENT_FACT", "REFERENCE_LOOKUP"]
    ] = Field(default_factory=dict)
    objectives: list[Literal["precision", "coverage", "cross_source", "auditability"]] = Field(
        default_factory=list
    )
    fuzzy: bool = False
    requested_lenses: list[str] = Field(default_factory=list)


def draft_hash(config: dict) -> str:
    """New versioned fingerprint; do not change historical warehouse config hashes."""
    semantic = {
        k: config.get(k) or []
        for k in (
            "sources",
            "rules",
            "fuzzy_rules",
            "survivorship",
            "exclusions",
            "overrides",
            "lenses",
        )
    }
    semantic["sources"] = [
        {"entity_type": "PERSON", "attributes": [], "identifiers": [], **source}
        for source in semantic["sources"]
    ]
    semantic["rules"] = [
        {
            "type": "EXACT",
            "priority": 1,
            "canonicalize": "LOWERCASE",
            "max_group_size": 10000,
            **rule,
            "id": str(rule["id"]),
        }
        for rule in semantic["rules"]
    ]
    semantic["survivorship"] = [
        {"source_priority": [], **rule} for rule in semantic["survivorship"]
    ]
    semantic["lenses"] = [
        {"include_types": [], "exclude_types": [], "source_filter": [], **lens}
        for lens in semantic["lenses"]
    ]
    semantic["exclusions"] = [
        {
            "identifier_type": e.get("identifier_type") or e.get("type"),
            "value": e.get("value", e.get("identifier_value_pattern")),
            "match_type": e.get("match_type") or e.get("match") or "EXACT",
        }
        for e in semantic["exclusions"]
    ]
    if config.get("grouped_outputs"):
        semantic["grouped_outputs"] = config["grouped_outputs"]
    semantic["use_default_exclusions"] = bool(config.get("use_default_exclusions", False))
    if config.get("ai_assistance"):
        semantic["ai_assistance"] = AIConfigContext.model_validate(
            config["ai_assistance"]
        ).model_dump(mode="json")

    def normalize(obj):
        if isinstance(obj, list):
            return [normalize(v) for v in obj]
        if isinstance(obj, dict):
            result = {
                k: normalize(v)
                for k, v in obj.items()
                if v is not None
                and k
                not in ("column", "data_type", "name" if "blocking_key" in obj else "__none__")
                and not (k == "approved_event_aggregates" and not v)
                and not (k in ("is_hashed", "source_is_hashed", "deduplicate") and v is False)
                and not (k == "watermark_lookback_minutes" and v == 0)
                and not (k in ("watermark_column", "dedup_order_by") and v == "")
            }
            if "column" in obj and "expr" not in result:
                result["expr"] = obj["column"]
            return result
        return obj

    return (
        "v1:"
        + hashlib.sha256(
            json.dumps(
                normalize(semantic), sort_keys=True, separators=(",", ":"), default=str
            ).encode()
        ).hexdigest()
    )


def restore_authoring_context(active: dict, saved: dict, dialect: str) -> dict:
    from .config import DEFAULT_EXCLUSIONS, resolve_entity_key_expr

    result = copy.deepcopy(active)

    def ordered(current, previous, key):
        by_key = {str(item[key]): item for item in current}
        order = [str(item[key]) for item in previous if str(item[key]) in by_key]
        return [by_key[k] for k in order] + [
            item for item in current if str(item[key]) not in order
        ]

    result["sources"] = ordered(result.get("sources", []), saved.get("sources", []), "id")
    old_sources = {s["id"]: s for s in saved.get("sources", [])}
    for source in result["sources"]:
        old = old_sources.get(source["id"], {})
        if old.get("table") != source["table"]:
            continue
        if old.get("entity_key", "").upper() == "AUTO" or old.get("key_columns"):
            if resolve_entity_key_expr(old, dialect) == source["entity_key"]:
                source["entity_key"] = old.get("entity_key", "AUTO")
                if old.get("key_columns"):
                    source["key_columns"] = old["key_columns"]
        source["identifiers"] = ordered(
            source.get("identifiers", []), old.get("identifiers", []), "type"
        )
        source["attributes"] = ordered(
            source.get("attributes", []), old.get("attributes", []), "name"
        )
    for field in ("rules", "fuzzy_rules", "lenses"):
        result[field] = ordered(result.get(field, []), saved.get(field, []), "id")
        previous = {str(item["id"]): item for item in saved.get(field, [])}
        for item in result[field]:
            old = previous.get(str(item["id"]), {})
            for expr_key in ("canonicalize", "score_expr"):
                value = old.get(expr_key)
                if isinstance(value, dict) and (
                    value.get(dialect) or value.get("default")
                ) == item.get(expr_key):
                    item[expr_key] = value
    result["survivorship"] = ordered(
        result.get("survivorship", []), saved.get("survivorship", []), "attribute"
    )

    def exclusion_key(e):
        return (
            (e.get("identifier_type") or e.get("type") or "ANY").upper(),
            e.get("value", e.get("identifier_value_pattern", "")).lower(),
            (e.get("match_type") or e.get("match") or "EXACT").upper(),
        )

    defaults = {exclusion_key(e) for e in DEFAULT_EXCLUSIONS}
    exclusions = result.get("exclusions", [])
    if saved.get("use_default_exclusions") and defaults <= {exclusion_key(e) for e in exclusions}:
        result["use_default_exclusions"] = True
        result["exclusions"] = [e for e in exclusions if exclusion_key(e) not in defaults]
    desired = {exclusion_key(e): e for e in saved.get("exclusions", [])}
    current = {exclusion_key(e): e for e in result.get("exclusions", [])}
    result["exclusions"] = [current[k] for k in desired if k in current] + [
        e for k, e in current.items() if k not in desired
    ]
    return result


def config_diff(before: dict, after: dict) -> list[dict]:
    """Local-only diff: never serialize its values into a model request."""
    return [
        {"path": "/" + k, "before": before.get(k), "after": after.get(k)}
        for k in sorted(set(before) | set(after))
        if before.get(k) != after.get(k)
    ]


def safe_name(value: Any) -> str:
    from .llm import mask_pii_text

    raw = str(value)
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]{0,199}", raw) and mask_pii_text(raw) == raw:
        # Digit runs in object names may be customer/account identifiers.
        if not re.search(r"\d{6,}", raw):
            return raw
    return "ref_" + hashlib.sha256(raw.encode()).hexdigest()[:12]


def local_goal_objectives(goal: str) -> list[str]:
    text = goal.lower()
    patterns = {
        "precision": r"precision|false.positive|conservative|accurate",
        "coverage": r"coverage|recall|missing.match",
        "cross_source": r"cross.source|across.source|cross.channel",
        "auditability": r"audit|explain|traceab",
    }
    return [key for key, pattern in patterns.items() if re.search(pattern, text)]


def model_goal(context: AIConfigContext) -> str:
    """No unrestricted goal prose, FQNs, literals, or locked values."""
    return json.dumps(
        {
            "grain": context.grain,
            "event_policy": context.event_policy,
            "approved_event_aggregates": {
                safe_name(table): {safe_name(attr): strategy for attr, strategy in attrs.items()}
                for table, attrs in context.approved_event_aggregates.items()
            },
            "fuzzy": context.fuzzy,
            "objectives": context.objectives,
            "requested_lenses": [safe_name(x) for x in context.requested_lenses],
            "excluded_tables": [safe_name(x) for x in context.excluded_tables],
            "allowed_matching_identifiers": [
                safe_name(x) for x in context.allowed_matching_identifiers
            ]
            if context.allowed_matching_identifiers is not None
            else None,
            "system_of_record": safe_name(context.system_of_record)
            if context.system_of_record
            else None,
        }
    )


def model_evidence(profile: Any) -> tuple[dict, dict[str, str]]:
    """Allowlist statistical evidence and maintain a local name reference map."""
    refs: dict[str, str] = {}

    def name(raw: str) -> str:
        token = safe_name(raw)
        refs[token] = raw
        return token

    tables = []
    for table in profile.tables:
        cols = []
        for col in table.columns:
            c = {
                "name": name(col.name),
                "type": safe_name(col.declared_type),
                "null_rate": col.null_rate,
                "uniqueness_ratio": col.uniqueness_ratio,
                "max_freq": col.max_frequency_count,
                "hint_role": col.hint_role,
                "declared_pk": col.is_declared_pk,
                "hint_confidence": col.hint_confidence,
            }
            if col.hint_identifier_type:
                c["hint_identifier_type"] = name(col.hint_identifier_type)
            if col.hint_canonicalizer:
                c["hint_canonicalizer"] = col.hint_canonicalizer
            c["hint_is_hashed"] = col.hint_is_hashed
            c["patterns"] = {
                k: getattr(col, k)
                for k in (
                    "email_rate",
                    "email_plus_tag_rate",
                    "phone_us10_rate",
                    "phone_e164_rate",
                    "hex_hash_rate",
                    "uuid_rate",
                )
            }
            cols.append(c)
        tables.append(
            {
                "table": name(table.table_fqn),
                "row_count": table.row_count,
                "sample_rows": table.sample_rows_analyzed,
                "table_role": table.table_role,
                "hint_entity_key": name(table.hint_entity_key) if table.hint_entity_key else None,
                "hint_watermark": name(table.hint_watermark) if table.hint_watermark else None,
                "duplicate_entity_keys": table.duplicate_entity_keys_detected,
                "null_entity_keys": table.null_entity_keys_detected,
                "key_and_signal_columns": cols,
            }
        )
    overlaps = []
    for o in profile.overlaps:
        overlaps.append(
            {
                "table_a": name(o.table_a),
                "column_a": name(o.column_a),
                "table_b": name(o.table_b),
                "column_b": name(o.column_b),
                "identifier_type": name(o.identifier_type),
                "overlap_a_in_b": o.overlap_a_in_b,
                "overlap_b_in_a": o.overlap_b_in_a,
                "shared_distinct_estimate": o.shared_distinct_estimate,
            }
        )
    return {"tables": tables, "cross_table_overlaps": overlaps}, refs


def restore_refs(value: Any, refs: dict[str, str]) -> Any:
    if isinstance(value, dict):
        return {k: restore_refs(v, refs) for k, v in value.items()}
    if isinstance(value, list):
        return [restore_refs(v, refs) for v in value]
    if isinstance(value, str):
        # Model mapping expressions are constrained to bare references by Pass A.
        return refs.get(value, value)
    return value


def identifier_allowed(kind: str, context: AIConfigContext, profile: Any = None) -> bool:
    k = kind.lower()
    if context.allowed_matching_identifiers is not None:
        return k in {x.lower() for x in context.allowed_matching_identifiers}
    if context.grain == "household":
        return not any(
            x in k for x in ("ip_address", "session", "product", "facility", "store", "merchant")
        )
    if context.grain == "account":
        return any(
            x in k
            for x in ("account", "company", "organization", "organisation", "domain", "business")
        )
    if any(
        x in k
        for x in (
            "phone",
            "address",
            "device",
            "ip_address",
            "session",
            "account",
            "company",
            "product",
            "store",
            "facility",
            "merchant",
        )
    ):
        return False
    if profile:
        occurrences = [
            c
            for t in profile.tables
            for c in t.columns
            if (c.hint_identifier_type or "").lower() == k
        ]
        return all(c.hint_confidence >= 0.8 for c in occurrences) if occurrences else True
    return True


def pointer(config: dict, path: str) -> Any:
    if not path.startswith("/"):
        raise ValueError("Locked fields must be JSON pointers")
    value: Any = config
    for part in path[1:].split("/"):
        key = part.replace("~1", "/").replace("~0", "~")
        value = value[int(key)] if isinstance(value, list) else value[key]
    return value


def candidate_errors(config: dict) -> list[str]:
    """Static contract shared by generation, preview, patching and config saves."""
    from .canonicalizers import validate_canonicalize_spec
    from .config import validate_config, validate_identifier, validate_sql_expr

    errors = []
    try:
        validate_config(config)
        for field in ("sources", "rules", "fuzzy_rules", "lenses"):
            ids = [str(item.get("id", "")) for item in config.get(field, [])]
            if len(ids) != len(set(ids)):
                errors.append(f"Duplicate IDs in {field}")
            for item_id in ids:
                # Legacy numeric rule IDs are supported by the execution engine.
                validate_identifier("r_" + item_id if item_id.isdigit() else item_id, field + " ID")
        mapped, attrs = set(), set()
        source_ids = {s["id"] for s in config.get("sources", [])}
        for source in config.get("sources", []):
            if source.get("entity_key") and source["entity_key"].upper() != "AUTO":
                validate_sql_expr(source["entity_key"], "entity key")
            local_types, local_attrs = set(), set()
            for mapping in source.get("identifiers", []):
                kind = mapping["type"].lower()
                validate_identifier(kind, "identifier type")
                validate_sql_expr(
                    mapping.get("expr") or mapping.get("column"), "identifier expression"
                )
                if kind in local_types:
                    errors.append(f"Duplicate identifier mapping in source {source['id']}")
                local_types.add(kind)
                mapped.add(kind)
            for attr in source.get("attributes", []):
                validate_identifier(attr["name"], "attribute name")
                validate_sql_expr(
                    attr.get("expr") or attr.get("column") or attr["name"], "attribute expression"
                )
                if attr["name"] in local_attrs:
                    errors.append(f"Duplicate attribute in source {source['id']}")
                local_attrs.add(attr["name"])
                attrs.add(attr["name"].lower())
        for rule in config.get("rules", []):
            keys = rule.get("match_keys", [])
            if len(keys) != 1 or any(k.lower() not in mapped for k in keys):
                errors.append(f"Rule {rule['id']} requires one mapped matching identifier")
            canonical = rule.get("canonicalize", "LOWERCASE")
            for spec in canonical.values() if isinstance(canonical, dict) else [canonical]:
                validate_canonicalize_spec(spec)
            limit = rule.get("max_group_size", 10000)
            if isinstance(limit, bool) or not isinstance(limit, int) or not 2 <= limit <= 100000:
                errors.append(f"Invalid group limit for rule {rule['id']}")
        for rule in config.get("fuzzy_rules", []):
            blocking = rule.get("blocking_key") or rule.get("blocking_key_expr")
            score = rule.get("score_expr")
            validate_sql_expr(blocking, "fuzzy blocking expression")
            score_values = list(score.values()) if isinstance(score, dict) else [score]
            for expression in score_values:
                validate_sql_expr(expression, "fuzzy score expression")
                if "<a>." not in expression or "<b>." not in expression:
                    errors.append(f"Fuzzy rule {rule['id']} requires paired attribute references")
            if not 0.5 <= float(rule.get("threshold", 0.85)) <= 0.99:
                errors.append(f"Invalid fuzzy threshold for rule {rule['id']}")
        for sv in config.get("survivorship", []):
            if sv["attribute"].lower() not in attrs:
                errors.append(f"Unmapped survivorship attribute {sv['attribute']}")
            if not set(sv.get("source_priority", [])) <= source_ids:
                errors.append(f"Unknown survivorship source for {sv['attribute']}")
        for lens in config.get("lenses", []):
            if (
                not {
                    x.lower() for x in lens.get("include_types", []) + lens.get("exclude_types", [])
                }
                <= mapped
            ):
                errors.append(f"Lens {lens['id']} references unmapped identifier types")
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        errors.append(str(exc))
    return errors


def policy_errors(
    config: dict, context: AIConfigContext, base: dict | None = None, profile: Any = None
) -> list[str]:
    errors = []
    table_map = profile.table_map() if profile else {}
    for s in config.get("sources", []):
        if s["table"] in context.excluded_tables:
            errors.append(f"Explicitly excluded source: {s['table']}")
        tp = table_map.get(s["table"])
        role = tp.table_role if tp else context.table_roles.get(s["table"])
        if role == "EVENT_FACT":
            if context.event_policy == "exclude":
                errors.append(f"Event source excluded by policy: {s['table']}")
            elif context.event_policy == "bridge":
                approved = context.approved_event_aggregates.get(s["table"], {})
                strategies = {
                    rule["attribute"]: rule["strategy"] for rule in config.get("survivorship", [])
                }
                for attr in s.get("attributes", []):
                    name = attr["name"]
                    strategy = strategies.get(name)
                    if name not in approved or approved[name] != strategy:
                        errors.append(
                            f"Bridge source {s['table']}.{name} requires explicit aggregate approval in Survivorship (AGG_SUM, AGG_MIN or AGG_MAX), or change Event handling to Include fully."
                        )
    for rule in config.get("rules", []):
        if any(not identifier_allowed(k, context, profile) for k in rule.get("match_keys", [])):
            errors.append(f"Rule {rule['id']} violates allowed matching identifiers")
    if config.get("fuzzy_rules") and not context.fuzzy:
        errors.append("Fuzzy matching requires explicit policy selection")
    for path in context.locked_fields:
        try:
            if base is None or pointer(base, path) != pointer(config, path):
                errors.append(f"Locked field changed: {path}")
        except (KeyError, IndexError, ValueError, TypeError):
            errors.append(f"Locked field missing or invalid: {path}")
    return errors


def enforce_policy(config: dict, context: AIConfigContext, profile: Any) -> tuple[dict, list[dict]]:
    cfg = copy.deepcopy(config)
    decisions = []
    tmap = profile.table_map()
    kept = []
    for s in cfg.get("sources", []):
        tp = tmap.get(s["table"])
        if s["table"] in context.excluded_tables or (
            tp and tp.table_role == "EVENT_FACT" and context.event_policy == "exclude"
        ):
            decisions.append(
                {
                    "target": s["table"],
                    "origin": "user",
                    "reason": "Source excluded by policy",
                    "needs_review": False,
                }
            )
            continue
        if tp and tp.table_role == "EVENT_FACT" and context.event_policy == "bridge":
            approved = context.approved_event_aggregates.get(s["table"], {})
            strategies = {
                rule["attribute"]: rule["strategy"] for rule in cfg.get("survivorship", [])
            }
            s["attributes"] = [
                attr
                for attr in s.get("attributes", [])
                if attr["name"] in approved
                and approved[attr["name"]] == strategies.get(attr["name"])
            ]
        if context.grain == "account":
            s["entity_type"] = "ACCOUNT"
        kept.append(s)
    cfg["sources"] = kept
    mapped = {i["type"].lower() for s in kept for i in s.get("identifiers", [])}
    rules = []
    for rule in cfg.get("rules", []):
        keys = rule.get("match_keys", [])
        if keys and all(
            k.lower() in mapped and identifier_allowed(k, context, profile) for k in keys
        ):
            rules.append(rule)
        else:
            decisions.append(
                {
                    "target": rule["id"],
                    "origin": "deterministic",
                    "reason": "Matching identifier requires policy approval",
                    "needs_review": True,
                }
            )
    cfg["rules"] = rules
    if not context.fuzzy:
        cfg.pop("fuzzy_rules", None)
    attrs = {a["name"] for s in kept for a in s.get("attributes", [])}
    cfg["survivorship"] = [x for x in cfg.get("survivorship", []) if x["attribute"] in attrs]
    source_ids = {s["id"] for s in kept}
    sor = next(
        (
            s["id"]
            for s in kept
            if s["id"] == context.system_of_record or s["table"] == context.system_of_record
        ),
        None,
    )
    for sv in cfg["survivorship"]:
        sv["source_priority"] = [x for x in sv.get("source_priority", []) if x in source_ids]
        contributors = {
            s["id"]
            for s in kept
            if any(a["name"] == sv["attribute"] for a in s.get("attributes", []))
        }
        if (
            sor
            and sor in contributors
            and sv["strategy"] in {"RECENCY", "PRIORITY", "PRIORITY_RECENCY", "LATEST_FROM_SOURCE"}
        ):
            sv["source_priority"] = [sor] + [x for x in sv["source_priority"] if x != sor]
            sv["strategy"] = (
                "PRIORITY_RECENCY"
                if any(s.get("watermark_column") for s in kept if s["id"] == sor)
                else "PRIORITY"
            )
        if sv["strategy"] == "ANY_TRUE" and any(
            x in sv["attribute"].lower() for x in ("consent", "opt_in", "permission")
        ):
            sv["strategy"] = "ALL_TRUE"
            decisions.append(
                {
                    "target": sv["attribute"],
                    "origin": "deterministic",
                    "reason": "Consent aggregation requires review; conservative ALL_TRUE proposed",
                    "needs_review": True,
                }
            )
    cfg["ai_assistance"] = context.model_dump(mode="json")
    return cfg, decisions
