"""
Symmetric SQL round-trip serialization (`config_to_sql`) and deserialization (`sql_to_config`).

Co-located intentionally so any schema addition to `idr_meta.*` updates write and read paths
in lockstep.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from .sql_helpers import build_batched_upsert, build_bq_type_map, build_single_upsert
from .validators import (
    DEFAULT_EXCLUSIONS,
    resolve_entity_key_expr,
    validate_identifier,
    validate_sql_expr,
)


def config_to_sql(
    config: Dict[str, Any],
    dialect: str = "duckdb",
    batched: bool = False,
    chunk_size: int = 100,
) -> List[str]:
    """Convert YAML/dict configuration into dialect-specific SQL UPSERT statements."""
    statements: List[str] = []
    batched_groups: Dict[tuple, List[Dict[str, Any]]] = {}
    bq_type_map: Optional[Dict[tuple, Any]] = build_bq_type_map() if dialect == "bigquery" else None

    def generate_upsert(table: str, keys: List[str], data: Dict[str, Any]) -> str:
        if batched:
            group_key = (table, tuple(keys))
            batched_groups.setdefault(group_key, []).append(data)
            return ""
        return build_single_upsert(dialect, table, keys, data, bq_type_map)

    # Source tables
    for src in config.get("sources") or []:
        raw_wm = src.get("watermark_column")
        if raw_wm is None:
            raw_wm = src.get("watermark")
        wm_col = (
            None
            if raw_wm is None or str(raw_wm).strip().lower() in ("", "none", "null")
            else str(raw_wm).strip()
        )
        role = str(src.get("role") or "").strip().lower()
        entity_type = src.get("entity_type")
        if role in ("child", "event", "bridge") and (
            not entity_type or str(entity_type).upper() in ("PERSON", "PRIMARY")
        ):
            entity_type = "CHILD"
        elif role == "primary" and not entity_type:
            entity_type = "PRIMARY"
        elif role == "subject" and not entity_type:
            entity_type = "SUBJECT"
        raw_kind = str(src.get("entity_kind") or "").strip().lower()
        entity_kind = (
            validate_identifier(raw_kind, f"Source '{src['id']}' entity_kind")
            if raw_kind
            else "person"
        )
        data = {
            "table_id": src["id"],
            "table_fqn": (
                f"{config.get('schema_source')}.{src['table']}"
                if config.get("schema_source") and "." not in src["table"]
                else src["table"]
            ),
            "entity_type": entity_type or "PERSON",
            "entity_kind": entity_kind,
            "entity_key_expr": resolve_entity_key_expr(src, dialect),
            "watermark_column": wm_col,
            "deduplicate": bool(src.get("deduplicate", False)),
            "dedup_order_by": src.get("dedup_order_by"),
            "is_active": True,
        }
        statements.append(generate_upsert("idr_meta.source_table", ["table_id"], data))

        # Identifier mappings (explicit or inferred from exact rules)
        raw_ids = src.get("identifiers") or []
        inferred_ids = []
        if not raw_ids:
            source_attrs = {a["name"]: a["expr"] for a in (src.get("attributes") or [])}
            for rule in config.get("rules") or []:
                if rule.get("type", "EXACT") == "EXACT":
                    for key in rule.get("match_keys") or []:
                        if key in source_attrs:
                            inferred_ids.append({"type": key, "expr": source_attrs[key]})
            raw_ids = inferred_ids

        if isinstance(raw_ids, dict):
            id_iter = raw_ids.items()
        else:
            id_iter = [
                (item["type"], item.get("expr") or item.get("column") or item["type"])
                for item in raw_ids
                if isinstance(item, dict)
                and "type" in item
                and ("expr" in item or "column" in item)
            ]

        for id_type, col_expr in id_iter:
            matched_item = (
                next((i for i in raw_ids if isinstance(i, dict) and i.get("type") == id_type), {})
                if isinstance(raw_ids, list)
                else {}
            )
            raw_issuer = (
                matched_item.get("issuer")
                if matched_item.get("issuer") is not None
                else matched_item.get("issuer_expr")
            )
            issuer_expr = (
                validate_sql_expr(
                    str(raw_issuer).strip(),
                    f"Source '{src['id']}' identifier '{id_type}' issuer",
                )
                if raw_issuer is not None and str(raw_issuer).strip()
                else None
            )
            data = {
                "table_id": src["id"],
                "identifier_type": id_type,
                "identifier_value_expr": col_expr,
                "is_hashed": bool(matched_item.get("is_hashed", False)),
                "source_is_hashed": bool(matched_item.get("source_is_hashed", False)),
                "issuer_expr": issuer_expr,
            }
            statements.append(
                generate_upsert(
                    "idr_meta.identifier_mapping", ["table_id", "identifier_type"], data
                )
            )

        # Attribute mappings
        raw_attrs = src.get("attributes") or []
        if isinstance(raw_attrs, dict):
            attr_iter = raw_attrs.items()
        else:
            attr_iter = [
                (item["name"], item.get("expr") or item.get("column") or item["name"])
                for item in raw_attrs
                if isinstance(item, dict) and "name" in item
            ]

        for attr_name, attr_expr in attr_iter:
            data = {
                "table_id": src["id"],
                "attribute_name": attr_name,
                "attribute_expr": attr_expr,
            }
            statements.append(
                generate_upsert(
                    "idr_meta.entity_attribute_mapping", ["table_id", "attribute_name"], data
                )
            )

    # Rules
    rules_list = config.get("rules") or []
    if not rules_list and "rules" not in config:
        seen_types = set()
        inferred_rules = []
        prio = 1
        for src in config.get("sources") or []:
            for ident in src.get("identifiers") or []:
                if isinstance(ident, dict):
                    t = ident.get("type")
                    if t and t not in seen_types:
                        seen_types.add(t)
                        t_lower = t.lower()
                        canon = (
                            "EMAIL_STANDARD"
                            if "email" in t_lower
                            else ("PHONE_US_10" if "phone" in t_lower else "LOWERCASE")
                        )
                        inferred_rules.append(
                            {
                                "id": f"rule_exact_{t.lower()}",
                                "type": "EXACT",
                                "match_keys": [t],
                                "canonicalize": canon,
                                "priority": prio,
                                "max_group_size": 10000,
                            }
                        )
                        prio += 1
        rules_list = inferred_rules

    for rule in rules_list:
        raw_canon = rule.get("canonicalize", "LOWERCASE")
        if isinstance(raw_canon, dict):
            canon_val = raw_canon.get(dialect) or raw_canon.get("default") or "LOWERCASE"
        else:
            canon_val = raw_canon
        data = {
            "rule_id": rule.get("id") or rule.get("rule_id", "rule_1"),
            "identifier_type": (
                rule.get("identifier_type")
                or (
                    rule["match_keys"][0]
                    if rule.get("match_keys")
                    else rule.get("type", str(rule.get("id", "rule")).upper())
                )
            ),
            "canonicalize": canon_val,
            "max_group_size": rule.get("max_group_size", 10000),
            "priority": rule.get("priority", 100),
            "is_active": True,
            "confidence_weight": (
                float(rule["confidence_weight"])
                if rule.get("confidence_weight") is not None
                else None
            ),
            "fanout_threshold": (
                int(rule["fanout_threshold"])
                if rule.get("fanout_threshold") is not None
                else None
            ),
        }
        statements.append(generate_upsert("idr_meta.rule", ["rule_id"], data))

    # Exclusions (merge default pack if requested + explicit exclusions)
    combined_exclusions = []
    if config.get("use_default_exclusions", False):
        combined_exclusions.extend(DEFAULT_EXCLUSIONS)
    combined_exclusions.extend(config.get("exclusions") or [])

    seen_excl = set()
    for excl in combined_exclusions:
        match_type = excl.get("match_type") or excl.get("match") or "EXACT"
        id_type = excl.get("identifier_type") or excl.get("type")
        val_pattern = excl.get("value") or excl.get("identifier_value_pattern")
        if not id_type or not val_pattern:
            continue
        dedup_key = (str(id_type).upper(), str(val_pattern).lower())
        if dedup_key in seen_excl:
            continue
        seen_excl.add(dedup_key)
        data = {
            "identifier_type": id_type,
            "identifier_value_pattern": val_pattern,
            "match_type": match_type,
            "reason": excl.get("reason", "Config exclusion"),
        }
        statements.append(
            generate_upsert(
                "idr_meta.identifier_exclusion",
                ["identifier_type", "identifier_value_pattern"],
                data,
            )
        )

    # Manual Stewardship Overrides (FORCE_LINK, DO_NOT_LINK, ISOLATE)
    for idx, ov in enumerate(config.get("overrides") or []):
        ov_type = str(ov.get("type") or ov.get("override_type") or "").upper()
        if ov_type not in ("FORCE_LINK", "DO_NOT_LINK", "ISOLATE"):
            raise ValueError(
                f"Invalid override type '{ov_type}'. Must be FORCE_LINK, DO_NOT_LINK, or ISOLATE."
            )
        ekey_a = ov.get("entity_key_a") or ov.get("entity_key")
        ekey_b = ov.get("entity_key_b") or ("__ISOLATE__" if ov_type == "ISOLATE" else "")
        if not ekey_a:
            raise ValueError(f"Override {idx} missing 'entity_key_a' (or 'entity_key')")
        if ov_type in ("FORCE_LINK", "DO_NOT_LINK") and not ekey_b:
            raise ValueError(f"Override {idx} ({ov_type}) requires 'entity_key_b'")
        ov_id = ov.get("id") or ov.get("override_id") or f"ov_{ov_type.lower()}_{idx + 1}"
        data = {
            "override_id": str(ov_id),
            "override_type": ov_type,
            "entity_key_a": str(ekey_a),
            "entity_key_b": str(ekey_b),
            "reason": ov.get("reason", "Manual stewardship override"),
            "created_by": ov.get("created_by", "config"),
            "is_active": bool(ov.get("is_active", True)),
        }
        statements.append(
            generate_upsert(
                "idr_meta.identity_override",
                ["override_type", "entity_key_a", "entity_key_b"],
                data,
            )
        )

    # Fuzzy Rules
    for rule in config.get("fuzzy_rules") or []:
        rule_id = rule.get("rule_id", rule.get("id"))
        if not rule_id:
            raise ValueError("Fuzzy rule missing 'id' or 'rule_id'")

        data = {
            "rule_id": rule_id,
            "rule_name": rule.get("name", str(rule_id)),
            "blocking_key_expr": rule["blocking_key"],
            "score_expr": (
                rule["score_expr"].get(dialect, rule["score_expr"].get("default"))
                if isinstance(rule["score_expr"], dict)
                else rule["score_expr"]
            ),
            "threshold": rule.get("threshold", 0.85),
            "priority": rule.get("priority", 100),
            "is_active": True,
        }
        statements.append(generate_upsert("idr_meta.fuzzy_rule", ["rule_id"], data))

    # Survivorship Rules
    for rule in config.get("survivorship") or []:
        priority_list = rule.get("source_priority", [])
        if isinstance(priority_list, list):
            priority_list_str = str(priority_list).replace("'", '"')
        else:
            priority_list_str = str(priority_list)

        raw_recency = rule.get("recency_field")
        recency_field = (
            None
            if raw_recency is None or str(raw_recency).strip().lower() in ("", "none", "null")
            else str(raw_recency).strip()
        )
        data = {
            "attribute_name": rule["attribute"],
            "strategy": rule["strategy"],
            "source_priority_list": priority_list_str,
            "recency_field": recency_field,
            "row_group": (
                validate_identifier(rule["row_group"], "row_group")
                if rule.get("row_group")
                else None
            ),
            "is_active": True,
        }
        statements.append(generate_upsert("idr_meta.survivorship_rule", ["attribute_name"], data))

    for extension in ("evaluation", "grouped_outputs", "activation_delta"):
        if config.get(extension):
            statements.append(
                generate_upsert(
                    "idr_meta.config_extension",
                    ["extension_key"],
                    {"extension_key": extension, "value_json": json.dumps(config[extension])},
                )
            )
        elif extension in config:
            statements.append(
                f"DELETE FROM idr_meta.config_extension WHERE extension_key = '{extension}';"
            )

    if "generate_evidence" in config:
        if config.get("generate_evidence") is not None:
            statements.append(
                generate_upsert(
                    "idr_meta.config_extension",
                    ["extension_key"],
                    {
                        "extension_key": "generate_evidence",
                        "value_json": json.dumps(bool(config["generate_evidence"])),
                    },
                )
            )
        else:
            statements.append(
                "DELETE FROM idr_meta.config_extension WHERE extension_key = 'generate_evidence';"
            )

    # Multi-Lens definitions (optional)
    for lens in config.get("lenses") or []:
        raw_lid = str(lens.get("id") or lens.get("lens_id") or "").strip()
        lens_id = validate_identifier(raw_lid, "lens.id")
        inc = [
            str(t).strip().upper()
            for t in (lens.get("include_types") or lens.get("include_rules") or [])
            if str(t).strip()
        ]
        exc = [
            str(t).strip().upper()
            for t in (lens.get("exclude_types") or lens.get("exclude_rules") or [])
            if str(t).strip()
        ]
        src_flt = [str(s).strip() for s in (lens.get("source_filter") or []) if str(s).strip()]
        mgs = int(lens["max_group_size"]) if lens.get("max_group_size") else None
        data = {
            "lens_id": lens_id,
            "include_types_json": json.dumps(inc),
            "exclude_types_json": json.dumps(exc),
            "source_filter_json": json.dumps(src_flt),
            "max_group_size": mgs,
            "is_active": True,
        }
        statements.append(generate_upsert("idr_meta.lens_definition", ["lens_id"], data))

    if batched:
        batched_statements: List[str] = []
        safe_chunk = max(1, int(chunk_size or 100))
        for (table, keys_tuple), rows in batched_groups.items():
            deduped: Dict[tuple, Dict[str, Any]] = {}
            for row in rows:
                pk = tuple(row.get(k) for k in keys_tuple)
                deduped[pk] = row
            unique_rows = list(deduped.values())
            keys_list = list(keys_tuple)
            for idx in range(0, len(unique_rows), safe_chunk):
                chunk = unique_rows[idx : idx + safe_chunk]
                batched_statements.append(
                    build_batched_upsert(dialect, table, keys_list, chunk, bq_type_map)
                )
        return batched_statements

    return [s for s in statements if s]


def sql_to_config(adapter: Any) -> Dict[str, Any]:
    """Reconstruct configuration dictionary from current `idr_meta.*` table state."""
    config: Dict[str, Any] = {"sources": [], "rules": [], "survivorship": []}
    try:

        def _query_active_sources(adp: Any):
            try:
                return adp.query("""
                    SELECT table_id, table_fqn, entity_type, entity_kind, entity_key_expr,
                           watermark_column, watermark_lookback_minutes, deduplicate, dedup_order_by
                    FROM idr_meta.source_table
                    WHERE is_active = TRUE
                """)
            except Exception as e:
                err_l = str(e).lower()
                if (
                    "no such column" in err_l
                    or "unrecognized name" in err_l
                    or "invalid identifier" in err_l
                    or "binder error" in err_l
                ):
                    try:
                        return adp.query("""
                            SELECT table_id, table_fqn, entity_type, entity_key_expr,
                                   watermark_column, watermark_lookback_minutes, deduplicate, dedup_order_by
                            FROM idr_meta.source_table
                            WHERE is_active = TRUE
                        """)
                    except Exception:
                        try:
                            return adp.query("""
                                SELECT table_id, table_fqn, entity_type, entity_key_expr, watermark_column
                                FROM idr_meta.source_table
                                WHERE is_active = TRUE
                            """)
                        except Exception:
                            print(
                                "Warning: 'is_active' column missing in source_table, assuming all active."
                            )
                            return adp.query("""
                                SELECT table_id, table_fqn, entity_type, entity_key_expr, watermark_column
                                FROM idr_meta.source_table
                            """)
                raise e

        def _query_ids():
            return adapter.query("SELECT * FROM idr_meta.identifier_mapping")

        def _query_attrs():
            return adapter.query("""
                SELECT table_id, attribute_name, attribute_expr
                FROM idr_meta.entity_attribute_mapping
            """)

        def _query_active_rules():
            try:
                return adapter.query("""
                    SELECT *
                    FROM idr_meta.rule
                    WHERE is_active = TRUE
                    ORDER BY priority ASC
                """)
            except Exception as e:
                if (
                    "no such column" in str(e).lower()
                    or "unrecognized name" in str(e).lower()
                    or "invalid identifier" in str(e).lower()
                ):
                    print("Warning: 'is_active' column missing in rule, assuming all active.")
                    return adapter.query("SELECT * FROM idr_meta.rule ORDER BY priority ASC")
                raise e

        def _query_active_survivorship():
            try:
                return adapter.query("SELECT * FROM idr_meta.survivorship_rule WHERE is_active = TRUE")
            except Exception as e:
                if (
                    "no such column" in str(e).lower()
                    or "unrecognized name" in str(e).lower()
                    or "invalid identifier" in str(e).lower()
                ):
                    print(
                        "Warning: 'is_active' column missing in survivorship_rule, assuming all active."
                    )
                    return adapter.query("SELECT * FROM idr_meta.survivorship_rule")
                raise e

        def _query_active_fuzzy():
            try:
                return adapter.query("""
                    SELECT rule_id, blocking_key_expr, score_expr, threshold, priority
                    FROM idr_meta.fuzzy_rule
                    WHERE is_active = TRUE
                    ORDER BY priority ASC
                """)
            except Exception:
                return []

        def _query_exclusions():
            try:
                return adapter.query("""
                    SELECT identifier_type, identifier_value_pattern, match_type, reason
                    FROM idr_meta.identifier_exclusion
                """)
            except Exception:
                return []

        def _query_overrides():
            try:
                return adapter.query("""
                    SELECT override_id, override_type, entity_key_a, entity_key_b, reason, created_by
                    FROM idr_meta.identity_override
                    WHERE is_active = TRUE
                """)
            except Exception:
                return []

        def _query_lenses():
            try:
                return adapter.query("""
                    SELECT lens_id, include_types_json, exclude_types_json, source_filter_json, max_group_size
                    FROM idr_meta.lens_definition
                    WHERE is_active = TRUE
                    ORDER BY lens_id ASC
                """)
            except Exception:
                return []

        tasks = {
            "sources": lambda: _query_active_sources(adapter),
            "ids": _query_ids,
            "attrs": _query_attrs,
            "rules": _query_active_rules,
            "survivorship": _query_active_survivorship,
            "fuzzy": _query_active_fuzzy,
            "exclusions": _query_exclusions,
            "overrides": _query_overrides,
            "lenses": _query_lenses,
        }

        dialect = str(getattr(adapter, "dialect", "") or "").lower()
        if dialect != "duckdb":
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(max_workers=6) as pool:
                futures = {k: pool.submit(fn) for k, fn in tasks.items()}
                results = {k: fut.result() for k, fut in futures.items()}
        else:
            results = {k: fn() for k, fn in tasks.items()}

        ids_map: Dict[str, List[Dict[str, Any]]] = {}
        for row in results["ids"]:
            tid = row["table_id"]
            id_entry: Dict[str, Any] = {
                "type": row["identifier_type"],
                "column": row["identifier_value_expr"],
                "expr": row["identifier_value_expr"],
                "is_hashed": bool(row.get("is_hashed", False)),
                "source_is_hashed": bool(row.get("source_is_hashed", False)),
            }
            raw_iss = row.get("issuer_expr")
            if raw_iss is not None and str(raw_iss).strip():
                id_entry["issuer"] = str(raw_iss).strip()
            ids_map.setdefault(tid, []).append(id_entry)

        attrs_map: Dict[str, List[Dict[str, Any]]] = {}
        for row in results["attrs"]:
            tid = row["table_id"]
            attrs_map.setdefault(tid, []).append(
                {
                    "name": row["attribute_name"],
                    "column": row["attribute_expr"],
                    "expr": row["attribute_expr"],
                }
            )

        for src_row in results["sources"]:
            tid = src_row["table_id"]
            raw_wm = src_row.get("watermark_column")
            clean_wm = (
                ""
                if raw_wm is None or str(raw_wm).strip().lower() in ("", "none", "null")
                else str(raw_wm).strip()
            )
            source: Dict[str, Any] = {
                "id": tid,
                "table": src_row["table_fqn"],
                "entity_type": src_row["entity_type"],
                "entity_key": src_row["entity_key_expr"],
                "watermark_column": clean_wm,
                "watermark_lookback_minutes": int(src_row.get("watermark_lookback_minutes") or 0),
                "identifiers": ids_map.get(tid, []),
                "attributes": attrs_map.get(tid, []),
            }
            raw_kind = str(src_row.get("entity_kind") or "").strip().lower()
            if raw_kind and raw_kind != "person":
                source["entity_kind"] = raw_kind
            if src_row.get("deduplicate"):
                source["deduplicate"] = True
            if src_row.get("dedup_order_by"):
                source["dedup_order_by"] = src_row["dedup_order_by"]
            config["sources"].append(source)

        for rule_row in results["rules"]:
            rule: Dict[str, Any] = {
                "id": rule_row["rule_id"],
                "type": "EXACT",
                "match_keys": [rule_row["identifier_type"]],
                "priority": rule_row["priority"],
                "canonicalize": rule_row.get("canonicalize", "LOWERCASE"),
                "max_group_size": int(rule_row.get("max_group_size") or 10000),
            }
            if rule_row.get("confidence_weight") is not None:
                rule["confidence_weight"] = float(rule_row["confidence_weight"])
            if rule_row.get("fanout_threshold") is not None:
                rule["fanout_threshold"] = int(rule_row["fanout_threshold"])
            config["rules"].append(rule)

        for srv_row in results["survivorship"]:
            p_list = []
            raw_plist = srv_row["source_priority_list"]
            if raw_plist:
                import ast

                try:
                    p_list = ast.literal_eval(raw_plist)
                except Exception:
                    p_list = [
                        s.strip()
                        for s in raw_plist.replace("[", "").replace("]", "").split(",")
                        if s.strip()
                    ]

            srv_rule: Dict[str, Any] = {
                "attribute": srv_row["attribute_name"],
                "strategy": srv_row["strategy"],
                "source_priority": p_list,
            }
            rec_f = srv_row.get("recency_field")
            if rec_f and str(rec_f).strip().lower() not in ("", "none", "null"):
                srv_rule["recency_field"] = str(rec_f).strip()
            if srv_row.get("row_group"):
                srv_rule["row_group"] = srv_row["row_group"]
            config["survivorship"].append(srv_rule)

        config["fuzzy_rules"] = [
            {
                "id": row["rule_id"],
                "type": "FUZZY",
                "blocking_key": row["blocking_key_expr"],
                "score_expr": row["score_expr"],
                "threshold": row["threshold"],
                "priority": row["priority"],
            }
            for row in results["fuzzy"]
        ]

        config["exclusions"] = [
            {
                "type": row["identifier_type"],
                "value": row["identifier_value_pattern"],
                "match": row.get("match_type") or "EXACT",
                "reason": row.get("reason") or "Config exclusion",
            }
            for row in results["exclusions"]
        ]

        if results["overrides"]:
            config["overrides"] = [
                {
                    "id": row["override_id"],
                    "type": row["override_type"],
                    "entity_key_a": row["entity_key_a"],
                    "entity_key_b": row.get("entity_key_b") or "",
                    "reason": row.get("reason") or "Manual stewardship override",
                    "created_by": row.get("created_by") or "config",
                }
                for row in results["overrides"]
            ]

        if results["lenses"]:
            config["lenses"] = []
            for lrow in results["lenses"]:
                ldef: Dict[str, Any] = {
                    "id": lrow["lens_id"],
                    "include_types": (
                        json.loads(lrow["include_types_json"])
                        if lrow.get("include_types_json")
                        else []
                    ),
                    "exclude_types": (
                        json.loads(lrow["exclude_types_json"])
                        if lrow.get("exclude_types_json")
                        else []
                    ),
                    "source_filter": (
                        json.loads(lrow["source_filter_json"])
                        if lrow.get("source_filter_json")
                        else []
                    ),
                }
                if lrow.get("max_group_size") is not None:
                    ldef["max_group_size"] = int(lrow["max_group_size"])
                config["lenses"].append(ldef)

        try:
            from .warehouse_sync import parse_config_snapshot_row

            rows = adapter.query(
                "SELECT sources_json, rules_json, mappings_json "
                "FROM idr_out.config_snapshot ORDER BY created_at DESC LIMIT 1"
            )
            if rows:
                saved = parse_config_snapshot_row(rows[0])
                if saved.get("ai_assistance"):
                    from ..ai_config import (
                        AIConfigContext,
                        policy_errors,
                        restore_authoring_context,
                    )

                    config = restore_authoring_context(config, saved, adapter.dialect)
                    ctx = AIConfigContext.model_validate(saved["ai_assistance"])

                    def _identities(cfg: Dict[str, Any]):
                        return {(s["id"], s["table"]) for s in cfg.get("sources", [])}

                    if _identities(saved) == _identities(config) and not policy_errors(
                        config, ctx, saved
                    ):
                        config["ai_assistance"] = ctx.model_dump(mode="json")
        except Exception:
            pass

        from ..config_extensions import read_extensions

        config.update(read_extensions(adapter))
        return config

    except Exception as e:
        print(f"Error reconstructing config from SQL: {e}")
        return (
            config
            if config.get("sources")
            else {"sources": [], "rules": [], "survivorship": []}
        )
