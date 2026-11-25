"""
Export validated IDR configuration dictionaries into `dbt_idr` seed CSV files.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, List

from .loader import validate_config
from .validators import DEFAULT_EXCLUSIONS


def config_to_dbt_seeds(config: Dict[str, Any], output_dir: str) -> Dict[str, str]:
    """
    Export a validated IDR configuration dictionary into the 7 standard `dbt_idr`
    seed CSV files (`idr_sources.csv`, `idr_identifier_mappings.csv`,
    `idr_attribute_mappings.csv`, `idr_rules.csv`, `idr_exclusions.csv`,
    `idr_survivorship_rules.csv`, `idr_overrides.csv`).
    """
    validate_config(config)
    out_path = Path(output_dir).resolve()
    out_path.mkdir(parents=True, exist_ok=True)

    written_files: Dict[str, str] = {}

    def _write_csv(filename: str, fieldnames: List[str], rows: List[Dict[str, Any]]) -> None:
        filepath = out_path / filename
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        written_files[filename] = str(filepath)

    sources_rows: List[Dict[str, Any]] = []
    id_mapping_rows: List[Dict[str, Any]] = []
    attr_mapping_rows: List[Dict[str, Any]] = []

    default_schema = config.get("schema_source") or "main"

    for src in config.get("sources", []):
        src_id = src["id"]
        raw_table = src["table"].replace("`", "").replace('"', "")
        parts = raw_table.split(".")
        if len(parts) == 3:
            db_name, schema_name, table_name = parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            db_name, schema_name, table_name = "", parts[0], parts[1]
        else:
            db_name, schema_name, table_name = "", default_schema, parts[0]

        watermark_col = src.get("watermark_column", "updated_at")
        sources_rows.append(
            {
                "source_id": src_id,
                "source_name": src.get("name", src_id.replace("_", " ").title()),
                "database": src.get("database", db_name),
                "schema": src.get("schema", schema_name),
                "table_name": table_name,
                "entity_key_column": src.get("entity_key", "id"),
                "watermark_column": watermark_col,
                "is_active": "true",
                "description": src.get("description", f"Source table {raw_table}"),
            }
        )

        raw_attrs = src.get("attributes", [])
        if isinstance(raw_attrs, dict):
            attr_iter = list(raw_attrs.items())
        else:
            attr_iter = [
                (item["name"], item.get("expr") or item.get("column") or item["name"])
                for item in raw_attrs
                if "name" in item
            ]

        seen_attrs = set()
        for attr_name, attr_expr in attr_iter:
            seen_attrs.add(attr_name)
            attr_mapping_rows.append(
                {
                    "source_id": src_id,
                    "attribute_name": attr_name,
                    "column_expr": attr_expr,
                    "description": f"{src_id} {attr_name}",
                }
            )

        if watermark_col and "record_updated_at" not in seen_attrs:
            attr_mapping_rows.append(
                {
                    "source_id": src_id,
                    "attribute_name": "record_updated_at",
                    "column_expr": watermark_col,
                    "description": f"{src_id} watermark timestamp",
                }
            )

        raw_ids = src.get("identifiers", [])
        if not raw_ids:
            source_attrs_map = dict(attr_iter)
            inferred_ids = []
            for rule in config.get("rules", []):
                if rule.get("type") == "EXACT":
                    for key in rule.get("match_keys", []):
                        if key in source_attrs_map:
                            inferred_ids.append({"type": key, "expr": source_attrs_map[key]})
            raw_ids = inferred_ids

        if isinstance(raw_ids, dict):
            id_iter = list(raw_ids.items())
        else:
            id_iter = [
                (item["type"], item.get("expr") or item.get("column") or item["type"])
                for item in raw_ids
                if "type" in item
            ]

        for id_type, col_expr in id_iter:
            id_mapping_rows.append(
                {
                    "source_id": src_id,
                    "identifier_type": str(id_type).upper(),
                    "column_name": col_expr,
                    "is_hashed": str(bool(src.get("is_hashed", False))).lower(),
                    "description": f"{src_id} {id_type}",
                }
            )

    rules_rows: List[Dict[str, Any]] = []
    for rule in config.get("rules", []):
        id_type = rule.get("identifier_type") or (
            rule["match_keys"][0]
            if rule.get("type") == "EXACT" and rule.get("match_keys")
            else rule.get("type", str(rule["id"]))
        )
        rules_rows.append(
            {
                "rule_id": rule["id"],
                "identifier_type": str(id_type).upper(),
                "priority": rule.get("priority", 100),
                "is_active": "true",
                "canonicalize": rule.get("canonicalize", "LOWERCASE"),
                "max_group_size": rule.get("max_group_size", 10000),
                "description": rule.get("description", f"Match rule for {id_type}"),
            }
        )

    exclusions_rows: List[Dict[str, Any]] = []
    combined_excl = []
    if config.get("use_default_exclusions", False):
        combined_excl.extend(DEFAULT_EXCLUSIONS)
    combined_excl.extend(config.get("exclusions", []))
    seen_dbt_excl = set()
    for excl in combined_excl:
        id_type = excl.get("identifier_type") or excl.get("type")
        val_pattern = excl.get("value") or excl.get("identifier_value_pattern")
        if not id_type or not val_pattern:
            continue
        dedup_k = (str(id_type).upper(), str(val_pattern).lower())
        if dedup_k in seen_dbt_excl:
            continue
        seen_dbt_excl.add(dedup_k)
        exclusions_rows.append(
            {
                "identifier_type": str(id_type).upper(),
                "identifier_value_pattern": val_pattern,
                "match_type": excl.get("match_type") or excl.get("match") or "EXACT",
                "reason": excl.get("reason", "Config exclusion"),
            }
        )

    surv_rows: List[Dict[str, Any]] = []
    for rule in config.get("survivorship", []):
        p_list = rule.get("source_priority", [])
        p_str = ",".join(str(x) for x in p_list) if isinstance(p_list, list) else str(p_list or "")
        surv_rows.append(
            {
                "attribute_name": rule["attribute"],
                "strategy": rule["strategy"],
                "source_priority_list": p_str,
                "recency_field": rule.get("recency_field") or "record_updated_at",
                "description": rule.get("description", f"Survivorship for {rule['attribute']}"),
            }
        )

    override_rows: List[Dict[str, Any]] = []
    for idx, ov in enumerate(config.get("overrides") or []):
        ov_type = str(ov.get("type") or ov.get("override_type") or "").upper().strip()
        ekey_a = ov.get("entity_key_a") or ov.get("entity_key")
        if not ov_type or not ekey_a:
            continue
        ekey_b = ov.get("entity_key_b") or ("__ISOLATE__" if ov_type == "ISOLATE" else "")
        ov_id = ov.get("id") or ov.get("override_id") or f"ov_{ov_type.lower()}_{idx + 1}"
        override_rows.append(
            {
                "override_id": str(ov_id),
                "override_type": ov_type,
                "entity_key_a": str(ekey_a),
                "entity_key_b": str(ekey_b),
                "reason": ov.get("reason", "Manual stewardship override"),
                "created_by": ov.get("created_by", "config"),
                "is_active": "true" if ov.get("is_active", True) else "false",
            }
        )
    if not override_rows:
        override_rows.append(
            {
                "override_id": "ov_sample_1",
                "override_type": "FORCE_LINK",
                "entity_key_a": "crm:1",
                "entity_key_b": "crm:1",
                "reason": "Sample inactive stewardship override",
                "created_by": "config",
                "is_active": "false",
            }
        )

    _write_csv(
        "idr_sources.csv",
        [
            "source_id",
            "source_name",
            "database",
            "schema",
            "table_name",
            "entity_key_column",
            "watermark_column",
            "is_active",
            "description",
        ],
        sources_rows,
    )
    _write_csv(
        "idr_identifier_mappings.csv",
        ["source_id", "identifier_type", "column_name", "is_hashed", "description"],
        id_mapping_rows,
    )
    _write_csv(
        "idr_attribute_mappings.csv",
        ["source_id", "attribute_name", "column_expr", "description"],
        attr_mapping_rows,
    )
    _write_csv(
        "idr_rules.csv",
        [
            "rule_id",
            "identifier_type",
            "priority",
            "is_active",
            "canonicalize",
            "max_group_size",
            "description",
        ],
        rules_rows,
    )
    _write_csv(
        "idr_exclusions.csv",
        ["identifier_type", "identifier_value_pattern", "match_type", "reason"],
        exclusions_rows,
    )
    _write_csv(
        "idr_survivorship_rules.csv",
        ["attribute_name", "strategy", "source_priority_list", "recency_field", "description"],
        surv_rows,
    )
    _write_csv(
        "idr_overrides.csv",
        [
            "override_id",
            "override_type",
            "entity_key_a",
            "entity_key_b",
            "reason",
            "created_by",
            "is_active",
        ],
        override_rows,
    )

    return written_files
