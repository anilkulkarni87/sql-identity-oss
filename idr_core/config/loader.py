"""
YAML configuration file loading and top-level schema validation.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict

from .validators import validate_fqn, validate_identifier, validate_sql_expr

try:
    import yaml

    HAS_YAML = True
except ImportError:
    HAS_YAML = False


def load_config(path: str) -> Dict[str, Any]:
    """Load and validate a YAML configuration file."""
    pkg = sys.modules.get("idr_core.config")
    has_yaml = getattr(pkg, "HAS_YAML", HAS_YAML) if pkg is not None else HAS_YAML
    if not has_yaml:
        raise ImportError("PyYAML required. Install with: pip install pyyaml")

    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Config file not found: {file_path}")

    with open(file_path) as f:
        config = yaml.safe_load(f)

    validate_config(config)
    return config


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate an IDR configuration dictionary."""
    if not config:
        raise ValueError("Configuration cannot be empty")

    if "sources" not in config:
        raise ValueError("Configuration must have 'sources' section")

    for i, src in enumerate(config.get("sources", [])):
        if "id" not in src:
            raise ValueError(f"Source {i}: missing 'id'")
        if "table" not in src:
            raise ValueError(f"Source {i}: missing 'table'")
        if "entity_key" not in src and not src.get("key_columns"):
            raise ValueError(f"Source {i}: missing 'entity_key'")

        validate_identifier(src["id"], f"Source {i} id")
        validate_fqn(src["table"], f"Source {i} table")

        if src.get("key_columns") is not None:
            kc = src["key_columns"]
            if not isinstance(kc, list) or not kc:
                raise ValueError(f"Source {i}: 'key_columns' must be a non-empty list")
            for col in kc:
                validate_sql_expr(str(col), f"Source {i} key_columns item")

        if "deduplicate" in src and src["deduplicate"] is not None:
            if not isinstance(src["deduplicate"], bool):
                raise ValueError(f"Source {i}: 'deduplicate' must be a boolean")

        if src.get("dedup_order_by"):
            validate_sql_expr(str(src["dedup_order_by"]), f"Source {i} dedup_order_by")

        if src.get("entity_kind") is not None and str(src["entity_kind"]).strip():
            validate_identifier(str(src["entity_kind"]).strip().lower(), f"Source {i} entity_kind")

        if src.get("entity_type") is not None and str(src["entity_type"]).strip():
            validate_identifier(str(src["entity_type"]).strip().upper(), f"Source {i} entity_type")

        raw_ids = src.get("identifiers")
        if isinstance(raw_ids, list):
            for j, ident in enumerate(raw_ids):
                if isinstance(ident, dict):
                    raw_issuer = (
                        ident.get("issuer")
                        if ident.get("issuer") is not None
                        else ident.get("issuer_expr")
                    )
                    if raw_issuer is not None and str(raw_issuer).strip():
                        validate_sql_expr(
                            str(raw_issuer).strip(), f"Source {i} identifier {j} issuer"
                        )

    if config.get("survivorship") is not None:
        from ..survivorship import (
            is_registered_survivorship_strategy,
            list_survivorship_strategies,
        )

        if not isinstance(config["survivorship"], list):
            raise ValueError("Config 'survivorship' must be a list of rules")
        for rule in config["survivorship"]:
            if not isinstance(rule, dict):
                continue
            if "attribute" not in rule:
                raise ValueError("Survivorship rule missing 'attribute'")
            if "strategy" not in rule:
                raise ValueError(f"Survivorship rule for {rule['attribute']} missing 'strategy'")
            if not is_registered_survivorship_strategy(rule["strategy"]):
                valid = ", ".join(list_survivorship_strategies())
                raise ValueError(
                    f"Invalid strategy {rule['strategy']} for {rule['attribute']}. "
                    f"Supported: {valid}"
                )

    if config.get("rules") is not None and not isinstance(config["rules"], list):
        raise ValueError("Config 'rules' must be a list")

    if config.get("fuzzy_rules") is not None and not isinstance(config["fuzzy_rules"], list):
        raise ValueError("Config 'fuzzy_rules' must be a list")

    if "generate_evidence" in config and config.get("generate_evidence") is not None:
        if not isinstance(config["generate_evidence"], bool):
            raise ValueError("Config 'generate_evidence' must be a boolean (true/false)")

    if config.get("grouped_outputs"):
        from ..grouped_outputs import validate_grouped_outputs

        validate_grouped_outputs(config)
    if config.get("evaluation"):
        from ..evaluation_config import EvaluationOptions

        EvaluationOptions.model_validate(config["evaluation"])
    if config.get("activation_delta") is not None:
        from ..activation_delta import validate_activation_delta

        validate_activation_delta(config["activation_delta"])
    return True
