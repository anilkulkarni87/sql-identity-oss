"""
Base stage infrastructure for IDR pipeline.

Provides StageContext (shared state) and BaseStage (validation helpers)
used by all pipeline stages.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List

from ..adapters.base import IDRAdapter
from ..config import (
    validate_float,
    validate_fqn,
    validate_identifier,
    validate_integer,
    validate_sql_expr,
)


@dataclass
class StageContext:
    """Shared context passed to all pipeline stages.

    Contains everything a stage needs to execute SQL and track state.
    """

    adapter: IDRAdapter
    dialect: Dict[str, str]
    run_id: str
    logger: logging.Logger
    warnings: List[str]
    generate_evidence: bool = False


class BaseStage:
    """Base class for pipeline stages with shared validation helpers."""

    _CANONICALIZE_ENUM = frozenset({"LOWERCASE", "UPPERCASE", "NONE", ""})

    def __init__(self, ctx: StageContext):
        self.ctx = ctx
        self.adapter = ctx.adapter
        self._dialect = ctx.dialect
        self.run_id = ctx.run_id
        self.logger = ctx.logger
        self._warnings = ctx.warnings
        self._generate_evidence = ctx.generate_evidence

    def _validate_metadata_value(self, value, value_type: str, field_name: str):
        """
        Validate a value read from idr_meta tables before SQL interpolation.

        Args:
            value: The value to validate
            value_type: One of 'identifier', 'fqn', 'expr', 'integer', 'float', 'enum'
            field_name: Human-readable field name for error messages

        Returns:
            The validated (and possibly coerced) value

        Raises:
            RuntimeError: If validation fails (wraps ValueError for clearer pipeline errors)
        """
        try:
            if value_type == "identifier":
                return validate_identifier(str(value), field_name)
            elif value_type == "fqn":
                return validate_fqn(str(value), field_name)
            elif value_type == "expr":
                return validate_sql_expr(str(value), field_name)
            elif value_type == "integer":
                return validate_integer(value, field_name)
            elif value_type == "float":
                return validate_float(value, field_name)
            elif value_type == "enum":
                str_val = str(value).upper() if value else ""
                if str_val not in self._CANONICALIZE_ENUM:
                    raise ValueError(
                        f"Invalid {field_name}: '{value}'. "
                        f"Must be one of: {', '.join(sorted(self._CANONICALIZE_ENUM))}"
                    )
                return str_val
            else:
                raise ValueError(f"Unknown validation type: {value_type}")
        except ValueError as e:
            raise RuntimeError(
                f"Metadata validation failed for {field_name}: {e}. "
                f"Check your idr_meta configuration for unsafe values."
            ) from e
