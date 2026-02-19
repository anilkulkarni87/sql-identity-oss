"""
Centralized schema definitions for IDR system tables.
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class ColumnType(Enum):
    STRING = "string_type"
    INT = "int_type"
    BOOL = "bool_type"
    TIMESTAMP = "timestamp_type"
    DOUBLE = "DOUBLE"  # Fixed type, not dialect dependent usually (except maybe float64)


@dataclass
class ColumnDef:
    name: str
    type: ColumnType
    is_pk: bool = False
    default: Optional[str] = None
    description: Optional[str] = None


@dataclass
class TableDef:
    schema: str
    name: str
    columns: List[ColumnDef]
    description: Optional[str] = None

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.name}"


# =============================================================================
# System Table Definitions
# =============================================================================

SYSTEM_TABLES: List[TableDef] = [
    # --- idr_meta ---
    TableDef(
        schema="idr_meta",
        name="source_table",
        description="Registry of source tables to be processed for identity resolution.",
        columns=[
            ColumnDef(
                "table_id",
                ColumnType.STRING,
                is_pk=True,
                description="Unique identifier for the source table",
            ),
            ColumnDef(
                "table_fqn", ColumnType.STRING, description="Fully qualified name (schema.table)"
            ),
            ColumnDef(
                "entity_type", ColumnType.STRING, description="Type of entity (e.g., user, account)"
            ),
            ColumnDef(
                "entity_key_expr",
                ColumnType.STRING,
                description="SQL expression to generate unique entity key",
            ),
            ColumnDef(
                "watermark_column",
                ColumnType.STRING,
                description="Column used for incremental processing",
            ),
            ColumnDef(
                "watermark_lookback_minutes",
                ColumnType.INT,
                description="Buffer time for late arriving data",
            ),
            ColumnDef(
                "is_active",
                ColumnType.BOOL,
                default="TRUE",
                description="Whether this source is enabled",
            ),
        ],
    ),
    TableDef(
        schema="idr_meta",
        name="rule",
        description="Deterministic matching rules defining how identifiers are linked.",
        columns=[
            ColumnDef(
                "rule_id",
                ColumnType.STRING,
                is_pk=True,
                description="Unique identifier for the rule",
            ),
            ColumnDef(
                "identifier_type",
                ColumnType.STRING,
                description="Type of identifier to match on (e.g., email)",
            ),
            ColumnDef(
                "canonicalize",
                ColumnType.STRING,
                description="Normalization strategy (LOWERCASE, UPPERCASE, EXACT)",
            ),
            ColumnDef(
                "max_group_size",
                ColumnType.INT,
                description="Maximum allowed cluster size for this rule to prevent explosion",
            ),
            ColumnDef("priority", ColumnType.INT, description="Processing priority"),
            ColumnDef(
                "is_active",
                ColumnType.BOOL,
                default="TRUE",
                description="Whether this rule is enabled",
            ),
        ],
    ),
    TableDef(
        schema="idr_meta",
        name="identifier_mapping",
        description="Mapping of source columns to standardized identifier types.",
        columns=[
            ColumnDef(
                "table_id",
                ColumnType.STRING,
                is_pk=True,
                description="Reference to source_table.table_id",
            ),
            ColumnDef(
                "identifier_type",
                ColumnType.STRING,
                is_pk=True,
                description="Standardized type (e.g., email, phone)",
            ),
            ColumnDef(
                "identifier_value_expr",
                ColumnType.STRING,
                description="SQL expression to extract value",
            ),
            ColumnDef(
                "is_hashed",
                ColumnType.BOOL,
                default="FALSE",
                description="Whether the source value is already hashed",
            ),
        ],
    ),
    TableDef(
        schema="idr_meta",
        name="fuzzy_rule",
        description="Probabilistic matching rules (disabled in Strict Mode).",
        columns=[
            ColumnDef("rule_id", ColumnType.STRING, is_pk=True),
            ColumnDef("rule_name", ColumnType.STRING),
            ColumnDef("blocking_key_expr", ColumnType.STRING),
            ColumnDef("score_expr", ColumnType.STRING),
            ColumnDef("threshold", ColumnType.DOUBLE),
            ColumnDef("priority", ColumnType.INT),
            ColumnDef("is_active", ColumnType.BOOL, default="TRUE"),
        ],
    ),
    TableDef(
        schema="idr_meta",
        name="entity_attribute_mapping",
        description="Mapping of source columns to golden profile attributes.",
        columns=[
            ColumnDef("table_id", ColumnType.STRING, is_pk=True),
            ColumnDef("attribute_name", ColumnType.STRING, is_pk=True),
            ColumnDef("attribute_expr", ColumnType.STRING),
        ],
    ),
    TableDef(
        schema="idr_meta",
        name="identifier_exclusion",
        description="Blocklist for specific identifier values (e.g., support@company.com).",
        columns=[
            ColumnDef("identifier_type", ColumnType.STRING, is_pk=True),
            ColumnDef("identifier_value_pattern", ColumnType.STRING, is_pk=True),
            ColumnDef("match_type", ColumnType.STRING),
            ColumnDef("rule_id", ColumnType.STRING),
            ColumnDef("reason", ColumnType.STRING),
        ],
    ),
    TableDef(
        schema="idr_meta",
        name="survivorship_rule",
        description="Rules for determining golden record attribute values.",
        columns=[
            ColumnDef("attribute_name", ColumnType.STRING, is_pk=True),
            ColumnDef("strategy", ColumnType.STRING),
            ColumnDef("source_priority_list", ColumnType.STRING),
            ColumnDef("recency_field", ColumnType.STRING),
            ColumnDef("is_active", ColumnType.BOOL, default="TRUE"),
        ],
    ),
    TableDef(
        schema="idr_meta",
        name="run_state",
        description="Internal state tracking for incremental processing.",
        columns=[
            ColumnDef("table_id", ColumnType.STRING, is_pk=True),
            ColumnDef("last_watermark_value", ColumnType.TIMESTAMP),
            ColumnDef("last_run_id", ColumnType.STRING),
            ColumnDef("updated_at", ColumnType.TIMESTAMP),
        ],
    ),
    # --- idr_out ---
    TableDef(
        schema="idr_out",
        name="run_history",
        columns=[
            ColumnDef("run_id", ColumnType.STRING, is_pk=True),
            ColumnDef("run_mode", ColumnType.STRING),
            ColumnDef("status", ColumnType.STRING),
            ColumnDef("started_at", ColumnType.TIMESTAMP),
            ColumnDef("ended_at", ColumnType.TIMESTAMP),
            ColumnDef("created_at", ColumnType.TIMESTAMP),
            ColumnDef("duration_seconds", ColumnType.INT),
            ColumnDef("entities_processed", ColumnType.INT),
            ColumnDef("edges_created", ColumnType.INT),
            ColumnDef("clusters_impacted", ColumnType.INT),
            ColumnDef("lp_iterations", ColumnType.INT),
            ColumnDef("source_tables_processed", ColumnType.INT),
            ColumnDef("error_message", ColumnType.STRING),
            ColumnDef(
                "config_hash", ColumnType.STRING
            ),  # Hash of metadata config for reproducibility
        ],
    ),
    TableDef(
        schema="idr_out",
        name="stage_metrics",
        columns=[
            ColumnDef("run_id", ColumnType.STRING),
            ColumnDef("stage_name", ColumnType.STRING),
            ColumnDef("stage_order", ColumnType.INT),
            ColumnDef("started_at", ColumnType.TIMESTAMP),
            ColumnDef("ended_at", ColumnType.TIMESTAMP),
            ColumnDef("duration_seconds", ColumnType.INT),
            ColumnDef("rows_out", ColumnType.INT),
        ],
    ),
    TableDef(
        schema="idr_out",
        name="config_snapshot",
        columns=[
            ColumnDef("config_hash", ColumnType.STRING, is_pk=True),
            ColumnDef("sources_json", ColumnType.STRING),
            ColumnDef("rules_json", ColumnType.STRING),
            ColumnDef("mappings_json", ColumnType.STRING),
            ColumnDef("created_at", ColumnType.TIMESTAMP),
        ],
    ),
    # --- Identity Output Tables ---
    # --- Identity Output Tables ---
    TableDef(
        schema="idr_out",
        name="identity_resolved_membership_current",
        description="Current mapping of source entity keys to resolved cluster IDs.",
        columns=[
            ColumnDef(
                "entity_key",
                ColumnType.STRING,
                is_pk=True,
                description="Unique key of the source entity",
            ),
            ColumnDef(
                "resolved_id", ColumnType.STRING, description="The resolved Identity Cluster ID"
            ),
            ColumnDef(
                "run_id",
                ColumnType.STRING,
                description="ID of the run that last updated this record",
            ),
            ColumnDef(
                "super_cluster_id",
                ColumnType.STRING,
                description="Higher-level grouping (optional)",
            ),
            ColumnDef("updated_at", ColumnType.TIMESTAMP, description="Timestamp of last update"),
        ],
    ),
    TableDef(
        schema="idr_out",
        name="identity_clusters_current",
        description="Master list of identity clusters with size and confidence metrics.",
        columns=[
            ColumnDef(
                "resolved_id",
                ColumnType.STRING,
                is_pk=True,
                description="The unique Identity Cluster ID",
            ),
            ColumnDef(
                "cluster_size",
                ColumnType.INT,
                description="Number of source entities in this cluster",
            ),
            ColumnDef(
                "run_id",
                ColumnType.STRING,
                description="ID of the run that last updated this cluster",
            ),
            ColumnDef("super_cluster_id", ColumnType.STRING),
            ColumnDef(
                "confidence_score",
                ColumnType.DOUBLE,
                description="Confidence score (0.0-1.0) of the cluster quality",
            ),
            ColumnDef(
                "primary_reason",
                ColumnType.STRING,
                description="Primary reason for the confidence score",
            ),
            ColumnDef("updated_at", ColumnType.TIMESTAMP),
        ],
    ),
    TableDef(
        schema="idr_out",
        name="identity_edges_current",
        description="Graph edges representing links between entities.",
        columns=[
            # Note: This table is upserted based on (left_entity_key, right_entity_key, identifier_type)
            # So these 3 should be PK or have Unique Constraint for DuckDB ON CONFLICT
            ColumnDef("left_entity_key", ColumnType.STRING, is_pk=True),
            ColumnDef("right_entity_key", ColumnType.STRING, is_pk=True),
            ColumnDef("identifier_type", ColumnType.STRING, is_pk=True),
            ColumnDef("identifier_value_norm", ColumnType.STRING),
            ColumnDef("first_seen_ts", ColumnType.TIMESTAMP),
            ColumnDef("last_seen_ts", ColumnType.TIMESTAMP),
            ColumnDef("run_id", ColumnType.STRING),
        ],
    ),
    TableDef(
        schema="idr_out",
        name="skipped_identifier_groups",
        description="Audit log of groups skipped due to size limits (hub flattening prevention).",
        columns=[
            ColumnDef("run_id", ColumnType.STRING),
            ColumnDef("identifier_type", ColumnType.STRING),
            ColumnDef("identifier_value_norm", ColumnType.STRING),
            ColumnDef("group_size", ColumnType.INT),
            ColumnDef("skipped_at", ColumnType.TIMESTAMP),
        ],
    ),
    # --- Edge Evidence (Optional - for debugging/auditing) ---
    TableDef(
        schema="idr_out",
        name="edge_evidence",
        description="Detailed evidence trail for *why* an edge was created.",
        columns=[
            ColumnDef("run_id", ColumnType.STRING),
            ColumnDef("entity_key_a", ColumnType.STRING),
            ColumnDef("entity_key_b", ColumnType.STRING),
            ColumnDef("rule_id", ColumnType.STRING),
            ColumnDef("identifier_type", ColumnType.STRING),
            ColumnDef("match_value", ColumnType.STRING),
            ColumnDef("score", ColumnType.DOUBLE),
            ColumnDef("created_at", ColumnType.TIMESTAMP),
        ],
    ),
]
