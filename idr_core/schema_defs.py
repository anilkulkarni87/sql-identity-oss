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
    TableDef(
        schema="idr_meta",
        name="identity_override",
        description="Manual stewardship overrides (FORCE_LINK, DO_NOT_LINK, ISOLATE).",
        columns=[
            ColumnDef("override_type", ColumnType.STRING, is_pk=True),
            ColumnDef("entity_key_a", ColumnType.STRING, is_pk=True),
            ColumnDef("entity_key_b", ColumnType.STRING, is_pk=True),
            ColumnDef("override_id", ColumnType.STRING),
            ColumnDef("reason", ColumnType.STRING),
            ColumnDef("created_by", ColumnType.STRING),
            ColumnDef("is_active", ColumnType.BOOL, default="TRUE"),
            ColumnDef("created_at", ColumnType.TIMESTAMP),
        ],
    ),
    TableDef(
        schema="idr_meta",
        name="privacy_audit_log",
        description="Immutable zero-PII compliance audit log for GDPR/CCPA Right to be Forgotten.",
        columns=[
            ColumnDef("request_id", ColumnType.STRING, is_pk=True),
            ColumnDef("search_type", ColumnType.STRING),
            ColumnDef("target_hash", ColumnType.STRING),
            ColumnDef("resolved_id_hash", ColumnType.STRING),
            ColumnDef("entities_purged", ColumnType.INT),
            ColumnDef("edges_purged", ColumnType.INT),
            ColumnDef("profiles_purged", ColumnType.INT),
            ColumnDef("status", ColumnType.STRING),
            ColumnDef("requested_by", ColumnType.STRING),
            ColumnDef("reason", ColumnType.STRING),
            ColumnDef("executed_at", ColumnType.TIMESTAMP),
        ],
    ),
    TableDef(
        schema="idr_meta",
        name="privacy_suppression",
        description="Zero-PII SHA-256 entity suppression list so GDPR-forgotten records are not rebuilt on FULL/INCR reruns.",
        columns=[
            ColumnDef("entity_key_hash", ColumnType.STRING, is_pk=True),
            ColumnDef("request_id", ColumnType.STRING),
            ColumnDef("suppressed_at", ColumnType.TIMESTAMP),
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
            ColumnDef("warnings", ColumnType.STRING),
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
                description="Calibrated confidence score (0.0-1.0) of the cluster quality",
            ),
            ColumnDef(
                "edge_diversity",
                ColumnType.INT,
                default="0",
                description="Number of distinct identifier types linking entities in this cluster",
            ),
            ColumnDef(
                "edge_count",
                ColumnType.INT,
                default="0",
                description="Total number of edges connecting entities in this cluster",
            ),
            ColumnDef(
                "max_fanout",
                ColumnType.INT,
                default="1",
                description="Maximum single-value fanout (entities sharing one identifier value) in this cluster",
            ),
            ColumnDef(
                "match_density",
                ColumnType.DOUBLE,
                default="1.0",
                description="Graph connectivity ratio (edges / (cluster_size - 1))",
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
        name="identity_identifiers_current",
        description="Current registry of all entity identifiers for fast incremental candidate expansion.",
        columns=[
            ColumnDef("entity_key", ColumnType.STRING, is_pk=True),
            ColumnDef("identifier_type", ColumnType.STRING, is_pk=True),
            ColumnDef("identifier_value_norm", ColumnType.STRING),
            ColumnDef("is_hashed", ColumnType.BOOL, default="FALSE"),
            ColumnDef("updated_at", ColumnType.TIMESTAMP),
            ColumnDef("run_id", ColumnType.STRING),
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
            ColumnDef("max_allowed", ColumnType.INT),
            ColumnDef("sample_entity_keys", ColumnType.STRING),
            ColumnDef("reason", ColumnType.STRING),
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
    # --- Golden Profiles (Unified Customer 360 Record) ---
    TableDef(
        schema="idr_out",
        name="golden_profiles",
        description="Unified golden customer profiles resolved by survivorship rules.",
        columns=[
            ColumnDef("resolved_id", ColumnType.STRING, is_pk=True),
            ColumnDef("run_id", ColumnType.STRING),
            ColumnDef("updated_at", ColumnType.TIMESTAMP),
        ],
    ),
    TableDef(
        schema="idr_out",
        name="resolved_id_crosswalk",
        description="Crosswalk tracking cluster merges, splits, and retired resolved_ids across runs.",
        columns=[
            ColumnDef("previous_resolved_id", ColumnType.STRING, is_pk=True),
            ColumnDef("current_resolved_id", ColumnType.STRING, is_pk=True),
            ColumnDef("change_type", ColumnType.STRING),
            ColumnDef("entities_moved", ColumnType.INT),
            ColumnDef("run_id", ColumnType.STRING),
            ColumnDef("updated_at", ColumnType.TIMESTAMP),
        ],
    ),
    TableDef(
        schema="idr_out",
        name="evaluation_sample",
        description="Stratified pair sample for precision/recall auditing and human/LLM labeling.",
        columns=[
            ColumnDef("sample_id", ColumnType.STRING, is_pk=True),
            ColumnDef("stratum", ColumnType.STRING),
            ColumnDef("resolved_id_a", ColumnType.STRING),
            ColumnDef("resolved_id_b", ColumnType.STRING),
            ColumnDef("entity_key_a", ColumnType.STRING),
            ColumnDef("entity_key_b", ColumnType.STRING),
            ColumnDef("identifier_type", ColumnType.STRING),
            ColumnDef("match_value", ColumnType.STRING),
            ColumnDef("confidence_score", ColumnType.DOUBLE),
            ColumnDef("cluster_size", ColumnType.INT),
            ColumnDef("human_label", ColumnType.STRING),
            ColumnDef("sampled_at", ColumnType.TIMESTAMP),
        ],
    ),
]


_TABLE_DESCRIPTIONS = {
    "idr_out.run_history": "Audit log of every IDR pipeline execution, including status, duration, row counts, and config hash.",
    "idr_out.stage_metrics": "Per-stage execution timing and row count breakdown for each pipeline run.",
    "idr_out.config_snapshot": "Versioned JSON snapshots of source, rule, and survivorship configurations per run.",
}

_COLUMN_DESCRIPTIONS = {
    "rule_id": "Unique identifier of the matching rule.",
    "rule_name": "Human-readable name of the matching rule.",
    "blocking_key_expr": "SQL expression used to block candidate pairs before fuzzy scoring.",
    "score_expr": "SQL similarity scoring expression returning a value in [0.0, 1.0].",
    "threshold": "Minimum similarity score required to emit a fuzzy match edge.",
    "priority": "Execution priority order (lower runs first).",
    "is_active": "Whether this rule or source is currently active.",
    "table_id": "Logical identifier of the source table.",
    "attribute_name": "Standardized golden attribute name (e.g., first_name, city).",
    "attribute_expr": "SQL expression or column name extracting the attribute from the source table.",
    "identifier_type": "Semantic identifier type (e.g., email, phone, customer_id).",
    "identifier_value_pattern": "Exact value, SQL LIKE pattern, or regex pattern to exclude.",
    "match_type": "Pattern matching mode: EXACT, LIKE, or REGEX.",
    "reason": "Human-readable justification for the exclusion, override, or purge.",
    "strategy": "Survivorship strategy (RECENCY, FREQUENCY, PRIORITY, LONGEST, etc.).",
    "source_priority_list": "Ordered JSON array of source table_ids for PRIORITY survivorship.",
    "recency_field": "Timestamp column used to break ties in RECENCY survivorship.",
    "last_watermark_value": "Latest processed watermark timestamp or cursor for incremental runs.",
    "last_run_id": "Run ID of the most recent pipeline run that updated this watermark.",
    "updated_at": "UTC timestamp when this record was last updated.",
    "override_type": "Manual graph override type: FORCE_LINK or FORCE_SPLIT.",
    "entity_key_a": "First source entity key in the pair (<table_id>:<source_pk>).",
    "entity_key_b": "Second source entity key in the pair (<table_id>:<source_pk>).",
    "override_id": "Unique identifier of the manual override rule.",
    "created_by": "Actor or interface that created this record (e.g., ui, config, sql).",
    "created_at": "UTC timestamp when this record was created.",
    "request_id": "Unique identifier of the GDPR/CCPA Right-to-be-Forgotten request.",
    "search_type": "Lookup key type used for the purge (identifier_value, entity_key, or resolved_id).",
    "target_hash": "Zero-PII SHA-256 hash of the requested purge target.",
    "resolved_id_hash": "Zero-PII SHA-256 hash of the affected cluster resolved_id(s).",
    "entities_purged": "Number of entity records removed across idr_out and idr_work tables.",
    "edges_purged": "Number of identity edges removed during the cascading purge.",
    "profiles_purged": "Number of golden profile records removed.",
    "status": "Execution outcome status (e.g., SUCCESS, FAILED, COMPLETED, DRY_RUN).",
    "requested_by": "User or system that initiated the request.",
    "executed_at": "UTC timestamp when the purge was executed.",
    "entity_key_hash": "Zero-PII SHA-256 hash of a suppressed entity_key excluded from future runs.",
    "suppressed_at": "UTC timestamp when the entity was added to the suppression list.",
    "run_id": "Unique identifier of the pipeline run.",
    "run_mode": "Execution mode: FULL or INCR (incremental).",
    "started_at": "UTC timestamp when execution started.",
    "ended_at": "UTC timestamp when execution finished.",
    "duration_seconds": "Total wall-clock execution duration in seconds.",
    "entities_processed": "Total number of entity records processed in this run.",
    "edges_created": "Total number of identity match edges created.",
    "clusters_impacted": "Number of resolved clusters created or updated.",
    "lp_iterations": "Number of label propagation iterations required for graph convergence.",
    "source_tables_processed": "Number of active source tables processed.",
    "warnings": "JSON or text summary of non-fatal pipeline warnings.",
    "error_message": "Error details if the run failed.",
    "config_hash": "MD5 hash of the active configuration snapshot for reproducibility.",
    "stage_name": "Internal name of the pipeline stage.",
    "stage_order": "Sequential execution order (1-based) of the stage.",
    "rows_out": "Number of rows produced or affected by this stage.",
    "sources_json": "Serialized JSON of source table, identifier, and attribute mappings.",
    "rules_json": "Serialized JSON of exact, fuzzy, exclusion, and override rules.",
    "mappings_json": "Serialized JSON of survivorship rules.",
    "super_cluster_id": "Optional higher-level household or account group cluster ID.",
    "entity_key": "Globally unique source entity key (<table_id>:<source_pk>).",
    "identifier_value_norm": "Canonicalized/normalized identifier value used for matching.",
    "is_hashed": "Whether the identifier value is already pre-hashed.",
    "left_entity_key": "Left entity_key of the matched edge pair.",
    "right_entity_key": "Right entity_key of the matched edge pair.",
    "first_seen_ts": "UTC timestamp when this edge was first observed.",
    "last_seen_ts": "UTC timestamp when this edge was most recently confirmed.",
    "group_size": "Total number of entities sharing this identifier value.",
    "max_allowed": "Configured max_group_size threshold that triggered blocking.",
    "sample_entity_keys": "Sample of entity keys belonging to the skipped high-fanout group.",
    "skipped_at": "UTC timestamp when this identifier group was skipped.",
    "match_value": "Canonical identifier value or similarity score that linked the pair.",
    "score": "Match confidence score in [0.0, 1.0] for this edge.",
    "resolved_id": "Stable resolved identity cluster ID.",
    "previous_resolved_id": "Prior cluster resolved_id before a merge or split.",
    "current_resolved_id": "New cluster resolved_id after a merge or split.",
    "change_type": "Cluster transition type (MERGED, SPLIT, REKEYED, or UNCHANGED).",
    "entities_moved": "Number of entity keys that moved into the current_resolved_id.",
    "sample_id": "Unique identifier of the sampled evaluation pair.",
    "stratum": "Sampling stratum (e.g., high_fanout, boundary, singleton, multi_source).",
    "resolved_id_a": "Resolved cluster ID of the first entity in the pair.",
    "resolved_id_b": "Resolved cluster ID of the second entity in the pair.",
    "confidence_score": "Cluster confidence score in [0.0, 1.0].",
    "cluster_size": "Number of entity keys in the cluster.",
    "human_label": "Optional human/LLM ground-truth label (MATCH, NON_MATCH, UNCERTAIN).",
    "sampled_at": "UTC timestamp when the evaluation pair was sampled.",
}

for _tbl in SYSTEM_TABLES:
    if not _tbl.description and _tbl.fqn in _TABLE_DESCRIPTIONS:
        _tbl.description = _TABLE_DESCRIPTIONS[_tbl.fqn]
    for _col in _tbl.columns:
        if not _col.description and _col.name in _COLUMN_DESCRIPTIONS:
            _col.description = _COLUMN_DESCRIPTIONS[_col.name]
