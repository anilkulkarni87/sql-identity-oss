"""
Pydantic models for the IDR API.

Shared across all router modules.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel

# ============================================================
# Connection
# ============================================================


class ConnectionRequest(BaseModel):
    platform: str  # duckdb, snowflake, bigquery, databricks
    connection_string: Optional[str] = None
    project_id: Optional[str] = None  # BigQuery
    database: Optional[str] = None  # DuckDB path
    # Snowflake fields
    account: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    warehouse: Optional[str] = None
    sf_database: Optional[str] = None
    schema_name: Optional[str] = "PUBLIC"
    # Databricks fields
    server_hostname: Optional[str] = None
    http_path: Optional[str] = None
    access_token: Optional[str] = None
    catalog: Optional[str] = None


class ConnectionResponse(BaseModel):
    status: str
    platform: str
    message: str


# ============================================================
# Dashboard Metrics
# ============================================================


class MetricsSummary(BaseModel):
    total_clusters: int
    total_entities: int
    total_edges: int
    avg_confidence: float
    last_run_id: Optional[str] = None
    last_run_duration: Optional[int] = None
    last_run_started_at: Optional[str] = None


class ClusterDistribution(BaseModel):
    bucket: str
    count: int


class RuleStats(BaseModel):
    rule_id: str
    identifier_type: Optional[str] = None
    edges_created: int
    percentage: float


class Alert(BaseModel):
    severity: str
    message: str
    count: Optional[int] = None
    timestamp: Optional[str] = None


# ============================================================
# Cluster / Entity Explorer
# ============================================================


class ClusterSummary(BaseModel):
    resolved_id: str
    cluster_size: int
    confidence_score: Optional[float] = None


class EntityInfo(BaseModel):
    entity_key: str
    source_id: str
    source_key: str


class EdgeInfo(BaseModel):
    left_entity_key: str
    right_entity_key: str
    identifier_type: str
    identifier_value: str
    rule_id: str


class ClusterDetail(BaseModel):
    resolved_id: str
    cluster_size: int
    confidence_score: Optional[float] = None
    super_cluster_id: Optional[str] = None
    entities: List[EntityInfo]
    edges: List[EdgeInfo]
    golden_profile: Optional[Dict[str, Any]] = None
    crosswalk_history: Optional[List[Dict[str, Any]]] = None


class OverrideRequest(BaseModel):
    action: str = "add"  # add, remove, list
    override_type: Optional[str] = None  # FORCE_LINK, DO_NOT_LINK, ISOLATE
    entity_key_a: Optional[str] = None
    entity_key_b: Optional[str] = None
    reason: Optional[str] = "UI stewardship override"


class PrivacyForgetRequest(BaseModel):
    identifier_value: Optional[str] = None
    entity_key: Optional[str] = None
    resolved_id: Optional[str] = None
    dry_run: bool = False
    reason: str = "GDPR_CCPA_RTBF"


# ============================================================
# Run History
# ============================================================


class RunSummary(BaseModel):
    run_id: str
    run_mode: str
    status: str
    started_at: str
    duration_seconds: Optional[int] = None
    entities_processed: Optional[int] = 0
    edges_created: Optional[int] = 0
    clusters_impacted: Optional[int] = 0


class StageMetricInfo(BaseModel):
    stage_name: str
    stage_order: int
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    duration_seconds: Optional[int] = 0
    rows_out: Optional[int] = 0


class RunDetail(RunSummary):
    lp_iterations: Optional[int] = 0
    error_message: Optional[str] = None
    warnings: List[str] = []
    stages: List[StageMetricInfo] = []


# ============================================================
# Schema Documentation
# ============================================================


class SchemaColumn(BaseModel):
    name: str
    type: str
    is_pk: bool
    description: Optional[str] = None


class SchemaTable(BaseModel):
    schema_name: str
    table_name: str
    fqn: str
    description: Optional[str] = None
    columns: List[SchemaColumn]
