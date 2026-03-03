"""
Pydantic models for the IDR API.

Shared across all router modules.
"""

from typing import List, Optional

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
    last_run_id: Optional[str]
    last_run_duration: Optional[int]
    last_run_started_at: Optional[str]


class ClusterDistribution(BaseModel):
    bucket: str
    count: int


class RuleStats(BaseModel):
    rule_id: str
    identifier_type: Optional[str]
    edges_created: int
    percentage: float


class Alert(BaseModel):
    severity: str
    message: str
    count: Optional[int]
    timestamp: Optional[str]


# ============================================================
# Cluster / Entity Explorer
# ============================================================


class ClusterSummary(BaseModel):
    resolved_id: str
    cluster_size: int
    confidence_score: Optional[float]


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
    confidence_score: Optional[float]
    entities: List[EntityInfo]
    edges: List[EdgeInfo]


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
