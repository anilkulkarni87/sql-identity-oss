"""Local label adapters and repeatable acceptance checks; no label values go to AI."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .adapters.base import get_dialect_config
from .config import resolve_entity_key_expr, validate_fqn, validate_identifier, validate_sql_expr


class LabelSourceMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_id: str
    key_columns: dict[str, str]
    table: str | None = None
    truth_id_col: str | None = None
    source_column: str | None = None
    source_value: str | None = None

    @model_validator(mode="after")
    def validate_filter(self):
        if (self.source_column is None) != (self.source_value is None):
            raise ValueError("source_column and source_value must be supplied together")
        if not self.key_columns:
            raise ValueError("Map source key columns to label columns")
        return self


class EvaluationOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")
    ground_truth_table: str
    entity_key_col: str = "entity_key"
    truth_id_col: str = "truth_id"
    source_mappings: list[LabelSourceMapping] = Field(default_factory=list)
    top_n_errors: int = Field(default=10, ge=0, le=100)
    max_label_rows: int = Field(default=1000000, ge=1, le=10000000)
    min_evaluated_entities: int = Field(default=2, ge=1)
    min_label_overlap: float = Field(default=0.0, ge=0, le=1)
    min_true_pairs: int = Field(default=1, ge=0)
    min_precision: float | None = Field(default=None, ge=0, le=1)
    min_recall: float | None = Field(default=None, ge=0, le=1)


def _literal(value, dialect):
    text = str(value)
    if dialect in ("bigquery", "databricks"):
        text = text.replace("\\", "\\\\")
    return "'" + text.replace("'", "''") + "'"


def materialize_labels(
    adapter, config, options: EvaluationOptions, destination="idr_work.ai_ground_truth"
):
    """Normalize a bounded label cohort using the source's real entity-key expression."""
    target = validate_fqn(destination)
    root = validate_fqn(options.ground_truth_table)
    string = get_dialect_config(adapter.dialect)["string_type"]
    truth = validate_identifier(options.truth_id_col)
    columns = {}

    def label_column(table, name):
        name = validate_identifier(name, "label column")
        if table not in columns:
            if not adapter.table_exists(table):
                raise ValueError(f"Evaluation label table does not exist: {table}")
            columns[table] = {c["name"].lower() for c in adapter.get_table_columns(table)}
        if name.lower() not in columns[table]:
            raise ValueError(f"Evaluation label column {name} is missing from {table}")
        return name

    selects = []
    sources = {s["id"]: s for s in config.get("sources", [])}
    for mapping in options.source_mappings:
        if mapping.source_id not in sources:
            raise ValueError("Evaluation mapping references an unknown configured source")
        table = validate_fqn(mapping.table or root)
        truth_col = label_column(table, mapping.truth_id_col or truth)
        fields = []
        for source_col, label_col in mapping.key_columns.items():
            name = validate_identifier(source_col)
            if name.startswith("_idr_"):
                raise ValueError("Evaluation mappings cannot use reserved internal column names")
            fields.append(f"{label_column(table, label_col)} AS {name}")
        where = ""
        if mapping.source_column:
            where = f" WHERE {label_column(table, mapping.source_column)} = {_literal(mapping.source_value, adapter.dialect)}"
        key = validate_sql_expr(
            resolve_entity_key_expr(sources[mapping.source_id], adapter.dialect),
            "evaluation source key",
        )
        source_id = validate_identifier(mapping.source_id)
        projection = ",".join(fields + [f"CAST({truth_col} AS {string}) AS _idr_truth"])
        selects.append(
            f"SELECT CASE WHEN ({key}) IS NOT NULL THEN CONCAT('{source_id}:', CAST(({key}) AS {string})) ELSE NULL END AS entity_key, _idr_truth AS truth_id FROM (SELECT {projection} FROM {table}{where}) mapped"
        )
    if not selects:
        key = label_column(root, options.entity_key_col)
        truth = label_column(root, options.truth_id_col)
        selects = [
            f"SELECT CAST({key} AS {string}) AS entity_key, CAST({truth} AS {string}) AS truth_id FROM {root}"
        ]
    sql = " UNION ALL ".join(selects)
    adapter.execute(
        f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM ({sql}) labels WHERE truth_id IS NOT NULL LIMIT {options.max_label_rows + 1}"
    )
    n = int(adapter.query_one(f"SELECT COUNT(*) FROM {target}") or 0)
    if n > options.max_label_rows:
        raise ValueError(
            "Evaluation label cohort exceeds max_label_rows; choose a smaller reviewed cohort or raise the explicit limit"
        )
    validate_labels(adapter, target, "entity_key", "truth_id")
    return {
        "ground_truth_table": target,
        "entity_key_col": "entity_key",
        "truth_id_col": "truth_id",
        "top_n_errors": options.top_n_errors,
    }


def validate_labels(adapter, table, key, truth):
    table, key, truth = validate_fqn(table), validate_identifier(key), validate_identifier(truth)
    invalid = int(
        adapter.query_one(
            f"SELECT COUNT(*) FROM {table} WHERE {truth} IS NOT NULL AND ({key} IS NULL OR CAST({key} AS {get_dialect_config(adapter.dialect)['string_type']})='' OR CAST({truth} AS {get_dialect_config(adapter.dialect)['string_type']})='')"
        )
        or 0
    )
    if invalid:
        raise ValueError(
            f"Evaluation labels contain {invalid} empty record keys or identity groups"
        )
    duplicate = int(
        adapter.query_one(
            f"SELECT COUNT(*) FROM (SELECT {key} FROM {table} WHERE {truth} IS NOT NULL GROUP BY {key} HAVING COUNT(*)>1) duplicate_keys"
        )
        or 0
    )
    if duplicate:
        raise ValueError(
            f"Evaluation labels contain {duplicate} duplicate/conflicting record keys; provide one reviewed group per record"
        )


def acceptance(result, options: EvaluationOptions):
    reasons = []
    entities = result.get("evaluated_entities", 0)
    if entities < options.min_evaluated_entities:
        reasons.append("Too few evaluated records")
    if result.get("label_overlap", 0) < options.min_label_overlap:
        reasons.append("Label overlap is below the required fraction")
    if result.get("truth_pairs", 0) < options.min_true_pairs:
        reasons.append("Too few labeled same-identity pairs to measure recall")
    evidence_reasons = list(reasons)
    for metric, threshold in [("precision", options.min_precision), ("recall", options.min_recall)]:
        if threshold is not None:
            value = result.get(metric)
            if value is None:
                reasons.append(f"{metric} is not measurable on this cohort")
                evidence_reasons.append(reasons[-1])
            elif value < threshold:
                reasons.append(f"{metric} is below the required threshold")
    return {
        "status": "insufficient_evidence"
        if evidence_reasons
        else "failed"
        if reasons
        else "passed",
        "reasons": reasons,
        "scope": "labeled_population_only",
    }
