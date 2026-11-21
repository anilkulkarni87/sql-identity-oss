"""Domain-independent current state per resolved identity and configured dimensions."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .adapters.base import get_dialect_config
from .config import resolve_entity_key_expr, validate_identifier, validate_sql_expr
from .config_extensions import read_extensions
from .source_relation import source_relation_sql


class GroupedSource(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_id: str
    dimensions: dict[str, str]
    fields: dict[str, str]
    recency_field: str | None = None


class GroupedStateOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(max_length=48)
    kind: Literal["grouped_state"] = "grouped_state"
    dimensions: list[str] = Field(min_length=1, max_length=8)
    fields: list[str] = Field(min_length=1, max_length=50)
    sources: list[GroupedSource] = Field(min_length=1)
    strategy: Literal["RECENCY", "PRIORITY_RECENCY"] = "RECENCY"
    source_priority: list[str] = Field(default_factory=list)
    conflict_policy: Literal["error", "mark"] = "error"
    null_dimensions: Literal["error", "keep"] = "error"

    @model_validator(mode="after")
    def validate_definition(self):
        validate_identifier(self.id, "grouped output ID")
        columns = self.dimensions + self.fields
        if len({c.lower() for c in columns}) != len(columns):
            raise ValueError("Grouped output dimension and field names must be unique")
        for col in columns:
            validate_identifier(col, "grouped output column")
            if col.lower() in {"resolved_id", "run_id", "updated_at"} or col.startswith("_idr_"):
                raise ValueError("Grouped output column collides with internal provenance columns")
        ids = [s.source_id for s in self.sources]
        if (
            len(set(ids)) != len(ids)
            or not set(self.source_priority) <= set(ids)
            or len(set(self.source_priority)) != len(self.source_priority)
        ):
            raise ValueError(
                "Grouped output source priorities must reference unique configured contributors"
            )
        for source in self.sources:
            validate_identifier(source.source_id)
            if set(source.dimensions) != set(self.dimensions) or not set(source.fields) <= set(
                self.fields
            ):
                raise ValueError(
                    "Each grouped source must map every dimension and only declared output fields"
                )
            for expr in [*source.dimensions.values(), *source.fields.values()]:
                validate_sql_expr(expr, "grouped output expression")
            if source.recency_field:
                validate_identifier(source.recency_field, "recency field")
        if not set(self.fields) <= set().union(*(set(s.fields) for s in self.sources)):
            raise ValueError("Every grouped output field requires a source mapping")
        return self


def validate_grouped_outputs(config):
    definitions = [GroupedStateOutput.model_validate(v) for v in config.get("grouped_outputs", [])]
    ids = [d.id for d in definitions]
    if len(set(ids)) != len(ids):
        raise ValueError("Grouped output IDs must be unique")
    sources = {s["id"] for s in config.get("sources", [])}
    if any(s.source_id not in sources for d in definitions for s in d.sources):
        raise ValueError(
            "Grouped output references an unknown source; include it in Sources & Mappings"
        )
    return definitions


def definitions_from_warehouse(adapter):
    return [
        GroupedStateOutput.model_validate(v)
        for v in read_extensions(adapter).get("grouped_outputs", [])
    ]


def materialize_grouped_outputs(
    adapter,
    run_id,
    run_mode="FULL",
    config=None,
    membership_table="idr_out.identity_resolved_membership_current",
    output_ids=None,
):
    definitions = (
        validate_grouped_outputs(config)
        if config is not None
        else definitions_from_warehouse(adapter)
    )
    if output_ids is not None:
        definitions = [d for d in definitions if d.id in output_ids]
    if not definitions:
        return {}
    from .config import sql_to_config

    config = config if config is not None else sql_to_config(adapter)
    from .config import validate_fqn

    membership_table = validate_fqn(membership_table)
    source_config = {s["id"]: s for s in config.get("sources", [])}
    dialect = get_dialect_config(adapter.dialect)
    string = dialect["string_type"]
    timestamp = dialect["timestamp_type"]
    prepared = []
    for definition in definitions:
        dims = definition.dimensions
        fields = definition.fields
        projections = []
        for mapping in definition.sources:
            source = source_config[mapping.source_id]
            key = resolve_entity_key_expr(source, adapter.dialect)
            relation = source_relation_sql(
                source["table"],
                key,
                bool(source.get("deduplicate")),
                source.get("dedup_order_by") or "",
                source.get("watermark_column") or "",
            )
            recency = mapping.recency_field or source.get("watermark_column")
            recency_expr = (
                f"CAST({validate_identifier(recency)} AS {timestamp})"
                if recency
                else f"CAST(NULL AS {timestamp})"
            )
            values = [f"({mapping.dimensions[c]}) AS {c}" for c in dims]
            values += [
                f"({mapping.fields[c]}) AS {c}" if c in mapping.fields else f"NULL AS {c}"
                for c in fields
            ]
            # Compute source expressions before the membership join, so arbitrary
            # input key/field names cannot become ambiguous with resolved_id.
            raw = f"SELECT CAST(({key}) AS {string}) AS _idr_source_key, {recency_expr} AS _idr_effective_at, {','.join(values)} FROM {relation} src"
            priority = (
                definition.source_priority.index(mapping.source_id)
                if mapping.source_id in definition.source_priority
                else len(definition.source_priority)
            )
            where = (
                "WHERE m.resolved_id IN (SELECT resolved_id FROM idr_work.impacted_resolved_ids)"
                if run_mode == "INCR"
                else ""
            )
            projections.append(
                f"SELECT m.resolved_id, '{mapping.source_id}' AS _idr_source_id, {priority} AS _idr_priority, src.* FROM ({raw}) src JOIN {membership_table} m ON m.entity_key=CONCAT('{mapping.source_id}:',src._idr_source_key) {where}"
            )
        stage = f"idr_work.grouped_values_{definition.id}"
        adapter.execute(f"CREATE OR REPLACE TABLE {stage} AS " + " UNION ALL ".join(projections))
        null_condition = " OR ".join(c + " IS NULL" for c in dims)
        if definition.null_dimensions == "error" and adapter.query_one(
            f"SELECT COUNT(*) FROM {stage} WHERE {null_condition}"
        ):
            raise ValueError(
                f"Grouped output {definition.id} has missing dimensions; specify a null-dimension policy"
            )
        partition = ",".join(["resolved_id"] + dims)
        recency = "COALESCE(_idr_effective_at,TIMESTAMP '1970-01-01 00:00:00') DESC"
        authority = (
            f"_idr_priority ASC, {recency}"
            if definition.strategy == "PRIORITY_RECENCY"
            else f"{recency}, _idr_priority ASC"
        )
        stable = ", ".join(
            [authority, "_idr_source_id ASC", "_idr_source_key ASC"]
            + [f"CAST({c} AS {string}) ASC" for c in fields]
        )
        ranked = f"idr_work.grouped_ranked_{definition.id}"
        adapter.execute(
            f"CREATE OR REPLACE TABLE {ranked} AS SELECT *, DENSE_RANK() OVER(PARTITION BY {partition} ORDER BY {authority}) AS _idr_authority_rank, ROW_NUMBER() OVER(PARTITION BY {partition} ORDER BY {stable}) AS _idr_row_rank FROM {stage}"
        )
        conflicts = f"idr_work.grouped_conflicts_{definition.id}"
        mismatch = " OR ".join(
            f"(COUNT(DISTINCT CAST({c} AS {string}))+MAX(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END)>1)"
            for c in fields
        )
        adapter.execute(
            f"CREATE OR REPLACE TABLE {conflicts} AS SELECT {partition}, CASE WHEN {mismatch} THEN TRUE ELSE FALSE END AS _idr_conflict FROM {ranked} WHERE _idr_authority_rank=1 GROUP BY {partition}"
        )
        if definition.conflict_policy == "error" and adapter.query_one(
            f"SELECT COUNT(*) FROM {conflicts} WHERE _idr_conflict=TRUE"
        ):
            raise ValueError(
                f"Grouped output {definition.id} has conflicting equally authoritative states; resolve the data or explicitly choose conflict_policy=mark"
            )
        join = " AND ".join(
            f"(r.{c}=c.{c} OR (r.{c} IS NULL AND c.{c} IS NULL))" for c in ["resolved_id"] + dims
        )
        output = f"idr_work.grouped_prepared_{definition.id}"
        selected = ",".join(
            "r." + c
            for c in ["resolved_id"]
            + dims
            + fields
            + ["_idr_source_id", "_idr_source_key", "_idr_effective_at"]
        )
        # Under mark, conflicting fields become NULL rather than arbitrary values.
        if definition.conflict_policy == "mark":
            selected = ",".join(
                ["r.resolved_id"]
                + ["r." + c for c in dims]
                + [f"CASE WHEN c._idr_conflict THEN NULL ELSE r.{c} END AS {c}" for c in fields]
                + [
                    f"CASE WHEN c._idr_conflict THEN NULL ELSE r.{c} END AS {c}"
                    for c in ["_idr_source_id", "_idr_source_key"]
                ]
                + ["r._idr_effective_at"]
            )
        safe_run_id = str(run_id).replace("'", "''")
        adapter.execute(
            f"CREATE OR REPLACE TABLE {output} AS SELECT {selected},c._idr_conflict,'{safe_run_id}' AS run_id,{dialect['current_timestamp']} AS updated_at FROM {ranked} r JOIN {conflicts} c ON {join} WHERE r._idr_row_rank=1"
        )
        prepared.append((definition, output))
    counts = {}
    for definition, prepared_table in prepared:
        target = "idr_out.grouped_" + definition.id
        if run_mode == "INCR" and adapter.table_exists(target):
            adapter.execute(
                f"DELETE FROM {target} WHERE resolved_id IN (SELECT resolved_id FROM idr_work.impacted_resolved_ids) OR NOT EXISTS(SELECT 1 FROM idr_out.identity_resolved_membership_current m WHERE m.resolved_id={target}.resolved_id)"
            )
            adapter.execute(f"INSERT INTO {target} SELECT * FROM {prepared_table}")
        else:
            adapter.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM {prepared_table}")
        counts[definition.id] = int(
            adapter.query_one(f"SELECT COUNT(*) FROM {prepared_table}") or 0
        )
    return counts
