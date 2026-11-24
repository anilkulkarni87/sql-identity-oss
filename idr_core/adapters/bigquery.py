"""
BigQuery adapter for IDR.

Provides SQL execution capabilities for Google BigQuery,
supporting both Python client library and direct execution.
"""

import concurrent.futures
import re
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

from .base import IDRAdapter


class BigQueryAdapter(IDRAdapter):
    """
    BigQuery adapter for IDR.

    Example:
        from google.cloud import bigquery
        client = bigquery.Client()
        adapter = BigQueryAdapter(client, project="my-project")
    """

    def __init__(
        self, client, project: str, location: str = "US", dataset_mapping: Dict[str, str] = None
    ):
        """
        Initialize with BigQuery client.

        Args:
            client: google.cloud.bigquery.Client instance
            project: GCP project ID
            location: BigQuery location (default: US)
            dataset_mapping: Map of schema names to dataset names (e.g. {'idr_out': 'my_dataset'})
        """
        self.client = client
        self.project = project or getattr(client, "project", None)
        self.location = location
        self.init_schema_mapping(dataset_mapping)
        self.dataset_mapping = dict(self.schema_mapping)
        self._budget_local = threading.local()

    supports_scan_cost_cap = True

    @contextmanager
    def query_budget(self, seconds: float, max_bytes_billed: Optional[int] = None):
        """Enforce per-job timeout + job.cancel() and optional maximum_bytes_billed."""
        deadline = time.monotonic() + seconds
        self._budget_local.deadline = deadline
        self._budget_local.max_bytes_billed = int(max_bytes_billed) if max_bytes_billed else None

        def check():
            if time.monotonic() >= deadline:
                raise ValueError(
                    "Profile preview exceeded its time budget; no production outputs were changed"
                )

        try:
            yield check
            check()
        finally:
            self._budget_local.deadline = None
            self._budget_local.max_bytes_billed = None

    def _apply_budget_config(self, job_config: Any = None) -> Any:
        max_bytes = getattr(self._budget_local, "max_bytes_billed", None)
        if max_bytes is None:
            return job_config
        if job_config is None:
            from google.cloud import bigquery

            job_config = bigquery.QueryJobConfig()
        job_config.maximum_bytes_billed = max_bytes
        return job_config

    def _wait_job(self, job: Any) -> Any:
        deadline = getattr(self._budget_local, "deadline", None)
        if deadline is None:
            return job.result()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            try:
                job.cancel()
            except Exception:
                pass
            raise ValueError(
                "Profile preview exceeded its time budget; no production outputs were changed"
            )
        try:
            return job.result(timeout=remaining)
        except (TimeoutError, concurrent.futures.TimeoutError) as exc:
            try:
                job.cancel()
            except Exception:
                pass
            raise ValueError(
                "Profile preview exceeded its time budget; no production outputs were changed"
            ) from exc

    @property
    def dialect(self) -> str:
        return "bigquery"

    def _prepare_sql(self, sql: str) -> str:
        """Replace schema aliases with fully qualified dataset names."""
        # 1. Apply dataset mappings (e.g. idr_out -> custom_dataset)
        for schema, dataset in self.dataset_mapping.items():
            sql = sql.replace(f"{schema}.", f"{dataset}.")

        # 2. Fully qualify with project ID
        # Note: If no mapping provided, idr_out -> project.idr_out
        # If mapping provided, custom_dataset -> project.custom_dataset
        # We handle standard schemas if not mapped
        schemas = ["idr_out", "idr_meta", "idr_work"]
        for schema in schemas:
            if schema not in self.dataset_mapping:
                sql = sql.replace(f"{schema}.", f"{self.project}.{schema}.")

        # Also fully qualify the mapped datasets if not already done
        for dataset in self.dataset_mapping.values():
            if not dataset.startswith(f"{self.project}.") and "." not in dataset:
                # Replacing just the dataset name is risky, simpler to ensure mapping target was used
                pass

        # Simple approach: Replace schema directly with Project.Dataset
        # This overwrites previous step but cleaner logic:
        # idr_out. -> project.mapped_dataset. OR project.idr_out.

        # Reset SQL to original for clean logic
        # Actually, let's just do the standard replacements based on final intention
        return sql

    def execute(self, sql: str) -> None:
        """Execute a single SQL statement, auto-migrating clustering spec on CREATE OR REPLACE TABLE."""
        prepared = self._prepare_sql_simple(sql)
        job_config = self._apply_budget_config()
        try:
            job = self.client.query(prepared, location=self.location, job_config=job_config)
            self._wait_job(job)  # Wait for completion
        except Exception as exc:
            if "Cannot replace a table with a different partitioning spec" in str(exc):
                m = re.search(
                    r"\bCREATE\s+OR\s+REPLACE\s+TABLE\s+([`A-Za-z0-9_.\-]+)",
                    prepared,
                    flags=re.IGNORECASE,
                )
                if m:
                    target_table = m.group(1)
                    drop_job = self.client.query(
                        f"DROP TABLE IF EXISTS {target_table}", location=self.location
                    )
                    self._wait_job(drop_job)
                    retry_job = self.client.query(
                        prepared, location=self.location, job_config=job_config
                    )
                    self._wait_job(retry_job)
                    return
            raise

    def execute_script(self, sql: str) -> None:
        """Execute multiple SQL statements as a script."""
        # BigQuery supports multi-statement scripts
        sql = self._prepare_sql_simple(sql)
        job = self.client.query(
            sql, location=self.location, job_config=self._apply_budget_config()
        )
        self._wait_job(job)

    def _prepare_sql_simple(self, sql: str) -> str:
        """Replace canonical schema references with `project`.dataset using word boundaries."""
        project_ref = self.project
        if project_ref and not project_ref.startswith("`"):
            project_ref = f"`{project_ref}`"

        for schema in ("idr_out", "idr_meta", "idr_work"):
            target_dataset = self.dataset_mapping.get(schema, schema)
            prefix = f"{project_ref}.{target_dataset}" if project_ref else target_dataset
            sql = re.sub(
                rf"(?<![\w`\.]){schema}(?=\s*\.)",
                prefix,
                sql,
                flags=re.IGNORECASE,
            )
            sql = re.sub(
                rf"(\b(?:CREATE|DROP)\s+SCHEMA(?:\s+IF\s+(?:NOT\s+)?EXISTS)?\s+){schema}\b",
                rf"\1{prefix}",
                sql,
                flags=re.IGNORECASE,
            )
        return sql

    def query(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Execute SQL and return results as list of dicts."""
        sql = self._prepare_sql_simple(sql)

        job_config = None
        if params:
            from google.cloud import bigquery

            def infer_type(val):
                if isinstance(val, bool):
                    return "BOOL"
                if isinstance(val, int):
                    return "INT64"
                if isinstance(val, float):
                    return "FLOAT64"
                return "STRING"  # Default

            # Basic support for positional parameters (list)
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(None, infer_type(p), p) for p in params
                ]
            )

        job_config = self._apply_budget_config(job_config)
        job = self.client.query(sql, location=self.location, job_config=job_config)
        res_iter = self._wait_job(job)
        rows_list = list(res_iter)
        if rows_list and hasattr(rows_list[0], "keys"):
            return [{str(k).lower(): row[k] for k in row.keys()} for row in rows_list]
        if not rows_list and hasattr(res_iter, "total_rows"):
            return []
        df = job.to_dataframe()
        df.columns = [str(c).lower() for c in df.columns]
        return df.to_dict("records")

    def query_one(self, sql: str, params: Optional[List[Any]] = None) -> Any:
        """Execute SQL and return first value of first row."""
        sql = self._prepare_sql_simple(sql)

        job_config = None
        if params:
            from google.cloud import bigquery

            def infer_type(val):
                if isinstance(val, bool):
                    return "BOOL"
                if isinstance(val, int):
                    return "INT64"
                if isinstance(val, float):
                    return "FLOAT64"
                return "STRING"  # Default

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(None, infer_type(p), p) for p in params
                ]
            )

        job_config = self._apply_budget_config(job_config)
        job = self.client.query(sql, location=self.location, job_config=job_config)
        result = list(self._wait_job(job))
        if result and len(result) > 0:
            # BigQuery Row objects are subscriptable by index
            return result[0][0]
        return None

    def table_exists(self, table_fqn: str) -> bool:
        """Check if table exists."""
        try:
            # Normalize to project.dataset.table format
            parts = table_fqn.replace("`", "").split(".")
            if len(parts) == 2:
                # Handle schema aliases (idr_out -> mapped_dataset)
                schema, table = parts
                dataset = self.dataset_mapping.get(schema, schema)
                table_ref = f"{self.project}.{dataset}.{table}"
            else:
                table_ref = ".".join(parts)

            self.client.get_table(table_ref)
            return True
        except Exception:
            return False

    def get_table_columns(self, table_fqn: str) -> List[Dict[str, str]]:
        """Get column names, types, and optional catalog descriptions for a table (lowercase)."""
        parts = table_fqn.replace("`", "").split(".")
        if len(parts) == 2:
            # Handle schema alias if present
            dataset = self.dataset_mapping.get(parts[0], parts[0])
            table_ref = f"{self.project}.{dataset}.{parts[1]}"
        else:
            table_ref = ".".join(parts)

        try:
            table = self.client.get_table(table_ref)
            if not table or not table.schema:
                return []
            tbl_desc = getattr(table, "description", None)
            if isinstance(tbl_desc, str) and tbl_desc.strip():
                cache = getattr(self, "_table_descriptions", None)
                if cache is None:
                    cache = {}
                    self._table_descriptions = cache
                cache[table_fqn] = tbl_desc.strip()[:240]
            cols: List[Dict[str, str]] = []
            for field in table.schema:
                entry: Dict[str, str] = {"name": field.name.lower(), "type": field.field_type}
                f_desc = getattr(field, "description", None)
                if isinstance(f_desc, str) and f_desc.strip():
                    entry["description"] = f_desc.strip()[:200]
                cols.append(entry)
            return cols
        except Exception:
            return []

    def get_table_description(self, table_fqn: str) -> Optional[str]:
        """Return cached table-level description when present in BigQuery metadata."""
        return getattr(self, "_table_descriptions", {}).get(table_fqn)

    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """List tables in a dataset, or all datasets if schema is None."""
        if schema:
            datasets = [self.dataset_mapping.get(schema, schema)]
        else:
            try:
                datasets = [
                    d.dataset_id
                    for d in self.client.list_datasets()
                    if not (
                        d.dataset_id.startswith("idr_meta")
                        or d.dataset_id.startswith("idr_work")
                        or d.dataset_id.startswith("idr_out")
                        or re.fullmatch(r"ai_p[0-9a-f]{24}_idr_(meta|work|out)", d.dataset_id)
                    )
                ]
            except Exception:
                datasets = []

        all_tables = []
        for dataset_id in datasets:
            if "." not in dataset_id:
                dataset_ref = f"{self.project}.{dataset_id}"
            else:
                dataset_ref = dataset_id

            try:
                tables = list(self.client.list_tables(dataset_ref))
                all_tables.extend([f"{t.dataset_id}.{t.table_id}" for t in tables])
            except Exception:
                continue

        return all_tables

    def close(self) -> None:
        """No-op - BigQuery client doesn't need explicit close."""
        pass
