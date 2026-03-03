"""
BigQuery adapter for IDR.

Provides SQL execution capabilities for Google BigQuery,
supporting both Python client library and direct execution.
"""

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
        self.project = project
        self.location = location
        self.dataset_mapping = dataset_mapping or {}

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
        """Execute a single SQL statement."""
        sql = self._prepare_sql_simple(sql)
        job = self.client.query(sql, location=self.location)
        job.result()  # Wait for completion

    def execute_script(self, sql: str) -> None:
        """Execute multiple SQL statements as a script."""
        # BigQuery supports multi-statement scripts
        sql = self._prepare_sql_simple(sql)
        job = self.client.query(sql, location=self.location)
        job.result()

    def _prepare_sql_simple(self, sql: str) -> str:
        """Replace standard schemas with `project`.dataset."""
        # Ensure project ID is backticked if not already
        project_ref = self.project
        if not project_ref.startswith("`"):
            project_ref = f"`{project_ref}`"

        for schema in ["idr_out", "idr_meta", "idr_work"]:
            # Determine target dataset name
            target_dataset = self.dataset_mapping.get(schema, schema)
            # Fully qualify
            replacement = f"{project_ref}.{target_dataset}."
            sql = sql.replace(f"{schema}.", replacement)
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

        job = self.client.query(sql, location=self.location, job_config=job_config)
        df = job.to_dataframe()
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

        job = self.client.query(sql, location=self.location, job_config=job_config)
        result = list(job.result())
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
                table_ref = table_fqn

            self.client.get_table(table_ref)
            return True
        except Exception:
            return False

    def get_table_columns(self, table_fqn: str) -> List[Dict[str, str]]:
        """Get column names for a table (lowercase)."""
        parts = table_fqn.replace("`", "").split(".")
        if len(parts) == 2:
            # Handle schema alias if present
            dataset = self.dataset_mapping.get(parts[0], parts[0])
            table_ref = f"{self.project}.{dataset}.{parts[1]}"
        else:
            table_ref = table_fqn

        table = self.client.get_table(table_ref)
        return [{"name": field.name.lower(), "type": field.field_type} for field in table.schema]

    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """List tables in a dataset."""
        if not schema:
            return []

        dataset_id = self.dataset_mapping.get(schema, schema)
        # Use simple string dataset_id, client handles project prefix if needed or we prepend
        # client.list_tables expects dataset reference
        if "." not in dataset_id:
            dataset_ref = f"{self.project}.{dataset_id}"
        else:
            dataset_ref = dataset_id

        try:
            tables = list(self.client.list_tables(dataset_ref))
            return [f"{t.dataset_id}.{t.table_id}" for t in tables]
        except Exception:
            return []

    def close(self) -> None:
        """No-op - BigQuery client doesn't need explicit close."""
        pass
