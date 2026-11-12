"""
Preflight validation stage for IDR pipeline.

Validates metadata configuration, checks for concurrent runs,
ensures schema upgrades, and validates identifier columns.
"""

from .base import BaseStage


class PreflightStage(BaseStage):
    """Validates environment and metadata before pipeline execution."""

    def run(self) -> None:
        """Validate metadata configuration and prevent concurrent runs."""
        # Clean up stale RUNNING runs (older than 4 hours) from previous failures
        if self.adapter.dialect == "bigquery":
            time_filter = "started_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 HOUR)"
        elif self.adapter.dialect == "snowflake":
            time_filter = "started_at < DATEADD('hour', -4, CURRENT_TIMESTAMP())"
        elif self.adapter.dialect == "databricks":
            time_filter = "started_at < TIMESTAMPADD(HOUR, -4, CURRENT_TIMESTAMP())"
        else:
            time_filter = "started_at < CURRENT_TIMESTAMP - INTERVAL '4 hours'"

        # Ensure schema is up to date (Migration for v0.5.0)
        self._ensure_schema_upgrades()

        self.adapter.execute(f"""
            UPDATE idr_out.run_history
            SET status = 'INTERRUPTED',
                error_message = 'Run interrupted - stale RUNNING state detected'
            WHERE status = 'RUNNING'
              AND {time_filter}
        """)

        # Check for recent concurrent runs
        running = self.adapter.query(
            "SELECT run_id FROM idr_out.run_history WHERE status = 'RUNNING'"
        )
        if running:
            raise RuntimeError(f"Concurrent run detected: {running[0]['run_id']}")

        # Validate source tables exist
        sources = self.adapter.query(
            "SELECT table_id, table_fqn FROM idr_meta.source_table WHERE is_active = TRUE"
        )
        if not sources:
            raise RuntimeError("No active source tables configured")

        for src in sources:
            if not self.adapter.table_exists(src["table_fqn"]):
                raise RuntimeError(f"Source table not found: {src['table_fqn']}")

        # Validate rules exist
        rules = self.adapter.query(
            "SELECT COUNT(*) as cnt FROM idr_meta.rule WHERE is_active = TRUE"
        )
        if not rules or rules[0]["cnt"] == 0:
            self._warnings.append("No active matching rules configured")

        # Validate identifier mappings reference real columns
        self._validate_identifier_columns()

    def _validate_identifier_columns(self) -> None:
        """Validate that identifier mappings reference real columns in source tables."""
        mappings = self.adapter.query("""
            SELECT im.table_id, im.identifier_type, im.identifier_value_expr, st.table_fqn
            FROM idr_meta.identifier_mapping im
            JOIN idr_meta.source_table st ON im.table_id = st.table_id
        """)

        if not mappings:
            return

        for m in mappings:
            # Extract column name from expression (handle simple cases)
            expr = m["identifier_value_expr"]
            # Skip complex expressions with functions
            if "(" in expr or " " in expr:
                continue

            # Validate column exists
            try:
                columns = self.adapter.get_table_columns(m["table_fqn"])
                col_names = [c["name"].lower() for c in columns]
                if columns and expr.lower() not in col_names:
                    self._warnings.append(
                        f"Column '{expr}' may not exist in {m['table_fqn']} (identifier: {m['identifier_type']})"
                    )
            except Exception as e:
                self.logger.warning(
                    f"Skipped column validation for {m['table_fqn']}: {e}",
                    extra={"run_id": self.run_id},
                )
                pass  # Skip validation if we can't get columns

    def _ensure_schema_upgrades(self) -> None:
        """Apply necessary schema updates and ensure all system tables exist for existing deployments."""
        try:
            from ..schema_defs import SYSTEM_TABLES
            from ..schema_manager import SchemaManager

            sm = SchemaManager(self.adapter)
            for table_def in SYSTEM_TABLES:
                try:
                    if not self.adapter.table_exists(table_def.fqn):
                        sm._create_table(table_def)
                except Exception:
                    pass
        except Exception as e:
            self.logger.warning(f"System table check failed: {e}")

        try:
            # check if config_hash exists in run_history
            columns = self.adapter.get_table_columns("idr_out.run_history")
            col_names = [c["name"].lower() for c in columns]
            if "config_hash" not in col_names:
                self.logger.info("Migrating schema: Adding config_hash to idr_out.run_history")

                # Determine type
                col_type = "STRING"
                if self.adapter.dialect in ("duckdb", "postgres", "snowflake"):
                    col_type = "VARCHAR"

                self.adapter.execute(
                    f"ALTER TABLE idr_out.run_history ADD COLUMN config_hash {col_type}"
                )
        except Exception as e:
            self.logger.warning(f"Schema migration check failed: {e}")

        try:
            # Ensure corroboration columns exist on idr_out.identity_clusters_current
            cluster_cols = self.adapter.get_table_columns("idr_out.identity_clusters_current")
            cluster_col_names = [c["name"].lower() for c in cluster_cols] if cluster_cols else []
            if cluster_col_names:
                int_type = "INT64" if self.adapter.dialect == "bigquery" else ("INTEGER" if self.adapter.dialect in ("duckdb", "snowflake") else "INT")
                float_type = "FLOAT64" if self.adapter.dialect == "bigquery" else "DOUBLE"
                for col_name, col_type in [
                    ("edge_diversity", int_type),
                    ("edge_count", int_type),
                    ("max_fanout", int_type),
                    ("match_density", float_type),
                ]:
                    if col_name not in cluster_col_names:
                        self.logger.info(
                            f"Migrating schema: Adding {col_name} to idr_out.identity_clusters_current"
                        )
                        self.adapter.execute(
                            f"ALTER TABLE idr_out.identity_clusters_current ADD COLUMN {col_name} {col_type}"
                        )
        except Exception as e:
            self.logger.warning(f"Cluster corroboration schema migration failed: {e}")

    def load_evidence_flag(self) -> bool:
        """Load the generate_evidence flag from run config or DB.

        The flag controls whether edge evidence is populated for debugging.
        Default is False for production performance.
        """
        try:
            exists = self.adapter.table_exists("idr_out.edge_evidence")
            return exists  # If table exists, populate it
        except Exception:
            return False
