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

        # Validate identifier mappings reference real columns and check cross-table coverage
        self._validate_identifier_columns()
        self._validate_identifier_mapping_coverage(len(sources))
        return len(sources)

    def _validate_identifier_columns(self) -> None:
        """Validate that identifier mappings reference real columns in source tables."""
        mappings = self.adapter.query("""
            SELECT im.table_id, im.identifier_type, im.identifier_value_expr, st.table_fqn
            FROM idr_meta.identifier_mapping im
            JOIN idr_meta.source_table st ON im.table_id = st.table_id
        """)

        if not mappings:
            return

        col_cache = {}
        for m in mappings:
            # Extract column name from expression (handle simple cases)
            expr = m["identifier_value_expr"]
            # Skip complex expressions with functions
            if "(" in expr or " " in expr:
                continue

            # Validate column exists (cached per table_fqn)
            try:
                fqn = m["table_fqn"]
                if fqn not in col_cache:
                    columns = self.adapter.get_table_columns(fqn)
                    col_cache[fqn] = [c["name"].lower() for c in columns] if columns else []
                col_names = col_cache[fqn]
                if col_names and expr.lower() not in col_names:
                    self._warnings.append(
                        f"Column '{expr}' may not exist in {fqn} (identifier: {m['identifier_type']})"
                    )
            except Exception as e:
                self.logger.warning(
                    f"Skipped column validation for {m['table_fqn']}: {e}",
                    extra={"run_id": self.run_id},
                )
                pass  # Skip validation if we can't get columns

    def _validate_identifier_mapping_coverage(self, active_source_count: int) -> None:
        """Warn on identifier mappings that have no active rule or are only mapped on one source table."""
        try:
            mappings = self.adapter.query("""
                SELECT DISTINCT im.identifier_type, im.table_id
                FROM idr_meta.identifier_mapping im
                JOIN idr_meta.source_table st ON im.table_id = st.table_id
                WHERE st.is_active = TRUE
            """)
            if not mappings:
                return

            active_rules = self.adapter.query(
                "SELECT DISTINCT identifier_type FROM idr_meta.rule WHERE is_active = TRUE"
            )
            active_rule_types = {r["identifier_type"] for r in (active_rules or [])}

            tables_by_id_type: dict[str, list[str]] = {}
            for row in mappings:
                id_type = row["identifier_type"]
                tables_by_id_type.setdefault(id_type, []).append(row["table_id"])

            for id_type in sorted(tables_by_id_type):
                tables = sorted(tables_by_id_type[id_type])
                if id_type not in active_rule_types:
                    msg = (
                        f"Identifier '{id_type}' is mapped on {len(tables)} source table(s) "
                        f"({', '.join(tables)}) but has no active rule in idr_meta.rule — "
                        f"it will be ignored during extraction."
                    )
                    self._warnings.append(msg)
                    self.logger.warning(msg, extra={"run_id": self.run_id})
                elif active_source_count > 1 and len(tables) == 1:
                    msg = (
                        f"Identifier '{id_type}' is only mapped on 1 active source table "
                        f"({tables[0]}) — it can only deduplicate within that table and will "
                        f"not form cross-table edges."
                    )
                    self._warnings.append(msg)
                    self.logger.warning(msg, extra={"run_id": self.run_id})
        except Exception as e:
            self.logger.warning(
                f"Skipped identifier mapping coverage check: {e}",
                extra={"run_id": self.run_id},
            )

    def _ensure_schema_upgrades(self) -> None:
        """Declaratively reconcile missing system tables and additive columns from SYSTEM_TABLES."""
        try:
            from ..schema_manager import SchemaManager

            sm = SchemaManager(self.adapter)
            applied = sm.ensure_schema()
            if applied:
                self.logger.info(
                    f"Schema reconciliation applied {len(applied)} change(s): {', '.join(applied[:8])}",
                    extra={"run_id": self.run_id},
                )
        except Exception as e:
            self.logger.warning(f"System schema reconciliation failed: {e}")

    def load_evidence_flag(self, run_override: bool | None = None) -> bool:
        """Load the opt-in generate_evidence flag from RunConfig, environment, or config_extension.

        Default is False for production performance (never inferred merely from table existence).
        """
        if run_override is not None:
            return bool(run_override)

        import os

        env_val = os.environ.get("IDR_GENERATE_EVIDENCE", "").strip().lower()
        if env_val in ("1", "true", "yes", "on"):
            return True
        if env_val in ("0", "false", "no", "off"):
            return False

        try:
            from ..config_extensions import read_extensions

            ext = read_extensions(self.adapter)
            if "generate_evidence" in ext and ext["generate_evidence"] is not None:
                return bool(ext["generate_evidence"])
        except Exception:
            pass
        return False
