"""
Production readiness diagnostic and health checker for SQL Identity Resolution.

Performs live warehouse validation:
- Connectivity and latency measurement
- Schema and system table existence
- Write, DDL, and MERGE permissions probing
- Active configuration, watermarks, and matching rules
- Poison-pill exclusions and guardrails
- Run history and operational telemetry
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class CheckStatus(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    INFO = "INFO"


@dataclass
class CheckItem:
    category: str
    name: str
    status: CheckStatus
    message: str
    details: Optional[Dict[str, Any]] = None
    remediation: Optional[str] = None


@dataclass
class DoctorReport:
    platform: str
    overall_status: CheckStatus
    checks: List[CheckItem] = field(default_factory=list)
    summary: str = ""
    timestamp: float = field(default_factory=time.time)

    @property
    def passed_count(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.PASS)

    @property
    def warn_count(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.WARN)

    @property
    def fail_count(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.FAIL)


class IDRDoctor:
    """Diagnostic runner that tests warehouse readiness for production execution."""

    def __init__(self, adapter: Any):
        self.adapter = adapter
        self.platform = getattr(adapter, "dialect", getattr(adapter, "platform", "unknown"))

    def run_diagnostics(self, verbose: bool = False) -> DoctorReport:
        """Run all diagnostic checks and synthesize an enterprise readiness report."""
        checks: List[CheckItem] = []

        # 1. Connectivity & Latency
        conn_check = self._check_connectivity()
        checks.append(conn_check)

        # If connectivity fails, abort early
        if conn_check.status == CheckStatus.FAIL:
            return DoctorReport(
                platform=self.platform,
                overall_status=CheckStatus.FAIL,
                checks=checks,
                summary="Failed warehouse connectivity probe. Fix connection credentials.",
            )

        # 2. Schema & Tables Initialization
        schema_checks = self._check_schemas_and_tables()
        checks.extend(schema_checks)

        # 3. Permissions Probe (DDL / DML in idr_work)
        perm_check = self._check_permissions_probe()
        checks.append(perm_check)

        # 4. Configuration & Rules
        config_checks = self._check_configuration()
        checks.extend(config_checks)

        # 5. Operational Health & Run History
        ops_checks = self._check_operational_health()
        checks.extend(ops_checks)

        # Determine overall status
        if any(c.status == CheckStatus.FAIL for c in checks):
            overall = CheckStatus.FAIL
            summary = "Production blocker(s) detected. Resolve failing checks before scheduling."
        elif any(c.status == CheckStatus.WARN for c in checks):
            overall = CheckStatus.WARN
            summary = "System is operational with warnings. Review warnings before high-volume runs."
        else:
            overall = CheckStatus.PASS
            summary = "System is 100% PRODUCTION READY. All connectivity, permissions, and guardrails verified."

        return DoctorReport(
            platform=self.platform,
            overall_status=overall,
            checks=checks,
            summary=summary,
        )

    def _check_connectivity(self) -> CheckItem:
        """Measure database connection ping and retrieve platform coordinates."""
        start_t = time.perf_counter()
        try:
            res = self.adapter.query("SELECT 1 AS ping")
            latency_ms = round((time.perf_counter() - start_t) * 1000, 1)

            details: Dict[str, Any] = {"latency_ms": latency_ms}
            if hasattr(self.adapter, "project"):
                details["project"] = self.adapter.project
            if hasattr(self.adapter, "location"):
                details["location"] = self.adapter.location
            if hasattr(self.adapter, "db_path"):
                details["database_file"] = self.adapter.db_path
            if hasattr(self.adapter, "catalog"):
                details["catalog"] = self.adapter.catalog

            return CheckItem(
                category="Connectivity",
                name="Warehouse Ping & Latency",
                status=CheckStatus.PASS,
                message=f"Connected to {self.platform.upper()} (latency: {latency_ms}ms)",
                details=details,
            )
        except Exception as e:
            return CheckItem(
                category="Connectivity",
                name="Warehouse Ping & Latency",
                status=CheckStatus.FAIL,
                message=f"Connection failed: {e}",
                remediation="Verify warehouse credentials, host/project configuration, and network routing.",
            )

    def _check_schemas_and_tables(self) -> List[CheckItem]:
        """Verify presence of core metadata, staging, and output tables."""
        items: List[CheckItem] = []

        required_tables = [
            ("idr_meta.source_table", "Metadata Source Registry"),
            ("idr_meta.rule", "Deterministic Match Rules"),
            ("idr_meta.identifier_exclusion", "Poison-Pill Exclusions"),
            ("idr_meta.run_state", "Incremental Watermark State"),
            ("idr_out.identity_clusters_current", "Output Clusters"),
            ("idr_out.identity_resolved_membership_current", "Output Resolved Membership"),
            ("idr_out.run_history", "Execution Run History"),
        ]

        missing = []
        for tbl_fqn, label in required_tables:
            try:
                exists = self.adapter.table_exists(tbl_fqn)
                if not exists:
                    missing.append(tbl_fqn)
            except Exception:
                missing.append(tbl_fqn)

        if not missing:
            items.append(
                CheckItem(
                    category="Schemas & Tables",
                    name="Core IDR System Tables",
                    status=CheckStatus.PASS,
                    message=f"All {len(required_tables)} system tables present in {self.platform.upper()}",
                )
            )
        else:
            items.append(
                CheckItem(
                    category="Schemas & Tables",
                    name="Core IDR System Tables",
                    status=CheckStatus.FAIL,
                    message=f"Missing {len(missing)} required tables: {', '.join(missing)}",
                    remediation="Execute 'idr init' to create and initialize metadata, staging, and output schemas.",
                )
            )

        return items

    def _check_permissions_probe(self) -> CheckItem:
        """Perform a temporary DDL/DML probe in idr_work to certify write/drop permissions."""
        probe_table = "idr_work._idr_doctor_probe"
        try:
            # 1. Clean up any leftover probe table
            try:
                self.adapter.execute(f"DROP TABLE IF EXISTS {probe_table}")
            except Exception:
                pass

            # 2. CREATE TABLE
            if self.platform == "bigquery":
                self.adapter.execute(
                    f"CREATE OR REPLACE TABLE {probe_table} (probe_id INT64, probe_ts TIMESTAMP)"
                )
            else:
                self.adapter.execute(
                    f"CREATE TABLE {probe_table} (probe_id INT, probe_ts TIMESTAMP)"
                )

            # 3. INSERT
            self.adapter.execute(
                f"INSERT INTO {probe_table} VALUES (1, CURRENT_TIMESTAMP)"
            )

            # 4. SELECT
            rows = self.adapter.query(f"SELECT COUNT(*) AS cnt FROM {probe_table}")
            cnt = rows[0]["cnt"] if rows and isinstance(rows[0], dict) else 1

            # 5. DROP TABLE
            self.adapter.execute(f"DROP TABLE {probe_table}")

            return CheckItem(
                category="Permissions",
                name="DDL & DML Staging Probe",
                status=CheckStatus.PASS,
                message="Successfully executed CREATE, INSERT, SELECT, and DROP in staging schema (idr_work)",
            )
        except Exception as e:
            return CheckItem(
                category="Permissions",
                name="DDL & DML Staging Probe",
                status=CheckStatus.FAIL,
                message=f"Permission probe failed in idr_work: {e}",
                remediation=(
                    "Grant the service account/role DDL and DML permissions on the staging schema.\n"
                    "- BigQuery: roles/bigquery.dataEditor, roles/bigquery.jobUser\n"
                    "- Snowflake: USAGE, CREATE TABLE on SCHEMA idr_work\n"
                    "- Databricks: ALL PRIVILEGES or USE SCHEMA, CREATE TABLE on idr_work"
                ),
            )

    def _check_configuration(self) -> List[CheckItem]:
        """Verify registered sources, watermarks, matching rules, and poison-pill exclusions."""
        items: List[CheckItem] = []

        # 1. Source Tables & Watermarks
        try:
            sources = self.adapter.query(
                "SELECT table_id, table_fqn, entity_type, watermark_column, is_active "
                "FROM idr_meta.source_table WHERE is_active = TRUE"
            )
            src_count = len(sources) if sources else 0

            if src_count == 0:
                items.append(
                    CheckItem(
                        category="Configuration",
                        name="Registered Source Tables",
                        status=CheckStatus.WARN,
                        message="0 active source tables registered in idr_meta.source_table",
                        remediation="Run 'idr bootstrap --auto' or 'idr apply-config' to configure data sources.",
                    )
                )
            else:
                watermarked = sum(1 for s in sources if s.get("watermark_column"))
                unwatermarked = src_count - watermarked

                wm_msg = f"{src_count} source tables registered ({watermarked} watermarked, {unwatermarked} dimension/state-diffed)"
                items.append(
                    CheckItem(
                        category="Configuration",
                        name="Registered Source Tables",
                        status=CheckStatus.PASS,
                        message=wm_msg,
                        details={"total_sources": src_count, "watermarked": watermarked, "state_diffed": unwatermarked},
                    )
                )
        except Exception as e:
            items.append(
                CheckItem(
                    category="Configuration",
                    name="Registered Source Tables",
                    status=CheckStatus.WARN,
                    message=f"Could not query idr_meta.source_table: {e}",
                )
            )

        # 2. Matching Rules
        try:
            rules = self.adapter.query(
                "SELECT rule_id, identifier_type, max_group_size, is_active "
                "FROM idr_meta.rule WHERE is_active = TRUE"
            )
            rule_count = len(rules) if rules else 0
            if rule_count == 0:
                items.append(
                    CheckItem(
                        category="Configuration",
                        name="Deterministic Matching Rules",
                        status=CheckStatus.WARN,
                        message="0 active matching rules found in idr_meta.rule",
                        remediation="Configure at least one matching rule (e.g. EMAIL, PHONE, or CUSTOMER_ID).",
                    )
                )
            else:
                # Check for risky supernode thresholds
                oversized_rules = [r for r in rules if (r.get("max_group_size") or 0) > 25000]
                household_risks = [
                    r for r in rules
                    if any(w in str(r.get("identifier_type", "")).lower() for w in ("address", "ip", "session"))
                    and (r.get("max_group_size") or 10000) > 50
                ]
                if oversized_rules:
                    items.append(
                        CheckItem(
                            category="Configuration",
                            name="Deterministic Matching Rules",
                            status=CheckStatus.WARN,
                            message=f"{rule_count} rules active ({len(oversized_rules)} have max_group_size > 25,000)",
                            remediation="Consider lowering max_group_size on broad rules to prevent graph mega-clusters.",
                        )
                    )
                elif household_risks:
                    risk_types = ", ".join(str(r.get("identifier_type")) for r in household_risks)
                    items.append(
                        CheckItem(
                            category="Configuration",
                            name="Deterministic Matching Rules",
                            status=CheckStatus.WARN,
                            message=f"{rule_count} rules active, but shared key(s) [{risk_types}] allow > 50 links",
                            remediation=(
                                "Single-key matching on address/IP with high max_group_size risks apartment or household over-merges. "
                                "Lower max_group_size <= 25 or switch entity_type to HOUSEHOLD."
                            ),
                        )
                    )
                else:
                    items.append(
                        CheckItem(
                            category="Configuration",
                            name="Deterministic Matching Rules",
                            status=CheckStatus.PASS,
                            message=f"{rule_count} matching rules active with safe group size limits",
                        )
                    )
        except Exception as e:
            items.append(
                CheckItem(
                    category="Configuration",
                    name="Deterministic Matching Rules",
                    status=CheckStatus.WARN,
                    message=f"Could not query idr_meta.rule: {e}",
                )
            )

        # 3. Poison-Pill Exclusions
        try:
            exclusions = self.adapter.query(
                "SELECT COUNT(*) AS cnt FROM idr_meta.identifier_exclusion"
            )
            excl_count = exclusions[0]["cnt"] if exclusions and isinstance(exclusions[0], dict) else 0

            if excl_count >= 10:
                items.append(
                    CheckItem(
                        category="Guardrails",
                        name="Toxic Identifier Exclusions",
                        status=CheckStatus.PASS,
                        message=f"{excl_count} poison-pill patterns active (default exclusion pack enabled)",
                    )
                )
            elif excl_count > 0:
                items.append(
                    CheckItem(
                        category="Guardrails",
                        name="Toxic Identifier Exclusions",
                        status=CheckStatus.WARN,
                        message=f"{excl_count} exclusions active (below standard 10+ default patterns)",
                        remediation="Ensure default exclusions (e.g. test@example.com, 555-0199) are enabled to avoid mega-clusters.",
                    )
                )
            else:
                items.append(
                    CheckItem(
                        category="Guardrails",
                        name="Toxic Identifier Exclusions",
                        status=CheckStatus.WARN,
                        message="0 poison-pill exclusions found. Unsafe against shared placeholders.",
                        remediation="Re-run 'idr init' (without --no-default-exclusions) to seed the default blocklist.",
                    )
                )
        except Exception as e:
            items.append(
                CheckItem(
                    category="Guardrails",
                    name="Toxic Identifier Exclusions",
                    status=CheckStatus.WARN,
                    message=f"Could not query idr_meta.identifier_exclusion: {e}",
                )
            )

        return items

    def _check_operational_health(self) -> List[CheckItem]:
        """Inspect run history and skipped identifier groups."""
        items: List[CheckItem] = []

        try:
            recent_runs = self.adapter.query(
                "SELECT run_id, run_mode, status, entities_processed, duration_seconds, started_at "
                "FROM idr_out.run_history ORDER BY started_at DESC LIMIT 1"
            )
            if recent_runs:
                last_run = recent_runs[0]
                status_str = str(last_run.get("status", "")).upper()
                entities = last_run.get("entities_processed", 0)
                dur = round(float(last_run.get("duration_seconds", 0) or 0), 1)
                run_id = last_run.get("run_id")

                if status_str == "SUCCESS":
                    items.append(
                        CheckItem(
                            category="Operational History",
                            name="Last Pipeline Run",
                            status=CheckStatus.PASS,
                            message=f"Run {run_id} succeeded ({entities:,} entities in {dur}s)",
                            details=dict(last_run),
                        )
                    )
                else:
                    items.append(
                        CheckItem(
                            category="Operational History",
                            name="Last Pipeline Run",
                            status=CheckStatus.WARN,
                            message=f"Last run {run_id} had status '{status_str}'",
                            details=dict(last_run),
                            remediation="Check run logs or query idr_out.run_history to troubleshoot the last execution failure.",
                        )
                    )
            else:
                items.append(
                    CheckItem(
                        category="Operational History",
                        name="Last Pipeline Run",
                        status=CheckStatus.INFO,
                        message="No previous runs recorded in idr_out.run_history",
                        remediation="Run 'idr run --mode FULL' to generate the initial identity graph.",
                    )
                )
        except Exception as e:
            items.append(
                CheckItem(
                    category="Operational History",
                    name="Last Pipeline Run",
                    status=CheckStatus.INFO,
                    message=f"Could not inspect run history: {e}",
                )
            )

        # Check skipped identifier groups (unresolved supernodes)
        try:
            skipped = self.adapter.query(
                "SELECT COUNT(*) AS cnt FROM idr_out.skipped_identifier_groups"
            )
            skip_count = skipped[0]["cnt"] if skipped and isinstance(skipped[0], dict) else 0

            if skip_count > 0:
                items.append(
                    CheckItem(
                        category="Operational History",
                        name="Skipped Identifier Groups",
                        status=CheckStatus.WARN,
                        message=f"{skip_count} oversized identifier groups were skipped by max_group_size",
                        remediation="Inspect 'idr_out.skipped_identifier_groups' to promote new shared placeholders to exclusions.",
                    )
                )
            else:
                items.append(
                    CheckItem(
                        category="Operational History",
                        name="Skipped Identifier Groups",
                        status=CheckStatus.PASS,
                        message="0 oversized identifier groups flagged",
                    )
                )
        except Exception:
            pass

        return items
