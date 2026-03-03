"""
Unified IDR Runner - Single implementation for all platforms.

This is the core orchestration logic that works with any platform adapter.
Platform differences are handled by the adapters, making this code
completely platform-agnostic.

Pipeline stages are implemented in idr_core.stages:
- PreflightStage: validation, concurrent run detection, schema upgrades
- ExtractionStage: entity, identifier, and attribute extraction
- GraphStage: edge building, label propagation, fuzzy matching
- OutputStage: confidence scoring, output generation, dry run analysis
"""

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from .adapters.base import IDRAdapter, get_dialect_config
from .logger_utils import get_logger
from .stages.base import StageContext
from .stages.extraction import ExtractionStage
from .stages.graph import GraphStage
from .stages.output import OutputStage
from .stages.preflight import PreflightStage


@dataclass
class RunConfig:
    """Configuration for an IDR run."""

    run_mode: str = "FULL"
    max_iters: int = 30
    dry_run: bool = False
    strict: bool = False

    def __post_init__(self):
        mode = str(self.run_mode or "").upper()
        if mode == "FULL":
            self.run_mode = "FULL"
        elif mode in ("INCR", "INCREMENTAL"):
            self.run_mode = "INCR"
        else:
            raise ValueError(f"Invalid run_mode: {self.run_mode}")
        if self.max_iters < 1:
            raise ValueError(f"max_iters must be >= 1, got {self.max_iters}")


@dataclass
class RunResult:
    """Result of an IDR run."""

    run_id: str
    status: str
    entities_processed: int = 0
    identifiers_extracted: int = 0
    edges_created: int = 0
    clusters_impacted: int = 0
    lp_iterations: int = 0
    duration_seconds: float = 0.0
    warnings: List[str] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class StageMetric:
    """Timing metric for a processing stage."""

    stage_name: str
    started_at: datetime
    ended_at: Optional[datetime] = None
    rows_affected: int = 0

    @property
    def duration_seconds(self) -> float:
        if self.ended_at:
            return (self.ended_at - self.started_at).total_seconds()
        return 0.0


class IDRRunner:
    """
    Platform-agnostic IDR runner.

    This class contains all the orchestration logic for identity resolution.
    It works with any platform by using the adapter interface.

    Example:
        from idr_core import IDRRunner, RunConfig
        from idr_core.adapters.duckdb import DuckDBAdapter

        adapter = DuckDBAdapter("my_db.duckdb")
        runner = IDRRunner(adapter)
        result = runner.run(RunConfig(run_mode="FULL"))
    """

    def __init__(self, adapter: IDRAdapter):
        """
        Initialize runner with a platform adapter.

        Args:
            adapter: Platform-specific adapter (DuckDB, Snowflake, etc.)
        """
        self.adapter = adapter
        self.run_id: Optional[str] = None
        self._stage_metrics: List[StageMetric] = []
        self._warnings: List[str] = []
        self._dialect = get_dialect_config(adapter.dialect)
        self._generate_evidence: bool = False  # Set per-run from config
        self.logger = get_logger("IDRRunner")

    def run(self, config: Optional[RunConfig] = None) -> RunResult:
        """
        Execute the full IDR pipeline.

        Args:
            config: Run configuration (defaults to FULL mode)

        Returns:
            RunResult with status and metrics
        """
        config = config or RunConfig()
        self.config = config
        prefix = "dry_" if config.dry_run else ""
        self.run_id = f"{prefix}run_{uuid.uuid4().hex[:12]}"
        start_time = datetime.utcnow()
        self._warnings = []
        self._stage_metrics = []

        self.logger.info(
            f"Starting IDR run ({config.run_mode})",
            extra={
                "run_id": self.run_id,
                "event": "run_start",
                "mode": config.run_mode,
                "dry_run": config.dry_run,
            },
        )

        # Build shared context for stage classes
        ctx = StageContext(
            adapter=self.adapter,
            dialect=self._dialect,
            run_id=self.run_id,
            logger=self.logger,
            warnings=self._warnings,
            generate_evidence=False,  # Updated after preflight
        )

        # Initialize stages
        preflight = PreflightStage(ctx)
        extraction = ExtractionStage(ctx)
        graph = GraphStage(ctx)
        output = OutputStage(ctx, config=config)

        # Check for optional evidence generation
        self._generate_evidence = preflight.load_evidence_flag()
        ctx.generate_evidence = self._generate_evidence

        try:
            # 1. Preflight validation (before recording run to avoid self-detection)
            self._start_stage("preflight")
            preflight.run()
            self._end_stage("preflight")

            # Record run start (after preflight passes)
            self._record_run_start(config)

            # 2. Entity extraction
            self._start_stage("entity_extraction")
            self.logger.info(
                "Extracting entities from sources...",
                extra={"run_id": self.run_id, "stage": "entity_extraction"},
            )
            entities = extraction.extract_entities(config.run_mode)
            self._end_stage("entity_extraction", entities)

            if entities == 0:
                return self._complete_run(
                    start_time, config, entities=0, edges=0, clusters=0, status="SUCCESS_NO_CHANGES"
                )

            # 3. Identifier extraction
            self._start_stage("identifier_extraction")
            self.logger.info(
                "Extracting identifiers...",
                extra={"run_id": self.run_id, "stage": "identifier_extraction"},
            )
            identifiers = extraction.extract_identifiers()
            self._end_stage("identifier_extraction", identifiers)

            # 3b. Attribute extraction
            self._start_stage("attribute_extraction")
            self.logger.info(
                "Extracting attributes for fuzzy matching...",
                extra={"run_id": self.run_id, "stage": "attribute_extraction"},
            )
            attributes_count = extraction.extract_attributes()
            self._end_stage("attribute_extraction", attributes_count)

            # 4. Edge building
            self._start_stage("edge_building")
            self.logger.info(
                "Building identity edges...",
                extra={"run_id": self.run_id, "stage": "edge_building"},
            )
            edges = graph.build_edges()
            self._end_stage("edge_building", edges)

            # 5. Label propagation (Deterministic)
            self._start_stage("label_propagation")
            self.logger.info(
                f"Resolving connected components (max_iters={config.max_iters})...",
                extra={
                    "run_id": self.run_id,
                    "stage": "label_propagation",
                    "max_iters": config.max_iters,
                },
            )
            iterations, clusters = graph.label_propagation(config.max_iters)
            self._end_stage("label_propagation", clusters)

            # 5b. Fuzzy Matching (Skip in strict/deterministic mode)
            if config.strict:
                self._warnings.append("Fuzzy matching skipped (--strict mode)")
                fuzzy_clusters = 0
                # Create empty fuzzy_results table to prevent LEFT JOIN failure in _generate_output
                self.adapter.execute(f"""
                    CREATE OR REPLACE TABLE idr_work.fuzzy_results (
                        resolved_id {self._dialect["string_type"]},
                        super_cluster_id {self._dialect["string_type"]}
                    )
                """)
            else:
                self._start_stage("fuzzy_matching")
                self.logger.info(
                    "Running fuzzy matching refinement...",
                    extra={"run_id": self.run_id, "stage": "fuzzy_matching"},
                )
                fuzzy_clusters = graph.run_fuzzy_matching(config.max_iters)
                self._end_stage("fuzzy_matching", fuzzy_clusters)

            # 6. Output generation
            if not config.dry_run:
                self._start_stage("output_generation")
                self.logger.info(
                    "Materializing final output tables...",
                    extra={"run_id": self.run_id, "stage": "output_generation"},
                )
                output.generate_output()
                self._end_stage("output_generation")
                status = "SUCCESS"
            else:
                self._start_stage("dry_run_analysis")
                self.logger.info(
                    "Generating dry run analysis...",
                    extra={"run_id": self.run_id, "stage": "dry_run_analysis"},
                )
                output.generate_dry_run_output()
                self._end_stage("dry_run_analysis")
                status = "DRY_RUN_COMPLETE"

            return self._complete_run(
                start_time,
                config,
                entities=entities,
                identifiers=identifiers,
                edges=edges,
                clusters=clusters,
                iterations=iterations,
                status=status,
            )

        except Exception as e:
            self.logger.error(
                f"IDR run failed: {e}",
                exc_info=True,
                extra={"run_id": self.run_id, "event": "run_failed"},
            )
            return self._complete_run(start_time, config, status="FAILED", error=str(e))

    # ===== Run Tracking =====

    def _start_stage(self, stage_name: str) -> None:
        """Record start of a processing stage."""
        self.logger.info(
            f"Starting stage: {stage_name}",
            extra={"run_id": self.run_id, "stage": stage_name, "event": "stage_start"},
        )
        self._stage_metrics.append(StageMetric(stage_name=stage_name, started_at=datetime.utcnow()))

    def _end_stage(self, stage_name: str, rows_affected: int = 0) -> None:
        """Record end of a processing stage."""
        for metric in reversed(self._stage_metrics):
            if metric.stage_name == stage_name and metric.ended_at is None:
                metric.ended_at = datetime.utcnow()
                metric.rows_affected = rows_affected

                self.logger.info(
                    f"Completed stage: {stage_name}",
                    extra={
                        "run_id": self.run_id,
                        "stage": stage_name,
                        "event": "stage_end",
                        "rows_affected": rows_affected,
                        "duration_seconds": metric.duration_seconds,
                    },
                )
                break

    def _record_run_start(self, config: RunConfig) -> None:
        """Record run start in history table."""
        # Compute hash of metadata configuration for reproducibility
        config_hash = self._compute_config_hash()

        # Note: We track dry run via run_id prefix (dry_run_*) instead of a column
        # This ensures compatibility with existing DDL schemas
        self.adapter.execute(f"""
            INSERT INTO idr_out.run_history
            (run_id, run_mode, status, started_at, source_tables_processed, created_at, config_hash)
            VALUES ('{self.run_id}', '{config.run_mode}', 'RUNNING',
                    {self._dialect["current_timestamp"]}, 0, {self._dialect["current_timestamp"]}, '{config_hash}')
        """)

    def _compute_config_hash(self) -> str:
        """Compute a hash of metadata tables for reproducibility tracking.

        Returns a SHA256 hash of the combined metadata configuration:
        - source_table definitions
        - rule definitions
        - identifier_mapping definitions
        """
        try:
            # Query key metadata tables
            sources = self.adapter.query("SELECT * FROM idr_meta.source_table ORDER BY table_id")
            rules = self.adapter.query("SELECT * FROM idr_meta.rule ORDER BY rule_id")
            mappings = self.adapter.query(
                "SELECT * FROM idr_meta.identifier_mapping ORDER BY table_id, identifier_type"
            )

            # Convert to serializable format
            config_data = {
                "sources": sources or [],
                "rules": rules or [],
                "mappings": mappings or [],
            }

            # Compute hash
            config_json = json.dumps(config_data, sort_keys=True, default=str)
            config_hash = hashlib.sha256(config_json.encode()).hexdigest()[:16]  # Short hash

            # Store snapshot (dedupe by hash using INSERT OR IGNORE / INSERT IGNORE)
            sources_json = json.dumps(sources or [], default=str).replace("'", "''")
            rules_json = json.dumps(rules or [], default=str).replace("'", "''")
            mappings_json = json.dumps(mappings or [], default=str).replace("'", "''")

            try:
                # Use INSERT OR IGNORE for DuckDB, INSERT IGNORE for others
                if self.adapter.dialect == "duckdb":
                    self.adapter.execute(f"""
                        INSERT OR IGNORE INTO idr_out.config_snapshot
                        (config_hash, sources_json, rules_json, mappings_json, created_at)
                        VALUES ('{config_hash}', '{sources_json}', '{rules_json}', '{mappings_json}',
                                {self._dialect["current_timestamp"]})
                    """)
                else:
                    # For BigQuery/Snowflake, check if exists first
                    exists = self.adapter.query_one(
                        f"SELECT 1 FROM idr_out.config_snapshot WHERE config_hash = '{config_hash}'"
                    )
                    if not exists:
                        self.adapter.execute(f"""
                            INSERT INTO idr_out.config_snapshot
                            (config_hash, sources_json, rules_json, mappings_json, created_at)
                            VALUES ('{config_hash}', '{sources_json}', '{rules_json}', '{mappings_json}',
                                    {self._dialect["current_timestamp"]})
                        """)
            except Exception:
                pass  # Snapshot storage is best-effort

            return config_hash
        except Exception:
            return "unknown"

    def _complete_run(
        self,
        start_time: datetime,
        config: RunConfig,
        entities: int = 0,
        identifiers: int = 0,
        edges: int = 0,
        clusters: int = 0,
        iterations: int = 0,
        status: str = "SUCCESS",
        error: Optional[str] = None,
    ) -> RunResult:
        """Record run completion and return result."""
        duration = (datetime.utcnow() - start_time).total_seconds()

        # Escape strings for SQL
        status_safe = status.replace("'", "''")
        # Replace single quotes with double quotes to avoid SQL syntax issues with escaping
        error_safe = (error or "").replace("'", '"').replace("\n", "\\n").replace("\r", "")

        self.logger.info(
            f"Run finished with status: {status}",
            extra={
                "run_id": self.run_id,
                "event": "run_complete",
                "status": status,
                "duration_seconds": duration,
                "entities": entities,
                "clusters": clusters,
                "error": error,
            },
        )

        # Update run history
        error_clause = f", error_message = '{error_safe}'" if error else ""

        self.adapter.execute(f"""
            UPDATE idr_out.run_history SET
                status = '{status_safe}',
                duration_seconds = {int(duration)},
                entities_processed = {entities},
                edges_created = {edges},
                clusters_impacted = {clusters},
                lp_iterations = {iterations}
                {error_clause}
            WHERE run_id = '{self.run_id}'
        """)

        # Record stage metrics
        for idx, metric in enumerate(self._stage_metrics):
            self.adapter.execute(f"""
                INSERT INTO idr_out.stage_metrics
                (run_id, stage_name, stage_order, started_at, ended_at, duration_seconds, rows_out)
                VALUES ('{self.run_id}', '{metric.stage_name}', {idx + 1},
                        TIMESTAMP '{metric.started_at.strftime("%Y-%m-%d %H:%M:%S")}',
                        TIMESTAMP '{(metric.ended_at or metric.started_at).strftime("%Y-%m-%d %H:%M:%S")}',
                        {int(metric.duration_seconds)}, {metric.rows_affected})
            """)

        return RunResult(
            run_id=self.run_id,
            status=status,
            entities_processed=entities,
            identifiers_extracted=identifiers,
            edges_created=edges,
            clusters_impacted=clusters,
            lp_iterations=iterations,
            duration_seconds=duration,
            warnings=self._warnings,
            error=error,
        )
