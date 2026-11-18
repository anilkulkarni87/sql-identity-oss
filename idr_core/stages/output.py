"""
Output generation stage for IDR pipeline.

Handles confidence scoring, output table generation (membership, edges, clusters,
watermarks, profiles), dry run analysis, and platform-specific upserts.
"""

from typing import Dict

from ..profile_builder import ProfileBuilder
from .base import BaseStage, StageContext

_CURRENT_OUTPUT_TABLES = (
    "identity_resolved_membership_current",
    "identity_edges_current",
    "identity_identifiers_current",
    "identity_clusters_current",
)


class OutputStage(BaseStage):
    """Generates output tables, confidence scores, and dry run analysis."""

    def __init__(self, ctx: StageContext, config=None):
        super().__init__(ctx)
        self.config = config

    def upsert(self, target_table, source_query, key_cols, update_cols, insert_cols):
        """Execute platform-specific upsert (MERGE or ON CONFLICT)."""
        if self.adapter.dialect in ("duckdb", "postgres"):
            # Use ON CONFLICT
            keys = ", ".join(key_cols)
            updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
            inserts = ", ".join(insert_cols)

            sql = f"""
                INSERT INTO {target_table} ({inserts})
                {source_query}
                ON CONFLICT ({keys}) DO UPDATE SET
                {updates}
            """
            self.adapter.execute(sql)

        else:
            # Use MERGE for BigQuery, Snowflake, Databricks
            join_cond = " AND ".join([f"T.{col} = S.{col}" for col in key_cols])
            updates = ", ".join([f"{col} = S.{col}" for col in update_cols])
            inserts = ", ".join(insert_cols)
            values = ", ".join([f"S.{col}" for col in insert_cols])

            sql = f"""
                MERGE INTO {target_table} T
                USING ({source_query}) S
                ON {join_cond}
                WHEN MATCHED THEN
                    UPDATE SET {updates}
                WHEN NOT MATCHED THEN
                    INSERT ({inserts})
                    VALUES ({values})
            """
            self.adapter.execute(sql)

    def compute_confidence_scores(self) -> None:
        """Compute confidence metrics for updated clusters."""
        float_type = self._dialect.get("float_type", "DOUBLE")
        str_type = self._dialect["string_type"]

        source_prefix_expr = (
            "SPLIT(entity_key, ':')[SAFE_OFFSET(0)]"
            if self.adapter.dialect == "bigquery"
            else "SPLIT_PART(entity_key, ':', 1)"
        )

        # 0. Calculate cluster sizes and source diversity for impacted clusters
        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.cluster_sizes_updates AS
            SELECT
                resolved_id,
                COUNT(*) AS cluster_size,
                COUNT(DISTINCT {source_prefix_expr}) AS source_diversity
            FROM idr_out.identity_resolved_membership_current
            WHERE resolved_id IN (SELECT resolved_id FROM idr_work.impacted_resolved_ids)
            GROUP BY resolved_id
        """)

        # 1. Edge stats per cluster with calibrated identifier fidelity & diversity
        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.cluster_edge_stats AS
            SELECT
                m.resolved_id,
                COUNT(DISTINCT e.identifier_type) AS edge_diversity,
                COUNT(*) AS edge_count,
                MAX(
                    CASE
                        WHEN LOWER(e.identifier_type) IN ('ssn', 'tax_id', 'national_id') THEN 0.85
                        WHEN LOWER(e.identifier_type) LIKE '%customer%'
                          OR LOWER(e.identifier_type) LIKE '%account%'
                          OR LOWER(e.identifier_type) LIKE '%loyalty%' THEN 0.80
                        WHEN LOWER(e.identifier_type) LIKE '%email%' THEN 0.76
                        WHEN LOWER(e.identifier_type) LIKE '%phone%' THEN 0.72
                        WHEN LOWER(e.identifier_type) LIKE '%device%'
                          OR LOWER(e.identifier_type) LIKE '%cookie%' THEN 0.62
                        WHEN LOWER(e.identifier_type) LIKE '%address%'
                          OR LOWER(e.identifier_type) LIKE '%ip%'
                          OR LOWER(e.identifier_type) LIKE '%session%' THEN 0.45
                        ELSE 0.55
                    END
                ) AS max_fidelity,
                MAX(
                    CASE
                        WHEN LOWER(e.identifier_type) LIKE '%customer%'
                          OR LOWER(e.identifier_type) LIKE '%account%'
                          OR LOWER(e.identifier_type) LIKE '%loyalty%'
                          OR LOWER(e.identifier_type) IN ('ssn', 'tax_id') THEN 1
                        ELSE 0
                    END
                ) AS has_strong_key,
                MAX(CASE WHEN LOWER(e.identifier_type) LIKE '%email%' THEN 1 ELSE 0 END) AS has_email,
                MAX(CASE WHEN LOWER(e.identifier_type) LIKE '%phone%' THEN 1 ELSE 0 END) AS has_phone
            FROM idr_out.identity_edges_current e
            JOIN idr_out.identity_resolved_membership_current m
              ON e.left_entity_key = m.entity_key
            JOIN idr_out.identity_resolved_membership_current m2
              ON e.right_entity_key = m2.entity_key
             AND m.resolved_id = m2.resolved_id
            WHERE m.resolved_id IN (SELECT resolved_id FROM idr_work.impacted_resolved_ids)
            GROUP BY m.resolved_id
        """)

        # 1b. Single-value fanout per cluster (detects mega-star hubs regardless of edge_diversity)
        self.adapter.execute("""
            CREATE OR REPLACE TABLE idr_work.cluster_value_fanout AS
            SELECT
                resolved_id,
                MAX(value_fanout) AS max_fanout
            FROM (
                SELECT
                    m.resolved_id,
                    e.identifier_type,
                    e.identifier_value_norm,
                    COUNT(DISTINCT e.left_entity_key) + 1 AS value_fanout
                FROM idr_out.identity_edges_current e
                JOIN idr_out.identity_resolved_membership_current m
                  ON e.left_entity_key = m.entity_key
                JOIN idr_out.identity_resolved_membership_current m2
                  ON e.right_entity_key = m2.entity_key
                 AND m.resolved_id = m2.resolved_id
                WHERE m.resolved_id IN (SELECT resolved_id FROM idr_work.impacted_resolved_ids)
                GROUP BY m.resolved_id, e.identifier_type, e.identifier_value_norm
            ) vf
            GROUP BY resolved_id
        """)

        # 2. Spanning Tree Connectivity, Source Diversity & Max Fanout
        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.cluster_density AS
            SELECT
                c.resolved_id,
                c.cluster_size,
                c.source_diversity,
                COALESCE(es.edge_diversity, 0) AS edge_diversity,
                COALESCE(es.edge_count, 0) AS edge_count,
                LEAST(c.cluster_size, COALESCE(vf.max_fanout, 1)) AS max_fanout,
                COALESCE(es.max_fidelity, 0.55) AS max_fidelity,
                COALESCE(es.has_strong_key, 0) AS has_strong_key,
                COALESCE(es.has_email, 0) AS has_email,
                COALESCE(es.has_phone, 0) AS has_phone,
                CASE
                    WHEN c.cluster_size <= 1 THEN 1.0
                    ELSE LEAST(
                        1.0,
                        CAST(COALESCE(es.edge_count, 0) AS {float_type}) /
                        GREATEST(1.0, CAST(c.cluster_size AS {float_type}) - 1.0)
                    )
                END AS connectivity_ratio
            FROM idr_work.cluster_sizes_updates c
            LEFT JOIN idr_work.cluster_edge_stats es ON es.resolved_id = c.resolved_id
            LEFT JOIN idr_work.cluster_value_fanout vf ON vf.resolved_id = c.resolved_id
        """)

        # 3. Continuous Confidence Ranking: Corroboration Base (Noisy-OR + Source Diversity) * Fan-out Damping (2/sqrt(max_fanout))
        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.cluster_confidence AS
            SELECT
                cd.resolved_id,
                cd.cluster_size,
                cd.edge_diversity,
                cd.edge_count,
                cd.max_fanout,
                cd.connectivity_ratio,
                CASE
                    WHEN cd.cluster_size = 1 THEN 1.0
                    ELSE ROUND(
                        GREATEST(
                            0.05,
                            LEAST(
                                0.995,
                                (
                                    CASE
                                        WHEN cd.edge_diversity >= 6 THEN 0.980 + 0.015 * LEAST(1.0, CAST(GREATEST(0, cd.source_diversity - 1) AS {float_type}) / 8.0)
                                        WHEN cd.edge_diversity = 5 THEN 0.960 + 0.015 * LEAST(1.0, CAST(GREATEST(0, cd.source_diversity - 1) AS {float_type}) / 8.0)
                                        WHEN cd.edge_diversity = 4 THEN 0.930 + 0.020 * LEAST(1.0, CAST(GREATEST(0, cd.source_diversity - 1) AS {float_type}) / 8.0)
                                        WHEN cd.edge_diversity = 3 THEN 0.895 + 0.025 * LEAST(1.0, CAST(GREATEST(0, cd.source_diversity - 1) AS {float_type}) / 6.0)
                                        WHEN cd.edge_diversity = 2 THEN 0.840 + 0.035 * LEAST(1.0, CAST(GREATEST(0, cd.source_diversity - 1) AS {float_type}) / 5.0)
                                        ELSE (cd.max_fidelity * cd.connectivity_ratio)
                                    END +
                                    CASE
                                        WHEN cd.edge_count > cd.cluster_size - 1 THEN 0.005
                                        ELSE 0.0
                                    END
                                ) * LEAST(
                                    1.0,
                                    2.0 / SQRT(GREATEST(4.0, CAST(cd.max_fanout AS {float_type})))
                                )
                            )
                        ),
                        3
                    )
                END AS confidence_score,
                CASE
                    WHEN cd.cluster_size = 1 THEN 'SINGLETON_NO_MATCH_REQUIRED'
                    WHEN cd.max_fanout >= 20 THEN
                        CONCAT('High fan-out over-merge risk (max_fanout=', CAST(cd.max_fanout AS {str_type}), ', ', CAST(cd.edge_diversity AS {str_type}), ' identifier types)')
                    WHEN cd.edge_diversity >= 2 THEN
                        CONCAT(CAST(cd.edge_diversity AS {str_type}), ' identifier types across ', CAST(cd.source_diversity AS {str_type}), ' sources (High Confidence Corroboration)')
                    WHEN cd.has_strong_key = 1 THEN
                        'Deterministic account/customer key match (Single Key)'
                    WHEN cd.has_email = 1 THEN
                        'Deterministic exact email match (Single Key)'
                    WHEN cd.has_phone = 1 THEN
                        'Deterministic exact phone match (Single Key)'
                    ELSE
                        CONCAT('Deterministic match via ', CAST(cd.edge_diversity AS {str_type}), ' identifier type(s)')
                END AS primary_reason
            FROM idr_work.cluster_density cd
        """)

    def _record_resolved_id_crosswalk(self) -> None:
        """Record retired/merged/split resolved_ids into idr_out.resolved_id_crosswalk before membership update."""
        try:
            if not self.adapter.table_exists("idr_out.resolved_id_crosswalk"):
                return
            if not self.adapter.table_exists("idr_out.identity_resolved_membership_current"):
                return
            prev_cnt = self.adapter.query_one(
                "SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current"
            ) or 0
            if prev_cnt == 0:
                return

            current_ts = self._dialect["current_timestamp"]
            source_query = f"""
                WITH prev_destinations AS (
                    SELECT
                        m.resolved_id AS previous_resolved_id,
                        COUNT(DISTINCT l.label) AS dest_count
                    FROM idr_out.identity_resolved_membership_current m
                    JOIN idr_work.lp_labels l ON m.entity_key = l.entity_key
                    GROUP BY m.resolved_id
                )
                SELECT
                    m.resolved_id AS previous_resolved_id,
                    l.label AS current_resolved_id,
                    CASE WHEN pd.dest_count > 1 THEN 'SPLIT' ELSE 'MERGED' END AS change_type,
                    COUNT(*) AS entities_moved,
                    '{self.run_id}' AS run_id,
                    {current_ts} AS updated_at
                FROM idr_out.identity_resolved_membership_current m
                JOIN idr_work.lp_labels l ON m.entity_key = l.entity_key
                JOIN prev_destinations pd ON m.resolved_id = pd.previous_resolved_id
                WHERE m.resolved_id != l.label
                GROUP BY m.resolved_id, l.label, pd.dest_count
            """
            self.upsert(
                target_table="idr_out.resolved_id_crosswalk",
                source_query=source_query,
                key_cols=["previous_resolved_id", "current_resolved_id"],
                update_cols=["change_type", "entities_moved", "run_id", "updated_at"],
                insert_cols=[
                    "previous_resolved_id",
                    "current_resolved_id",
                    "change_type",
                    "entities_moved",
                    "run_id",
                    "updated_at",
                ],
            )
        except Exception as e:
            self.logger.warning(f"Failed to record resolved_id_crosswalk: {e}")

    def _snapshot_current_outputs(self) -> Dict[str, bool]:
        """Snapshot mutable idr_out.*_current tables before Stage 8 writes begin."""
        snapshots: Dict[str, bool] = {}
        for tbl in _CURRENT_OUTPUT_TABLES:
            exists = bool(self.adapter.table_exists(f"idr_out.{tbl}"))
            if exists:
                self.adapter.execute(
                    f"CREATE OR REPLACE TABLE idr_work._backup_{tbl} AS SELECT * FROM idr_out.{tbl}"
                )
            snapshots[tbl] = exists
        return snapshots

    def _restore_current_outputs(self, snapshots: Dict[str, bool]) -> None:
        """Restore idr_out.*_current tables from pre-Stage-8 snapshots after a failure."""
        for tbl, existed in snapshots.items():
            if not existed:
                continue
            try:
                if self.adapter.table_exists(f"idr_work._backup_{tbl}"):
                    # Preserve table DDL constraints (e.g., DuckDB/Postgres PRIMARY KEY) while restoring rows.
                    self.adapter.execute(f"DELETE FROM idr_out.{tbl} WHERE 1=1")
                    self.adapter.execute(
                        f"INSERT INTO idr_out.{tbl} SELECT * FROM idr_work._backup_{tbl}"
                    )
            except Exception as restore_err:
                self.logger.error(
                    f"Failed to rollback idr_out.{tbl} from idr_work._backup_{tbl}: {restore_err}",
                    extra={"run_id": self.run_id},
                )

    def _cleanup_output_snapshots(self, snapshots: Dict[str, bool]) -> None:
        """Remove temporary Stage 8 backup tables from idr_work."""
        for tbl, existed in snapshots.items():
            if not existed:
                continue
            try:
                self.adapter.execute(f"DROP TABLE IF EXISTS idr_work._backup_{tbl}")
            except Exception as cleanup_err:
                self.logger.warning(
                    f"Failed to cleanup idr_work._backup_{tbl}: {cleanup_err}",
                    extra={"run_id": self.run_id},
                )

    def _enforce_blast_radius_gate(self, snapshots: Dict[str, bool]) -> None:
        """Abort Stage 8 commit if topological deltas exceed configured safety thresholds."""
        if not self.config:
            return
        max_moved_pct = getattr(self.config, "max_moved_entities_pct", None)
        max_cluster_delta_pct = getattr(self.config, "max_cluster_delta_pct", None)
        if max_moved_pct is None and max_cluster_delta_pct is None:
            return

        if max_moved_pct is not None and snapshots.get("identity_resolved_membership_current"):
            prev_total = int(
                self.adapter.query_one(
                    "SELECT COUNT(*) FROM idr_work._backup_identity_resolved_membership_current"
                )
                or 0
            )
            if prev_total > 0:
                moved_cnt = int(
                    self.adapter.query_one("""
                        SELECT COUNT(*)
                        FROM idr_work.lp_labels lp
                        JOIN idr_work._backup_identity_resolved_membership_current m
                          ON lp.entity_key = m.entity_key
                        WHERE lp.label != m.resolved_id
                    """)
                    or 0
                )
                moved_pct = (moved_cnt / prev_total) * 100.0
                if moved_pct > float(max_moved_pct):
                    raise RuntimeError(
                        f"Blast-radius circuit breaker triggered: moved_entities={moved_cnt:,}/{prev_total:,} "
                        f"({moved_pct:.2f}%) exceeds max_moved_entities_pct={float(max_moved_pct):.2f}%"
                    )

        if max_cluster_delta_pct is not None and snapshots.get("identity_resolved_membership_current"):
            run_mode = str(getattr(self.config, "run_mode", "FULL")).upper()
            if run_mode == "INCR":
                prev_clusters = int(
                    self.adapter.query_one("""
                        SELECT COUNT(DISTINCT m.resolved_id)
                        FROM idr_work._backup_identity_resolved_membership_current m
                        JOIN idr_work.lp_labels lp ON lp.entity_key = m.entity_key
                    """)
                    or 0
                )
            elif snapshots.get("identity_clusters_current"):
                prev_clusters = int(
                    self.adapter.query_one(
                        "SELECT COUNT(*) FROM idr_work._backup_identity_clusters_current"
                    )
                    or 0
                )
            else:
                prev_clusters = int(
                    self.adapter.query_one(
                        "SELECT COUNT(DISTINCT resolved_id) FROM idr_work._backup_identity_resolved_membership_current"
                    )
                    or 0
                )

            if prev_clusters > 0:
                proposed_clusters = int(
                    self.adapter.query_one("SELECT COUNT(DISTINCT label) FROM idr_work.lp_labels")
                    or 0
                )
                delta = proposed_clusters - prev_clusters
                cluster_delta_pct = (abs(delta) / prev_clusters) * 100.0
                if cluster_delta_pct > float(max_cluster_delta_pct):
                    raise RuntimeError(
                        f"Blast-radius circuit breaker triggered: cluster_delta={delta:+d} "
                        f"({cluster_delta_pct:.2f}%) exceeds max_cluster_delta_pct={float(max_cluster_delta_pct):.2f}%"
                    )

    def generate_output(self) -> int:
        """Update output tables with resolved identities with rollback on mid-stage failure."""
        current_ts = self._dialect["current_timestamp"]
        snapshots = self._snapshot_current_outputs()

        # Pre-write blast-radius veto runs BEFORE any current-output mutation and
        # bypasses _restore_current_outputs so vetoed runs perform zero current-table writes.
        try:
            self._enforce_blast_radius_gate(snapshots)
        except Exception:
            self._cleanup_output_snapshots(snapshots)
            raise

        try:

            # 1. Identify impacted clusters for confidence scoring
            self.adapter.execute("""
                CREATE OR REPLACE TABLE idr_work.impacted_resolved_ids AS
                SELECT DISTINCT label AS resolved_id
                FROM idr_work.lp_labels
            """)

            # 1b. Record resolved_id crosswalk for any clusters that merged or split
            self._record_resolved_id_crosswalk()

            # 2. Update membership table
            self.upsert(
                target_table="idr_out.identity_resolved_membership_current",
                source_query=f"""
                    SELECT
                        l.entity_key,
                        l.label AS resolved_id,
                        {current_ts} AS updated_at,
                        '{self.run_id}' AS run_id,
                        COALESCE(fr.super_cluster_id, l.label) AS super_cluster_id
                    FROM idr_work.lp_labels l
                    LEFT JOIN idr_work.fuzzy_results fr ON l.label = fr.resolved_id
                """,
                key_cols=["entity_key"],
                update_cols=["resolved_id", "updated_at", "run_id", "super_cluster_id"],
                insert_cols=["entity_key", "resolved_id", "updated_at", "run_id", "super_cluster_id"],
            )

            # 3. Update edges table (Insert / Upsert) BEFORE clusters to enable scoring.
            # In INCR mode, remove stale historical edges for re-extracted entities and
            # any edges invalidated by new exclusions before upserting freshly computed edges.
            if self.config and self.config.run_mode == "INCR":
                if self.adapter.table_exists("idr_work.reextracted_entities"):
                    self.adapter.execute("""
                        DELETE FROM idr_out.identity_edges_current
                        WHERE left_entity_key IN (SELECT entity_key FROM idr_work.reextracted_entities)
                           OR right_entity_key IN (SELECT entity_key FROM idr_work.reextracted_entities)
                    """)
                if self.adapter.table_exists("idr_meta.identifier_exclusion"):
                    excl_edge = self._exclusion_match_predicate(
                        "idr_out.identity_edges_current.identifier_type",
                        "idr_out.identity_edges_current.identifier_value_norm",
                    )
                    self.adapter.execute(f"""
                        DELETE FROM idr_out.identity_edges_current
                        WHERE {excl_edge}
                    """)

            edges_dedup_query = f"""
                SELECT
                    left_entity_key,
                    right_entity_key,
                    identifier_type,
                    MAX(identifier_value_norm) AS identifier_value_norm,
                    {current_ts} AS first_seen_ts,
                    '{self.run_id}' AS run_id
                FROM idr_work.edges_new
                GROUP BY left_entity_key, right_entity_key, identifier_type
            """
            if self.adapter.dialect in ("duckdb", "postgres"):
                self.adapter.execute(f"""
                    INSERT INTO idr_out.identity_edges_current
                    (left_entity_key, right_entity_key, identifier_type, identifier_value_norm, first_seen_ts, run_id)
                    {edges_dedup_query}
                    ON CONFLICT (left_entity_key, right_entity_key, identifier_type)
                    DO UPDATE SET last_seen_ts = EXCLUDED.first_seen_ts, run_id = EXCLUDED.run_id
                """)
            else:
                # MERGE for inserting new edges and updating last_seen_ts
                self.adapter.execute(f"""
                    MERGE INTO idr_out.identity_edges_current T
                    USING ({edges_dedup_query}) S
                    ON T.left_entity_key = S.left_entity_key
                       AND T.right_entity_key = S.right_entity_key
                       AND T.identifier_type = S.identifier_type
                    WHEN MATCHED THEN
                        UPDATE SET last_seen_ts = S.first_seen_ts, run_id = S.run_id
                    WHEN NOT MATCHED THEN
                        INSERT (left_entity_key, right_entity_key, identifier_type, identifier_value_norm, first_seen_ts, last_seen_ts, run_id)
                        VALUES (S.left_entity_key, S.right_entity_key, S.identifier_type, S.identifier_value_norm, S.first_seen_ts, S.first_seen_ts, S.run_id)
                """)

            # 3b. Cleanup stale edges (FULL run only)
            if self.config and self.config.run_mode == "FULL":
                self.adapter.execute(f"""
                    DELETE FROM idr_out.identity_edges_current
                    WHERE run_id != '{self.run_id}'
                """)

            # 3c. Update persistent identifiers store (if table exists)
            if self.adapter.table_exists("idr_out.identity_identifiers_current"):
                if self.config and self.config.run_mode == "INCR":
                    if self.adapter.table_exists("idr_work.reextracted_entities"):
                        self.adapter.execute("""
                            DELETE FROM idr_out.identity_identifiers_current
                            WHERE entity_key IN (SELECT entity_key FROM idr_work.reextracted_entities)
                        """)
                    if self.adapter.table_exists("idr_meta.identifier_exclusion"):
                        excl_id = self._exclusion_match_predicate(
                            "idr_out.identity_identifiers_current.identifier_type",
                            "idr_out.identity_identifiers_current.identifier_value_norm",
                        )
                        self.adapter.execute(f"""
                            DELETE FROM idr_out.identity_identifiers_current
                            WHERE {excl_id}
                        """)
                self.upsert(
                    target_table="idr_out.identity_identifiers_current",
                    source_query=f"""
                        SELECT
                            entity_key,
                            identifier_type,
                            MAX(identifier_value_norm) AS identifier_value_norm,
                            MAX(CAST(is_hashed AS INTEGER)) = 1 AS is_hashed,
                            {current_ts} AS updated_at,
                            '{self.run_id}' AS run_id
                        FROM idr_work.identifiers_all
                        GROUP BY entity_key, identifier_type
                    """,
                    key_cols=["entity_key", "identifier_type"],
                    update_cols=["identifier_value_norm", "is_hashed", "updated_at", "run_id"],
                    insert_cols=["entity_key", "identifier_type", "identifier_value_norm", "is_hashed", "updated_at", "run_id"],
                )
                if self.config and self.config.run_mode == "FULL":
                    self.adapter.execute(f"""
                        DELETE FROM idr_out.identity_identifiers_current
                        WHERE run_id != '{self.run_id}'
                    """)

            # 4. Compute Confidence Scores
            self.compute_confidence_scores()

            # 5. Update clusters table (Now with calibrated confidence, fanout, & corroboration signals)
            self.upsert(
                target_table="idr_out.identity_clusters_current",
                source_query=f"""
                    SELECT
                        l.label AS resolved_id,
                        COUNT(*) AS cluster_size,
                        {current_ts} AS updated_at,
                        '{self.run_id}' AS run_id,
                        COALESCE(fr.super_cluster_id, l.label) AS super_cluster_id,
                        COALESCE(cc.confidence_score, 1.0) AS confidence_score,
                        COALESCE(cc.edge_diversity, 0) AS edge_diversity,
                        COALESCE(cc.edge_count, 0) AS edge_count,
                        COALESCE(cc.max_fanout, 1) AS max_fanout,
                        COALESCE(cc.connectivity_ratio, 1.0) AS match_density,
                        COALESCE(cc.primary_reason, 'Single-node cluster') AS primary_reason
                    FROM idr_work.lp_labels l
                    LEFT JOIN idr_work.fuzzy_results fr ON l.label = fr.resolved_id
                    LEFT JOIN idr_work.cluster_confidence cc ON l.label = cc.resolved_id
                    GROUP BY
                        l.label,
                        COALESCE(fr.super_cluster_id, l.label),
                        cc.confidence_score,
                        cc.edge_diversity,
                        cc.edge_count,
                        cc.max_fanout,
                        cc.connectivity_ratio,
                        cc.primary_reason
                """,
                key_cols=["resolved_id"],
                update_cols=[
                    "cluster_size",
                    "updated_at",
                    "run_id",
                    "super_cluster_id",
                    "confidence_score",
                    "edge_diversity",
                    "edge_count",
                    "max_fanout",
                    "match_density",
                    "primary_reason",
                ],
                insert_cols=[
                    "resolved_id",
                    "cluster_size",
                    "updated_at",
                    "run_id",
                    "super_cluster_id",
                    "confidence_score",
                    "edge_diversity",
                    "edge_count",
                    "max_fanout",
                    "match_density",
                    "primary_reason",
                ],
            )

            # 5b. Cleanup stale clusters (FULL run or merged phantom clusters in INCR run)
            if self.config and self.config.run_mode == "FULL":
                self.adapter.execute(f"""
                    DELETE FROM idr_out.identity_clusters_current
                    WHERE run_id != '{self.run_id}'
                """)
            else:
                self.adapter.execute("""
                    DELETE FROM idr_out.identity_clusters_current
                    WHERE NOT EXISTS (
                        SELECT 1 FROM idr_out.identity_resolved_membership_current m
                        WHERE m.resolved_id = idr_out.identity_clusters_current.resolved_id
                    )
                """)

            # 4. Build Unified Profiles (Golden Record) BEFORE advancing watermarks so
            # a failed profile build does not advance watermarks and skip entities on retry.
            try:
                builder = ProfileBuilder(self.adapter)
                run_mode = self.config.run_mode if self.config else "FULL"
                builder.build_profiles(self.run_id, run_mode=run_mode)
            except Exception as e:
                self.logger.error(f"Profile generation failed: {e}", extra={"run_id": self.run_id})
                self._warnings.append(f"Profile generation failed: {str(e)}")
                raise RuntimeError(f"Golden profile generation failed: {e}") from e

            # 5. Update watermarks (ignore NULL watermark_value rows from candidate expansion / overrides)
            self.upsert(
                target_table="idr_meta.run_state",
                source_query=f"""
                    SELECT
                        table_id,
                        MAX(watermark_value) AS last_watermark_value,
                        '{self.run_id}' AS last_run_id,
                        {current_ts} AS updated_at
                    FROM idr_work.entities_delta
                    WHERE watermark_value IS NOT NULL
                    GROUP BY table_id
                """,
                key_cols=["table_id"],
                update_cols=["last_watermark_value", "last_run_id", "updated_at"],
                insert_cols=["table_id", "last_watermark_value", "last_run_id", "updated_at"],
            )

            # Return total golden profiles or clusters generated
            try:
                res = self.adapter.query("SELECT COUNT(*) as cnt FROM idr_out.golden_profiles")
                return int(res[0]["cnt"]) if res else 0
            except Exception:
                try:
                    res = self.adapter.query("SELECT COUNT(*) as cnt FROM idr_out.identity_clusters_current")
                    return int(res[0]["cnt"]) if res else 0
                except Exception:
                    return 0
        except Exception:
            self._restore_current_outputs(snapshots)
            raise
        finally:
            self._cleanup_output_snapshots(snapshots)

    def generate_dry_run_output(self) -> None:
        """Generate dry run analysis without updating production tables."""
        # Analyze what would change
        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_out.dry_run_results AS
            SELECT
                lp.entity_key,
                m.resolved_id AS current_resolved_id,
                lp.label AS new_resolved_id,
                CASE
                    WHEN m.resolved_id IS NULL THEN 'NEW'
                    WHEN m.resolved_id != lp.label THEN 'MOVED'
                    ELSE 'UNCHANGED'
                END AS change_type,
                '{self.run_id}' AS run_id
            FROM idr_work.lp_labels lp
            LEFT JOIN idr_out.identity_resolved_membership_current m
                ON lp.entity_key = m.entity_key
        """)

        # Summary statistics
        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_out.dry_run_summary AS
            SELECT
                '{self.run_id}' AS run_id,
                COUNT(CASE WHEN change_type = 'NEW' THEN 1 END) AS new_entities,
                COUNT(CASE WHEN change_type = 'MOVED' THEN 1 END) AS moved_entities,
                COUNT(CASE WHEN change_type = 'UNCHANGED' THEN 1 END) AS unchanged_entities,
                COUNT(*) AS total_entities,
                {self._dialect["current_timestamp"]} AS analyzed_at
            FROM idr_out.dry_run_results
            WHERE run_id = '{self.run_id}'
        """)

    def materialize_lens_output(self, lens_id: str) -> int:
        """Persist per-lens cluster memberships and cluster summaries."""
        safe_lens = self._validate_metadata_value(lens_id, "identifier", "lens_id")
        current_ts = self._dialect["current_timestamp"]

        self.upsert(
            target_table="idr_out.identity_lens_membership_current",
            source_query=f"""
                SELECT
                    '{safe_lens}' AS lens_id,
                    entity_key,
                    label AS resolved_id,
                    {current_ts} AS updated_at,
                    '{self.run_id}' AS run_id
                FROM idr_work.lp_labels_lens
            """,
            key_cols=["lens_id", "entity_key"],
            update_cols=["resolved_id", "updated_at", "run_id"],
            insert_cols=["lens_id", "entity_key", "resolved_id", "updated_at", "run_id"],
        )

        if self.config and self.config.run_mode == "FULL":
            self.adapter.execute(f"""
                DELETE FROM idr_out.identity_lens_membership_current
                WHERE lens_id = '{safe_lens}' AND run_id != '{self.run_id}'
            """)

        self.upsert(
            target_table="idr_out.identity_lens_clusters_current",
            source_query=f"""
                SELECT
                    '{safe_lens}' AS lens_id,
                    label AS resolved_id,
                    COUNT(*) AS cluster_size,
                    {current_ts} AS updated_at,
                    '{self.run_id}' AS run_id
                FROM idr_work.lp_labels_lens
                GROUP BY label
            """,
            key_cols=["lens_id", "resolved_id"],
            update_cols=["cluster_size", "updated_at", "run_id"],
            insert_cols=["lens_id", "resolved_id", "cluster_size", "updated_at", "run_id"],
        )

        if self.config and self.config.run_mode == "FULL":
            self.adapter.execute(f"""
                DELETE FROM idr_out.identity_lens_clusters_current
                WHERE lens_id = '{safe_lens}' AND run_id != '{self.run_id}'
            """)
        else:
            self.adapter.execute(f"""
                DELETE FROM idr_out.identity_lens_clusters_current
                WHERE lens_id = '{safe_lens}'
                  AND resolved_id NOT IN (
                      SELECT DISTINCT resolved_id
                      FROM idr_out.identity_lens_membership_current
                      WHERE lens_id = '{safe_lens}'
                  )
            """)

        cnt = self.adapter.query_one(
            f"SELECT COUNT(DISTINCT resolved_id) FROM idr_out.identity_lens_membership_current WHERE lens_id = '{safe_lens}'"
        )
        return int(cnt or 0)
