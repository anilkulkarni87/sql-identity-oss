"""
Output generation stage for IDR pipeline.

Handles confidence scoring, output table generation (membership, edges, clusters,
watermarks, profiles), dry run analysis, and platform-specific upserts.
"""

from ..profile_builder import ProfileBuilder
from .base import BaseStage, StageContext


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

        # 0. Calculate cluster sizes for impacted clusters
        self.adapter.execute("""
            CREATE OR REPLACE TABLE idr_work.cluster_sizes_updates AS
            SELECT
                resolved_id,
                COUNT(*) as cluster_size
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

        # 2. Spanning Tree Connectivity & Multi-Edge Redundancy
        # In identity graphs, clusters are hub-and-spoke / trees where E >= V - 1, NOT cliques!
        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.cluster_density AS
            SELECT
                c.resolved_id,
                c.cluster_size,
                COALESCE(es.edge_diversity, 0) AS edge_diversity,
                COALESCE(es.edge_count, 0) AS edge_count,
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
        """)

        # 3. Final Confidence Score & Reason (dialect-safe string and float types)
        # Calibrated to separate weak single-key fanout, single-key deterministic, and multi-key corroborated clusters
        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.cluster_confidence AS
            SELECT
                cd.resolved_id,
                cd.cluster_size,
                cd.edge_diversity,
                cd.edge_count,
                cd.connectivity_ratio,
                CASE
                    WHEN cd.cluster_size = 1 THEN 1.0
                    ELSE ROUND(
                        LEAST(
                            0.99,
                            GREATEST(
                                0.10,
                                -- Base fidelity scaled by spanning tree connectivity
                                (cd.max_fidelity * cd.connectivity_ratio) +
                                -- Corroboration bonus for multi-identifier confirmation
                                CASE
                                    WHEN cd.edge_diversity >= 4 THEN 0.18
                                    WHEN cd.edge_diversity = 3 THEN 0.14
                                    WHEN cd.edge_diversity = 2 THEN 0.09
                                    ELSE 0.0
                                END +
                                -- Edge redundancy bonus when clusters have extra confirming cycles beyond minimal tree
                                CASE
                                    WHEN cd.edge_count > cd.cluster_size - 1 THEN 0.02
                                    ELSE 0.0
                                END -
                                -- Runaway cluster & household over-merge fan-out penalties
                                CASE
                                    WHEN cd.cluster_size > 250 AND cd.edge_diversity <= 1 THEN 0.35
                                    WHEN cd.cluster_size > 15 AND cd.edge_diversity <= 1 THEN 0.15
                                    WHEN cd.cluster_size > 5 AND cd.max_fidelity < 0.60 AND cd.edge_diversity <= 1 THEN 0.10
                                    ELSE 0.0
                                END
                            )
                        ),
                        3
                    )
                END AS confidence_score,
                CASE
                    WHEN cd.cluster_size = 1 THEN 'SINGLETON_NO_MATCH_REQUIRED'
                    WHEN cd.cluster_size > 250 AND cd.edge_diversity <= 1 THEN
                        CONCAT('Over-merge risk: large cluster via single identifier (', CAST(cd.cluster_size AS {str_type}), ' entities)')
                    WHEN cd.edge_diversity >= 3 THEN
                        CONCAT(CAST(cd.edge_diversity AS {str_type}), ' identifier types (High Confidence Corroboration)')
                    WHEN cd.edge_diversity = 2 THEN
                        CONCAT(CAST(cd.edge_diversity AS {str_type}), ' identifier types (High Confidence Corroboration)')
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

    def generate_output(self) -> None:
        """Update output tables with resolved identities."""
        current_ts = self._dialect["current_timestamp"]

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

        # 3. Update edges table (Insert / Upsert) BEFORE clusters to enable scoring
        if self.adapter.dialect in ("duckdb", "postgres"):
            self.adapter.execute(f"""
                INSERT INTO idr_out.identity_edges_current
                (left_entity_key, right_entity_key, identifier_type, identifier_value_norm, first_seen_ts, run_id)
                SELECT
                    left_entity_key,
                    right_entity_key,
                    identifier_type,
                    identifier_value_norm,
                    {current_ts},
                    '{self.run_id}'
                FROM idr_work.edges_new
                ON CONFLICT (left_entity_key, right_entity_key, identifier_type)
                DO UPDATE SET last_seen_ts = EXCLUDED.first_seen_ts, run_id = EXCLUDED.run_id
            """)
        else:
            # MERGE for inserting new edges and updating last_seen_ts
            self.adapter.execute(f"""
                MERGE INTO idr_out.identity_edges_current T
                USING (
                    SELECT
                        left_entity_key,
                        right_entity_key,
                        identifier_type,
                        identifier_value_norm,
                        {current_ts} AS first_seen_ts,
                        '{self.run_id}' AS run_id
                    FROM idr_work.edges_new
                ) S
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
            self.upsert(
                target_table="idr_out.identity_identifiers_current",
                source_query=f"""
                    SELECT DISTINCT
                        entity_key,
                        identifier_type,
                        identifier_value_norm,
                        is_hashed,
                        {current_ts} AS updated_at,
                        '{self.run_id}' AS run_id
                    FROM idr_work.identifiers_all
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

        # 5. Update clusters table (Now with calibrated confidence & corroboration signals)
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

        # 4. Update watermarks (ignore NULL watermark_value rows from candidate expansion / overrides)
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

        # 5. Build Unified Profiles (Golden Record)
        try:
            builder = ProfileBuilder(self.adapter)
            run_mode = self.config.run_mode if self.config else "FULL"
            builder.build_profiles(self.run_id, run_mode=run_mode)
        except Exception as e:
            self.logger.error(f"Profile generation failed: {e}", extra={"run_id": self.run_id})
            self._warnings.append(f"Profile generation failed: {str(e)}")
            raise RuntimeError(f"Golden profile generation failed: {e}") from e

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
