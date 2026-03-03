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

        # 0. Calculate cluster sizes for impacted clusters
        self.adapter.execute("""
            DROP TABLE IF EXISTS idr_work.cluster_sizes_updates;
            CREATE TABLE idr_work.cluster_sizes_updates AS
            SELECT
                resolved_id,
                COUNT(*) as cluster_size
            FROM idr_out.identity_resolved_membership_current
            WHERE resolved_id IN (SELECT resolved_id FROM idr_work.impacted_resolved_ids)
            GROUP BY resolved_id;
        """)

        # 1. Edge stats per cluster
        self.adapter.execute("""
            DROP TABLE IF EXISTS idr_work.cluster_edge_stats;
            CREATE TABLE idr_work.cluster_edge_stats AS
            SELECT
                m.resolved_id,
                COUNT(DISTINCT e.identifier_type) AS edge_diversity,
                COUNT(*) AS edge_count
            FROM idr_out.identity_resolved_membership_current m
            JOIN idr_out.identity_edges_current e
              ON e.left_entity_key = m.entity_key OR e.right_entity_key = m.entity_key
            WHERE m.resolved_id IN (SELECT resolved_id FROM idr_work.impacted_resolved_ids)
            GROUP BY m.resolved_id;
        """)

        # 2. Cluster density
        self.adapter.execute("""
            DROP TABLE IF EXISTS idr_work.cluster_density;
            CREATE TABLE idr_work.cluster_density AS
            SELECT
                c.resolved_id,
                c.cluster_size,
                COALESCE(es.edge_diversity, 0) AS edge_diversity,
                COALESCE(es.edge_count, 0) AS edge_count,
                CASE
                    WHEN c.cluster_size <= 1 THEN 1.0
                    ELSE LEAST(1.0, CAST(COALESCE(es.edge_count, 0) AS FLOAT) /
                         (CAST(c.cluster_size AS FLOAT) - 1))
                END AS match_density
            FROM idr_work.cluster_sizes_updates c
            LEFT JOIN idr_work.cluster_edge_stats es ON es.resolved_id = c.resolved_id;
        """)

        # 3. Max diversity (for normalization)
        self.adapter.execute("""
            DROP TABLE IF EXISTS idr_work.max_diversity;
            CREATE TABLE idr_work.max_diversity AS
            SELECT GREATEST(1, MAX(edge_diversity)) AS max_div FROM idr_work.cluster_density;
        """)

        # 4. Final Confidence Score & Reason
        self.adapter.execute("""
            DROP TABLE IF EXISTS idr_work.cluster_confidence;
            CREATE TABLE idr_work.cluster_confidence AS
            SELECT
                cd.resolved_id,
                cd.cluster_size,
                cd.edge_diversity,
                cd.match_density,
                CASE
                    WHEN cd.cluster_size = 1 THEN 1.0
                    ELSE ROUND(
                        0.50 * (CAST(cd.edge_diversity AS FLOAT) / md.max_div) +
                        0.35 * cd.match_density +
                        0.15 * 1.0,
                        3
                    )
                END AS confidence_score,
                CASE
                    WHEN cd.cluster_size = 1 THEN 'SINGLETON_NO_MATCH_REQUIRED'
                    WHEN cd.edge_diversity >= 3 AND cd.match_density >= 0.8 THEN
                        CAST(cd.edge_diversity AS VARCHAR) || ' identifier types, high density'
                    WHEN cd.edge_diversity >= 2 AND cd.match_density >= 0.5 THEN
                        CAST(cd.edge_diversity AS VARCHAR) || ' identifier types, moderate density'
                    WHEN cd.edge_diversity = 1 AND cd.match_density >= 0.8 THEN
                        'Single identifier type, high density'
                    WHEN cd.edge_diversity = 1 AND cd.match_density < 0.5 THEN
                        'Single identifier type, chain pattern'
                    ELSE
                        CAST(cd.edge_diversity AS VARCHAR) || ' identifier type(s), ' ||
                        CASE WHEN cd.match_density >= 0.5 THEN 'moderate' ELSE 'low' END || ' density'
                END AS primary_reason
            FROM idr_work.cluster_density cd
            CROSS JOIN idr_work.max_diversity md;
        """)

    def generate_output(self) -> None:
        """Update output tables with resolved identities."""
        current_ts = self._dialect["current_timestamp"]

        # 1. Identify impacted clusters for confidence scoring
        self.adapter.execute("""
            DROP TABLE IF EXISTS idr_work.impacted_resolved_ids;
            CREATE TABLE idr_work.impacted_resolved_ids AS
            SELECT DISTINCT label AS resolved_id
            FROM idr_work.lp_labels;
        """)

        # 2. Update membership table
        self.upsert(
            target_table="idr_out.identity_resolved_membership_current",
            source_query=f"""
                SELECT
                    l.entity_key,
                    l.label AS resolved_id,
                    {current_ts} AS updated_at,
                    '{self.run_id}' AS run_id,
                    fr.super_cluster_id
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

        # 4. Compute Confidence Scores
        self.compute_confidence_scores()

        # 5. Update clusters table (Now with confidence scores)
        self.upsert(
            target_table="idr_out.identity_clusters_current",
            source_query=f"""
                SELECT
                    l.label AS resolved_id,
                    COUNT(*) AS cluster_size,
                    {current_ts} AS updated_at,
                    '{self.run_id}' AS run_id,
                    fr.super_cluster_id,
                    COALESCE(cc.confidence_score, 1.0) as confidence_score,
                    COALESCE(cc.primary_reason, 'Single-node cluster') as primary_reason
                FROM idr_work.lp_labels l
                LEFT JOIN idr_work.fuzzy_results fr ON l.label = fr.resolved_id
                LEFT JOIN idr_work.cluster_confidence cc ON l.label = cc.resolved_id
                GROUP BY label, fr.super_cluster_id, cc.confidence_score, cc.primary_reason
            """,
            key_cols=["resolved_id"],
            update_cols=[
                "cluster_size",
                "updated_at",
                "run_id",
                "super_cluster_id",
                "confidence_score",
                "primary_reason",
            ],
            insert_cols=[
                "resolved_id",
                "cluster_size",
                "updated_at",
                "run_id",
                "super_cluster_id",
                "confidence_score",
                "primary_reason",
            ],
        )

        # 5b. Cleanup stale clusters (FULL run only)
        if self.config and self.config.run_mode == "FULL":
            self.adapter.execute(f"""
                DELETE FROM idr_out.identity_clusters_current
                WHERE run_id != '{self.run_id}'
            """)

        # 4. Update watermarks
        self.upsert(
            target_table="idr_meta.run_state",
            source_query=f"""
                SELECT
                    table_id,
                    MAX(watermark_value) AS last_watermark_value,
                    '{self.run_id}' AS last_run_id,
                    {current_ts} AS updated_at
                FROM idr_work.entities_delta
                GROUP BY table_id
            """,
            key_cols=["table_id"],
            update_cols=["last_watermark_value", "last_run_id", "updated_at"],
            insert_cols=["table_id", "last_watermark_value", "last_run_id", "updated_at"],
        )

        # 5. Build Unified Profiles (Golden Record)
        try:
            builder = ProfileBuilder(self.adapter)
            builder.build_profiles(self.run_id)
        except Exception as e:
            self.logger.warning(f"Profile generation failed: {e}", extra={"run_id": self.run_id})
            self._warnings.append(f"Profile generation failed: {str(e)}")

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
