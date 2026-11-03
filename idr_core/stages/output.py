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

        # 1. Edge stats per cluster (count each undirected edge once per cluster)
        self.adapter.execute("""
            CREATE OR REPLACE TABLE idr_work.cluster_edge_stats AS
            SELECT
                m.resolved_id,
                COUNT(DISTINCT e.identifier_type) AS edge_diversity,
                COUNT(*) AS edge_count
            FROM idr_out.identity_edges_current e
            JOIN idr_out.identity_resolved_membership_current m
              ON e.left_entity_key = m.entity_key
            JOIN idr_out.identity_resolved_membership_current m2
              ON e.right_entity_key = m2.entity_key
             AND m.resolved_id = m2.resolved_id
            WHERE m.resolved_id IN (SELECT resolved_id FROM idr_work.impacted_resolved_ids)
            GROUP BY m.resolved_id
        """)

        # 2. Cluster density (actual edges / max possible edges for N nodes)
        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.cluster_density AS
            SELECT
                c.resolved_id,
                c.cluster_size,
                COALESCE(es.edge_diversity, 0) AS edge_diversity,
                COALESCE(es.edge_count, 0) AS edge_count,
                CASE
                    WHEN c.cluster_size <= 2 THEN 1.0
                    ELSE LEAST(1.0, (2.0 * CAST(COALESCE(es.edge_count, 0) AS {float_type})) /
                         (CAST(c.cluster_size AS {float_type}) * (CAST(c.cluster_size AS {float_type}) - 1.0)))
                END AS match_density
            FROM idr_work.cluster_sizes_updates c
            LEFT JOIN idr_work.cluster_edge_stats es ON es.resolved_id = c.resolved_id
        """)

        # 3. Final Confidence Score & Reason (dialect-safe string and float types)
        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.cluster_confidence AS
            SELECT
                cd.resolved_id,
                cd.cluster_size,
                cd.edge_diversity,
                cd.match_density,
                CASE
                    WHEN cd.cluster_size = 1 THEN 1.0
                    ELSE ROUND(
                        0.50 * LEAST(1.0, CAST(cd.edge_diversity AS {float_type}) / 3.0) +
                        0.35 * cd.match_density +
                        0.15 * 1.0,
                        3
                    )
                END AS confidence_score,
                CASE
                    WHEN cd.cluster_size = 1 THEN 'SINGLETON_NO_MATCH_REQUIRED'
                    WHEN cd.edge_diversity >= 3 AND cd.match_density >= 0.8 THEN
                        CONCAT(CAST(cd.edge_diversity AS {str_type}), ' identifier types, high density')
                    WHEN cd.edge_diversity >= 2 AND cd.match_density >= 0.5 THEN
                        CONCAT(CAST(cd.edge_diversity AS {str_type}), ' identifier types, moderate density')
                    WHEN cd.edge_diversity = 1 AND cd.match_density >= 0.8 THEN
                        'Single identifier type, high density'
                    WHEN cd.edge_diversity = 1 AND cd.match_density < 0.5 THEN
                        'Single identifier type, chain pattern'
                    ELSE
                        CONCAT(
                            CAST(cd.edge_diversity AS {str_type}),
                            ' identifier type(s), ',
                            CASE WHEN cd.match_density >= 0.5 THEN 'moderate' ELSE 'low' END,
                            ' density'
                        )
                END AS primary_reason
            FROM idr_work.cluster_density cd
        """)

    def generate_output(self) -> None:
        """Update output tables with resolved identities."""
        current_ts = self._dialect["current_timestamp"]

        # 1. Identify impacted clusters for confidence scoring
        self.adapter.execute("""
            CREATE OR REPLACE TABLE idr_work.impacted_resolved_ids AS
            SELECT DISTINCT label AS resolved_id
            FROM idr_work.lp_labels
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

        # 5. Update clusters table (Now with confidence scores)
        self.upsert(
            target_table="idr_out.identity_clusters_current",
            source_query=f"""
                SELECT
                    l.label AS resolved_id,
                    COUNT(*) AS cluster_size,
                    {current_ts} AS updated_at,
                    '{self.run_id}' AS run_id,
                    COALESCE(fr.super_cluster_id, l.label) AS super_cluster_id,
                    COALESCE(cc.confidence_score, 1.0) as confidence_score,
                    COALESCE(cc.primary_reason, 'Single-node cluster') as primary_reason
                FROM idr_work.lp_labels l
                LEFT JOIN idr_work.fuzzy_results fr ON l.label = fr.resolved_id
                LEFT JOIN idr_work.cluster_confidence cc ON l.label = cc.resolved_id
                GROUP BY label, COALESCE(fr.super_cluster_id, l.label), cc.confidence_score, cc.primary_reason
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
