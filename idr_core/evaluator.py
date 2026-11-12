"""
Evaluation harness & stratified pair sampler for SQL Identity Resolution.

Measures true identity resolution quality beyond internal graph consistency:
1. Ground-Truth Evaluation: Exact O(N) SQL contingency-table computation of
   Pairwise Precision, Recall, F1, Over-Merge Rate, and Split Rate.
2. Unsupervised Production Audit: Stratified pair sampling across
   OVERMERGE_RISK, CORROBORATED_MERGE, SINGLE_KEY_MERGE, and NEAR_MISS_SINGLETON
   buckets written directly to `idr_out.evaluation_sample` for human/LLM review.
"""

from typing import Any, Dict, List, Optional

from idr_core.adapters.base import get_dialect_config
from idr_core.config import validate_fqn, validate_identifier


class IDREvaluator:
    """Warehouse-native precision/recall evaluator and quality auditor."""

    def __init__(self, adapter: Any):
        self.adapter = adapter
        self.dialect_name = getattr(adapter, "dialect", "duckdb")
        self._dialect = get_dialect_config(self.dialect_name)

    def evaluate_ground_truth(
        self,
        ground_truth_table: str,
        entity_key_col: str = "entity_key",
        truth_id_col: str = "truth_id",
        top_n_errors: int = 10,
    ) -> Dict[str, Any]:
        """
        Compute exact Pairwise Precision, Recall, F1, Over-Merge & Split diagnostics
        in O(N) using SQL contingency tables without O(N^2) pair materialization.
        """
        safe_table = validate_fqn(ground_truth_table, "ground_truth_table")
        safe_ek = validate_identifier(entity_key_col, "entity_key_col")
        safe_tid = validate_identifier(truth_id_col, "truth_id_col")

        # 1. Contingency table pairwise counts
        metrics_sql = f"""
            WITH joined AS (
                SELECT
                    m.entity_key,
                    m.resolved_id,
                    CAST(gt.{safe_tid} AS {self._dialect["string_type"]}) AS truth_id
                FROM idr_out.identity_resolved_membership_current m
                JOIN {safe_table} gt
                  ON m.entity_key = CAST(gt.{safe_ek} AS {self._dialect["string_type"]})
                WHERE gt.{safe_tid} IS NOT NULL
            ),
            cell_counts AS (
                SELECT resolved_id, truth_id, COUNT(*) AS n
                FROM joined
                GROUP BY resolved_id, truth_id
            ),
            pred_counts AS (
                SELECT resolved_id, COUNT(*) AS n
                FROM joined
                GROUP BY resolved_id
            ),
            truth_counts AS (
                SELECT truth_id, COUNT(*) AS n
                FROM joined
                GROUP BY truth_id
            ),
            pair_totals AS (
                SELECT
                    (SELECT COUNT(*) FROM joined) AS evaluated_entities,
                    (SELECT COUNT(DISTINCT resolved_id) FROM joined) AS predicted_clusters,
                    (SELECT COUNT(DISTINCT truth_id) FROM joined) AS true_clusters,
                    COALESCE((SELECT SUM(n * (n - 1) / 2) FROM cell_counts), 0) AS tp_pairs,
                    COALESCE((SELECT SUM(n * (n - 1) / 2) FROM pred_counts), 0) AS pred_pairs,
                    COALESCE((SELECT SUM(n * (n - 1) / 2) FROM truth_counts), 0) AS truth_pairs
            )
            SELECT
                evaluated_entities,
                predicted_clusters,
                true_clusters,
                tp_pairs,
                (pred_pairs - tp_pairs) AS fp_pairs,
                (truth_pairs - tp_pairs) AS fn_pairs,
                pred_pairs,
                truth_pairs
            FROM pair_totals
        """
        rows = self.adapter.query(metrics_sql)
        if not rows:
            return {"status": "ERROR", "message": "No overlapping entities found with ground truth table."}

        row = rows[0]
        evaluated_entities = int(row.get("evaluated_entities") or 0)
        tp = int(row.get("tp_pairs") or 0)
        fp = int(row.get("fp_pairs") or 0)
        fn = int(row.get("fn_pairs") or 0)
        pred_pairs = int(row.get("pred_pairs") or 0)
        truth_pairs = int(row.get("truth_pairs") or 0)

        precision = (tp / pred_pairs) if pred_pairs > 0 else (1.0 if fp == 0 else 0.0)
        recall = (tp / truth_pairs) if truth_pairs > 0 else (1.0 if fn == 0 else 0.0)
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0

        # 2. Top Over-Merged Clusters (1 resolved_id spanning multiple truth_ids)
        overmerge_sql = f"""
            SELECT
                m.resolved_id,
                COUNT(*) AS cluster_entities,
                COUNT(DISTINCT gt.{safe_tid}) AS distinct_truth_ids
            FROM idr_out.identity_resolved_membership_current m
            JOIN {safe_table} gt
              ON m.entity_key = CAST(gt.{safe_ek} AS {self._dialect["string_type"]})
            WHERE gt.{safe_tid} IS NOT NULL
            GROUP BY m.resolved_id
            HAVING COUNT(DISTINCT gt.{safe_tid}) > 1
            ORDER BY distinct_truth_ids DESC, cluster_entities DESC
            LIMIT {int(top_n_errors)}
        """
        overmerged_clusters = [dict(r) for r in self.adapter.query(overmerge_sql)]

        # 3. Top Split Truth Entities (1 truth_id fractured across multiple resolved_ids)
        split_sql = f"""
            SELECT
                CAST(gt.{safe_tid} AS {self._dialect["string_type"]}) AS truth_id,
                COUNT(*) AS truth_entities,
                COUNT(DISTINCT m.resolved_id) AS split_into_clusters
            FROM idr_out.identity_resolved_membership_current m
            JOIN {safe_table} gt
              ON m.entity_key = CAST(gt.{safe_ek} AS {self._dialect["string_type"]})
            WHERE gt.{safe_tid} IS NOT NULL
            GROUP BY CAST(gt.{safe_tid} AS {self._dialect["string_type"]})
            HAVING COUNT(DISTINCT m.resolved_id) > 1
            ORDER BY split_into_clusters DESC, truth_entities DESC
            LIMIT {int(top_n_errors)}
        """
        split_entities = [dict(r) for r in self.adapter.query(split_sql)]

        return {
            "status": "SUCCESS",
            "mode": "GROUND_TRUTH",
            "evaluated_entities": evaluated_entities,
            "predicted_clusters": int(row.get("predicted_clusters") or 0),
            "true_clusters": int(row.get("true_clusters") or 0),
            "tp_pairs": tp,
            "fp_pairs": fp,
            "fn_pairs": fn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
            "top_overmerged_clusters": overmerged_clusters,
            "top_split_entities": split_entities,
        }

    def audit_quality_and_sample(
        self,
        sample_per_stratum: int = 20,
        persist_sample: bool = True,
    ) -> Dict[str, Any]:
        """
        Run unsupervised quality diagnostics on current IDR outputs and build
        a stratified audit sample in `idr_out.evaluation_sample`.
        """
        # 1. Cluster distribution & corroboration telemetry
        dist_sql = """
            SELECT
                COUNT(*) AS total_clusters,
                SUM(cluster_size) AS total_entities,
                SUM(CASE WHEN cluster_size = 1 THEN 1 ELSE 0 END) AS singleton_clusters,
                SUM(CASE WHEN cluster_size > 1 THEN 1 ELSE 0 END) AS multi_entity_clusters,
                SUM(CASE WHEN cluster_size > 1 AND COALESCE(edge_diversity, 0) >= 2 THEN 1 ELSE 0 END) AS corroborated_clusters,
                SUM(CASE WHEN cluster_size > 1 AND COALESCE(edge_diversity, 0) <= 1 THEN 1 ELSE 0 END) AS single_key_clusters,
                SUM(CASE WHEN cluster_size > 1 AND confidence_score < 0.50 THEN 1 ELSE 0 END) AS low_confidence_risk_clusters,
                MAX(cluster_size) AS max_cluster_size,
                ROUND(AVG(CASE WHEN cluster_size > 1 THEN confidence_score ELSE NULL END), 3) AS avg_multi_entity_confidence
            FROM idr_out.identity_clusters_current
        """
        dist_rows = self.adapter.query(dist_sql)
        dist = dict(dist_rows[0]) if dist_rows else {}

        total_clusters = int(dist.get("total_clusters") or 0)
        singletons = int(dist.get("singleton_clusters") or 0)
        singleton_rate = round(singletons / total_clusters, 4) if total_clusters > 0 else 0.0
        dist["singleton_rate"] = singleton_rate

        # 2. Edge evidence coverage check
        evidence_count = 0
        try:
            if self.adapter.table_exists("idr_out.edge_evidence"):
                evidence_count = int(self.adapter.query_one("SELECT COUNT(*) FROM idr_out.edge_evidence") or 0)
        except Exception:
            pass
        dist["edge_evidence_rows"] = evidence_count

        # 3. Extract stratified samples across key risk/quality strata
        stratum_limit = max(1, int(sample_per_stratum))
        samples: List[Dict[str, Any]] = []

        # Stratum A: Over-merge risk (low confidence or large single-key clusters)
        risk_sql = f"""
            SELECT
                'OVERMERGE_RISK' AS stratum,
                c.resolved_id AS resolved_id_a,
                c.resolved_id AS resolved_id_b,
                e.left_entity_key AS entity_key_a,
                e.right_entity_key AS entity_key_b,
                e.identifier_type,
                e.identifier_value_norm AS match_value,
                c.confidence_score,
                c.cluster_size
            FROM idr_out.identity_clusters_current c
            JOIN idr_out.identity_resolved_membership_current m ON m.resolved_id = c.resolved_id
            JOIN idr_out.identity_edges_current e
              ON e.left_entity_key = m.entity_key OR e.right_entity_key = m.entity_key
            WHERE c.cluster_size > 1 AND (c.confidence_score < 0.65 OR (c.cluster_size > 10 AND COALESCE(c.edge_diversity, 1) <= 1))
            ORDER BY c.confidence_score ASC, c.cluster_size DESC
            LIMIT {stratum_limit}
        """
        try:
            samples.extend([dict(r) for r in self.adapter.query(risk_sql)])
        except Exception:
            pass

        # Stratum B: Corroborated merges (edge_diversity >= 2)
        corr_sql = f"""
            SELECT
                'CORROBORATED_MERGE' AS stratum,
                c.resolved_id AS resolved_id_a,
                c.resolved_id AS resolved_id_b,
                e.left_entity_key AS entity_key_a,
                e.right_entity_key AS entity_key_b,
                e.identifier_type,
                e.identifier_value_norm AS match_value,
                c.confidence_score,
                c.cluster_size
            FROM idr_out.identity_clusters_current c
            JOIN idr_out.identity_resolved_membership_current m ON m.resolved_id = c.resolved_id
            JOIN idr_out.identity_edges_current e
              ON e.left_entity_key = m.entity_key
            WHERE c.cluster_size > 1 AND COALESCE(c.edge_diversity, 0) >= 2
            ORDER BY c.confidence_score DESC, c.cluster_size DESC
            LIMIT {stratum_limit}
        """
        try:
            samples.extend([dict(r) for r in self.adapter.query(corr_sql)])
        except Exception:
            pass

        # Stratum C: Single-key deterministic pairs (2 <= cluster_size <= 15, edge_diversity = 1)
        single_key_sql = f"""
            SELECT
                'SINGLE_KEY_MERGE' AS stratum,
                c.resolved_id AS resolved_id_a,
                c.resolved_id AS resolved_id_b,
                e.left_entity_key AS entity_key_a,
                e.right_entity_key AS entity_key_b,
                e.identifier_type,
                e.identifier_value_norm AS match_value,
                c.confidence_score,
                c.cluster_size
            FROM idr_out.identity_clusters_current c
            JOIN idr_out.identity_resolved_membership_current m ON m.resolved_id = c.resolved_id
            JOIN idr_out.identity_edges_current e
              ON e.left_entity_key = m.entity_key
            WHERE c.cluster_size BETWEEN 2 AND 15 AND COALESCE(c.edge_diversity, 1) = 1
            ORDER BY c.confidence_score ASC
            LIMIT {stratum_limit}
        """
        try:
            samples.extend([dict(r) for r in self.adapter.query(single_key_sql)])
        except Exception:
            pass

        # Stratum D: Near-Miss Singletons (potential false negatives if golden_profiles has last_name + zip_code/city)
        near_miss_count = 0
        try:
            if self.adapter.table_exists("idr_out.golden_profiles"):
                gp_cols = {c["name"].lower() for c in self.adapter.get_table_columns("idr_out.golden_profiles")}
                if {"last_name", "zip_code"}.issubset(gp_cols):
                    nm_sql = f"""
                        WITH singletons AS (
                            SELECT c.resolved_id, LOWER(TRIM(CAST(g.last_name AS {self._dialect["string_type"]}))) AS ln,
                                   LOWER(TRIM(CAST(g.zip_code AS {self._dialect["string_type"]}))) AS zc
                            FROM idr_out.identity_clusters_current c
                            JOIN idr_out.golden_profiles g ON c.resolved_id = g.resolved_id
                            WHERE c.cluster_size = 1
                              AND g.last_name IS NOT NULL AND g.zip_code IS NOT NULL
                        )
                        SELECT
                            'NEAR_MISS_SINGLETON' AS stratum,
                            a.resolved_id AS resolved_id_a,
                            b.resolved_id AS resolved_id_b,
                            a.resolved_id AS entity_key_a,
                            b.resolved_id AS entity_key_b,
                            'name_zip_candidate' AS identifier_type,
                            CONCAT(a.ln, '|', a.zc) AS match_value,
                            0.0 AS confidence_score,
                            1 AS cluster_size
                        FROM singletons a
                        JOIN singletons b ON a.ln = b.ln AND a.zc = b.zc AND a.resolved_id < b.resolved_id
                        LIMIT {stratum_limit}
                    """
                    nm_rows = [dict(r) for r in self.adapter.query(nm_sql)]
                    near_miss_count = len(nm_rows)
                    samples.extend(nm_rows)
        except Exception:
            pass

        # 4. Persist sample table if requested and table exists (batched multi-row insert)
        persisted_count = 0
        if persist_sample and samples and self.adapter.table_exists("idr_out.evaluation_sample"):
            try:
                self.adapter.execute("DELETE FROM idr_out.evaluation_sample WHERE 1=1")
                value_tuples = []
                for idx, s in enumerate(samples):
                    sample_id = f"smp_{idx + 1}_{s['stratum'].lower()}"
                    val_esc = str(s.get("match_value") or "").replace("'", "''")[:200]
                    ek_a = str(s.get("entity_key_a") or "").replace("'", "''")
                    ek_b = str(s.get("entity_key_b") or "").replace("'", "''")
                    r_a = str(s.get("resolved_id_a") or "").replace("'", "''")
                    r_b = str(s.get("resolved_id_b") or "").replace("'", "''")
                    id_t = str(s.get("identifier_type") or "").replace("'", "''")
                    conf = float(s.get("confidence_score") or 0.0)
                    c_sz = int(s.get("cluster_size") or 1)
                    value_tuples.append(
                        f"('{sample_id}', '{s['stratum']}', '{r_a}', '{r_b}', '{ek_a}', '{ek_b}', "
                        f"'{id_t}', '{val_esc}', {conf}, {c_sz}, NULL, {self._dialect['current_timestamp']})"
                    )
                if value_tuples:
                    self.adapter.execute(f"""
                        INSERT INTO idr_out.evaluation_sample
                        (sample_id, stratum, resolved_id_a, resolved_id_b, entity_key_a, entity_key_b,
                         identifier_type, match_value, confidence_score, cluster_size, human_label, sampled_at)
                        VALUES {', '.join(value_tuples)}
                    """)
                    persisted_count = len(value_tuples)
            except Exception:
                pass

        return {
            "status": "SUCCESS",
            "mode": "UNSUPERVISED_AUDIT",
            "distribution": dist,
            "near_miss_candidates_found": near_miss_count,
            "samples_extracted": len(samples),
            "samples_persisted": persisted_count,
            "sample_preview": samples[:15],
        }
