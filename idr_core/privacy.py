"""
GDPR / CCPA Right-to-be-Forgotten (RTBF) Cascading Identity Graph Purge.

Discovers all linked entity_keys and clusters for a given identifier, entity_key,
or resolved_id across the identity graph, purges all matching records from
idr_out.* and idr_work.* tables, and writes a zero-PII SHA-256 cryptographic
compliance receipt to idr_meta.privacy_audit_log.
"""

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from .adapters.base import IDRAdapter, get_dialect_config


class PrivacyManager:
    """Manages GDPR/CCPA Right-to-be-Forgotten (RTBF) graph discovery and cascading purge."""

    def __init__(self, adapter: IDRAdapter):
        self.adapter = adapter
        self._dialect = get_dialect_config(adapter.dialect)

    @staticmethod
    def _sha256(value: str) -> str:
        return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()

    @staticmethod
    def _escape_sql(val: str) -> str:
        return str(val).replace("'", "''")

    def discover_subject(
        self,
        identifier_value: Optional[str] = None,
        entity_key: Optional[str] = None,
        resolved_id: Optional[str] = None,
    ) -> Dict[str, List[str]]:
        """
        Discover all resolved_ids and linked entity_keys across the identity graph
        associated with the given identifier_value, entity_key, or resolved_id.
        """
        if not any([identifier_value, entity_key, resolved_id]):
            raise ValueError(
                "At least one of identifier_value, entity_key, or resolved_id must be provided."
            )

        seed_entities: Set[str] = set()
        seed_clusters: Set[str] = set()

        if entity_key:
            seed_entities.add(entity_key.strip())

        if resolved_id:
            seed_clusters.add(resolved_id.strip())

        if identifier_value:
            norm_val = self._escape_sql(identifier_value.strip().lower())
            raw_val = self._escape_sql(identifier_value.strip())

            # Search persistent identifiers store
            if self.adapter.table_exists("idr_out.identity_identifiers_current"):
                rows = self.adapter.query(f"""
                    SELECT DISTINCT entity_key
                    FROM idr_out.identity_identifiers_current
                    WHERE LOWER(identifier_value_norm) = '{norm_val}'
                       OR identifier_value_norm = '{raw_val}'
                """)
                for r in rows:
                    if r.get("entity_key"):
                        seed_entities.add(r["entity_key"])

            # Search working identifiers store
            if self.adapter.table_exists("idr_work.identifiers_all"):
                rows = self.adapter.query(f"""
                    SELECT DISTINCT entity_key
                    FROM idr_work.identifiers_all
                    WHERE LOWER(identifier_value_norm) = '{norm_val}'
                       OR identifier_value_norm = '{raw_val}'
                """)
                for r in rows:
                    if r.get("entity_key"):
                        seed_entities.add(r["entity_key"])

            # Search current edges
            if self.adapter.table_exists("idr_out.identity_edges_current"):
                rows = self.adapter.query(f"""
                    SELECT left_entity_key, right_entity_key
                    FROM idr_out.identity_edges_current
                    WHERE LOWER(identifier_value_norm) = '{norm_val}'
                       OR identifier_value_norm = '{raw_val}'
                """)
                for r in rows:
                    if r.get("left_entity_key"):
                        seed_entities.add(r["left_entity_key"])
                    if r.get("right_entity_key"):
                        seed_entities.add(r["right_entity_key"])

        # Expand seed_entities -> resolved_ids via membership
        if seed_entities and self.adapter.table_exists("idr_out.identity_resolved_membership_current"):
            in_list = ", ".join(f"'{self._escape_sql(k)}'" for k in sorted(seed_entities))
            rows = self.adapter.query(f"""
                SELECT DISTINCT resolved_id
                FROM idr_out.identity_resolved_membership_current
                WHERE entity_key IN ({in_list})
            """)
            for r in rows:
                if r.get("resolved_id"):
                    seed_clusters.add(r["resolved_id"])

        # Expand seed_clusters -> all member entity_keys in those clusters
        if seed_clusters and self.adapter.table_exists("idr_out.identity_resolved_membership_current"):
            cluster_list = ", ".join(f"'{self._escape_sql(c)}'" for c in sorted(seed_clusters))
            rows = self.adapter.query(f"""
                SELECT DISTINCT entity_key, resolved_id
                FROM idr_out.identity_resolved_membership_current
                WHERE resolved_id IN ({cluster_list})
                   OR super_cluster_id IN ({cluster_list})
            """)
            for r in rows:
                if r.get("entity_key"):
                    seed_entities.add(r["entity_key"])
                if r.get("resolved_id"):
                    seed_clusters.add(r["resolved_id"])

        return {
            "entity_keys": sorted(seed_entities),
            "resolved_ids": sorted(seed_clusters),
        }

    def forget(
        self,
        identifier_value: Optional[str] = None,
        entity_key: Optional[str] = None,
        resolved_id: Optional[str] = None,
        dry_run: bool = False,
        requested_by: str = "cli",
        reason: str = "GDPR_CCPA_RTBF",
    ) -> Dict[str, Any]:
        """
        Execute or preview a cascading Right-to-be-Forgotten (RTBF) purge across
        all IDR output and staging tables, logging a SHA-256 compliance receipt.
        """
        discovered = self.discover_subject(
            identifier_value=identifier_value,
            entity_key=entity_key,
            resolved_id=resolved_id,
        )
        entity_keys = discovered["entity_keys"]
        resolved_ids = discovered["resolved_ids"]

        if identifier_value:
            search_type = "IDENTIFIER"
            raw_target = identifier_value
        elif resolved_id:
            search_type = "RESOLVED_ID"
            raw_target = resolved_id
        else:
            search_type = "ENTITY_KEY"
            raw_target = entity_key or ""

        request_id = f"rtbf_{uuid.uuid4().hex[:12]}"
        target_hash = self._sha256(raw_target)
        resolved_id_hash = (
            self._sha256(",".join(resolved_ids)) if resolved_ids else self._sha256("none")
        )

        entities_purged = len(entity_keys)
        edges_purged = 0
        profiles_purged = 0
        tables_cleaned: List[str] = []

        if entity_keys or resolved_ids:
            ek_in = (
                ", ".join(f"'{self._escape_sql(k)}'" for k in entity_keys)
                if entity_keys
                else "''"
            )
            rid_in = (
                ", ".join(f"'{self._escape_sql(c)}'" for c in resolved_ids)
                if resolved_ids
                else "''"
            )

            # Count edges & profiles impacted
            if self.adapter.table_exists("idr_out.identity_edges_current") and entity_keys:
                edges_purged = (
                    self.adapter.query_one(f"""
                        SELECT COUNT(*) FROM idr_out.identity_edges_current
                        WHERE left_entity_key IN ({ek_in}) OR right_entity_key IN ({ek_in})
                    """)
                    or 0
                )

            if self.adapter.table_exists("idr_out.golden_profiles") and resolved_ids:
                profiles_purged = (
                    self.adapter.query_one(f"""
                        SELECT COUNT(*) FROM idr_out.golden_profiles
                        WHERE resolved_id IN ({rid_in})
                    """)
                    or 0
                )

            if not dry_run:
                # 1. Purge output tables
                delete_specs = [
                    (
                        "idr_out.identity_resolved_membership_current",
                        f"entity_key IN ({ek_in}) OR resolved_id IN ({rid_in})",
                    ),
                    (
                        "idr_out.identity_clusters_current",
                        f"resolved_id IN ({rid_in})",
                    ),
                    (
                        "idr_out.identity_edges_current",
                        f"left_entity_key IN ({ek_in}) OR right_entity_key IN ({ek_in})",
                    ),
                    (
                        "idr_out.identity_identifiers_current",
                        f"entity_key IN ({ek_in})",
                    ),
                    (
                        "idr_out.edge_evidence",
                        f"entity_key_a IN ({ek_in}) OR entity_key_b IN ({ek_in})",
                    ),
                    (
                        "idr_out.golden_profiles",
                        f"resolved_id IN ({rid_in})",
                    ),
                    (
                        "idr_out.resolved_id_crosswalk",
                        f"previous_resolved_id IN ({rid_in}) OR current_resolved_id IN ({rid_in})",
                    ),
                    # 2. Purge work/staging tables if present
                    (
                        "idr_work.entities_delta",
                        f"entity_key IN ({ek_in})",
                    ),
                    (
                        "idr_work.identifiers_all",
                        f"entity_key IN ({ek_in})",
                    ),
                    (
                        "idr_work.entity_attributes",
                        f"entity_key IN ({ek_in})",
                    ),
                    (
                        "idr_work.edges_new",
                        f"left_entity_key IN ({ek_in}) OR right_entity_key IN ({ek_in})",
                    ),
                    (
                        "idr_work.lp_edges",
                        f"src IN ({ek_in}) OR dst IN ({ek_in})",
                    ),
                    (
                        "idr_work.lp_labels",
                        f"entity_key IN ({ek_in}) OR label IN ({rid_in})",
                    ),
                ]

                for table_name, where_clause in delete_specs:
                    try:
                        if self.adapter.table_exists(table_name):
                            self.adapter.execute(f"DELETE FROM {table_name} WHERE {where_clause}")
                            tables_cleaned.append(table_name)
                    except Exception:
                        pass

        status = "DRY_RUN" if dry_run else ("COMPLETED" if (entity_keys or resolved_ids) else "NOT_FOUND")

        suppressed_count = 0
        if not dry_run and entity_keys:
            try:
                if not self.adapter.table_exists("idr_meta.privacy_suppression"):
                    from .schema_manager import SchemaManager

                    SchemaManager(self.adapter).initialize(reset=False)
                if self.adapter.table_exists("idr_meta.privacy_suppression"):
                    current_ts = self._dialect["current_timestamp"]
                    hashes = sorted({self._sha256(ek) for ek in entity_keys})
                    hash_in = ", ".join(f"'{h}'" for h in hashes)
                    existing_rows = self.adapter.query(
                        f"SELECT entity_key_hash FROM idr_meta.privacy_suppression "
                        f"WHERE entity_key_hash IN ({hash_in})"
                    )
                    existing_hashes = {
                        r["entity_key_hash"] for r in existing_rows if r.get("entity_key_hash")
                    }
                    new_hashes = [h for h in hashes if h not in existing_hashes]
                    for i in range(0, len(new_hashes), 100):
                        chunk = new_hashes[i : i + 100]
                        if self.adapter.dialect == "duckdb":
                            vals_sql = ", ".join(
                                f"('{h}', '{request_id}', {current_ts})" for h in chunk
                            )
                            self.adapter.execute(f"""
                                INSERT INTO idr_meta.privacy_suppression
                                    (entity_key_hash, request_id, suppressed_at)
                                VALUES {vals_sql}
                            """)
                        else:
                            select_sql = " UNION ALL ".join(
                                f"SELECT '{h}' AS entity_key_hash, '{request_id}' AS request_id, {current_ts} AS suppressed_at"
                                for h in chunk
                            )
                            self.adapter.execute(f"""
                                INSERT INTO idr_meta.privacy_suppression
                                    (entity_key_hash, request_id, suppressed_at)
                                {select_sql}
                            """)
                    suppressed_count = len(hashes)
            except Exception:
                pass

        # Write cryptographic audit log (zero cleartext PII)
        if not dry_run and self.adapter.table_exists("idr_meta.privacy_audit_log"):
            current_ts = self._dialect["current_timestamp"]
            req_by_safe = self._escape_sql(requested_by)
            reason_safe = self._escape_sql(reason)
            self.adapter.execute(f"""
                INSERT INTO idr_meta.privacy_audit_log (
                    request_id, search_type, target_hash, resolved_id_hash,
                    entities_purged, edges_purged, profiles_purged,
                    status, requested_by, reason, executed_at
                ) VALUES (
                    '{request_id}', '{search_type}', '{target_hash}', '{resolved_id_hash}',
                    {int(entities_purged)}, {int(edges_purged)}, {int(profiles_purged)},
                    '{status}', '{req_by_safe}', '{reason_safe}', {current_ts}
                )
            """)

        if not dry_run and status == "SUCCESS":
            from idr_core.webhooks import emit_webhook_event

            emit_webhook_event(
                "privacy.erasure_completed",
                {
                    "request_id": request_id,
                    "search_type": search_type,
                    "entities_purged": int(entities_purged),
                    "edges_purged": int(edges_purged),
                    "profiles_purged": int(profiles_purged),
                    "suppressed_entities": suppressed_count,
                    "tables_cleaned": tables_cleaned,
                },
            )

        return {
            "request_id": request_id,
            "status": status,
            "dry_run": dry_run,
            "search_type": search_type,
            "target_hash": target_hash,
            "resolved_id_hash": resolved_id_hash,
            "resolved_ids": resolved_ids,
            "entity_keys": entity_keys,
            "entities_purged": int(entities_purged),
            "edges_purged": int(edges_purged),
            "profiles_purged": int(profiles_purged),
            "suppressed_entities": suppressed_count,
            "tables_cleaned": tables_cleaned,
            "executed_at": datetime.now(timezone.utc).isoformat(),
        }

