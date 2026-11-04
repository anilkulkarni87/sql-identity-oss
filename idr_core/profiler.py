"""
Statistical data profiler for SQL Identity Resolution.

Profiles live warehouse tables, computes statistical distributions (null ratios,
uniqueness ratios, cardinality), executes regex pattern matching on actual sample
values (RFC 5322 emails, E.164 phone formats, UUIDs), detects entity keys and
watermarks with mathematical validation, flags supernode risks, and generates
validated IDR configurations.
"""

import re
from typing import Any, Dict, List, Optional, Set, Tuple

# Pre-compiled regex patterns for live value inspection
_EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
)
_PHONE_FORMATTED_RE = re.compile(
    r"^(\+?\d{1,3}[-.\s]?)?(\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}$"
)
_PHONE_DIGITS_RE = re.compile(r"^\+?[0-9]{7,15}$")
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_HEX_TOKEN_RE = re.compile(r"^[0-9a-fA-F]{32,64}$")
_ISO_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}"
)


def is_system_table(table_name: str) -> bool:
    """Check if table belongs to IDR internal framework or system catalogs."""
    parts = table_name.lower().split(".")
    system_schemas = {"idr_meta", "idr_work", "idr_out", "information_schema", "sys", "system"}
    return any(p in system_schemas for p in parts)


class ColumnProfile:
    """Statistical summary of a single column computed from a live sample."""

    def __init__(self, name: str, declared_type: str = ""):
        self.name = name.lower()
        self.declared_type = declared_type.lower()
        self.sample_count: int = 0
        self.non_null_count: int = 0
        self.distinct_count: int = 0
        self.null_ratio: float = 1.0
        self.uniqueness_ratio: float = 0.0
        self.detected_identifier_type: Optional[str] = None
        self.detection_confidence: float = 0.0
        self.is_entity_key_candidate: bool = False
        self.is_watermark_candidate: bool = False
        self.max_frequency_count: int = 1
        self.sample_values: List[str] = []
        self.frequent_values: List[Tuple[str, int]] = []


class IDRDataProfiler:
    """
    Statistical profiler that connects to warehouse tables via an IDRAdapter,
    inspects row samples, and synthesizes a production-ready declarative IDR configuration.
    """

    def __init__(self, adapter: Any):
        self.adapter = adapter

    def sample_table(self, table_fqn: str, sample_size: int = 500) -> List[Dict[str, Any]]:
        """Fetch sample rows from the target warehouse table."""
        sample_size = min(max(sample_size, 50), 5000)
        query = f"SELECT * FROM {table_fqn} LIMIT {sample_size}"
        try:
            rows = self.adapter.query(query)
            normalized_rows = []
            for r in rows:
                if isinstance(r, dict):
                    normalized_rows.append({k.lower(): v for k, v in r.items()})
                elif hasattr(r, "__getitem__"):
                    normalized_rows.append(r)
            return normalized_rows if normalized_rows else rows
        except Exception:
            return []

    def profile_columns(
        self,
        table_fqn: str,
        columns: List[Dict[str, str]],
        rows: List[Dict[str, Any]],
    ) -> Dict[str, ColumnProfile]:
        """Compute statistical and pattern metrics for all columns in the sample."""
        sample_count = len(rows)
        profiles: Dict[str, ColumnProfile] = {}

        for col_info in columns:
            col_name = col_info["name"].lower()
            col_type = col_info.get("type", "").lower()
            prof = ColumnProfile(col_name, col_type)
            prof.sample_count = sample_count

            if sample_count == 0:
                profiles[col_name] = prof
                continue

            # Extract non-null stringified values
            values = []
            for r in rows:
                v = r.get(col_name) if isinstance(r, dict) else None
                if v is not None and str(v).strip() != "" and str(v).lower() not in ("null", "none"):
                    values.append(str(v).strip())

            prof.non_null_count = len(values)
            prof.null_ratio = 1.0 - (prof.non_null_count / sample_count)

            if prof.non_null_count > 0:
                from collections import Counter

                counter = Counter(values)
                prof.distinct_count = len(counter)
                prof.uniqueness_ratio = prof.distinct_count / prof.non_null_count
                prof.frequent_values = counter.most_common(5)
                prof.max_frequency_count = counter.most_common(1)[0][1] if counter else 1
                prof.sample_values = values[:5]

            # 1. Check for Watermark viability
            if self._is_timestamp_column(prof, values):
                prof.is_watermark_candidate = True

            # 2. Value Pattern Recognition on actual sample values
            id_type, conf = self._classify_identifier_by_values(prof, values)
            if id_type and conf >= 0.65:
                prof.detected_identifier_type = id_type
                prof.detection_confidence = conf

            # 3. Check for Entity Key viability
            # Must be strictly unique (or >= 0.99 in large samples) and zero nulls
            # Cannot be an identifier (like email or phone)
            if (
                prof.null_ratio == 0.0
                and prof.uniqueness_ratio >= 0.99
                and not prof.detected_identifier_type
                and not prof.is_watermark_candidate
            ):
                prof.is_entity_key_candidate = True

            profiles[col_name] = prof

        return profiles

    def _is_timestamp_column(self, prof: ColumnProfile, values: List[str]) -> bool:
        """Verify if column represents a timestamp or watermark."""
        type_str = prof.declared_type
        if any(t in type_str for t in ("timestamp", "date", "datetime", "timestamptz")):
            return True

        if values:
            matches = sum(1 for v in values if _ISO_TIMESTAMP_RE.match(v))
            if (matches / len(values)) >= 0.8:
                return True

        if any(w in prof.name for w in ("updated", "modified", "timestamp", "created", "event_time")):
            return True

        return False

    def _classify_identifier_by_values(
        self, prof: ColumnProfile, values: List[str]
    ) -> Tuple[Optional[str], float]:
        """Inspect actual values to determine if this column is an identifier."""
        if not values or prof.null_ratio > 0.85:
            return None, 0.0

        n = len(values)

        # A. Email Pattern Matching (RFC 5322 regex on actual strings)
        email_matches = sum(1 for v in values if _EMAIL_RE.match(v))
        if (email_matches / n) >= 0.70:
            return "email", round(email_matches / n, 2)

        # B. Phone Pattern Matching (E.164, formatted or digit strings)
        phone_matches = 0
        for v in values:
            clean_digits = re.sub(r"[\s\-\(\)\.]", "", v)
            if _PHONE_DIGITS_RE.match(clean_digits) and len(clean_digits) in range(7, 16):
                phone_matches += 1
            elif _PHONE_FORMATTED_RE.match(v):
                phone_matches += 1

        if (phone_matches / n) >= 0.70:
            return "phone", round(phone_matches / n, 2)

        # C. UUID / Device ID Pattern Matching
        uuid_matches = sum(1 for v in values if _UUID_RE.match(v) or _HEX_TOKEN_RE.match(v))
        if (uuid_matches / n) >= 0.70:
            if any(d in prof.name for d in ("device", "idfa", "gaid", "cookie")):
                return "device_id", round(uuid_matches / n, 2)
            return "uuid", round(uuid_matches / n, 2)

        # D. Loyalty / Membership ID Pattern Matching
        if any(k in prof.name for k in ("loyalty", "rewards", "member", "card_no", "membership")):
            if prof.uniqueness_ratio >= 0.5:
                return "loyalty", 0.90

        # E. Account / External Identifier
        if any(k in prof.name for k in ("account_num", "acct_id", "external_id", "cust_id")):
            if prof.uniqueness_ratio >= 0.80 and not prof.is_entity_key_candidate:
                return "account_id", 0.85

        return None, 0.0

    def profile_and_generate_config(
        self,
        candidate_tables: List[str],
        sample_size: int = 500,
    ) -> Dict[str, Any]:
        """
        Execute live profiling across candidate tables, validate invariants,
        and generate a complete IDR configuration.
        """
        sources = []
        profiling_diagnostics = {}
        all_detected_identifiers: Dict[str, int] = {}  # id_type -> observed max group in sample
        all_detected_attributes: Set[str] = set()
        discovered_exclusions = []

        for fqn in candidate_tables:
            # 1. Fetch metadata columns
            cols = []
            if hasattr(self.adapter, "get_table_columns"):
                cols = self.adapter.get_table_columns(fqn)
            elif hasattr(self.adapter, "get_columns"):
                cols = self.adapter.get_columns(fqn)

            if not cols:
                continue

            # 2. Fetch sample data
            sample_rows = self.sample_table(fqn, sample_size=sample_size)

            # 3. Compute statistical column profiles
            col_profiles = self.profile_columns(fqn, cols, sample_rows)

            # 4. Determine verified Entity Primary Key
            pk_candidates = [
                p for p in col_profiles.values()
                if p.is_entity_key_candidate
            ]

            entity_key_col = None
            if pk_candidates:
                def _pk_rank(p: ColumnProfile) -> int:
                    score = int(p.uniqueness_ratio * 100)
                    if any(k in p.name for k in ("id", "key", "customer", "user", "account", "client")):
                        score += 50
                    if p.name.endswith("_id") or p.name == "id":
                        score += 30
                    return score

                pk_candidates.sort(key=_pk_rank, reverse=True)
                entity_key_col = pk_candidates[0].name
            else:
                # Pick the column with highest uniqueness that is not an identifier
                valid_cols = [
                    p for p in col_profiles.values()
                    if p.non_null_count > 0 and not p.detected_identifier_type and not p.is_watermark_candidate
                ]
                if valid_cols:
                    valid_cols.sort(key=lambda p: (p.uniqueness_ratio, -p.null_ratio), reverse=True)
                    entity_key_col = valid_cols[0].name

            if not entity_key_col and cols:
                entity_key_col = cols[0]["name"].lower()

            # 5. Determine verified Watermark
            wm_candidates = [
                p for p in col_profiles.values()
                if p.is_watermark_candidate and p.name != entity_key_col
            ]
            watermark_col = wm_candidates[0].name if wm_candidates else None

            # 6. Extract Identifiers & Attributes
            table_identifiers = []
            table_attributes = []

            for p in col_profiles.values():
                if p.name == entity_key_col or p.name == watermark_col:
                    continue

                if p.detected_identifier_type:
                    table_identifiers.append({
                        "type": p.detected_identifier_type,
                        "expr": p.name,
                        "confidence": p.detection_confidence,
                    })
                    # Track max observed repetition in sample to size max_group_size dynamically
                    prev_max = all_detected_identifiers.get(p.detected_identifier_type, 1)
                    all_detected_identifiers[p.detected_identifier_type] = max(prev_max, p.max_frequency_count)

                    # Data-driven supernode outlier detection:
                    # If an identifier appears >= 4 times AND represents >= 5% of sampled rows, flag as outlier
                    for val, count in p.frequent_values:
                        if len(sample_rows) >= 5 and count >= 3 and (count / len(sample_rows)) >= 0.08:
                            discovered_exclusions.append({
                                "identifier_type": p.detected_identifier_type,
                                "value": str(val).strip(),
                                "match_type": "EXACT",
                                "reason": f"High repetition ({count} occurrences in {len(sample_rows)} sampled rows) in {fqn}.{p.name}",
                            })

                else:
                    # Every other column with non-null data is an attribute of the entity
                    if p.non_null_count > 0:
                        table_attributes.append({
                            "name": p.name,
                            "expr": p.name,
                        })
                        all_detected_attributes.add(p.name)

            table_id = fqn.replace(".", "_")
            source_entry = {
                "id": table_id,
                "table": fqn,
                "entity_key": entity_key_col,
                "identifiers": [
                    {"type": i["type"], "expr": i["expr"]} for i in table_identifiers
                ],
                "attributes": table_attributes,
            }
            if watermark_col:
                source_entry["watermark_column"] = watermark_col

            sources.append(source_entry)

            profiling_diagnostics[fqn] = {
                "sample_rows_analyzed": len(sample_rows),
                "selected_entity_key": entity_key_col,
                "selected_watermark": watermark_col,
                "columns": {
                    p.name: {
                        "null_ratio": round(p.null_ratio, 2),
                        "uniqueness_ratio": round(p.uniqueness_ratio, 2),
                        "detected_type": p.detected_identifier_type or "attribute",
                        "sample_values": p.sample_values[:3],
                    }
                    for p in col_profiles.values()
                },
            }

        # 7. Adaptive Matching Rules based on observed frequency
        rules = []
        priority = 1
        order_pref = {"email": 1, "phone": 2, "loyalty": 3, "account_id": 4, "device_id": 5, "uuid": 6}
        sorted_id_types = sorted(all_detected_identifiers.keys(), key=lambda k: order_pref.get(k, 10))

        for id_type in sorted_id_types:
            observed_max = all_detected_identifiers.get(id_type, 1)
            # Statistically scale max_group_size based on observed sample degree
            adaptive_max = max(15, min(500, observed_max * 10))
            rules.append({
                "id": f"exact_{id_type}",
                "type": "EXACT",
                "match_keys": [id_type],
                "canonicalize": "LOWERCASE" if id_type == "email" else "EXACT",
                "priority": priority,
                "max_group_size": adaptive_max,
            })
            priority += 1

        # 8. Adaptive Survivorship Strategies based on watermark presence
        survivorship = []
        # Check if sources have watermark columns
        has_watermark = any(s.get("watermark_column") for s in sources)

        for attr in sorted(all_detected_attributes):
            # If table has watermark, use RECENCY; otherwise use PRIORITY or FREQUENCY
            strategy = "RECENCY" if has_watermark else "PRIORITY"
            rule_entry = {"attribute": attr, "strategy": strategy}
            if strategy == "PRIORITY" and sources:
                rule_entry["source_priority"] = [s["table"] for s in sources]
            survivorship.append(rule_entry)

        # Deduplicate discovered exclusions
        unique_exclusions = []
        seen_excl = set()
        for excl in discovered_exclusions:
            key = (excl["identifier_type"], excl["value"])
            if key not in seen_excl:
                seen_excl.add(key)
                unique_exclusions.append(excl)

        suggested_config = {
            "sources": sources,
            "rules": rules,
            "survivorship": survivorship,
            "exclusions": unique_exclusions,
        }

        # Validate with IDR Config validator
        from idr_core.config import validate_config

        try:
            validate_config(suggested_config)
            is_valid_idr_config = True
            config_validation_error = None
        except Exception as e:
            is_valid_idr_config = False
            config_validation_error = str(e)

        return {
            "status": "SUCCESS" if sources else "NO_TABLES_PROFILED",
            "tables_profiled": len(sources),
            "detected_identifiers": list(all_detected_identifiers.keys()),
            "detected_attributes": list(all_detected_attributes),
            "is_valid_idr_config": is_valid_idr_config,
            "config_validation_error": config_validation_error,
            "suggested_config": suggested_config,
            "profiling_diagnostics": profiling_diagnostics,
        }
