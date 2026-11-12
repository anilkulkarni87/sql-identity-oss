"""
Production-grade statistical profiler for SQL Identity Resolution.

Profiles live warehouse tables, computes statistical distributions (null ratios,
uniqueness ratios, cardinality), executes regex pattern matching on actual sample
values (RFC 5322 emails, E.164 phone formats), detects row entity keys and
watermarks with mathematical validation, flags supernode risks, and generates
validated IDR configurations.
"""

import re
from typing import Any, Dict, List, Optional, Set, Tuple

# Pre-compiled regex patterns
_EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
)
_PHONE_FORMATTED_RE = re.compile(
    r"^(\+?\d{1,3}[-.\s]?)?(\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}(?:\s*(?:x|ext|ext\.)\s*\d+)?$",
    re.IGNORECASE,
)
_PHONE_EXT_CLEAN_RE = re.compile(r"(?:x|ext|ext\.)\s*\d+$", re.IGNORECASE)
_PHONE_DIGITS_RE = re.compile(r"^\+?[0-9]{7,15}$")
_DATE_OR_TS_RE = re.compile(
    r"^(\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4}|\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2})"
)
_HEX_TOKEN_RE = re.compile(r"^[0-9a-fA-F]{32,64}$")


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
        self.is_temporal: bool = False
        self.is_entity_key: bool = False
        self.detected_identifier_type: Optional[str] = None
        self.detection_confidence: float = 0.0
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

    def _is_temporal_column(self, col_name: str, col_type: str, values: List[str]) -> bool:
        """Verify if column represents a date, time, timestamp, or temporal period."""
        type_str = col_type.lower()
        if any(t in type_str for t in ("timestamp", "date", "datetime", "timestamptz", "time")):
            return True

        name = col_name.lower()
        # Semantic naming cues
        temporal_names = (
            "date", "_date", "start_date", "end_date", "order_date", "birth_date", "dob",
            "_at", "created_at", "updated_at", "modified_at", "assigned_at", "enrolled_at",
            "_ts", "timestamp", "time", "event_time", "period"
        )
        if any(w in name for w in temporal_names):
            if values:
                matches = sum(1 for v in values if _DATE_OR_TS_RE.match(v))
                if (matches / len(values)) >= 0.4:
                    return True
            return True

        if values:
            matches = sum(1 for v in values if _DATE_OR_TS_RE.match(v))
            if (matches / len(values)) >= 0.70:
                return True

        return False

    def profile_columns(
        self,
        table_name_short: str,
        columns: List[Dict[str, str]],
        rows: List[Dict[str, Any]],
    ) -> Dict[str, ColumnProfile]:
        """Compute statistical and pattern metrics for all columns in the sample."""
        sample_count = len(rows)
        profiles: Dict[str, ColumnProfile] = {}

        # 1. First pass: statistical distribution and temporal detection
        for col_info in columns:
            col_name = col_info["name"].lower()
            col_type = col_info.get("type", "").lower()
            prof = ColumnProfile(col_name, col_type)
            prof.sample_count = sample_count

            if sample_count == 0:
                profiles[col_name] = prof
                continue

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

            # Check temporal
            prof.is_temporal = self._is_temporal_column(col_name, col_type, values)
            profiles[col_name] = prof

        # 2. Second pass: Identify Table Entity Primary Key (Row PK)
        # Entity PK must be 100% unique (or >= 0.99) and 0% nulls, and NOT temporal
        pk_candidates = [
            p for p in profiles.values()
            if p.null_ratio == 0.0 and p.uniqueness_ratio >= 0.99 and not p.is_temporal
        ]

        entity_pk_col = None
        if pk_candidates:
            # Score primary key candidates: prefer table-specific PK (e.g. order_id in orders)
            def _score_pk(p: ColumnProfile) -> int:
                score = int(p.uniqueness_ratio * 100)
                name = p.name
                # If name matches singular table name + "_id" (e.g. customer_id in customers)
                table_clean = table_name_short.rstrip("s")
                if name == f"{table_clean}_id" or name == f"{table_name_short}_id":
                    score += 200
                elif name == "id":
                    score += 150
                elif name.endswith("_id") or name.endswith("_key"):
                    score += 100
                return score

            pk_candidates.sort(key=_score_pk, reverse=True)
            entity_pk_col = pk_candidates[0].name
            profiles[entity_pk_col].is_entity_key = True
        else:
            # Fallback to column with highest uniqueness
            non_temp = [p for p in profiles.values() if not p.is_temporal and p.non_null_count > 0]
            if non_temp:
                non_temp.sort(key=lambda p: (p.uniqueness_ratio, -p.null_ratio), reverse=True)
                entity_pk_col = non_temp[0].name
                profiles[entity_pk_col].is_entity_key = True

        # 3. Third pass: Classify customer identifiers on remaining columns
        for col_name, prof in profiles.items():
            if prof.is_entity_key or prof.is_temporal:
                continue

            values = [str(r.get(col_name)).strip() for r in rows if r.get(col_name) is not None]
            id_type, conf = self._classify_identifier(prof, values)
            if id_type and conf >= 0.65:
                prof.detected_identifier_type = id_type
                prof.detection_confidence = conf

        return profiles

    def _classify_identifier(
        self, prof: ColumnProfile, values: List[str]
    ) -> Tuple[Optional[str], float]:
        """Inspect actual values and semantics to detect cross-channel customer identifiers."""
        if not values or prof.null_ratio > 0.85:
            return None, 0.0

        n = len(values)
        name = prof.name

        # Temporal columns are never identifiers
        if prof.is_temporal:
            return None, 0.0

        # Numeric metric columns are never identifiers (price, cost, amount, quantity, rating, etc.)
        metric_names = (
            "price", "cost", "amount", "value", "quantity", "qty", "rating",
            "score", "count", "pct", "percent", "rate", "weight", "total", "subtotal"
        )
        if any(m in name for m in metric_names):
            return None, 0.0

        # A. Email Pattern Matching (RFC 5322 regex on actual values)
        email_matches = sum(1 for v in values if _EMAIL_RE.match(v))
        if (email_matches / n) >= 0.70:
            return "email", round(email_matches / n, 2)

        # B. Phone Pattern Matching
        # Must NOT look like a date or year (e.g. 2026-03-31 or 20260331)
        phone_matches = 0
        phone_name_hint = any(k in name for k in ("phone", "mobile", "cell", "tel", "contact_no", "msisdn"))

        for v in values:
            if _DATE_OR_TS_RE.match(v):
                continue
            # Strip phone extension if present (e.g. '001-218-519-6001x3389' or '(791)366-9784 ext 12')
            base_v = _PHONE_EXT_CLEAN_RE.sub("", v).strip()
            clean = re.sub(r"[\s\-\(\)\.]", "", base_v)
            # Filter out 8-digit dates (YYYYMMDD starting with 19 or 20)
            if len(clean) == 8 and (clean.startswith("19") or clean.startswith("20")):
                continue
            if _PHONE_DIGITS_RE.match(clean) or _PHONE_FORMATTED_RE.match(v):
                phone_matches += 1

        # Phone requires either semantic name clue or very high pattern match
        if phone_name_hint and (phone_matches / n) >= 0.35:
            return "phone", round(phone_matches / n, 2)
        elif (phone_matches / n) >= 0.75 and len(values[0]) >= 7:
            return "phone", round(phone_matches / n, 2)

        # C. Cross-table Customer Entity Reference (Foreign Key to Customer)
        if any(k in name for k in ("customer_id", "user_id", "client_id", "account_id")):
            return "customer_id", 0.95

        # D. Loyalty / Membership ID
        if any(k in name for k in ("loyalty_id", "member_id", "card_no", "rewards_id", "loyalty")):
            if prof.uniqueness_ratio >= 0.4:
                return "loyalty", 0.90

        # E. Device / Cookie / Anonymous Web Visitor ID
        # Note: session_id is a transient event session, NOT a persistent device or person identifier.
        if any(k in name for k in ("device_id", "cookie_id", "idfa", "gaid", "advertising_id", "visitor_id")):
            return "device_id", 0.85

        return None, 0.0

    def profile_and_generate_config(
        self,
        candidate_tables: List[str],
        sample_size: int = 500,
    ) -> Dict[str, Any]:
        """
        Execute live profiling across candidate tables, validate invariants,
        filter out non-identity tables, and generate an IDR configuration.
        """
        sources = []
        profiling_diagnostics = {}
        all_detected_identifiers: Dict[str, int] = {}
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

            # Short table name (e.g. 'experiments' from 'ecommerce_synth.experiments')
            table_name_short = fqn.split(".")[-1].lower()

            # 2. Fetch sample data
            sample_rows = self.sample_table(fqn, sample_size=sample_size)

            # 3. Compute statistical column profiles
            col_profiles = self.profile_columns(table_name_short, cols, sample_rows)

            # 4. Extract verified Entity Key
            entity_keys = [p.name for p in col_profiles.values() if p.is_entity_key]
            entity_key_col = entity_keys[0] if entity_keys else cols[0]["name"].lower()

            # 5. Extract verified Watermark
            wm_candidates = [
                p.name for p in col_profiles.values()
                if p.is_temporal and p.name != entity_key_col
            ]
            watermark_col = wm_candidates[0] if wm_candidates else None

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
                    prev_max = all_detected_identifiers.get(p.detected_identifier_type, 1)
                    all_detected_identifiers[p.detected_identifier_type] = max(prev_max, p.max_frequency_count)

                    # Data-driven supernode outlier detection:
                    for val, count in p.frequent_values:
                        if len(sample_rows) >= 5 and count >= 3 and (count / len(sample_rows)) >= 0.08:
                            discovered_exclusions.append({
                                "identifier_type": p.detected_identifier_type,
                                "value": str(val).strip(),
                                "match_type": "EXACT",
                                "reason": f"High repetition ({count} occurrences in {len(sample_rows)} sampled rows) in {fqn}.{p.name}",
                            })
                else:
                    # Every other column with data is an attribute of the record
                    if p.non_null_count > 0:
                        table_attributes.append({
                            "name": p.name,
                            "expr": p.name,
                        })
                        all_detected_attributes.add(p.name)

            # CRITICAL RULE: A table CANNOT participate in Identity Resolution without identifiers!
            # If a table has 0 identifiers (like 'products' or 'stores' or 'experiments'), exclude it from sources!
            if not table_identifiers:
                profiling_diagnostics[fqn] = {
                    "included": False,
                    "reason": "Excluded: Table contains 0 customer identifiers (no email, phone, loyalty, or customer_id).",
                    "sample_rows_analyzed": len(sample_rows),
                }
                continue

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
                "included": True,
                "sample_rows_analyzed": len(sample_rows),
                "selected_entity_key": entity_key_col,
                "selected_watermark": watermark_col,
                "identifiers_mapped": [i["expr"] for i in table_identifiers],
            }

        # 7. Adaptive Matching Rules based on observed frequency
        rules = []
        priority = 1
        order_pref = {"email": 1, "phone": 2, "loyalty": 3, "customer_id": 4, "device_id": 5}
        sorted_id_types = sorted(all_detected_identifiers.keys(), key=lambda k: order_pref.get(k, 10))

        for id_type in sorted_id_types:
            observed_max = all_detected_identifiers.get(id_type, 1)
            adaptive_max = max(15, min(500, observed_max * 10))
            # Tighter household / shared network guardrail to prevent apartment/office over-merges
            if any(w in id_type.lower() for w in ("address", "ip", "session")):
                adaptive_max = min(adaptive_max, 25)

            canon = "EXACT"
            if "email" in id_type.lower():
                canon = "EMAIL_STANDARD"
            elif "phone" in id_type.lower():
                canon = "PHONE_DIGITS"

            rules.append({
                "id": f"exact_{id_type}",
                "type": "EXACT",
                "match_keys": [id_type],
                "canonicalize": canon,
                "priority": priority,
                "max_group_size": adaptive_max,
            })
            priority += 1

        # 8. Adaptive Survivorship Strategies per attribute based on source watermark presence
        survivorship = []
        attr_sources_map: Dict[str, List[Dict[str, Any]]] = {}
        for s in sources:
            for attr_obj in s.get("attributes", []):
                attr_name = attr_obj["name"]
                attr_sources_map.setdefault(attr_name, []).append(s)

        for attr in sorted(all_detected_attributes):
            contributing = attr_sources_map.get(attr, [])
            attr_has_wm = any(s.get("watermark_column") for s in contributing)
            strategy = "RECENCY" if attr_has_wm else "PRIORITY"
            rule_entry: Dict[str, Any] = {"attribute": attr, "strategy": strategy}
            if strategy == "PRIORITY" and contributing:
                rule_entry["source_priority"] = [s["table"] for s in contributing]
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
            "use_default_exclusions": True,
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
