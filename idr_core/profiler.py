"""
Production-grade statistical profiler for SQL Identity Resolution.

Profiles live warehouse tables, computes statistical distributions (null ratios,
uniqueness ratios, cardinality), executes regex pattern matching on actual sample
values (RFC 5322 emails, E.164 phone formats), detects row entity keys and
watermarks with mathematical validation, flags supernode risks, and generates
validated IDR configurations.
"""

import hashlib
import re
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

# Pre-compiled regex patterns
_EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
)
_EMAIL_PLUS_TAG_RE = re.compile(
    r"^[a-zA-Z0-9_.-]+\+[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
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
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_BOOL_VALUES = {"true", "false", "0", "1", "yes", "no", "y", "n", "t", "f"}
_CONSENT_HINTS = ("consent", "opt_in", "opt_out", "optin", "optout", "unsubscribe", "marketing_ok", "do_not_")
_METRIC_HINTS = (
    "price", "cost", "amount", "value", "quantity", "qty", "rating",
    "score", "count", "pct", "percent", "rate", "weight", "total", "subtotal", "revenue", "ltv",
)
# Generic column names that commonly have different semantics across tables
# (e.g., orders.status = PAID/CANCELLED vs shipments.status = IN_TRANSIT/DELIVERED)
GENERIC_HOMONYM_ATTRIBUTES: Set[str] = {
    "status", "state", "type", "category", "channel", "source", "reason",
    "stage", "code", "notes", "description", "comments", "comment",
    "created_at", "updated_at", "timestamp", "event_time", "event_date",
    "amount", "total", "subtotal", "price", "value", "score", "rating",
    "count", "quantity", "qty",
}


def _to_shape_skeleton(value: str) -> str:
    """Convert a string into a zero-PII character-class skeleton (e.g. '999-999-9999', 'a+@a+.a+')."""
    if not value:
        return ""
    if _EMAIL_RE.match(value):
        return "EMAIL_PLUS_TAG" if "+" in value.split("@", 1)[0] else "EMAIL_STANDARD"
    if _UUID_RE.match(value):
        return "UUID"
    if _HEX_TOKEN_RE.match(value):
        return f"HEX_{len(value)}"
    if _DATE_OR_TS_RE.match(value):
        return "ISO_DATETIME" if ("T" in value or ":" in value) else "DATE"
    # Replace digits with '9', uppercase with 'A', lowercase with 'a' and cap length
    chars = []
    for ch in value[:24]:
        if ch.isdigit():
            chars.append("9")
        elif ch.isupper():
            chars.append("A")
        elif ch.islower():
            chars.append("a")
        else:
            chars.append(ch)
    skeleton = "".join(chars)
    if len(value) > 24:
        skeleton += "..."
    return skeleton


@dataclass
class SafeColumnProfile:
    """
    Zero-PII statistical profile of a single column.
    Structurally stores ONLY counts, ratios, pattern hit rates, shape skeletons,
    and deterministic hints — never raw cell values.
    """

    name: str
    declared_type: str = ""
    description: Optional[str] = None
    is_declared_pk: bool = False
    sample_count: int = 0
    non_null_count: int = 0
    null_rate: float = 1.0
    distinct_count: int = 0
    uniqueness_ratio: float = 0.0
    min_len: int = 0
    max_len: int = 0
    max_frequency_count: int = 1
    top1_share_pct: float = 0.0
    email_rate: float = 0.0
    email_plus_tag_rate: float = 0.0
    phone_us10_rate: float = 0.0
    phone_e164_rate: float = 0.0
    hex_hash_rate: float = 0.0
    uuid_rate: float = 0.0
    date_ts_rate: float = 0.0
    bool_rate: float = 0.0
    top_shapes: List[str] = field(default_factory=list)
    # Deterministic pre-annotated hints (improves small local models like llama3.2)
    hint_role: str = "attribute"  # entity_key | watermark | identifier | consent_flag | metric | attribute
    hint_identifier_type: Optional[str] = None
    hint_canonicalizer: Optional[str] = None
    hint_is_hashed: bool = False
    hint_confidence: float = 0.0

    def to_compact_dict(self, verbose: bool = True) -> Dict[str, Any]:
        """Serialize to a token-efficient dict for LLM prompts (zero raw PII)."""
        if not verbose:
            out_min: Dict[str, Any] = {
                "name": self.name,
                "type": self.declared_type,
                "null_rate": round(self.null_rate, 3),
                "hint_role": self.hint_role,
            }
            if self.description:
                out_min["description"] = self.description[:160]
            return out_min
        out: Dict[str, Any] = {
            "name": self.name,
            "type": self.declared_type,
            "null_rate": round(self.null_rate, 3),
            "uniqueness_ratio": round(self.uniqueness_ratio, 3),
            "max_freq": self.max_frequency_count,
            "top1_share_pct": round(self.top1_share_pct, 3),
            "hint_role": self.hint_role,
        }
        if self.description:
            out["description"] = self.description[:160]
        if self.is_declared_pk:
            out["declared_pk"] = True
        if self.hint_identifier_type:
            out["hint_identifier_type"] = self.hint_identifier_type
        if self.hint_canonicalizer:
            out["hint_canonicalizer"] = self.hint_canonicalizer
        if self.hint_is_hashed:
            out["hint_is_hashed"] = True
        if self.hint_confidence > 0:
            out["hint_confidence"] = round(self.hint_confidence, 2)
        # Only include non-zero pattern rates to save tokens
        patterns: Dict[str, float] = {}
        for k, v in (
            ("email", self.email_rate),
            ("email_plus_tag", self.email_plus_tag_rate),
            ("phone_us10", self.phone_us10_rate),
            ("phone_e164", self.phone_e164_rate),
            ("hex_hash", self.hex_hash_rate),
            ("uuid", self.uuid_rate),
            ("date_ts", self.date_ts_rate),
            ("bool", self.bool_rate),
        ):
            if v >= 0.05:
                patterns[k] = round(v, 3)
        if patterns:
            out["patterns"] = patterns
        if self.top_shapes:
            out["shapes"] = self.top_shapes[:3]
        return out


@dataclass
class SafeTableProfile:
    """Zero-PII statistical profile of a warehouse table."""

    table_fqn: str
    description: Optional[str] = None
    row_count: int = 0
    sample_rows_analyzed: int = 0
    columns: List[SafeColumnProfile] = field(default_factory=list)
    hint_entity_key: Optional[str] = None
    hint_watermark: Optional[str] = None
    duplicate_entity_keys_detected: bool = False
    null_entity_keys_detected: bool = False
    has_candidate_identifiers: bool = False

    def column_names(self) -> Set[str]:
        return {c.name.lower() for c in self.columns}

    def to_prompt_dict(self, max_passive_attrs: int = 25) -> Dict[str, Any]:
        """Token-budgeted, zero-PII representation of this table for LLM prompts."""
        key_cols: List[Dict[str, Any]] = []
        passive_cols: List[Dict[str, Any]] = []
        for col in self.columns:
            if col.hint_role in ("entity_key", "watermark", "identifier", "consent_flag") or (
                col.email_rate >= 0.1 or col.phone_us10_rate >= 0.1 or col.phone_e164_rate >= 0.1 or col.hex_hash_rate >= 0.5
            ):
                key_cols.append(col.to_compact_dict(verbose=True))
            else:
                passive_cols.append(col.to_compact_dict(verbose=False))
        out: Dict[str, Any] = {
            "table": self.table_fqn,
            "row_count": self.row_count,
            "sample_rows": self.sample_rows_analyzed,
            "hint_entity_key": self.hint_entity_key,
            "hint_watermark": self.hint_watermark,
            "duplicate_entity_keys": self.duplicate_entity_keys_detected,
            "key_and_signal_columns": key_cols,
            "other_columns": passive_cols[:max_passive_attrs],
            **(
                {"omitted_other_columns_count": len(passive_cols) - max_passive_attrs}
                if len(passive_cols) > max_passive_attrs
                else {}
            ),
        }
        if self.description:
            out["description"] = self.description[:240]
        return out


@dataclass
class CrossTableOverlap:
    """Privacy-preserving overlap score between two candidate identifier columns."""

    table_a: str
    column_a: str
    table_b: str
    column_b: str
    identifier_type: str
    overlap_a_in_b: float  # fraction of distinct A values found in B
    overlap_b_in_a: float  # fraction of distinct B values found in A
    shared_distinct_estimate: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_a": self.table_a,
            "column_a": self.column_a,
            "table_b": self.table_b,
            "column_b": self.column_b,
            "identifier_type": self.identifier_type,
            "overlap_a_in_b": round(self.overlap_a_in_b, 3),
            "overlap_b_in_a": round(self.overlap_b_in_a, 3),
            "shared_distinct_estimate": self.shared_distinct_estimate,
        }


@dataclass
class SafeSchemaProfile:
    """Zero-PII profile across all selected warehouse tables + cross-table overlap matrix."""

    dialect: str
    tables: List[SafeTableProfile] = field(default_factory=list)
    overlaps: List[CrossTableOverlap] = field(default_factory=list)
    attribute_homonyms: List[Dict[str, Any]] = field(default_factory=list)

    def table_map(self) -> Dict[str, SafeTableProfile]:
        return {t.table_fqn: t for t in self.tables}

    def to_summary_dict(self) -> Dict[str, Any]:
        return {
            "dialect": self.dialect,
            "tables_profiled": len(self.tables),
            "tables_with_identifiers": sum(1 for t in self.tables if t.has_candidate_identifiers),
            "cross_table_overlaps": [o.to_dict() for o in self.overlaps],
            "attribute_homonyms": self.attribute_homonyms,
        }

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def is_system_table(table_name: str) -> bool:
    """Check if table belongs to IDR internal framework or system catalogs."""
    parts = table_name.lower().split(".")
    system_schemas = {"idr_meta", "idr_work", "idr_out", "information_schema", "sys", "system"}
    return any(p in system_schemas for p in parts)


class ColumnProfile:
    """Statistical summary of a single column computed from a live sample."""

    def __init__(
        self,
        name: str,
        declared_type: str = "",
        description: Optional[str] = None,
        is_declared_pk: bool = False,
    ):
        self.name = name.lower()
        self.declared_type = declared_type.lower()
        self.description = description.strip()[:200] if isinstance(description, str) and description.strip() else None
        self.is_declared_pk = bool(is_declared_pk)
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
        # Numeric/boolean columns (e.g. lifetime_points, response_time_ms) are never timestamps
        if any(t in type_str for t in ("int", "float", "double", "numeric", "decimal", "bool")):
            return False

        name = col_name.lower()
        # Semantic naming cues (avoid bare "time" matching "lifetime_points")
        temporal_names = (
            "date", "_date", "start_date", "end_date", "order_date", "birth_date", "dob",
            "_at", "created_at", "updated_at", "modified_at", "assigned_at", "enrolled_at",
            "_ts", "timestamp", "_time", "event_time", "period"
        )
        if name == "time" or any(w in name for w in temporal_names):
            if values:
                matches = sum(1 for v in values if _DATE_OR_TS_RE.match(v))
                if (matches / len(values)) >= 0.4:
                    return True
                return False
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
            col_desc = col_info.get("description") or col_info.get("comment")
            col_is_pk = bool(col_info.get("is_pk"))
            prof = ColumnProfile(col_name, col_type, description=col_desc, is_declared_pk=col_is_pk)
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
                desc_l = (p.description or "").lower()
                if p.is_declared_pk or any(k in desc_l for k in ("primary key", "unique row", "row identifier")):
                    score += 250
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

        # Opportunistic catalog description guardrails (zero penalty when description is absent)
        desc_l = (prof.description or "").lower()
        if desc_l and any(
            bad in desc_l
            for bad in (
                "deprecated",
                "do not use",
                "internal test",
                "sales rep",
                "account executive",
                "agent email",
                "employee email",
            )
        ):
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

            # Check candidate entity_key uniqueness & nulls on sample rows
            key_vals = []
            has_null_keys = False
            for r in sample_rows:
                # Match case-insensitively on column name
                val = next((v for k, v in r.items() if k.lower() == entity_key_col.lower()), None)
                if val is None:
                    has_null_keys = True
                else:
                    key_vals.append(val)
            has_dup_keys = len(key_vals) != len(set(key_vals))

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
            if has_dup_keys:
                source_entry["deduplicate"] = True
                if watermark_col:
                    source_entry["dedup_order_by"] = f"{watermark_col} DESC"

            sources.append(source_entry)

            profiling_diagnostics[fqn] = {
                "included": True,
                "sample_rows_analyzed": len(sample_rows),
                "selected_entity_key": entity_key_col,
                "selected_watermark": watermark_col,
                "identifiers_mapped": [i["expr"] for i in table_identifiers],
                "duplicate_entity_keys_detected": has_dup_keys,
                "null_entity_keys_detected": has_null_keys,
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

    # =========================================================================
    # Zero-PII Pushdown & Statistical Profiler for AI Rule Generation
    # =========================================================================

    def _get_table_row_count(self, table_fqn: str) -> int:
        """Compute exact row count in warehouse SQL (zero PII)."""
        try:
            cnt = self.adapter.query_one(f"SELECT COUNT(*) FROM {table_fqn}")
            return int(cnt or 0)
        except Exception:
            return 0

    @staticmethod
    def _normalize_for_overlap_hash(val: str, id_type: str) -> str:
        """Normalize a value in-memory and immediately SHA-256 hash it for zero-PII overlap math."""
        s = val.strip().lower()
        if id_type == "phone":
            base = _PHONE_EXT_CLEAN_RE.sub("", s).strip()
            digits = re.sub(r"[^0-9]", "", base)
            if len(digits) == 11 and digits.startswith("1"):
                digits = digits[1:]
            s = digits
        return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

    def _build_safe_column_profile(
        self,
        col_prof: ColumnProfile,
        raw_values: List[str],
    ) -> SafeColumnProfile:
        """
        Convert raw sampled values + basic ColumnProfile into a strictly zero-PII
        SafeColumnProfile with pattern hit rates, shape skeletons, and deterministic hints.
        Raw values are discarded immediately after computing aggregates.
        """
        n = len(raw_values)
        sp = SafeColumnProfile(
            name=col_prof.name,
            declared_type=col_prof.declared_type,
            description=col_prof.description,
            is_declared_pk=col_prof.is_declared_pk,
            sample_count=col_prof.sample_count,
            non_null_count=col_prof.non_null_count,
            null_rate=round(col_prof.null_ratio, 4),
            distinct_count=col_prof.distinct_count,
            uniqueness_ratio=round(col_prof.uniqueness_ratio, 4),
            max_frequency_count=col_prof.max_frequency_count,
            top1_share_pct=round(col_prof.max_frequency_count / n, 4) if n > 0 else 0.0,
        )

        if n > 0:
            lengths = [len(v) for v in raw_values]
            sp.min_len = min(lengths)
            sp.max_len = max(lengths)

            email_hits = 0
            email_plus_hits = 0
            phone_us10_hits = 0
            phone_e164_hits = 0
            hex_hits = 0
            uuid_hits = 0
            date_hits = 0
            bool_hits = 0
            shape_counter: Counter = Counter()

            for v in raw_values:
                if _EMAIL_RE.match(v):
                    email_hits += 1
                    if _EMAIL_PLUS_TAG_RE.match(v):
                        email_plus_hits += 1
                elif _UUID_RE.match(v):
                    uuid_hits += 1
                elif _HEX_TOKEN_RE.match(v):
                    hex_hits += 1
                elif _DATE_OR_TS_RE.match(v):
                    date_hits += 1
                else:
                    base_v = _PHONE_EXT_CLEAN_RE.sub("", v).strip()
                    clean_digits = re.sub(r"[\s\-\(\)\.]", "", base_v)
                    if not (len(clean_digits) == 8 and (clean_digits.startswith("19") or clean_digits.startswith("20"))):
                        if clean_digits.startswith("+") and _PHONE_DIGITS_RE.match(clean_digits):
                            phone_e164_hits += 1
                        elif _PHONE_DIGITS_RE.match(clean_digits) or _PHONE_FORMATTED_RE.match(v):
                            digits_only = re.sub(r"[^0-9]", "", base_v)
                            if len(digits_only) == 10 or (len(digits_only) == 11 and digits_only.startswith("1")):
                                phone_us10_hits += 1
                            elif 7 <= len(digits_only) <= 15:
                                phone_e164_hits += 1

                if v.lower() in _BOOL_VALUES:
                    bool_hits += 1
                shape_counter[_to_shape_skeleton(v)] += 1

            sp.email_rate = round(email_hits / n, 4)
            sp.email_plus_tag_rate = round(email_plus_hits / n, 4)
            sp.phone_us10_rate = round(phone_us10_hits / n, 4)
            sp.phone_e164_rate = round(phone_e164_hits / n, 4)
            sp.hex_hash_rate = round(hex_hits / n, 4)
            sp.uuid_rate = round(uuid_hits / n, 4)
            sp.date_ts_rate = round(date_hits / n, 4)
            sp.bool_rate = round(bool_hits / n, 4)
            sp.top_shapes = [s for s, _ in shape_counter.most_common(3) if s]

        # Deterministic pre-annotated hints (grounds small local models like llama3.2)
        name_l = sp.name.lower()
        if col_prof.is_entity_key:
            sp.hint_role = "entity_key"
            sp.hint_confidence = 0.95
        elif col_prof.is_temporal:
            sp.hint_role = "watermark"
            sp.hint_confidence = 0.90
        elif col_prof.detected_identifier_type:
            sp.hint_role = "identifier"
            sp.hint_identifier_type = col_prof.detected_identifier_type
            sp.hint_confidence = col_prof.detection_confidence
            if sp.hint_identifier_type == "email":
                sp.hint_canonicalizer = (
                    "EMAIL_STRIP_PLUS" if sp.email_plus_tag_rate >= 0.02 else "EMAIL_STANDARD"
                )
            elif sp.hint_identifier_type == "phone":
                sp.hint_canonicalizer = (
                    "PHONE_US_10" if sp.phone_us10_rate >= sp.phone_e164_rate else "PHONE_DIGITS"
                )
            elif sp.hint_identifier_type == "loyalty":
                sp.hint_canonicalizer = "ALPHANUMERIC_UPPER"
            else:
                sp.hint_canonicalizer = "EXACT"
        elif sp.hex_hash_rate >= 0.85 and any(
            k in name_l for k in ("email", "phone", "sha", "md5", "hash", "pii", "id")
        ):
            sp.hint_role = "identifier"
            sp.hint_is_hashed = True
            if "email" in name_l:
                sp.hint_identifier_type = "email_hash"
            elif "phone" in name_l:
                sp.hint_identifier_type = "phone_hash"
            else:
                sp.hint_identifier_type = name_l
            sp.hint_canonicalizer = "LOWERCASE"
            sp.hint_confidence = 0.85
        elif (sp.bool_rate >= 0.90 or "bool" in sp.declared_type) and any(
            k in name_l for k in _CONSENT_HINTS
        ):
            sp.hint_role = "consent_flag"
            sp.hint_confidence = 0.90
        elif any(m in name_l for m in _METRIC_HINTS):
            sp.hint_role = "metric"
            sp.hint_confidence = 0.85
        else:
            sp.hint_role = "attribute"

        return sp

    def profile_tables_safe(
        self,
        candidate_tables: List[str],
        sample_size: int = 2000,
        compute_overlap: bool = True,
    ) -> SafeSchemaProfile:
        """
        Profile candidate warehouse tables and return a strictly zero-PII SafeSchemaProfile
        (counts, ratios, pattern hit rates, shape skeletons, deterministic hints, and
        cross-table key overlap matrix). Raw cell values are never retained in the result.
        """
        dialect = getattr(self.adapter, "dialect", getattr(self.adapter, "platform", "duckdb"))
        safe_tables: List[SafeTableProfile] = []
        # Ephemeral hashed value sets used ONLY in-memory for cross-table overlap, then discarded
        hashed_sets: Dict[Tuple[str, str, str], Set[str]] = {}
        # Ephemeral categorical attribute hash sets for zero-PII homonym collision detection
        attr_hashed_sets: Dict[str, Dict[str, Set[str]]] = {}

        def _profile_one_table(
            fqn: str,
        ) -> Optional[
            Tuple[
                SafeTableProfile,
                Dict[Tuple[str, str, str], Set[str]],
                Dict[str, Set[str]],
            ]
        ]:
            cols = []
            if hasattr(self.adapter, "get_table_columns"):
                cols = self.adapter.get_table_columns(fqn)
            elif hasattr(self.adapter, "get_columns"):
                cols = self.adapter.get_columns(fqn)
            if not cols:
                return None

            table_name_short = fqn.split(".")[-1].lower()
            row_count = self._get_table_row_count(fqn)
            sample_rows = self.sample_table(fqn, sample_size=sample_size)
            col_profiles = self.profile_columns(table_name_short, cols, sample_rows)

            safe_cols: List[SafeColumnProfile] = []
            local_hashes: Dict[Tuple[str, str, str], Set[str]] = {}
            local_attr_hashes: Dict[str, Set[str]] = {}
            for col_info in cols:
                cname = col_info["name"].lower()
                cp = col_profiles.get(cname)
                if cp is None:
                    continue
                raw_vals = [
                    str(r.get(cname)).strip()
                    for r in sample_rows
                    if isinstance(r, dict)
                    and r.get(cname) is not None
                    and str(r.get(cname)).strip() != ""
                    and str(r.get(cname)).lower() not in ("null", "none")
                ]
                sp = self._build_safe_column_profile(cp, raw_vals)
                safe_cols.append(sp)

                if compute_overlap and sp.hint_role == "identifier" and sp.hint_identifier_type and raw_vals:
                    local_hashes[(fqn, sp.name, sp.hint_identifier_type)] = {
                        self._normalize_for_overlap_hash(v, sp.hint_identifier_type)
                        for v in raw_vals
                    }
                elif (
                    compute_overlap
                    and sp.hint_role in ("attribute", "metric", "consent_flag")
                    and 2 <= sp.distinct_count <= 50
                    and raw_vals
                ):
                    local_attr_hashes[sp.name.lower()] = {
                        hashlib.sha256(v.strip().lower().encode("utf-8")).hexdigest()[:16]
                        for v in raw_vals
                    }

            entity_keys = [c.name for c in safe_cols if c.hint_role == "entity_key"]
            hint_ek = entity_keys[0] if entity_keys else (safe_cols[0].name if safe_cols else None)
            wm_candidates = [c.name for c in safe_cols if c.hint_role == "watermark" and c.name != hint_ek]
            hint_wm = wm_candidates[0] if wm_candidates else None

            # Check duplicate/null keys on sample without retaining values
            has_dup_keys = False
            has_null_keys = False
            if hint_ek and sample_rows:
                seen_k: Set[str] = set()
                for r in sample_rows:
                    if not isinstance(r, dict):
                        continue
                    v = r.get(hint_ek)
                    if v is None or str(v).strip() == "":
                        has_null_keys = True
                    else:
                        hv = hashlib.sha256(str(v).encode("utf-8")).hexdigest()[:16]
                        if hv in seen_k:
                            has_dup_keys = True
                        seen_k.add(hv)

            tbl_desc = None
            if hasattr(self.adapter, "get_table_description"):
                try:
                    raw_td = self.adapter.get_table_description(fqn)
                    if isinstance(raw_td, str) and raw_td.strip():
                        tbl_desc = raw_td.strip()[:240]
                except Exception:
                    tbl_desc = None

            return (
                SafeTableProfile(
                    table_fqn=fqn,
                    description=tbl_desc,
                    row_count=row_count,
                    sample_rows_analyzed=len(sample_rows),
                    columns=safe_cols,
                    hint_entity_key=hint_ek,
                    hint_watermark=hint_wm,
                    duplicate_entity_keys_detected=has_dup_keys,
                    null_entity_keys_detected=has_null_keys,
                    has_candidate_identifiers=any(c.hint_role == "identifier" for c in safe_cols),
                ),
                local_hashes,
                local_attr_hashes,
            )

        if len(candidate_tables) > 1:
            from concurrent.futures import ThreadPoolExecutor

            workers = min(6, len(candidate_tables))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                results = list(pool.map(_profile_one_table, candidate_tables))
        else:
            results = [_profile_one_table(fqn) for fqn in candidate_tables]

        for res in results:
            if res is None:
                continue
            tprof, local_hashes, local_attr_hashes = res
            safe_tables.append(tprof)
            hashed_sets.update(local_hashes)
            for col_l, hset in local_attr_hashes.items():
                attr_hashed_sets.setdefault(col_l, {})[tprof.table_fqn] = hset

        # Compute cross-table overlap matrix across tables sharing candidate identifier types
        overlaps: List[CrossTableOverlap] = []
        if compute_overlap and len(hashed_sets) >= 2:
            items = list(hashed_sets.items())
            for i in range(len(items)):
                (tbl_a, col_a, type_a), set_a = items[i]
                if not set_a:
                    continue
                for j in range(i + 1, len(items)):
                    (tbl_b, col_b, type_b), set_b = items[j]
                    if tbl_a == tbl_b or type_a != type_b or not set_b:
                        continue
                    shared = len(set_a & set_b)
                    overlaps.append(
                        CrossTableOverlap(
                            table_a=tbl_a,
                            column_a=col_a,
                            table_b=tbl_b,
                            column_b=col_b,
                            identifier_type=type_a,
                            overlap_a_in_b=round(shared / len(set_a), 4),
                            overlap_b_in_a=round(shared / len(set_b), 4),
                            shared_distinct_estimate=shared,
                        )
                    )

        # Detect semantic homonym attributes across tables (generic names or disjoint categorical domains)
        attribute_homonyms: List[Dict[str, Any]] = []
        if len(safe_tables) >= 2:
            attr_tables_by_col: Dict[str, List[str]] = {}
            for tp in safe_tables:
                for c in tp.columns:
                    if c.hint_role in ("attribute", "metric", "consent_flag"):
                        attr_tables_by_col.setdefault(c.name.lower(), []).append(tp.table_fqn)

            for col_l, tbls in sorted(attr_tables_by_col.items()):
                if len(tbls) < 2:
                    continue
                if col_l in GENERIC_HOMONYM_ATTRIBUTES:
                    attribute_homonyms.append(
                        {
                            "column": col_l,
                            "tables": sorted(tbls),
                            "reason": "generic_homonym_name",
                        }
                    )
                    continue
                # Check if categorical sample domains are completely disjoint (0% overlap)
                per_tbl_hashes = attr_hashed_sets.get(col_l, {})
                if len(per_tbl_hashes) >= 2:
                    hash_items = list(per_tbl_hashes.items())
                    disjoint_pair = False
                    for i in range(len(hash_items)):
                        _, sa = hash_items[i]
                        for j in range(i + 1, len(hash_items)):
                            _, sb = hash_items[j]
                            if sa and sb and len(sa & sb) == 0:
                                disjoint_pair = True
                                break
                        if disjoint_pair:
                            break
                    if disjoint_pair:
                        attribute_homonyms.append(
                            {
                                "column": col_l,
                                "tables": sorted(tbls),
                                "reason": "zero_value_overlap",
                            }
                        )

        return SafeSchemaProfile(
            dialect=dialect,
            tables=safe_tables,
            overlaps=overlaps,
            attribute_homonyms=attribute_homonyms,
        )
