"""
Canonical Pydantic v2 schemas for SQL Identity Resolution configurations
and AI-generated rule plans.

Serves as the single source of truth for:
- Constrained LLM structured outputs (Ollama `format`, OpenAI/vLLM `json_schema`,
  Gemini `responseSchema`, Anthropic `tool_use`)
- Strict validation & normalization before `config_to_sql()`
- Explainability & human-review metadata (`confidence`, `needs_review`, `evidence`, `risks`)
- Safe declarative fuzzy-rule template expansion (shielding LLMs from raw dialect SQL)
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

ConfidenceLevel = Literal["high", "medium", "low"]
GenerationMode = Literal["ai_validated", "ai_repaired", "deterministic_fallback"]


# =============================================================================
# Safe Fuzzy Rule Template Registry (future-proof for Fuzzy v2 evolution)
# =============================================================================

FUZZY_TEMPLATE_NAMES = (
    "NAME_SOUNDEX_JW",
    "NAME_FIRST_LAST_JW",
    "EMAIL_LOCAL_LEVENSHTEIN",
)


def expand_fuzzy_template(
    template: str,
    rule_id: str,
    name: str,
    columns: List[str],
    threshold: float = 0.85,
    priority: int = 100,
) -> Dict[str, Any]:
    """
    Deterministically expand a declarative fuzzy-rule template into dialect-safe
    `blocking_key` and `score_expr` compatible with `config_to_sql()`.
    """
    tmpl = (template or "").strip().upper()
    cols = [c.strip() for c in columns if c and c.strip()]

    if tmpl not in FUZZY_TEMPLATE_NAMES:
        raise ValueError(f"Unknown fuzzy template '{template}'")
    from .config import validate_identifier
    for col in cols:
        validate_identifier(col, "fuzzy template attribute")
    if not cols:
        raise ValueError("Fuzzy templates require mapped attributes")
    selected = cols[:2] if tmpl in ("NAME_SOUNDEX_JW", "NAME_FIRST_LAST_JW") else cols[:1]
    # Keep the legacy template names as API aliases. Prefix blocking is portable
    # across all supported warehouses and avoids deploying a phonetic UDF.
    blocking = f"SUBSTR(LOWER(TRIM({selected[-1]})), 1, 2)"
    def ratio(col, distance):
        a, b = f"<a>.{col}", f"<b>.{col}"
        return f"(1.0 - (1.0 * {distance}({a}, {b}) / GREATEST(LENGTH({a}), LENGTH({b}), 1)))"
    if tmpl == "EMAIL_LOCAL_LEVENSHTEIN":
        score_expr = {
            "default": ratio(selected[0], "LEVENSHTEIN"),
            "bigquery": ratio(selected[0], "EDIT_DISTANCE"),
            "snowflake": ratio(selected[0], "EDITDISTANCE"),
            "databricks": ratio(selected[0], "LEVENSHTEIN"),
        }
    else:
        score_expr = {
            "default": "(" + " + ".join(f"jaro_winkler_similarity(<a>.{col}, <b>.{col})" for col in selected) + f") / {len(selected)}.0",
            "snowflake": "(" + " + ".join(f"JAROWINKLER_SIMILARITY(<a>.{col}, <b>.{col}) / 100.0" for col in selected) + f") / {len(selected)}.0",
            "bigquery": "(" + " + ".join(ratio(col, "EDIT_DISTANCE") for col in selected) + f") / {len(selected)}.0",
            "databricks": "(" + " + ".join(ratio(col, "LEVENSHTEIN") for col in selected) + f") / {len(selected)}.0",
        }

    return {
        "id": rule_id,
        "name": name or rule_id,
        "blocking_key": blocking,
        "score_expr": score_expr,
        "threshold": round(min(max(float(threshold), 0.5), 0.99), 2),
        "priority": int(priority),
    }


# =============================================================================
# Core IDR Configuration Models
# =============================================================================


class IdentifierMappingSpec(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    type: str = Field(..., description="Semantic identifier type (e.g., email, phone, customer_id, loyalty, device_id).")
    expr: str = Field(
        ...,
        validation_alias=AliasChoices("expr", "column"),
        description="Source column name or safe SQL expression.",
    )
    is_hashed: bool = Field(default=False, description="True if source values are pre-hashed (e.g., SHA-256/MD5).")
    source_is_hashed: bool = Field(default=False, description="True if source already stores a digest that should not be hashed again.")
    issuer: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("issuer", "issuer_expr"),
        description="Optional SQL column or expression scoping the identifier to an issuing authority (e.g., facility_id for MRN).",
    )


class AttributeMappingSpec(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    name: str = Field(..., description="Canonical golden-profile attribute name (snake_case).")
    expr: str = Field(
        ...,
        validation_alias=AliasChoices("expr", "column"),
        description="Source column name or safe SQL expression.",
    )


class SourceSpec(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: str = Field(..., description="Unique alphanumeric/underscore source identifier.")
    table: str = Field(..., description="Fully qualified source table name (schema.table).")
    entity_type: str = Field(default="PERSON", description="Structural graph role (e.g., PRIMARY, PERSON, EVENT, CHILD).")
    entity_kind: Optional[str] = Field(default=None, description="Semantic entity domain kind (e.g., person, organization, product, asset).")
    entity_key: str = Field(default="AUTO", description="Primary key column or 'AUTO'.")
    key_columns: List[str] = Field(default_factory=list)
    watermark_lookback_minutes: int = Field(default=0, ge=0)
    watermark_column: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("watermark_column", "watermark"),
        description="Timestamp/date column for incremental watermarking and recency.",
    )
    deduplicate: bool = Field(default=False, description="Deduplicate duplicate entity_key rows before extraction.")
    dedup_order_by: Optional[str] = Field(default=None, description="ORDER BY expression when deduplicate=True.")
    identifiers: List[IdentifierMappingSpec] = Field(default_factory=list)
    attributes: List[AttributeMappingSpec] = Field(default_factory=list)


class ExactRuleSpec(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: str = Field(
        ...,
        validation_alias=AliasChoices("id", "rule_id"),
        description="Unique deterministic rule ID (e.g., exact_email).",
    )
    type: str = Field(default="EXACT", description="Rule type, typically EXACT.")
    match_keys: List[str] = Field(
        default_factory=list,
        description="List containing the semantic identifier type(s) matched by this rule.",
    )
    canonicalize: str = Field(
        default="LOWERCASE",
        description="Registered canonicalizer preset (e.g., EMAIL_STANDARD, EMAIL_STRIP_PLUS, PHONE_US_10, PHONE_DIGITS, ALPHANUMERIC_UPPER, LOWERCASE, UPPERCASE, EXACT).",
    )
    priority: int = Field(default=1, ge=1, le=1000, description="Lower number runs first (stronger match).")
    max_group_size: int = Field(default=500, ge=2, le=100000, description="Max entities sharing one identifier value before hub skipping.")

    @field_validator("priority", mode="before")
    @classmethod
    def _clamp_priority(cls, v: Any) -> int:
        try:
            return min(max(int(float(v)), 1), 1000)
        except Exception:
            return 100

    @field_validator("max_group_size", mode="before")
    @classmethod
    def _clamp_max_group_size(cls, v: Any) -> int:
        try:
            return min(max(int(float(v)), 2), 100000)
        except Exception:
            return 500

    @field_validator("match_keys", mode="before")
    @classmethod
    def _coerce_match_keys(cls, v: Any) -> List[str]:
        if isinstance(v, str) and v.strip():
            return [v.strip()]
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        return []


class FuzzyRuleTemplateSelection(BaseModel):
    """Safe declarative fuzzy-rule selection emitted by the LLM and compiled deterministically."""

    model_config = ConfigDict(extra="ignore")

    id: str = Field(..., description="Unique fuzzy rule ID (e.g., fuzzy_name).")
    name: str = Field(..., description="Human-readable fuzzy rule name.")
    template: Literal["NAME_SOUNDEX_JW", "NAME_FIRST_LAST_JW", "EMAIL_LOCAL_LEVENSHTEIN"] = Field(
        ..., description="Safe built-in fuzzy template."
    )
    columns: List[str] = Field(
        default_factory=list,
        description="Mapped attribute names used by the template (e.g., ['first_name', 'last_name']).",
    )
    threshold: float = Field(default=0.85, ge=0.5, le=0.99)
    priority: int = Field(default=100, ge=1, le=1000)

    @field_validator("threshold", mode="before")
    @classmethod
    def _clamp_threshold(cls, v: Any) -> float:
        try:
            fv = float(v)
            if fv < 0.5 or fv > 0.99:
                return 0.85
            return round(fv, 2)
        except Exception:
            return 0.85

    @field_validator("priority", mode="before")
    @classmethod
    def _clamp_fuzzy_priority(cls, v: Any) -> int:
        try:
            return min(max(int(float(v)), 1), 1000)
        except Exception:
            return 100


class SurvivorshipSpec(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    attribute: str = Field(..., description="Golden profile attribute name.")
    strategy: str = Field(..., description="Registered survivorship strategy (e.g., RECENCY, PRIORITY, PRIORITY_RECENCY, FREQUENCY, ANY_TRUE, ALL_TRUE).")
    source_priority: List[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices("source_priority", "order", "source_priority_list"),
        description="Ordered list of source IDs for PRIORITY / PRIORITY_RECENCY / LATEST_FROM_SOURCE.",
    )
    recency_field: Optional[str] = Field(default=None, description="Optional recency column override.")
    row_group: Optional[str] = Field(default=None, description="Select related attributes from the same winning source row.")


class ExclusionSpec(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    identifier_type: str = Field(
        ...,
        validation_alias=AliasChoices("identifier_type", "type"),
    )
    value: str = Field(
        ...,
        validation_alias=AliasChoices("value", "identifier_value_pattern"),
    )
    match_type: Literal["EXACT", "LIKE", "REGEX"] = Field(
        default="EXACT",
        validation_alias=AliasChoices("match_type", "match"),
    )
    reason: str = Field(default="Config exclusion")


class LensSpec(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: str = Field(..., validation_alias=AliasChoices("id", "lens_id"))
    include_types: List[str] = Field(default_factory=list)
    exclude_types: List[str] = Field(default_factory=list)
    source_filter: List[str] = Field(default_factory=list)
    max_group_size: Optional[int] = Field(default=None, ge=2, le=100000)

    @field_validator("max_group_size", mode="before")
    @classmethod
    def _clamp_lens_mgs(cls, v: Any) -> Optional[int]:
        if v is None or v == "":
            return None
        try:
            iv = int(float(v))
            if iv < 2:
                return None
            return min(iv, 100000)
        except Exception:
            return None


class IDRConfigSpec(BaseModel):
    """Canonical declarative IDR config model convertible directly to `config_to_sql()`."""

    model_config = ConfigDict(extra="ignore")

    use_default_exclusions: bool = True
    sources: List[SourceSpec] = Field(default_factory=list)
    rules: List[ExactRuleSpec] = Field(default_factory=list)
    fuzzy_rules: List[Dict[str, Any]] = Field(default_factory=list)
    survivorship: List[SurvivorshipSpec] = Field(default_factory=list)
    exclusions: List[ExclusionSpec] = Field(default_factory=list)
    lenses: List[LensSpec] = Field(default_factory=list)

    def to_idr_config_dict(self) -> Dict[str, Any]:
        """Serialize into a clean YAML/JSON-ready dict matching `idr_core.config.config_to_sql()`."""
        sources_out: List[Dict[str, Any]] = []
        for s in self.sources:
            src_dict: Dict[str, Any] = {
                "id": s.id,
                "table": s.table,
                "entity_type": s.entity_type,
                "entity_key": s.entity_key,
                "identifiers": [
                    {
                        "type": i.type,
                        "expr": i.expr,
                        **({"source_is_hashed": True} if (i.source_is_hashed or i.is_hashed) else {}),
                        **({"issuer": i.issuer} if i.issuer else {}),
                    }
                    for i in s.identifiers
                ],
                "attributes": [{"name": a.name, "expr": a.expr} for a in s.attributes],
            }
            if s.entity_kind and s.entity_kind.strip().lower() != "person":
                src_dict["entity_kind"] = s.entity_kind.strip().lower()
            if s.key_columns:
                src_dict["key_columns"] = s.key_columns
            if s.watermark_lookback_minutes:
                src_dict["watermark_lookback_minutes"] = s.watermark_lookback_minutes
            if s.watermark_column:
                src_dict["watermark_column"] = s.watermark_column
            if s.deduplicate:
                src_dict["deduplicate"] = True
                if s.dedup_order_by:
                    src_dict["dedup_order_by"] = s.dedup_order_by
            sources_out.append(src_dict)

        rules_out: List[Dict[str, Any]] = [
            {
                "id": r.id,
                "type": r.type,
                "match_keys": r.match_keys,
                "canonicalize": r.canonicalize,
                "priority": r.priority,
                "max_group_size": r.max_group_size,
            }
            for r in self.rules
        ]

        survivorship_out: List[Dict[str, Any]] = []
        for sv in self.survivorship:
            item: Dict[str, Any] = {
                "attribute": sv.attribute,
                "strategy": sv.strategy,
            }
            if sv.source_priority:
                item["source_priority"] = sv.source_priority
            if sv.recency_field:
                item["recency_field"] = sv.recency_field
            if sv.row_group:
                item["row_group"] = sv.row_group
            survivorship_out.append(item)

        out: Dict[str, Any] = {
            "use_default_exclusions": self.use_default_exclusions,
            "sources": sources_out,
            "rules": rules_out,
            "survivorship": survivorship_out,
        }
        if self.fuzzy_rules:
            out["fuzzy_rules"] = self.fuzzy_rules
        if self.exclusions:
            out["exclusions"] = [
                {
                    "identifier_type": e.identifier_type,
                    "value": e.value,
                    "match_type": e.match_type,
                    "reason": e.reason,
                }
                for e in self.exclusions
            ]
        if self.lenses:
            out["lenses"] = [
                {
                    "id": ln.id,
                    "include_types": ln.include_types,
                    "exclude_types": ln.exclude_types,
                    "source_filter": ln.source_filter,
                    **({"max_group_size": ln.max_group_size} if ln.max_group_size else {}),
                }
                for ln in self.lenses
            ]
        return out


# =============================================================================
# Explainability & Steward Review Models
# =============================================================================


class ExplanationItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    target: str = Field(..., description="Table name, rule_id, attribute name, or lens_id.")
    summary: str = Field(..., description="Concise plain-English reasoning for this decision.")
    evidence: List[str] = Field(
        default_factory=list,
        description="Cited profile metrics (e.g., null_rate=0.02, overlap=0.84).",
    )
    risks: List[str] = Field(
        default_factory=list,
        description="Potential data-quality or over-merge risks.",
    )
    confidence: ConfidenceLevel = Field(default="high")
    needs_review: bool = Field(
        default=False,
        description="True when a human steward should double-check this choice.",
    )


class ExplanationBundle(BaseModel):
    model_config = ConfigDict(extra="ignore")

    tables: List[ExplanationItem] = Field(default_factory=list)
    rules: List[ExplanationItem] = Field(default_factory=list)
    survivorship: List[ExplanationItem] = Field(default_factory=list)
    lenses: List[ExplanationItem] = Field(default_factory=list)


# =============================================================================
# Two-Pass LLM Structured Output Contracts
# =============================================================================


class PassATableDecision(BaseModel):
    model_config = ConfigDict(extra="ignore")

    table: str = Field(..., description="Exact FQN from the input profile.")
    include_in_idr: bool = Field(..., description="True if this table participates in identity resolution.")
    source_id: str = Field(..., description="Short alphanumeric/underscore ID (e.g., crm_users).")
    entity_type: str = Field(default="PERSON")
    entity_key: Optional[str] = Field(default=None, description="Exact column name from this table, or 'AUTO'.")
    watermark_column: Optional[str] = Field(default=None, description="Exact temporal column name or null.")
    deduplicate: bool = Field(default=False)
    dedup_order_by: Optional[str] = Field(default=None)
    identifiers: List[IdentifierMappingSpec] = Field(default_factory=list)
    attributes: List[AttributeMappingSpec] = Field(default_factory=list)
    reasoning: str = Field(..., description="1-2 sentence explanation of why this table was included/excluded and how keys/identifiers were chosen.")
    confidence: ConfidenceLevel = Field(default="high")
    needs_review: bool = Field(default=False)
    risks: List[str] = Field(default_factory=list)


class PassAMappingOutput(BaseModel):
    """Structured output for Pass A (batched table & column metadata mapping)."""

    model_config = ConfigDict(extra="ignore")

    tables: List[PassATableDecision] = Field(default_factory=list)


class PassBRuleItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    rule: ExactRuleSpec
    reasoning: str = Field(..., description="Why this rule, canonicalizer, priority, and max_group_size were chosen.")
    evidence: List[str] = Field(default_factory=list)
    risks: List[str] = Field(default_factory=list)
    confidence: ConfidenceLevel = Field(default="high")
    needs_review: bool = Field(default=False)


class PassBSurvivorshipItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    rule: SurvivorshipSpec
    reasoning: str = Field(..., description="Why this survivorship strategy and source order fit this attribute.")
    confidence: ConfidenceLevel = Field(default="high")
    needs_review: bool = Field(default=False)


class PassBLensItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    lens: LensSpec
    reasoning: str = Field(..., description="Use case for this identity lens.")
    confidence: ConfidenceLevel = Field(default="high")
    needs_review: bool = Field(default=False)


class PassBRuleOutput(BaseModel):
    """Structured output for Pass B (global rules, fuzzy templates, survivorship & lenses)."""

    model_config = ConfigDict(extra="ignore")

    use_default_exclusions: bool = True
    rules: List[PassBRuleItem] = Field(default_factory=list)
    fuzzy_templates: List[FuzzyRuleTemplateSelection] = Field(default_factory=list)
    survivorship: List[PassBSurvivorshipItem] = Field(default_factory=list)
    lenses: List[PassBLensItem] = Field(default_factory=list)


# =============================================================================
# Validation & Final Envelope Models
# =============================================================================


class ValidationReport(BaseModel):
    model_config = ConfigDict(extra="ignore")

    valid: bool = True
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    repair_attempts: int = 0
    sql_verified: bool = False


class GeneratedIDRPlan(BaseModel):
    """Complete result returned by AIRuleGenerator (CLI, FastAPI, MCP)."""

    model_config = ConfigDict(extra="ignore")

    proposal_id: str = ""
    base_draft_hash: str = ""
    candidate_draft_hash: str = ""
    status: Literal["ready_for_preview", "needs_decisions", "invalid"] = "invalid"
    decisions: List[dict[str, Any]] = Field(default_factory=list)
    diff: List[dict[str, Any]] = Field(default_factory=list)
    generation_mode: GenerationMode
    provider: str = "deterministic"
    model: str = "profiler"
    pii_masked: bool = True
    suggested_config: Dict[str, Any] = Field(default_factory=dict)
    explanations: ExplanationBundle = Field(default_factory=ExplanationBundle)
    validation_report: ValidationReport = Field(default_factory=ValidationReport)
    profile_summary: Dict[str, Any] = Field(default_factory=dict)
    dry_run_preview: Optional[Dict[str, Any]] = None
