"""
Automated, zero-PII, AI-powered Identity Resolution Rule & Config Generator.

End-to-end pipeline:
1. Zero-PII Statistical Profiling (`SafeSchemaProfile` + cross-table overlap matrix)
2. Two-Pass Constrained LLM Synthesis (Ollama / vLLM / OpenAI / Anthropic / Gemini)
   - Pass A: Chunked table & column metadata mapping (with pre-annotated hints)
   - Pass B: Cross-table matching rules, safe fuzzy templates, survivorship & lenses
3. Multi-Layer Validation & Self-Repair Loop
   - Pydantic v2 schema + `validate_config()`
   - Grounding & referential integrity (real tables/columns, registered canonicalizers/strategies)
   - Optional warehouse `EXPLAIN` SQL dry-parse verification
4. Seamless Deterministic Fallback (never leaves the user empty-handed)
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

from .canonicalizers import list_canonicalizers, validate_canonicalize_spec
from .config import (
    validate_config,
    validate_fqn,
    validate_identifier,
    validate_sql_expr,
)
from .config_schema import (
    AttributeMappingSpec,
    ExactRuleSpec,
    ExclusionSpec,
    ExplanationBundle,
    ExplanationItem,
    GeneratedIDRPlan,
    IDRConfigSpec,
    IdentifierMappingSpec,
    LensSpec,
    PassAMappingOutput,
    PassATableDecision,
    PassBRuleOutput,
    SourceSpec,
    SurvivorshipSpec,
    ValidationReport,
    expand_fuzzy_template,
)
from .llm import LLMClient
from .profiler import IDRDataProfiler, SafeSchemaProfile, SafeTableProfile
from .survivorship import is_registered_survivorship_strategy, list_survivorship_strategies

_SAFE_COL_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_DISALLOWED_ID_TYPES = {
    "string", "varchar", "text", "char", "int", "integer", "bigint", "smallint",
    "float", "double", "decimal", "numeric", "boolean", "bool", "timestamp",
    "date", "datetime", "json", "object", "array", "null", "none", "unknown",
}


_BIRTH_DATE_HINTS = ("dob", "birth_date", "date_of_birth", "birthdate")


def _is_valid_watermark_col(tprof: SafeTableProfile, col_name: str) -> bool:
    """Return True if `col_name` exists in `tprof`, is temporal, and is not a birth date."""
    cl = col_name.lower()
    if any(b in cl for b in _BIRTH_DATE_HINTS):
        return False
    for c in tprof.columns:
        if c.name.lower() == cl:
            return c.hint_role == "watermark" or c.date_ts_rate >= 0.4 or any(
                t in c.declared_type for t in ("timestamp", "date", "time")
            )
    return False


def _sanitize_source_id(raw_id: str, fallback_table: str) -> str:
    """Coerce a source ID into a safe SQL identifier."""
    candidate = re.sub(r"[^a-zA-Z0-9_]+", "_", (raw_id or "").strip()).strip("_")
    if not candidate or candidate[0].isdigit():
        candidate = re.sub(r"[^a-zA-Z0-9_]+", "_", fallback_table.replace(".", "_")).strip("_")
    if not candidate or candidate[0].isdigit():
        candidate = f"src_{candidate}"
    return candidate


def _extract_column_refs(expr: str, valid_cols: Set[str]) -> List[str]:
    """Return valid column references found in `expr` (empty list if bare column is unknown)."""
    s = (expr or "").strip()
    if _SAFE_COL_RE.match(s):
        return [s.lower()] if s.lower() in valid_cols else []
    # For compound SQL expressions, check that at least one valid column is referenced
    tokens = {t.lower() for t in re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", s)}
    return sorted(tokens & valid_cols)


class AIRuleGenerator:
    """
    Orchestrates Zero-PII profiling, two-pass constrained LLM synthesis,
    multi-layer validation/repair, and deterministic fallback.
    """

    def __init__(
        self,
        adapter: Any,
        client: Optional[LLMClient] = None,
        batch_size: int = 6,
        max_repairs: int = 2,
    ):
        self.adapter = adapter
        self.client = client or LLMClient()
        self.profiler = IDRDataProfiler(adapter)
        # Compact Pass A (identifiers/keys only) fits 5 tables cleanly per batch for both local and frontier models
        default_batch = 5 if self.client.config.is_local else 8
        self.batch_size = max(1, int(batch_size if batch_size != 6 else default_batch))
        self.max_repairs = max(0, int(max_repairs))

    def generate(
        self,
        tables: List[str],
        goal: str = "Unify customer records accurately across sources with safe supernode guardrails and practical golden-profile survivorship.",
        sample_size: int = 2000,
        verify_sql: bool = True,
    ) -> GeneratedIDRPlan:
        """
        Run the full profiling -> mapping -> rule synthesis -> validation pipeline.
        Never mutates `idr_meta` tables.
        """
        if not tables:
            return GeneratedIDRPlan(
                generation_mode="deterministic_fallback",
                provider=self.client.config.provider,
                model=self.client.config.resolved_model() if self.client.config.is_enabled else "profiler",
                pii_masked=True,
                suggested_config={},
                validation_report=ValidationReport(
                    valid=False, errors=["No tables selected for identity resolution."]
                ),
                profile_summary={"tables_profiled": 0},
            )

        # Stage 1: Zero-PII Statistical & Overlap Profiling
        schema_profile = self.profiler.profile_tables_safe(
            tables, sample_size=sample_size, compute_overlap=True
        )
        if not schema_profile.tables:
            return GeneratedIDRPlan(
                generation_mode="deterministic_fallback",
                provider=self.client.config.provider,
                model=self.client.config.resolved_model() if self.client.config.is_enabled else "profiler",
                pii_masked=True,
                suggested_config={},
                validation_report=ValidationReport(
                    valid=False,
                    errors=["None of the selected tables could be read or had columns."],
                ),
                profile_summary=schema_profile.to_summary_dict(),
            )

        # If LLM is disabled, go straight to deterministic baseline with structured explanations
        if not self.client.config.is_enabled:
            return self._build_deterministic_fallback(
                schema_profile,
                tables,
                sample_size=sample_size,
                reason="AI provider is disabled; generated deterministic baseline from statistical profile.",
            )

        total_repairs = 0
        warnings: List[str] = []

        try:
            # Stage 2A: Pass A — Chunked Table & Column Metadata Mapping
            pass_a_decisions, repairs_a, warnings_a = self._run_pass_a(schema_profile, goal)
            total_repairs += repairs_a
            warnings.extend(warnings_a)

            sources, table_explanations = self._materialize_sources(pass_a_decisions, schema_profile)
            if not sources:
                return self._build_deterministic_fallback(
                    schema_profile,
                    tables,
                    sample_size=sample_size,
                    reason="AI mapped 0 active identity sources; fell back to deterministic profiler.",
                )

            # Stage 2B: Pass B — Cross-Table Rules, Fuzzy Templates, Survivorship & Lenses
            pass_b_out, repairs_b, warnings_b = self._run_pass_b(
                sources, schema_profile, goal
            )
            total_repairs += repairs_b
            warnings.extend(warnings_b)

            # Assemble canonical IDRConfigSpec + ExplanationBundle
            config_spec, explanations = self._assemble_plan(
                sources, table_explanations, pass_b_out, schema_profile
            )

            # Stage 3: Final Grounding, Registry, and optional SQL EXPLAIN verification
            val_report = self.validate_plan(
                config_spec,
                schema_profile,
                verify_sql=verify_sql,
            )
            val_report.repair_attempts = total_repairs
            val_report.warnings.extend(warnings)

            if not val_report.valid:
                return self._build_deterministic_fallback(
                    schema_profile,
                    tables,
                    sample_size=sample_size,
                    reason=(
                        f"AI plan failed validation after {total_repairs} repair attempt(s): "
                        + "; ".join(val_report.errors[:3])
                    ),
                )

            mode = "ai_repaired" if total_repairs > 0 else "ai_validated"
            return GeneratedIDRPlan(
                generation_mode=mode,
                provider=self.client.config.provider,
                model=self.client.config.resolved_model(),
                pii_masked=True,  # Zero-PII by construction
                suggested_config=config_spec.to_idr_config_dict(),
                explanations=explanations,
                validation_report=val_report,
                profile_summary=schema_profile.to_summary_dict(),
            )
        except Exception as exc:
            return self._build_deterministic_fallback(
                schema_profile,
                tables,
                sample_size=sample_size,
                reason=f"AI generation unavailable ({exc}); fell back to deterministic profiler.",
            )

    # =========================================================================
    # Stage 2A: Pass A (Chunked Metadata Mapping)
    # =========================================================================

    def _run_pass_a(
        self,
        schema_profile: SafeSchemaProfile,
        goal: str,
    ) -> Tuple[List[PassATableDecision], int, List[str]]:
        schema_json = PassAMappingOutput.model_json_schema()
        all_decisions: List[PassATableDecision] = []
        total_repairs = 0
        warnings: List[str] = []
        table_map = schema_profile.table_map()

        for i in range(0, len(schema_profile.tables), self.batch_size):
            batch = schema_profile.tables[i : i + self.batch_size]
            batch_payload = [t.to_prompt_dict() for t in batch]
            batch_fqns = {t.table_fqn for t in batch}

            system_msg = (
                "You are a Principal MarTech & Identity Resolution Architect. "
                "Map warehouse tables and columns into an Identity Resolution schema. "
                "STRICT RULES:\n"
                "1. Use ONLY exact table names and column names present in the provided profile.\n"
                "2. Review the deterministic hints (hint_entity_key, hint_watermark, hint_role, hint_identifier_type, hint_is_hashed) "
                "and confirm or refine them based on column names, stats, and the user's goal.\n"
                "3. Exclude non-identity lookup/catalog tables (include_in_idr=false) that have no person/account identifiers.\n"
                "4. Use consistent lowercase snake_case semantic identifier types across tables (e.g., email, phone, customer_id, loyalty, device_id, address_id) — NEVER SQL data types like 'string'.\n"
                "5. Leave `attributes` as an empty list [] unless you need to rename a specific profile attribute; passive non-key/non-identifier columns are mapped automatically.\n"
                "6. Set needs_review=true and add concise risks if a key has duplicates/nulls, or if an identifier has high fanout (max_freq >= 5) or high null_rate (> 0.5).\n"
                "Return JSON strictly matching the schema."
            )
            user_msg = (
                f"User Goal: {goal}\n\n"
                f"Zero-PII Table Profiles (batch {i // self.batch_size + 1}):\n"
                f"{json.dumps(batch_payload, indent=2)}"
            )
            messages = [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ]

            best_decisions: Optional[List[PassATableDecision]] = None
            for attempt in range(self.max_repairs + 1):
                raw_dict = self.client.chat_structured(
                    messages,
                    json_schema=schema_json,
                    schema_name="pass_a_table_mapping",
                    temperature=0.0,
                )
                try:
                    parsed = PassAMappingOutput.model_validate(raw_dict)
                except Exception as val_exc:
                    if attempt < self.max_repairs:
                        total_repairs += 1
                        messages.append({"role": "assistant", "content": json.dumps(raw_dict)})
                        messages.append(
                            {
                                "role": "user",
                                "content": f"Your JSON failed schema validation: {val_exc}\nFix the JSON to strictly match the schema.",
                            }
                        )
                        continue
                    raise
                errors = self._validate_pass_a_batch(parsed.tables, batch_fqns, table_map)
                if not errors:
                    best_decisions = parsed.tables
                    break
                if attempt < self.max_repairs:
                    total_repairs += 1
                    messages.append({"role": "assistant", "content": json.dumps(raw_dict)})
                    messages.append(
                        {
                            "role": "user",
                            "content": (
                                "Fix these validation errors using ONLY exact column names from the profile:\n- "
                                + "\n- ".join(errors)
                            ),
                        }
                    )
                else:
                    # Prune invalid columns deterministically so one minor mistake doesn't discard good reasoning
                    best_decisions = self._prune_invalid_pass_a(parsed.tables, table_map)
                    warnings.append(
                        f"Auto-pruned {len(errors)} ungrounded Pass A field(s) after {self.max_repairs} repairs."
                    )

            if best_decisions:
                all_decisions.extend(best_decisions)

        return all_decisions, total_repairs, warnings

    def _validate_pass_a_batch(
        self,
        decisions: List[PassATableDecision],
        batch_fqns: Set[str],
        table_map: Dict[str, SafeTableProfile],
    ) -> List[str]:
        errors: List[str] = []
        for dec in decisions:
            if dec.table not in table_map:
                errors.append(
                    f"Unknown table '{dec.table}'. Valid tables: {sorted(batch_fqns)}"
                )
                continue
            if not dec.include_in_idr:
                continue
            valid_cols = table_map[dec.table].column_names()
            if not dec.entity_key:
                errors.append(f"{dec.table}: missing entity_key (valid columns: {sorted(valid_cols)})")
            elif dec.entity_key.upper() != "AUTO" and not _extract_column_refs(dec.entity_key, valid_cols):
                errors.append(
                    f"{dec.table}: entity_key '{dec.entity_key}' not in columns {sorted(valid_cols)}"
                )
            if dec.watermark_column:
                if dec.watermark_column.lower() not in valid_cols:
                    errors.append(
                        f"{dec.table}: watermark_column '{dec.watermark_column}' not in columns {sorted(valid_cols)}"
                    )
                elif not _is_valid_watermark_col(table_map[dec.table], dec.watermark_column):
                    errors.append(
                        f"{dec.table}: watermark_column '{dec.watermark_column}' is not an event/update timestamp (cannot use metrics or birth dates)."
                    )
            for ident in dec.identifiers:
                if ident.type.strip().lower() in _DISALLOWED_ID_TYPES:
                    errors.append(
                        f"{dec.table}: identifier type '{ident.type}' is a SQL data type, not a semantic identifier (use e.g. email, phone, customer_id, loyalty, device_id)."
                    )
                if not _extract_column_refs(ident.expr, valid_cols):
                    errors.append(
                        f"{dec.table}: identifier expr '{ident.expr}' not in columns {sorted(valid_cols)}"
                    )
            for attr in dec.attributes:
                if not _extract_column_refs(attr.expr, valid_cols):
                    errors.append(
                        f"{dec.table}: attribute expr '{attr.expr}' not in columns {sorted(valid_cols)}"
                    )
        return errors

    def _prune_invalid_pass_a(
        self,
        decisions: List[PassATableDecision],
        table_map: Dict[str, SafeTableProfile],
    ) -> List[PassATableDecision]:
        cleaned: List[PassATableDecision] = []
        for dec in decisions:
            tprof = table_map.get(dec.table)
            if tprof is None:
                continue
            valid_cols = tprof.column_names()
            if dec.include_in_idr:
                if not dec.entity_key or (
                    dec.entity_key.upper() != "AUTO"
                    and not _extract_column_refs(dec.entity_key, valid_cols)
                ):
                    dec.entity_key = tprof.hint_entity_key or "AUTO"
                if dec.watermark_column and not _is_valid_watermark_col(tprof, dec.watermark_column):
                    dec.watermark_column = (
                        tprof.hint_watermark
                        if (tprof.hint_watermark and _is_valid_watermark_col(tprof, tprof.hint_watermark))
                        else None
                    )
                dec.identifiers = [
                    i
                    for i in dec.identifiers
                    if _extract_column_refs(i.expr, valid_cols)
                    and i.type.strip().lower() not in _DISALLOWED_ID_TYPES
                ]
                dec.attributes = [
                    a for a in dec.attributes if _extract_column_refs(a.expr, valid_cols)
                ]
                if not dec.identifiers:
                    dec.include_in_idr = False
            cleaned.append(dec)
        return cleaned

    def _materialize_sources(
        self,
        decisions: List[PassATableDecision],
        schema_profile: SafeSchemaProfile,
    ) -> Tuple[List[SourceSpec], List[ExplanationItem]]:
        sources: List[SourceSpec] = []
        explanations: List[ExplanationItem] = []
        seen_ids: Set[str] = set()
        table_map = schema_profile.table_map()

        for dec in decisions:
            tprof = table_map.get(dec.table)
            evidence: List[str] = []
            if tprof:
                evidence.append(f"rows={tprof.row_count}")
                if tprof.duplicate_entity_keys_detected:
                    evidence.append("duplicate_entity_keys=true")
                if tprof.hint_watermark:
                    evidence.append(f"watermark={dec.watermark_column or tprof.hint_watermark}")

            explanations.append(
                ExplanationItem(
                    target=dec.table,
                    summary=dec.reasoning,
                    evidence=evidence,
                    risks=dec.risks,
                    confidence=dec.confidence,
                    needs_review=dec.needs_review,
                )
            )
            if not dec.include_in_idr or not dec.identifiers:
                continue

            src_id = _sanitize_source_id(dec.source_id, dec.table)
            if src_id in seen_ids:
                src_id = _sanitize_source_id(dec.table, dec.table)
            seen_ids.add(src_id)

            # Deduplicate identifiers by type per table
            uniq_ids: List[IdentifierMappingSpec] = []
            seen_types: Set[str] = set()
            for ident in dec.identifiers:
                itype = ident.type.strip().lower()
                if itype and itype not in _DISALLOWED_ID_TYPES and itype not in seen_types:
                    seen_types.add(itype)
                    uniq_ids.append(
                        IdentifierMappingSpec(
                            type=itype,
                            expr=ident.expr.strip(),
                            is_hashed=ident.is_hashed,
                        )
                    )
            # If the LLM dropped high-confidence deterministic identifier columns (common on 3B local models),
            # merge them back in so cross-table linkages are preserved.
            if tprof:
                existing_exprs = {u.expr.lower() for u in uniq_ids}
                for col in tprof.columns:
                    if (
                        col.hint_role == "identifier"
                        and col.hint_identifier_type
                        and col.hint_confidence >= 0.80
                        and col.name.lower() not in existing_exprs
                        and col.hint_identifier_type.lower() not in seen_types
                    ):
                        seen_types.add(col.hint_identifier_type.lower())
                        existing_exprs.add(col.name.lower())
                        uniq_ids.append(
                            IdentifierMappingSpec(
                                type=col.hint_identifier_type.lower(),
                                expr=col.name,
                                is_hashed=col.hint_is_hashed,
                            )
                        )
            if not uniq_ids:
                continue

            # Deduplicate attributes by name per table; auto-populate passive non-key/non-id columns
            # when the LLM leaves `attributes` empty (saves ~75% output tokens on local & frontier models).
            uniq_attrs: List[AttributeMappingSpec] = []
            seen_attrs: Set[str] = set()
            for attr in dec.attributes:
                aname = re.sub(r"[^a-zA-Z0-9_]+", "_", attr.name.strip().lower()).strip("_")
                if aname and aname not in seen_attrs:
                    seen_attrs.add(aname)
                    uniq_attrs.append(AttributeMappingSpec(name=aname, expr=attr.expr.strip()))

            if not uniq_attrs and tprof:
                ek_l = (dec.entity_key or tprof.hint_entity_key or "").lower()
                wm_l = (dec.watermark_column or tprof.hint_watermark or "").lower()
                id_exprs_l = {u.expr.lower() for u in uniq_ids}
                for col in tprof.columns:
                    cl = col.name.lower()
                    if cl not in (ek_l, wm_l) and cl not in id_exprs_l and col.non_null_count > 0:
                        uniq_attrs.append(AttributeMappingSpec(name=col.name, expr=col.name))

            sources.append(
                SourceSpec(
                    id=src_id,
                    table=dec.table,
                    entity_type=(dec.entity_type or "PERSON").upper(),
                    entity_key=(dec.entity_key or (tprof.hint_entity_key if tprof else "AUTO") or "AUTO"),
                    watermark_column=dec.watermark_column,
                    deduplicate=bool(dec.deduplicate or (tprof.duplicate_entity_keys_detected if tprof else False)),
                    dedup_order_by=dec.dedup_order_by
                    or (f"{dec.watermark_column} DESC" if dec.watermark_column and dec.deduplicate else None),
                    identifiers=uniq_ids,
                    attributes=uniq_attrs,
                )
            )

        # Completeness floor: backfill any profiled table with verified candidate identifiers
        # that a small local model (e.g. 3B) silently omitted or dropped.
        included_tables = {s.table for s in sources}
        explicitly_excluded = {
            d.table for d in decisions if not d.include_in_idr and not table_map.get(d.table, SafeTableProfile(table_fqn="")).has_candidate_identifiers
        }
        for tprof in schema_profile.tables:
            if tprof.table_fqn in included_tables or tprof.table_fqn in explicitly_excluded:
                continue
            if not tprof.has_candidate_identifiers:
                continue
            hint_ids = [
                IdentifierMappingSpec(
                    type=c.hint_identifier_type.lower(),
                    expr=c.name,
                    is_hashed=c.hint_is_hashed,
                )
                for c in tprof.columns
                if c.hint_role == "identifier" and c.hint_identifier_type
            ]
            if not hint_ids:
                continue
            src_id = _sanitize_source_id(tprof.table_fqn.replace(".", "_"), tprof.table_fqn)
            if src_id in seen_ids:
                src_id = f"{src_id}_src"
            seen_ids.add(src_id)
            ek = tprof.hint_entity_key or "AUTO"
            wm = tprof.hint_watermark
            id_exprs = {h.expr.lower() for h in hint_ids}
            attrs = [
                AttributeMappingSpec(name=c.name, expr=c.name)
                for c in tprof.columns
                if c.name != ek and c.name != wm and c.name.lower() not in id_exprs and c.non_null_count > 0
            ]
            sources.append(
                SourceSpec(
                    id=src_id,
                    table=tprof.table_fqn,
                    entity_type="PERSON",
                    entity_key=ek,
                    watermark_column=wm,
                    deduplicate=tprof.duplicate_entity_keys_detected,
                    dedup_order_by=f"{wm} DESC" if (wm and tprof.duplicate_entity_keys_detected) else None,
                    identifiers=hint_ids,
                    attributes=attrs,
                )
            )
            explanations.append(
                ExplanationItem(
                    target=tprof.table_fqn,
                    summary=(
                        f"Retained via profiler completeness floor: detected identifier(s) "
                        f"{[h.type for h in hint_ids]} with entity_key='{ek}'."
                    ),
                    evidence=[f"rows={tprof.row_count}"],
                    risks=["Model omitted this identifier-bearing table; auto-populated from statistical hints."],
                    confidence="medium",
                    needs_review=True,
                )
            )

        return sources, explanations

    # =========================================================================
    # Stage 2B: Pass B (Global Rules, Fuzzy Templates, Survivorship & Lenses)
    # =========================================================================

    def _run_pass_b(
        self,
        sources: List[SourceSpec],
        schema_profile: SafeSchemaProfile,
        goal: str,
    ) -> Tuple[PassBRuleOutput, int, List[str]]:
        canonicalizers = list_canonicalizers()
        survivorship_strategies = list_survivorship_strategies()
        schema_json = PassBRuleOutput.model_json_schema()

        # Build compact summary of mapped sources + identifier stats
        id_type_stats: Dict[str, Dict[str, Any]] = {}
        attr_sources: Dict[str, List[str]] = {}
        table_map = schema_profile.table_map()

        for s in sources:
            tprof = table_map.get(s.table)
            col_map = {c.name: c for c in (tprof.columns if tprof else [])}
            for ident in s.identifiers:
                entry = id_type_stats.setdefault(
                    ident.type,
                    {"sources": [], "max_freq": 1, "max_null_rate": 0.0, "plus_tag_rate": 0.0, "phone_us10_rate": 0.0, "is_hashed": False},
                )
                entry["sources"].append(s.id)
                cp = col_map.get(ident.expr.lower())
                if cp:
                    entry["max_freq"] = max(entry["max_freq"], cp.max_frequency_count)
                    entry["max_null_rate"] = max(entry["max_null_rate"], cp.null_rate)
                    entry["plus_tag_rate"] = max(entry["plus_tag_rate"], cp.email_plus_tag_rate)
                    entry["phone_us10_rate"] = max(entry["phone_us10_rate"], cp.phone_us10_rate)
                if ident.is_hashed:
                    entry["is_hashed"] = True
            for attr in s.attributes:
                attr_sources.setdefault(attr.name, []).append(s.id)

        compact_context = {
            "sources": [
                {
                    "id": s.id,
                    "table": s.table,
                    "entity_type": s.entity_type,
                    "has_watermark": bool(s.watermark_column),
                    "identifiers": [i.type for i in s.identifiers],
                    "attributes": [a.name for a in s.attributes][:20],
                }
                for s in sources
            ],
            "identifier_type_stats": id_type_stats,
            "cross_table_overlaps": [o.to_dict() for o in schema_profile.overlaps],
            "mapped_attributes_and_sources": {k: v for k, v in list(attr_sources.items())[:30]},
            "allowed_canonicalizers": canonicalizers,
            "allowed_survivorship_strategies": survivorship_strategies,
            "allowed_fuzzy_templates": ["NAME_SOUNDEX_JW", "NAME_FIRST_LAST_JW", "EMAIL_LOCAL_LEVENSHTEIN"],
        }

        system_msg = (
            "You are a Principal MarTech & Identity Resolution Architect. "
            "Synthesize deterministic matching rules, optional fuzzy templates, survivorship rules, and multi-lens views.\n"
            "STRICT RULES:\n"
            "1. Only create exact rules for identifier types present in `identifier_type_stats`.\n"
            "2. Use ONLY canonicalizers from `allowed_canonicalizers` (e.g., EMAIL_STRIP_PLUS if plus_tag_rate >= 0.02 else EMAIL_STANDARD; PHONE_US_10 for US 10-digit phones; ALPHANUMERIC_UPPER for loyalty IDs).\n"
            "3. Set priority=1 for highest-precision personal identifiers (email, customer_id), lower priority (higher number) for shared ones (phone, device_id).\n"
            "4. Set `max_group_size` to a realistic full-warehouse cluster cap (>= 25, typically 50-1000) — remember a single customer spans many order/event rows across all tables, so NEVER copy raw sample `max_freq` (like 2 or 3) as `max_group_size`!\n"
            "5. Use ONLY strategies from `allowed_survivorship_strategies` and ONLY real attribute names from `mapped_attributes_and_sources`.\n"
            "6. For `lenses`, `include_types` and `exclude_types` MUST be UPPERCASE identifier types from `identifier_type_stats` (e.g., EMAIL, PHONE, CUSTOMER_ID, LOYALTY) — NEVER table `entity_type`s (like CUSTOMER, ORDER, ADDRESS).\n"
            "7. Only propose `fuzzy_templates` if name/email attributes exist across sources and the user goal benefits from probabilistic matching; leave empty if strict/deterministic.\n"
            "8. Provide concise plain-English reasoning, cited evidence metrics, risks, confidence, and needs_review for every item.\n"
            "Return JSON strictly matching the schema."
        )
        user_msg = (
            f"User Goal: {goal}\n\n"
            f"Mapped Sources & Overlap Telemetry:\n{json.dumps(compact_context, indent=2)}"
        )
        messages = [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg},
        ]

        valid_id_types = set(id_type_stats.keys())
        valid_source_ids = {s.id for s in sources}
        valid_attr_names = {a.name.lower() for s in sources for a in s.attributes}
        total_repairs = 0
        warnings: List[str] = []

        for attempt in range(self.max_repairs + 1):
            raw_dict = self.client.chat_structured(
                messages,
                json_schema=schema_json,
                schema_name="pass_b_rules_and_survivorship",
                temperature=0.0,
            )
            try:
                parsed = PassBRuleOutput.model_validate(raw_dict)
            except Exception as val_exc:
                if attempt < self.max_repairs:
                    total_repairs += 1
                    messages.append({"role": "assistant", "content": json.dumps(raw_dict)})
                    messages.append(
                        {
                            "role": "user",
                            "content": f"Your JSON failed schema validation: {val_exc}\nFix the JSON to strictly match the schema.",
                        }
                    )
                    continue
                raise
            errors = self._validate_pass_b(parsed, valid_id_types, valid_source_ids, valid_attr_names)
            if not errors:
                return parsed, total_repairs, warnings
            if attempt < self.max_repairs:
                total_repairs += 1
                messages.append({"role": "assistant", "content": json.dumps(raw_dict)})
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            "Fix these validation errors strictly using allowed enums and mapped IDs:\n- "
                            + "\n- ".join(errors)
                        ),
                    }
                )
            else:
                warnings.append(
                    f"Auto-sanitized {len(errors)} Pass B field(s) after {self.max_repairs} repairs."
                )
                return self._sanitize_pass_b(parsed, valid_id_types, valid_source_ids, valid_attr_names), total_repairs, warnings

        return PassBRuleOutput(), total_repairs, warnings

    def _validate_pass_b(
        self,
        out: PassBRuleOutput,
        valid_id_types: Set[str],
        valid_source_ids: Set[str],
        valid_attr_names: Optional[Set[str]] = None,
    ) -> List[str]:
        errors: List[str] = []
        if not out.rules:
            errors.append(f"At least one exact rule is required for mapped identifier types: {sorted(valid_id_types)}")
        for item in out.rules:
            r = item.rule
            if not r.match_keys:
                errors.append(f"Rule '{r.id}': match_keys cannot be empty.")
            for mk in r.match_keys:
                if mk.lower() not in valid_id_types:
                    errors.append(
                        f"Rule '{r.id}': match_key '{mk}' not in mapped identifier types {sorted(valid_id_types)}"
                    )
            try:
                validate_canonicalize_spec(r.canonicalize)
            except ValueError as exc:
                errors.append(f"Rule '{r.id}': {exc}")

        for sv_item in out.survivorship:
            sv = sv_item.rule
            if valid_attr_names and sv.attribute.lower() not in valid_attr_names:
                errors.append(
                    f"Survivorship '{sv.attribute}': not in mapped attribute names {sorted(valid_attr_names)[:20]}"
                )
            if not is_registered_survivorship_strategy(sv.strategy):
                errors.append(
                    f"Survivorship '{sv.attribute}': invalid strategy '{sv.strategy}'. "
                    f"Valid: {', '.join(list_survivorship_strategies())}"
                )
            for sp in sv.source_priority:
                if sp not in valid_source_ids:
                    errors.append(
                        f"Survivorship '{sv.attribute}': source_priority '{sp}' not in valid source IDs {sorted(valid_source_ids)}"
                    )

        valid_id_types_upper = {t.upper() for t in valid_id_types}
        for ln_item in out.lenses:
            try:
                validate_identifier(ln_item.lens.id, "lens.id")
            except ValueError as exc:
                errors.append(str(exc))
            for it in ln_item.lens.include_types:
                if it.strip().upper() not in valid_id_types_upper:
                    errors.append(
                        f"Lens '{ln_item.lens.id}': include_types '{it}' is not a mapped identifier type. "
                        f"Use UPPERCASE identifier types from {sorted(valid_id_types_upper)} (not entity_types)."
                    )
            for et in ln_item.lens.exclude_types:
                if et.strip().upper() not in valid_id_types_upper:
                    errors.append(
                        f"Lens '{ln_item.lens.id}': exclude_types '{et}' is not a mapped identifier type. "
                        f"Use UPPERCASE identifier types from {sorted(valid_id_types_upper)}."
                    )
            for sf in ln_item.lens.source_filter:
                if sf not in valid_source_ids:
                    errors.append(
                        f"Lens '{ln_item.lens.id}': source_filter '{sf}' not in valid source IDs {sorted(valid_source_ids)}"
                    )

        for ft in out.fuzzy_templates:
            try:
                validate_identifier(ft.id, "fuzzy_rule.id")
            except ValueError as exc:
                errors.append(str(exc))

        return errors

    def _sanitize_pass_b(
        self,
        out: PassBRuleOutput,
        valid_id_types: Set[str],
        valid_source_ids: Set[str],
        valid_attr_names: Optional[Set[str]] = None,
    ) -> PassBRuleOutput:
        clean_rules = []
        for item in out.rules:
            mks = [k.lower() for k in item.rule.match_keys if k.lower() in valid_id_types]
            if not mks:
                continue
            item.rule.id = _sanitize_source_id(item.rule.id, f"exact_{mks[0]}")
            item.rule.match_keys = mks
            try:
                item.rule.canonicalize = validate_canonicalize_spec(item.rule.canonicalize)
            except ValueError:
                item.rule.canonicalize = "LOWERCASE"
            clean_rules.append(item)
        out.rules = clean_rules

        clean_fuzzy = []
        for ft in out.fuzzy_templates:
            try:
                validate_identifier(ft.id, "fuzzy_rule.id")
                clean_fuzzy.append(ft)
            except ValueError:
                # Drop hallucinated/corrupted fuzzy rule IDs (e.g. ': 0.034, ' from 3B models)
                continue
        out.fuzzy_templates = clean_fuzzy

        clean_surv = []
        for sv_item in out.survivorship:
            if valid_attr_names and sv_item.rule.attribute.lower() not in valid_attr_names:
                continue
            if not is_registered_survivorship_strategy(sv_item.rule.strategy):
                sv_item.rule.strategy = "RECENCY"
            sv_item.rule.source_priority = [
                sp for sp in sv_item.rule.source_priority if sp in valid_source_ids
            ]
            clean_surv.append(sv_item)
        out.survivorship = clean_surv

        valid_id_types_upper = {t.upper() for t in valid_id_types}
        clean_lenses = []
        for ln_item in out.lenses:
            ln_item.lens.id = _sanitize_source_id(ln_item.lens.id, "custom_lens")
            ln_item.lens.include_types = [
                it.strip().upper() for it in ln_item.lens.include_types if it.strip().upper() in valid_id_types_upper
            ]
            ln_item.lens.exclude_types = [
                et.strip().upper() for et in ln_item.lens.exclude_types if et.strip().upper() in valid_id_types_upper
            ]
            ln_item.lens.source_filter = [
                sf for sf in ln_item.lens.source_filter if sf in valid_source_ids
            ]
            if ln_item.lens.include_types or ln_item.lens.exclude_types or ln_item.lens.source_filter:
                clean_lenses.append(ln_item)
        out.lenses = clean_lenses
        return out

    # =========================================================================
    # Assembly & Stage 3 Verification
    # =========================================================================

    def _assemble_plan(
        self,
        sources: List[SourceSpec],
        table_explanations: List[ExplanationItem],
        pass_b: PassBRuleOutput,
        schema_profile: SafeSchemaProfile,
    ) -> Tuple[IDRConfigSpec, ExplanationBundle]:
        fuzzy_rules_dicts: List[Dict[str, Any]] = []
        rule_explanations: List[ExplanationItem] = []
        final_rules: List[ExactRuleSpec] = []
        covered_id_types: Set[str] = set()

        for item in pass_b.rules:
            item.rule.id = _sanitize_source_id(item.rule.id, "exact_rule")
            # Normalize match_keys to lowercase so they always match sources[].identifiers[].type
            # in SQL JOIN idr_meta.identifier_mapping ON im.identifier_type = r.identifier_type
            item.rule.match_keys = [mk.strip().lower() for mk in item.rule.match_keys if mk.strip()]
            # Guardrail: prevent tiny sample max_freq (e.g. 2 or 3) from skipping real multi-table customers
            if item.rule.max_group_size < 25:
                item.rule.max_group_size = 50
            final_rules.append(item.rule)
            for mk in item.rule.match_keys:
                covered_id_types.add(mk.lower())
            rule_explanations.append(
                ExplanationItem(
                    target=item.rule.id,
                    summary=item.reasoning,
                    evidence=item.evidence,
                    risks=item.risks,
                    confidence=item.confidence,
                    needs_review=item.needs_review,
                )
            )

        # Completeness floor for exact rules: ensure every mapped identifier type has a matching rule
        table_map = schema_profile.table_map()
        id_type_meta: Dict[str, Dict[str, Any]] = {}
        for s in sources:
            tprof = table_map.get(s.table)
            col_map = {c.name.lower(): c for c in (tprof.columns if tprof else [])}
            for ident in s.identifiers:
                itype = ident.type.lower()
                meta = id_type_meta.setdefault(
                    itype, {"sources": 0, "max_freq": 1, "canon": None}
                )
                meta["sources"] += 1
                cp = col_map.get(ident.expr.lower())
                if cp:
                    meta["max_freq"] = max(meta["max_freq"], cp.max_frequency_count)
                    if cp.hint_canonicalizer and not meta["canon"]:
                        meta["canon"] = cp.hint_canonicalizer

        order_pref = {"email": 1, "phone": 2, "loyalty": 3, "loyalty_id": 3, "customer_id": 4, "device_id": 5}
        next_prio = max((r.priority for r in final_rules), default=0) + 1
        had_llm_rules = bool(final_rules)
        for itype in sorted(id_type_meta.keys(), key=lambda k: (order_pref.get(k, 10), k)):
            if itype in covered_id_types:
                continue
            meta = id_type_meta[itype]
            # If the LLM already proposed rules, only backfill multi-source identifier types it missed
            if had_llm_rules and meta["sources"] < 2:
                continue
            canon = meta["canon"] or (
                "EMAIL_STANDARD" if "email" in itype else (
                    "PHONE_DIGITS" if "phone" in itype else (
                        "ALPHANUMERIC_UPPER" if "loyalty" in itype else "LOWERCASE"
                    )
                )
            )
            mgs = max(25, min(500, meta["max_freq"] * 10))
            rid = _sanitize_source_id(f"exact_{itype}", "exact_rule")
            final_rules.append(
                ExactRuleSpec(
                    id=rid,
                    type="EXACT",
                    match_keys=[itype],
                    canonicalize=canon,
                    priority=next_prio,
                    max_group_size=mgs,
                )
            )
            rule_explanations.append(
                ExplanationItem(
                    target=rid,
                    summary=(
                        f"Deterministic exact match on '{itype}' across {meta['sources']} source(s) "
                        f"using {canon} (max_group_size={mgs})."
                    ),
                    evidence=[f"sources={meta['sources']}", f"max_freq={meta['max_freq']}"],
                    confidence="medium",
                    needs_review=meta["sources"] == 1,
                )
            )
            next_prio += 1

        # Ground fuzzy templates strictly against mapped attribute names
        mapped_attr_names: Set[str] = {
            a.name.lower() for s in sources for a in s.attributes
        }
        for ft in pass_b.fuzzy_templates:
            try:
                validate_identifier(ft.id, "fuzzy_rule.id")
            except ValueError:
                continue
            # Ensure template columns exist in mapped attributes
            req_cols = [c.lower() for c in ft.columns if c.lower() in mapped_attr_names]
            if not req_cols:
                continue
            try:
                expanded = expand_fuzzy_template(
                    template=ft.template,
                    rule_id=ft.id,
                    name=ft.name,
                    columns=req_cols,
                    threshold=ft.threshold,
                    priority=ft.priority,
                )
                fuzzy_rules_dicts.append(expanded)
                rule_explanations.append(
                    ExplanationItem(
                        target=ft.id,
                        summary=f"Fuzzy matching template {ft.template} on {', '.join(req_cols)} (threshold={ft.threshold}).",
                        evidence=[f"template={ft.template}", f"threshold={ft.threshold}"],
                        risks=["Probabilistic matching (beta); review false-positive rate in dry run."],
                        confidence="medium",
                        needs_review=True,
                    )
                )
            except ValueError:
                continue

        final_surv: List[SurvivorshipSpec] = [sv.rule for sv in pass_b.survivorship]
        surv_explanations = [
            ExplanationItem(
                target=sv.rule.attribute,
                summary=sv.reasoning,
                evidence=[f"strategy={sv.rule.strategy}"],
                confidence=sv.confidence,
                needs_review=sv.needs_review,
            )
            for sv in pass_b.survivorship
        ]

        # Completeness floor for survivorship: if LLM returned none, populate sensible defaults
        if not final_surv and mapped_attr_names:
            attr_to_sources: Dict[str, List[SourceSpec]] = {}
            consent_attrs: Set[str] = set()
            for s in sources:
                tprof = table_map.get(s.table)
                col_map = {c.name.lower(): c for c in (tprof.columns if tprof else [])}
                for a in s.attributes:
                    attr_to_sources.setdefault(a.name, []).append(s)
                    cp = col_map.get(a.expr.lower())
                    if cp and cp.hint_role == "consent_flag":
                        consent_attrs.add(a.name)
            for aname in sorted(attr_to_sources.keys()):
                srcs = attr_to_sources[aname]
                if aname in consent_attrs:
                    strat = "ANY_TRUE"
                    sp = []
                elif any(s.watermark_column for s in srcs):
                    strat = "RECENCY"
                    sp = [s.id for s in srcs]
                else:
                    strat = "PRIORITY"
                    sp = [s.id for s in srcs]
                final_surv.append(
                    SurvivorshipSpec(attribute=aname, strategy=strat, source_priority=sp)
                )
                surv_explanations.append(
                    ExplanationItem(
                        target=aname,
                        summary=f"Baseline {strat} survivorship across {len(srcs)} source(s).",
                        evidence=[f"strategy={strat}"],
                        confidence="medium",
                        needs_review=False,
                    )
                )

        lens_explanations = [
            ExplanationItem(
                target=ln.lens.id,
                summary=ln.reasoning,
                evidence=[f"include_types={ln.lens.include_types}"],
                confidence=ln.confidence,
                needs_review=ln.needs_review,
            )
            for ln in pass_b.lenses
        ]

        spec = IDRConfigSpec(
            use_default_exclusions=pass_b.use_default_exclusions,
            sources=sources,
            rules=final_rules,
            fuzzy_rules=fuzzy_rules_dicts,
            survivorship=final_surv,
            lenses=[ln.lens for ln in pass_b.lenses],
        )
        explanations = ExplanationBundle(
            tables=table_explanations,
            rules=rule_explanations,
            survivorship=surv_explanations,
            lenses=lens_explanations,
        )
        return spec, explanations

    def validate_plan(
        self,
        config_spec: IDRConfigSpec,
        schema_profile: SafeSchemaProfile,
        verify_sql: bool = True,
    ) -> ValidationReport:
        """
        Multi-layer validation:
        1. Core `validate_config()` + identifier/FQN/SQL-injection checks
        2. Grounding against `SafeSchemaProfile` (zero hallucinated tables/columns)
        3. Registry checks (canonicalizers + survivorship strategies)
        4. Optional warehouse `EXPLAIN` check on source expressions
        """
        errors: List[str] = []
        warnings: List[str] = []
        cfg_dict = config_spec.to_idr_config_dict()

        # Layer 1: Core config validator
        try:
            validate_config(cfg_dict)
        except Exception as exc:
            errors.append(f"validate_config: {exc}")

        table_map = schema_profile.table_map()
        mapped_id_type_counts: Dict[str, int] = {}
        valid_source_ids = {s.id for s in config_spec.sources}

        # Layer 2: Grounding & SQL expression safety
        for src in config_spec.sources:
            try:
                validate_identifier(src.id, "source.id")
                validate_fqn(src.table, "source.table")
            except ValueError as exc:
                errors.append(str(exc))

            tprof = table_map.get(src.table)
            if tprof is None:
                errors.append(f"Source '{src.id}' references unprofiled table '{src.table}'.")
                continue

            valid_cols = tprof.column_names()
            if src.entity_key.upper() != "AUTO":
                try:
                    validate_sql_expr(src.entity_key, f"{src.id}.entity_key")
                except ValueError as exc:
                    errors.append(str(exc))
                if not _extract_column_refs(src.entity_key, valid_cols):
                    errors.append(f"Source '{src.id}': entity_key '{src.entity_key}' not in {src.table}.")

            if src.watermark_column and src.watermark_column.lower() not in valid_cols:
                errors.append(
                    f"Source '{src.id}': watermark_column '{src.watermark_column}' not in {src.table}."
                )

            for ident in src.identifiers:
                try:
                    validate_sql_expr(ident.expr, f"{src.id}.identifier.{ident.type}")
                except ValueError as exc:
                    errors.append(str(exc))
                if not _extract_column_refs(ident.expr, valid_cols):
                    errors.append(
                        f"Source '{src.id}': identifier '{ident.type}' expr '{ident.expr}' not in {src.table}."
                    )
                mapped_id_type_counts[ident.type.lower()] = (
                    mapped_id_type_counts.get(ident.type.lower(), 0) + 1
                )

            for attr in src.attributes:
                try:
                    validate_sql_expr(attr.expr, f"{src.id}.attribute.{attr.name}")
                except ValueError as exc:
                    errors.append(str(exc))
                if not _extract_column_refs(attr.expr, valid_cols):
                    errors.append(
                        f"Source '{src.id}': attribute '{attr.name}' expr '{attr.expr}' not in {src.table}."
                    )

        # Layer 3: Rules & survivorship registry checks
        if mapped_id_type_counts and not config_spec.rules:
            errors.append("Configuration has mapped identifiers but 0 exact matching rules.")
        for rule in config_spec.rules:
            try:
                validate_identifier(rule.id, "rule.id")
            except ValueError as exc:
                errors.append(str(exc))
            try:
                validate_canonicalize_spec(rule.canonicalize)
            except ValueError as exc:
                errors.append(f"Rule '{rule.id}': {exc}")
            for mk in rule.match_keys:
                cnt = mapped_id_type_counts.get(mk.lower(), 0)
                if cnt == 0:
                    errors.append(f"Rule '{rule.id}': match_key '{mk}' is not mapped in any source.")
                elif cnt == 1 and len(config_spec.sources) > 1:
                    warnings.append(
                        f"Rule '{rule.id}' ({mk}) is only mapped in 1 source; it will only deduplicate within that table."
                    )

        for fr in config_spec.fuzzy_rules:
            try:
                validate_identifier(str(fr.get("id") or ""), "fuzzy_rule.id")
            except ValueError as exc:
                errors.append(str(exc))

        for sv in config_spec.survivorship:
            if not is_registered_survivorship_strategy(sv.strategy):
                errors.append(f"Survivorship '{sv.attribute}': unknown strategy '{sv.strategy}'.")
            for sp in sv.source_priority:
                if sp not in valid_source_ids:
                    errors.append(
                        f"Survivorship '{sv.attribute}': unknown source_priority '{sp}'."
                    )

        # Layer 4: Live warehouse SQL dry-parse verification (read-only, parallelized)
        sql_verified = False
        if verify_sql and not errors and self.adapter is not None:
            def _probe_source(src: SourceSpec) -> Optional[str]:
                select_exprs: List[str] = []
                if src.entity_key.upper() != "AUTO":
                    select_exprs.append(f"({src.entity_key}) AS _ek")
                if src.watermark_column:
                    select_exprs.append(f"({src.watermark_column}) AS _wm")
                for idx, ident in enumerate(src.identifiers):
                    select_exprs.append(f"({ident.expr}) AS _id_{idx}")
                for idx, attr in enumerate(src.attributes[:20]):
                    select_exprs.append(f"({attr.expr}) AS _at_{idx}")
                if not select_exprs:
                    return None
                probe_sql = f"SELECT {', '.join(select_exprs)} FROM {src.table} LIMIT 0"
                try:
                    self.adapter.query(probe_sql)
                    return None
                except Exception as exc:
                    return f"SQL verification failed for {src.table}: {exc}"

            if len(config_spec.sources) > 1:
                from concurrent.futures import ThreadPoolExecutor

                with ThreadPoolExecutor(max_workers=min(6, len(config_spec.sources))) as pool:
                    probe_errs = [e for e in pool.map(_probe_source, config_spec.sources) if e]
            else:
                probe_errs = [e for e in (_probe_source(s) for s in config_spec.sources) if e]

            if probe_errs:
                errors.extend(probe_errs)
                sql_verified = False
            else:
                sql_verified = True

        return ValidationReport(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            sql_verified=sql_verified,
        )

    # =========================================================================
    # Deterministic Fallback (Zero-Dependency Guarantee)
    # =========================================================================

    def _build_deterministic_fallback(
        self,
        schema_profile: SafeSchemaProfile,
        tables: List[str],
        sample_size: int,
        reason: str,
    ) -> GeneratedIDRPlan:
        det = self.profiler.profile_and_generate_config(tables, sample_size=min(sample_size, 1000))
        cfg = det.get("suggested_config") or {}
        table_map = schema_profile.table_map()

        tbl_exps: List[ExplanationItem] = []
        for fqn, diag in (det.get("profiling_diagnostics") or {}).items():
            tprof = table_map.get(fqn)
            included = bool(diag.get("included"))
            if included:
                tbl_exps.append(
                    ExplanationItem(
                        target=fqn,
                        summary=(
                            f"Included with entity_key='{diag.get('selected_entity_key')}' "
                            f"and identifiers={diag.get('identifiers_mapped')}."
                        ),
                        evidence=[
                            f"rows={tprof.row_count if tprof else 'N/A'}",
                            f"sample_rows={diag.get('sample_rows_analyzed', 0)}",
                        ],
                        risks=(
                            ["Duplicate entity keys detected; deduplicate enabled."]
                            if diag.get("duplicate_entity_keys_detected")
                            else []
                        ),
                        confidence="medium",
                        needs_review=bool(diag.get("duplicate_entity_keys_detected")),
                    )
                )
            else:
                tbl_exps.append(
                    ExplanationItem(
                        target=fqn,
                        summary=str(diag.get("reason") or "Excluded: no identifiers detected."),
                        confidence="high",
                        needs_review=False,
                    )
                )

        rule_exps = [
            ExplanationItem(
                target=str(r.get("id")),
                summary=(
                    f"Deterministic exact match on {r.get('match_keys')} "
                    f"using {r.get('canonicalize')} (max_group_size={r.get('max_group_size')})."
                ),
                evidence=[f"priority={r.get('priority')}"],
                confidence="medium",
                needs_review=False,
            )
            for r in (cfg.get("rules") or [])
        ]

        surv_exps = [
            ExplanationItem(
                target=str(s.get("attribute")),
                summary=f"Selected {s.get('strategy')} survivorship based on source watermark availability.",
                confidence="medium",
                needs_review=False,
            )
            for s in (cfg.get("survivorship") or [])
        ]

        return GeneratedIDRPlan(
            generation_mode="deterministic_fallback",
            provider=self.client.config.provider,
            model=self.client.config.resolved_model() if self.client.config.is_enabled else "profiler",
            pii_masked=True,
            suggested_config=cfg,
            explanations=ExplanationBundle(
                tables=tbl_exps,
                rules=rule_exps,
                survivorship=surv_exps,
            ),
            validation_report=ValidationReport(
                valid=bool(det.get("is_valid_idr_config")),
                errors=[det["config_validation_error"]] if det.get("config_validation_error") else [],
                warnings=[reason],
            ),
            profile_summary=schema_profile.to_summary_dict(),
        )


def render_config_yaml_with_reasoning(plan: GeneratedIDRPlan) -> str:
    """Render a generated IDR config to YAML with concise top-level reasoning comments."""
    lines = [
        "# =====================================================================",
        "# SQL Identity Resolution (IDR) — AI-Generated Configuration",
        f"# Mode: {plan.generation_mode} | Provider: {plan.provider} | Model: {plan.model}",
        f"# Zero-PII Profiling: True | Validated: {plan.validation_report.valid}",
        "# =====================================================================",
    ]
    if plan.explanations.rules:
        lines.append("#")
        lines.append("# Rule Reasoning Summary:")
        for r in plan.explanations.rules:
            flag = " [NEEDS REVIEW]" if r.needs_review else ""
            lines.append(f"#   - {r.target} ({r.confidence}{flag}): {r.summary}")
    if plan.validation_report.warnings:
        lines.append("#")
        lines.append("# Warnings:")
        for w in plan.validation_report.warnings:
            lines.append(f"#   ! {w}")
    lines.append("")
    yaml_body = yaml.safe_dump(
        plan.suggested_config, sort_keys=False, default_flow_style=False, indent=2
    )
    return "\n".join(lines) + yaml_body
