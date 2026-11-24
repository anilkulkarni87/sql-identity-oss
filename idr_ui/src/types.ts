export interface Identifier {
    type: string;
    column: string; // Used in UI state (locally 'column', mapped to 'expr' in config)
    expr?: string;  // Used in final config
    issuer?: string;
    issuer_expr?: string;
    is_hashed?: boolean;
    source_is_hashed?: boolean;
    is_match_key?: boolean; // If true, used in matching rules
}

export interface Attribute {
    name: string;
    column: string; // Used in UI state
    expr?: string; // Used in final config
    data_type?: string; // Warehouse SQL column type (UI metadata)
}

export interface SourceConfig {
    id: string;
    table: string; // Table FQN
    entity_type?: string;
    entity_kind?: string;
    entity_key?: string;
    key_columns?: string[];
    deduplicate?: boolean;
    dedup_order_by?: string;
    watermark_column?: string;
    watermark_lookback_minutes?: number;
    identifiers?: Identifier[];
    attributes?: Attribute[];
    // UI specific helpers might be added here if needed, but keeping it clean
}

export interface MatchingRule {
    id: number | string;
    type: string; // 'EXACT' or 'FUZZY'
    match_keys: string[]; // For EXACT: identifiers to match
    priority: number;
    canonicalize?: string; // Presets (EMAIL_STANDARD, EMAIL_STRIP_PLUS, PHONE_US_10, PHONE_DIGITS, ALPHANUMERIC_UPPER, LOWERCASE, UPPERCASE, EXACT) or custom {val} SQL
    max_group_size?: number;
    // Fuzzy Specific
    blocking_key?: string;
    fuzzy_attribute?: string;
    score_expr?: string;
    threshold?: number;
}

export interface ExclusionRule {
    type: string;
    value: string;
    match?: 'EXACT' | 'LIKE';
    match_type?: 'EXACT' | 'LIKE';
    reason?: string;
}

export interface OverrideRule {
    override_type: 'FORCE_LINK' | 'DO_NOT_LINK' | 'ISOLATE';
    entity_key_a: string;
    entity_key_b?: string;
    reason?: string;
}

export interface SurvivorshipRule {
    attribute: string;
    strategy:
        | 'RECENCY'
        | 'PRIORITY'
        | 'PRIORITY_RECENCY'
        | 'LATEST_FROM_SOURCE'
        | 'FREQUENCY'
        | 'ANY_TRUE'
        | 'ALL_TRUE'
        | 'EARLIEST'
        | 'AGG_MAX'
        | 'AGG_MIN'
        | 'AGG_SUM'
        | 'COLLECT_DISTINCT'
        | (string & {});
    source_priority?: string[]; // List of table names or IDs
    recency_field?: string;
    row_group?: string;
    strict_source?: boolean;
}

export interface LensRule {
    id: string;
    include_types?: string[];
    exclude_types?: string[];
    source_filter?: string[];
    max_group_size?: number;
}

export interface AIConfigContext {
    version: 1;
    grain: 'individual' | 'household' | 'account';
    event_policy: 'bridge' | 'include' | 'exclude';
    approved_event_aggregates?: Record<string, Record<string, 'AGG_SUM' | 'AGG_MIN' | 'AGG_MAX'>>;
    excluded_tables: string[];
    system_of_record?: string | null;
    allowed_matching_identifiers?: string[] | null;
    locked_fields: string[];
    table_roles?: Record<string, string>;
    objectives?: Array<'precision' | 'coverage' | 'cross_source' | 'auditability'>;
    fuzzy: boolean;
    requested_lenses: string[];
}

export const defaultAIContext: AIConfigContext = {
    version: 1, grain: 'individual', event_policy: 'bridge', excluded_tables: [],
    locked_fields: [], fuzzy: false, requested_lenses: [],
};

export function localGoalObjectives(note: string): NonNullable<AIConfigContext['objectives']> {
    const matches: NonNullable<AIConfigContext['objectives']> = [];
    if (/precision|false.positive|conservative|accurate/i.test(note)) matches.push('precision');
    if (/coverage|recall|missing.match/i.test(note)) matches.push('coverage');
    if (/cross.source|across.source|cross.channel/i.test(note)) matches.push('cross_source');
    if (/audit|explain|traceab/i.test(note)) matches.push('auditability');
    return matches;
}

export interface ReviewPatch {
    action: 'remove_rule' | 'set_group_limit' | 'raise_fuzzy_threshold' | 'set_lens_types';
    label: string;
    target: string;
    value: unknown;
    base_draft_hash: string;
    measured: boolean;
    tradeoff: string;
}

export interface AIReview {
    run_id: string;
    run_mode?: string;
    execution_options?: {run_mode?: string; strict?: boolean; max_iters?: number; max_group_size?: number | null};
    draft_hash: string;
    lens: string;
    status: 'no_detected_risks' | 'needs_review' | 'high_risk' | 'insufficient_evidence';
    health: string;
    headline: string;
    provider: string;
    model: string;
    pii_masked: boolean;
    telemetry: {
        total_entities: number; total_clusters: number; singletons: number;
        singleton_pct: number; singleton_entity_pct?: number; max_cluster_size: number; large_clusters: number;
        edges_by_type: Record<string, number>;
        rule_contributions?: Array<{rule_id: string; edge_count: number; causal_effect_measured: boolean}>;
        source_coverage?: Array<{source_id: string; total_entities: number; singleton_entities: number}>;
        skipped_supernodes: Array<{ identifier_type: string; skipped_groups: number; max_group_size_seen: number }>;
    };
    findings: Array<{ severity: 'high' | 'medium' | 'info'; title: string; detail: string; metric: string; recommendation?: string; expected_effect?: string; tradeoff?: string; current_value?: number; proposed_value?: number | null; patch?: ReviewPatch | null }>;
    evaluation?: Record<string, number | null> | null;
    evaluation_acceptance?: {status:string; reasons:string[]; scope:string};
    profile_validation?: ProfileValidation;
    limitations: string[];
    comparison?: { baseline_run_id: string; changes: Record<string, number>; same_extraction: boolean; patch: ReviewPatch };
}

export interface ActivationDeltaConfig {
    enabled: boolean;
    include_lenses?: boolean;
    seed_initial_full?: boolean;
    retention_days?: number;
}

export interface ActivationDeltaRow {
    run_id: string;
    lens_id: string;
    resolved_id: string;
    activation_status: 'ACTIVE' | 'RETIRED' | string;
    change_type: 'NEW' | 'MERGED' | 'SPLIT' | 'PROFILE_CHANGED' | 'RETIRED_MERGED' | 'RETIRED_DELETED' | string;
    previous_resolved_ids_json: string;
    superseded_by_resolved_id?: string | null;
    cluster_size: number;
    entities_added: number;
    entities_removed: number;
    profile_changed: boolean;
    confidence_score?: number | null;
    emitted_at: string;
}

export interface IDRConfig {
    grouped_outputs?: Array<{id:string;kind?:'grouped_state';dimensions:string[];fields:string[];sources:Array<{source_id:string;dimensions:Record<string,string>;fields:Record<string,string>;recency_field?:string}>;strategy?:'RECENCY'|'PRIORITY_RECENCY';source_priority?:string[];conflict_policy?:'error'|'mark';null_dimensions?:'error'|'keep'}>;
    activation_delta?: ActivationDeltaConfig;
    evaluation?: EvaluationOptions;
    sources: SourceConfig[];
    ai_assistance?: AIConfigContext;
    rules?: MatchingRule[];
    fuzzy_rules?: MatchingRule[];
    survivorship?: SurvivorshipRule[];
    exclusions?: ExclusionRule[];
    overrides?: OverrideRule[];
    lenses?: LensRule[];
    use_default_exclusions?: boolean;
}

export interface TableColumn {
    name: string;
    type: string;
}

export interface EvaluationOptions {
    ground_truth_table: string; entity_key_col?: string; truth_id_col?: string;
    source_mappings?: Array<{source_id: string; key_columns: Record<string,string>; table?: string; truth_id_col?: string; source_column?: string; source_value?: string}>;
    max_label_rows?: number; top_n_errors?: number; min_evaluated_entities?: number; min_true_pairs?: number;
    min_label_overlap?: number; min_precision?: number; min_recall?: number;
}
export interface ProfileValidation {
    mode: 'validate'; status: string; data_scanned: boolean; source_scan_cost: string;
    attributes: Array<{attribute: string; strategy: string; sources: string[]}>; grouped_outputs?: Array<{id:string;dimensions:string[];fields:string[]}>; limitations: string[];
}
export interface ProfilePreviewResult {
    grouped_outputs?: Record<string,{rows:number;conflicts:number}>;
    mode: 'targeted' | 'full'; status: string; selected_clusters: number; selected_members: number; profile_count: number;
    attributes: Array<{attribute: string; changed_profiles?: number; paired_profiles?: number; after: {profiles:number; non_null:number; sample_sum?: number | string | null}; before?: {profiles:number; non_null:number; sample_sum?: number | string | null}}>;
    limitations: string[];
}
