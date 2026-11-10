export interface Identifier {
    type: string;
    column: string; // Used in UI state (locally 'column', mapped to 'expr' in config)
    expr?: string;  // Used in final config
    is_hashed?: boolean;
    is_match_key?: boolean; // If true, used in matching rules
}

export interface Attribute {
    name: string;
    column: string; // Used in UI state
    expr?: string; // Used in final config
}

export interface SourceConfig {
    id: string;
    table: string; // Table FQN
    entity_key?: string;
    watermark_column?: string;
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
    strategy: 'RECENCY' | 'PRIORITY' | 'FREQUENCY' | 'AGG_MAX' | 'AGG_SUM';
    source_priority?: string[]; // List of table names or IDs
    recency_field?: string;
}

export interface IDRConfig {
    sources: SourceConfig[];
    rules?: MatchingRule[];
    fuzzy_rules?: MatchingRule[];
    survivorship?: SurvivorshipRule[];
    exclusions?: ExclusionRule[];
    overrides?: OverrideRule[];
    use_default_exclusions?: boolean;
}

export interface TableColumn {
    name: string;
    type: string;
}
