import AIConfigPolicy from '../../components/AIConfigPolicy';
import AIProposalReview from '../../components/AIProposalReview';
import { defaultAIContext, localGoalObjectives } from '../../types';
import { useState, useEffect, useRef } from 'react';
import {
    Loader2, Key, Tag, Fingerprint, Clock, CheckSquare, Square,
    Plus, Search, Bot, Table as TableIcon, Trash2, ChevronDown,
    ChevronUp, AlertCircle, Check
} from "lucide-react";
import { IDRConfig, SourceConfig, TableColumn, Identifier, Attribute } from '../../types';
import { api } from '../../api/client';
import { CustomSelect } from '../../components/CustomSelect';
import { AiProgressStepper, AiProgressEvent } from '../../components/AiProgressStepper';

interface StepMapProps {
    tables: string[];
    initialConfig?: IDRConfig;
    connectionData?: any;
    onTablesChange?: (tables: string[]) => void;
    onChange?: (config: IDRConfig) => void;
    onNext: (config: IDRConfig) => void;
    onBack?: () => void;
}

const IDENTIFIER_PATTERNS: Array<{ pattern: RegExp; type: string }> = [
    { pattern: /email|e_mail/i, type: 'email' },
    { pattern: /phone|mobile|cell|tel/i, type: 'phone' },
    { pattern: /ssn|social_security|tax_id/i, type: 'ssn' },
    { pattern: /device_id|device/i, type: 'device_id' },
    { pattern: /cookie|maid|idfa|gaid|anonymous_id/i, type: 'cookie_id' },
    { pattern: /loyalty|member_id/i, type: 'loyalty_id' },
    { pattern: /crm_id|salesforce_id|hubspot_id|external_id/i, type: 'external_id' },
];

const GENERIC_HOMONYM_ATTRS = new Set([
    'status', 'state', 'type', 'category', 'channel', 'source', 'reason',
    'stage', 'code', 'notes', 'description', 'comments', 'comment',
    'created_at', 'updated_at', 'timestamp', 'event_time', 'event_date',
    'amount', 'total', 'subtotal', 'price', 'value', 'score', 'rating',
    'count', 'quantity', 'qty',
]);

function suggestAliasedAttrName(tableFqn: string, attrName: string): string {
    const shortTbl = (tableFqn.split('.').pop() || tableFqn)
        .toLowerCase()
        .replace(/[^a-z0-9_]+/g, '_')
        .replace(/^_+|_+$/g, '');
    const cleanAttr = attrName.toLowerCase().replace(/[^a-z0-9_]+/g, '_').replace(/^_+|_+$/g, '');
    if (!shortTbl || cleanAttr.startsWith(`${shortTbl}_`)) return cleanAttr;
    return `${shortTbl}_${cleanAttr}`;
}

function guessDefaultWatermark(cols: TableColumn[], entityKey?: string): string {
    const wmKeywords = [
        'updated_at', 'modified_at', 'last_modified', 'timestamp',
        'created_at', 'transaction_date', 'order_date', 'event_date',
        'event_ts', 'last_visit_date', 'enrolled_date', 'assigned_at',
        'flagged_at', 'review_date', '_at', '_date', '_ts'
    ];
    const match = (
        cols.find(c => c.name !== entityKey && wmKeywords.some(k => c.name.toLowerCase().includes(k))) ||
        cols.find(c => c.name !== entityKey && ['timestamp', 'date', 'datetime'].some(t => (c.type || '').toLowerCase().includes(t)))
    );
    return match?.name || '';
}

function defaultCanonicalizerFor(type: string): string {
    const lower = type.toLowerCase();
    if (lower.includes('email')) return 'EMAIL_STANDARD';
    if (lower.includes('phone')) return 'PHONE_US_10';
    return 'LOWERCASE';
}

export default function StepMap({
    tables,
    initialConfig,
    connectionData,
    onTablesChange,
    onChange,
    onNext,
    onBack,
}: StepMapProps) {
    const [columns, setColumns] = useState<Record<string, TableColumn[]>>({});
    const [config, setConfig] = useState<Record<string, SourceConfig>>(() => {
        const init: Record<string, SourceConfig> = {};
        for (const src of initialConfig?.sources || []) {
            if (src.table) {
                init[src.table] = {
                    ...src,
                    identifiers: (src.identifiers || []).map(i => ({
                        ...i,
                        column: i.column || i.expr || '',
                        expr: i.expr || i.column || '',
                    })),
                    attributes: (src.attributes || []).map(a => ({
                        ...a,
                        column: a.column || a.expr || a.name,
                        expr: a.expr || a.column || a.name,
                    })),
                };
            }
        }
        return init;
    });

    const [activeTable, setActiveTable] = useState<string | null>(tables[0] || null);
    const [loadingCols, setLoadingCols] = useState(false);
    const [colSearch, setColSearch] = useState('');

    // Inline Discovery Drawer State
    const [discoveryOpen, setDiscoveryOpen] = useState(tables.length === 0);
    const [schemaInput, setSchemaInput] = useState<string>(
        connectionData?.params?.schema || connectionData?.params?.dataset || ''
    );
    const [discoveredTables, setDiscoveredTables] = useState<string[]>([]);
    const [tableFilter, setTableFilter] = useState('');
    const [customTableFqn, setCustomTableFqn] = useState('');
    const [scanLoading, setScanLoading] = useState(false);
    const [scanError, setScanError] = useState<string | null>(null);
    const [validationError, setValidationError] = useState<string | null>(null);
    const [aiSuggesting, setAiSuggesting] = useState(false);
    const [aiProgress, setAiProgress] = useState<AiProgressEvent | null>(null);
    const [aiNotice, setAiNotice] = useState<string | null>(null);
    const [aiGoal, setAiGoal] = useState<string>('');
    const [aiGrain, setAiGrain] = useState<'individual' | 'household' | 'account'>(initialConfig?.ai_assistance?.grain || 'individual');
    const [aiEventPolicy, setAiEventPolicy] = useState<'bridge' | 'include' | 'exclude'>(initialConfig?.ai_assistance?.event_policy || 'bridge');
    const [aiSystemOfRecord, setAiSystemOfRecord] = useState<string>(initialConfig?.ai_assistance?.system_of_record || '');
    const [aiFuzzy, setAiFuzzy] = useState(initialConfig?.ai_assistance?.fuzzy || false);
    const [proposalBase, setProposalBase] = useState('');
    const [advancedPolicy, setAdvancedPolicy] = useState(initialConfig?.ai_assistance || {...defaultAIContext});
    const [aiReport, setAiReport] = useState<Awaited<ReturnType<typeof api.suggestConfigAi>> | null>(null);
    const [aiReportOpen, setAiReportOpen] = useState<boolean>(true);

    const generationContext = {...advancedPolicy, grain: aiGrain, event_policy: aiEventPolicy,
        system_of_record: aiSystemOfRecord || null, fuzzy: aiFuzzy,
        objectives: Array.from(new Set([...(advancedPolicy.objectives || []), ...localGoalObjectives(aiGoal)]))};

    useEffect(() => {
        const next = initialConfig?.ai_assistance;
        if (next) {
            setAiGrain(next.grain); setAiEventPolicy(next.event_policy);
            setAiSystemOfRecord(next.system_of_record || ''); setAiFuzzy(next.fuzzy); setAdvancedPolicy(next);
        }
    }, [initialConfig?.ai_assistance]);

    const latestInitialConfigRef = useRef<IDRConfig | undefined>(initialConfig);
    latestInitialConfigRef.current = initialConfig;

    const latestColumnsRef = useRef<Record<string, TableColumn[]>>(columns);
    latestColumnsRef.current = columns;

    const latestConfigRef = useRef<Record<string, SourceConfig>>(config);
    latestConfigRef.current = config;

    const initialAutoScannedRef = useRef(false);
    const lastEmittedSourcesSigRef = useRef<string>('');

    // Keep config synced when an external source (like JSON import or warehouse hydrate) replaces initialConfig
    useEffect(() => {
        if (!initialConfig?.sources) return;
        const incomingSig = JSON.stringify(initialConfig.sources);
        if (incomingSig === lastEmittedSourcesSigRef.current) return;
        lastEmittedSourcesSigRef.current = incomingSig;

        const next: Record<string, SourceConfig> = {};
        for (const src of initialConfig.sources) {
            if (src.table) {
                next[src.table] = {
                    ...src,
                    identifiers: (src.identifiers || []).map(i => ({
                        ...i,
                        column: i.column || i.expr || '',
                        expr: i.expr || i.column || '',
                    })),
                    attributes: (src.attributes || []).map(a => ({
                        ...a,
                        column: a.column || a.expr || a.name,
                        expr: a.expr || a.column || a.name,
                    })),
                };
            }
        }
        setConfig(next);
    }, [initialConfig?.sources]);

    // Ensure activeTable stays valid when tables change
    useEffect(() => {
        if (tables.length === 0) {
            setActiveTable(null);
            setDiscoveryOpen(true);
        } else if (!activeTable || !tables.includes(activeTable)) {
            setActiveTable(tables[0]);
        }
    }, [tables, activeTable]);

    // Auto-scan initial schema once when no tables are selected yet
    useEffect(() => {
        if (!initialAutoScannedRef.current && tables.length === 0) {
            initialAutoScannedRef.current = true;
            const defaultSchema = connectionData?.params?.schema || connectionData?.params?.dataset || '';
            if (defaultSchema || connectionData?.platform === 'duckdb' || !connectionData) {
                runSchemaScan(defaultSchema || undefined);
            }
        }
    }, [connectionData, tables.length]);

    const buildFinalConfig = (
        srcMap: Record<string, SourceConfig>,
        tableList: string[],
        colsMap: Record<string, TableColumn[]>
    ): IDRConfig => {
        const base = latestInitialConfigRef.current || { sources: [] };
        const builtSources: SourceConfig[] = tableList
            .filter(t => Boolean(srcMap[t]))
            .map(t => {
                const s = srcMap[t];
                return {
                    ...s,
                    id: s.id || t.replace(/\./g, '_'),
                    table: t,
                    entity_key: s.entity_key,
                    ...(s.key_columns && s.key_columns.length > 0 ? { key_columns: s.key_columns } : {}),
                    ...(s.deduplicate !== undefined ? { deduplicate: s.deduplicate } : {}),
                    ...(s.dedup_order_by ? { dedup_order_by: s.dedup_order_by } : {}),
                    watermark_column: s.watermark_column,
                    watermark_lookback_minutes: s.watermark_lookback_minutes || 0,
                    identifiers: (s.identifiers || []).map(i => ({
                        ...i,
                        type: i.type,
                        column: i.column || i.expr || '',
                        expr: i.column || i.expr || '',
                        ...(i.is_match_key === false ? { is_match_key: false } : {}),
                    })),
                    attributes: (s.attributes || []).map(a => {
                        const colMeta = (colsMap[t] || []).find(c => c.name === (a.column || a.name));
                        return a.data_type || !colMeta?.type ? a : { ...a, data_type: colMeta.type };
                    }),
                };
            });

        const allIdentifiers = builtSources.flatMap(s => s.identifiers || []);
        const enabledTypes = Array.from(
            new Set(
                allIdentifiers
                    .filter(i => i.is_match_key !== false && i.type && i.type.trim() !== '')
                    .map(i => i.type.trim())
            )
        );

        let rules = base.rules && base.rules.length > 0 ? base.rules : [];
        if (rules.length === 0 && enabledTypes.length > 0) {
            rules = enabledTypes.map((type, idx) => ({
                id: 100 + idx,
                type: 'EXACT',
                match_keys: [type],
                priority: idx + 1,
                canonicalize: defaultCanonicalizerFor(type),
                max_group_size: 10000,
            }));
        }

        return {
            ...base,
            sources: builtSources,
            rules,
            fuzzy_rules: base.fuzzy_rules || [],
            survivorship: base.survivorship || [],
        };
    };

    const commitUpdate = (
        nextMap: Record<string, SourceConfig>,
        nextTables: string[] = tables,
        nextCols: Record<string, TableColumn[]> = latestColumnsRef.current
    ) => {
        setConfig(nextMap);
        setValidationError(null);
        const built = buildFinalConfig(nextMap, nextTables, nextCols);
        lastEmittedSourcesSigRef.current = JSON.stringify(built.sources);
        onChange?.(built);
    };

    useEffect(() => {
        loadColumns();
    }, [tables]);

    const loadColumns = async () => {
        const missing = tables.filter(t => !latestColumnsRef.current[t] || !latestConfigRef.current[t]);
        if (missing.length === 0) return;

        setLoadingCols(true);
        const newCols: Record<string, TableColumn[]> = { ...latestColumnsRef.current };
        let initializedAny = false;

        try {
            for (const table of tables) {
                let cols = newCols[table];
                if (!cols) {
                    const data = await api.discoverColumns(table);
                    cols = (data.columns || []) as TableColumn[];
                    newCols[table] = cols;
                }
            }
            setColumns(newCols);

            // Re-read latest config AFTER async column discovery so we never overwrite hydrated warehouse/JSON config
            const currentMap: Record<string, SourceConfig> = { ...latestConfigRef.current };
            const initialSources = latestInitialConfigRef.current?.sources || [];
            for (const src of initialSources) {
                if (src.table && !currentMap[src.table]) {
                    currentMap[src.table] = {
                        ...src,
                        identifiers: (src.identifiers || []).map(i => ({
                            ...i,
                            column: i.column || i.expr || '',
                            expr: i.expr || i.column || '',
                        })),
                        attributes: (src.attributes || []).map(a => ({
                            ...a,
                            column: a.column || a.expr || a.name,
                            expr: a.expr || a.column || a.name,
                        })),
                    };
                }
            }

            for (const table of tables) {
                if (!currentMap[table]) {
                    const cols = newCols[table] || [];
                    const likelyKey =
                        cols.find(c => c.name.toLowerCase().includes('id') && !c.name.toLowerCase().includes('email'))?.name ||
                        cols[0]?.name ||
                        'AUTO';
                    const likelyWatermark = guessDefaultWatermark(cols, likelyKey);

                    currentMap[table] = {
                        id: table.replace(/\./g, '_'),
                        table,
                        entity_key: likelyKey,
                        watermark_column: likelyWatermark,
                        watermark_lookback_minutes: 0,
                        identifiers: [],
                        attributes: [],
                    };
                    initializedAny = true;
                }
            }
            if (initializedAny) {
                setConfig(currentMap);
                const built = buildFinalConfig(currentMap, tables, newCols);
                lastEmittedSourcesSigRef.current = JSON.stringify(built.sources);
                onChange?.(built);
            }
        } catch (err: any) {
            setScanError(err.message || String(err));
        } finally {
            setLoadingCols(false);
        }
    };

    const runSchemaScan = async (targetSchema?: string) => {
        setScanLoading(true);
        setScanError(null);
        try {
            const data = await api.discoverTables(targetSchema?.trim() || undefined);
            setDiscoveredTables(data.tables || []);
        } catch (err: any) {
            setScanError(err.message || String(err));
            setDiscoveredTables([]);
        } finally {
            setScanLoading(false);
        }
    };

    const handleToggleDiscoveredTable = (fqn: string) => {
        const exists = tables.includes(fqn);
        const nextTables = exists ? tables.filter(t => t !== fqn) : [...tables, fqn];
        onTablesChange?.(nextTables);

        if (!exists) {
            setActiveTable(fqn);
        } else {
            const nextConfig = { ...config };
            delete nextConfig[fqn];
            commitUpdate(nextConfig, nextTables);
        }
    };

    const handleAddCustomTable = () => {
        const fqn = customTableFqn.trim();
        if (!fqn) return;
        if (!tables.includes(fqn)) {
            const nextTables = [...tables, fqn];
            onTablesChange?.(nextTables);
            setActiveTable(fqn);
        }
        setCustomTableFqn('');
    };

    const updateSourceField = (table: string, field: keyof SourceConfig, value: any) => {
        const next = {
            ...config,
            [table]: {
                ...config[table],
                [field]: value,
            },
        };
        commitUpdate(next);
    };

    const toggleAttribute = (table: string, colName: string) => {
        const curr = config[table];
        if (!curr) return;
        const currentAttrs = curr.attributes || [];
        const exists = currentAttrs.find(a => (a.column || a.expr || a.name) === colName);
        const colMeta = (columns[table] || []).find(c => c.name === colName);

        const newAttrs: Attribute[] = exists
            ? currentAttrs.filter(a => (a.column || a.expr || a.name) !== colName)
            : [...currentAttrs, { name: colName, column: colName, expr: colName, data_type: colMeta?.type }];

        commitUpdate({
            ...config,
            [table]: { ...curr, attributes: newAttrs },
        });
    };

    const updateAttributeName = (table: string, colName: string, newName: string) => {
        const curr = config[table];
        if (!curr) return;
        const safeName = newName.replace(/[^a-zA-Z0-9_]/g, '').toLowerCase();
        const newAttrs = (curr.attributes || []).map(a =>
            (a.column || a.expr || a.name) === colName ? { ...a, name: safeName } : a
        );
        commitUpdate({ ...config, [table]: { ...curr, attributes: newAttrs } });
    };

    const toggleIdentifier = (table: string, colName: string) => {
        const curr = config[table];
        if (!curr) return;
        const currentIds = curr.identifiers || [];
        const exists = currentIds.find(i => (i.column || i.expr) === colName);

        let newIds: Identifier[];
        if (exists) {
            newIds = currentIds.filter(i => (i.column || i.expr) !== colName);
        } else {
            const matchedPattern = IDENTIFIER_PATTERNS.find(p => p.pattern.test(colName));
            const defaultType = matchedPattern ? matchedPattern.type : colName.toLowerCase().replace(/[^a-z0-9_]/g, '_');
            newIds = [...currentIds, { column: colName, type: defaultType, expr: colName, is_match_key: true }];
        }

        commitUpdate({
            ...config,
            [table]: { ...curr, identifiers: newIds },
        });
    };

    const updateIdentifierType = (table: string, colName: string, newType: string) => {
        const curr = config[table];
        if (!curr) return;
        const safeType = newType.replace(/[^a-zA-Z0-9_]/g, '').toLowerCase();
        const newIds = (curr.identifiers || []).map(i =>
            (i.column || i.expr) === colName ? { ...i, type: safeType } : i
        );
        commitUpdate({ ...config, [table]: { ...curr, identifiers: newIds } });
    };

    const toggleMatchKey = (table: string, colName: string) => {
        const curr = config[table];
        if (!curr) return;
        const newIds = (curr.identifiers || []).map(i =>
            (i.column || i.expr) === colName ? { ...i, is_match_key: i.is_match_key === false } : i
        );
        commitUpdate({ ...config, [table]: { ...curr, identifiers: newIds } });
    };

    const getColumnState = (table: string, colName: string) => {
        const attrMatch = (config[table]?.attributes || []).find(a => (a.column || a.expr || a.name) === colName);
        const idMatch = (config[table]?.identifiers || []).find(i => (i.column || i.expr) === colName);
        return {
            isAttr: Boolean(attrMatch), attrName: attrMatch?.name,
            isId: Boolean(idMatch), type: idMatch?.type,
            isMatch: idMatch ? idMatch.is_match_key !== false : false,
        };
    };

    // Track which target attribute names appear on multiple tables so we can flag homonym collisions
    const sharedAttrTables: Record<string, string[]> = {};
    for (const t of tables) {
        const seenInTable = new Set<string>();
        for (const a of config[t]?.attributes || []) {
            const k = (a.name || '').trim().toLowerCase();
            if (k && !seenInTable.has(k)) {
                seenInTable.add(k);
                sharedAttrTables[k] = [...(sharedAttrTables[k] || []), t];
            }
        }
    }

    const handleNext = () => {
        const allIdentifiers = tables.flatMap(t => config[t]?.identifiers || []);
        if (allIdentifiers.length === 0) {
            setValidationError("Map at least one Identifier column (e.g. email, phone) across your source tables to continue.");
            return;
        }
        const finalConfig = buildFinalConfig(config, tables, columns);
        onNext(finalConfig);
    };

    const filteredDiscovered = tableFilter.trim()
        ? discoveredTables.filter(t => t.toLowerCase().includes(tableFilter.toLowerCase()))
        : discoveredTables;

    const activeConfig = activeTable ? config[activeTable] : undefined;
    const activeCols = activeTable ? (columns[activeTable] || []) : [];
    const filteredActiveCols = colSearch.trim()
        ? activeCols.filter(c => c.name.toLowerCase().includes(colSearch.toLowerCase()) || (c.type || '').toLowerCase().includes(colSearch.toLowerCase()))
        : activeCols;

    return (
        <div className="space-y-5 flex flex-col">
            {/* Section Title & Inline Actions */}
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                <div>
                    <h2 className="text-xl font-bold tracking-tight text-white">Sources &amp; Column Mappings</h2>
                    <p className="text-sm text-slate-400">
                        Add tables across one or more warehouse schemas, assign Entity Keys, and map Identifiers &amp; Profile Attributes.
                    </p>
                </div>

                <div className="flex flex-wrap items-center gap-2 shrink-0">
                    {tables.length > 0 && (
                        <>
                            <label className="inline-flex items-center gap-2 text-xs text-ink-secondary">
                                <input type="checkbox" checked={aiFuzzy} onChange={e => setAiFuzzy(e.target.checked)} />
                                Allow fuzzy matching proposals
                            </label>
                            <button
                                type="button"
                                disabled={aiSuggesting}
                                onClick={async () => {
                                    setAiSuggesting(true);
                                    setAiProgress({
                                        stage: 'profiling',
                                        step: 1,
                                        total_steps: 5,
                                        message: `Connecting to warehouse to profile ${tables.length} table(s)…`,
                                    });
                                    setValidationError(null);
                                    setAiNotice(null);
                                    try {
                                        const base = latestInitialConfigRef.current || { sources: [] };
                                        const context = generationContext;
                                        setProposalBase(JSON.stringify({config:base, context}));
                                        const res = await api.suggestConfigAiStream(tables, '', (evt) => {
                                            setAiProgress(evt);
                                        }, {mode: 'config', current_config: base, context});
                                        setAiReport(res);
                                        setAiReportOpen(true);
                                        setAiNotice('Proposal generated. Review the diff and apply it to your draft before starting a preview. Free-text notes stay local; structured policy controls generation.');
                                    } catch (err: any) {
                                        setValidationError(`AI Suggest failed: ${err?.message || err}`);
                                    } finally {
                                        setAiSuggesting(false);
                                        setAiProgress(null);
                                    }
                                }}
                                className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-accent hover:bg-accent-hover disabled:opacity-50 text-white text-xs font-medium transition-colors"
                                title="Profiles selected tables with zero PII, discovers cross-table join keys & table roles, and drafts mappings, rules, survivorship, and lenses"
                            >
                                {aiSuggesting ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Bot className="w-3.5 h-3.5" />}
                                {aiSuggesting ? 'AI Suggesting...' : 'AI Suggest Config'}
                                {!aiSuggesting && (
                                    <span className="px-1.5 py-0.2 rounded bg-white/20 text-[10px] font-semibold uppercase tracking-wide">
                                        Beta
                                    </span>
                                )}
                            </button>
                        </>
                    )}
                    <button
                        type="button"
                        onClick={() => setDiscoveryOpen(prev => !prev)}
                        className={`inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-md text-xs font-medium border transition-colors ${
                            discoveryOpen
                                ? 'bg-accent text-white border-accent'
                                : 'bg-surface hover:bg-hover text-ink-primary border-line-default'
                        }`}
                    >
                        <Plus className="w-3.5 h-3.5" />
                        {discoveryOpen ? 'Hide Table Browser' : 'Add / Manage Source Tables'}
                        {discoveryOpen ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}
                    </button>
                </div>
            </div>

            {tables.length > 0 && (
                <div className="bg-subtle border border-line-default rounded-md px-3 py-2.5 space-y-2">
                    <div className="flex flex-wrap items-center justify-between gap-2 text-[11px] text-ink-muted">
                        <span className="inline-flex items-center gap-1.5">
                            <Bot className="w-3.5 h-3.5 text-accent shrink-0" />
                            <strong className="text-ink-primary font-semibold">AI-Assisted Configuration</strong>
                            <span className="px-1.5 py-0.2 rounded bg-accent/15 text-accent border border-accent/30 text-[10px] font-semibold uppercase">
                                Beta
                            </span>
                            <span className="hidden md:inline">
                                — Profiles column shapes &amp; cross-table overlaps (zero raw PII sent), detects table roles, and drafts mappings, rules, survivorship &amp; lenses for review.
                            </span>
                        </span>
                    </div>
                    <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-2">
                        <span className="text-[11px] font-semibold text-accent uppercase tracking-wider shrink-0">
                            Local objective hints (optional)
                        </span>
                        <input
                            type="text"
                            value={aiGoal}
                            onChange={e => setAiGoal(e.target.value)}
                            placeholder="Precision, coverage, cross-source matching, auditability. Other text stays local."
                            className="flex-1 bg-surface border border-line-default rounded px-2.5 py-1 text-xs text-ink-primary placeholder:text-ink-muted focus:outline-none focus:border-accent"
                        />
                    </div>
                    <div className="flex flex-wrap items-center gap-2 pt-1 border-t border-line-default/60 text-[11px]">
                        <label className="inline-flex items-center gap-1.5 text-ink-muted">
                            <span className="font-medium">Grain:</span>
                            <select
                                value={aiGrain}
                                onChange={e => setAiGrain(e.target.value as 'individual' | 'household' | 'account')}
                                className="bg-surface border border-line-default rounded px-2 py-0.5 text-xs text-ink-primary focus:outline-none focus:border-accent"
                            >
                                <option value="individual">Strict Individual</option>
                                <option value="household">Household</option>
                                <option value="account">Account / B2B</option>
                            </select>
                        </label>
                        <label className="inline-flex items-center gap-1.5 text-ink-muted">
                            <span className="font-medium">Event Tables:</span>
                            <select
                                value={aiEventPolicy}
                                onChange={e => setAiEventPolicy(e.target.value as 'bridge' | 'include' | 'exclude')}
                                className="bg-surface border border-line-default rounded px-2 py-0.5 text-xs text-ink-primary focus:outline-none focus:border-accent"
                            >
                                <option value="bridge">Bridge (Approve Aggregates Separately)</option>
                                <option value="include">Include Fully</option>
                                <option value="exclude">Exclude Event Tables</option>
                            </select>
                        </label>
                        <label className="inline-flex items-center gap-1.5 text-ink-muted">
                            <span className="font-medium">System of Record:</span>
                            <select
                                value={aiSystemOfRecord}
                                onChange={e => setAiSystemOfRecord(e.target.value)}
                                className="bg-surface border border-line-default rounded px-2 py-0.5 text-xs text-ink-primary focus:outline-none focus:border-accent max-w-[200px]"
                            >
                                <option value="">Auto (Master Entity)</option>
                                {tables.map(t => (
                                    <option key={t} value={t}>{t}</option>
                                ))}
                            </select>
                        </label>
                    </div>
                </div>
            )}

            {aiSuggesting && (
                <AiProgressStepper progress={aiProgress} tableCount={tables.length} />
            )}

            {aiNotice && (
                <div className="bg-selected border border-accent/40 rounded-md p-3 text-xs text-ink-primary flex items-center justify-between gap-2">
                    <span className="flex items-center gap-2">
                        <Bot className="w-4 h-4 shrink-0 text-accent" />
                        {aiNotice}
                    </span>
                    <div className="flex items-center gap-2 shrink-0">
                        {aiReport?.explanations && (
                            <button
                                type="button"
                                onClick={() => setAiReportOpen(prev => !prev)}
                                className="px-2 py-0.5 rounded bg-surface hover:bg-hover border border-line-default text-ink-primary text-[11px] font-medium"
                            >
                                {aiReportOpen ? 'Hide Reasoning' : 'Show Reasoning'}
                            </button>
                        )}
                        <button type="button" onClick={() => { setAiNotice(null); setAiReport(null); }} className="text-ink-muted hover:text-ink-primary">✕</button>
                    </div>
                </div>
            )}

            <AIConfigPolicy context={advancedPolicy} tables={tables} onChange={setAdvancedPolicy} />

            {aiReport && aiReportOpen && (
                <div className="bg-slate-900/90 border border-purple-500/30 rounded-xl p-4 space-y-3 text-xs">
                    <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-800 pb-2.5">
                        <div className="flex flex-wrap items-center gap-2">
                            <span className="font-semibold text-white">AI Proposal &amp; Reasoning</span>
                            <span className="text-slate-400">Decisions are proposals; confidence is not measured accuracy.</span>
                            <span className={`px-2 py-0.5 rounded-full text-[10px] font-semibold border ${
                                aiReport.status === 'invalid'
                                    ? 'bg-red-950/60 text-red-300 border-red-500/40'
                                    : aiReport.generation_mode === 'deterministic_fallback'
                                    ? 'bg-amber-950/60 text-amber-300 border-amber-500/40'
                                    : 'bg-emerald-950/60 text-emerald-300 border-emerald-500/40'
                            }`}>
                                {aiReport.status === 'invalid'
                                    ? 'Validation failed'
                                    : aiReport.generation_mode === 'deterministic_fallback'
                                    ? 'Deterministic Fallback'
                                    : `${aiReport.generation_mode === 'ai_repaired' ? 'AI Repaired' : 'AI Validated'} (${aiReport.provider}:${aiReport.model})`}
                            </span>
                            <span className="px-2 py-0.5 rounded-full text-[10px] font-medium bg-slate-800 text-slate-300 border border-slate-700">
                                Zero-PII Profile
                            </span>
                            {aiReport.validation_report?.sql_verified && (
                                <span className="px-2 py-0.5 rounded-full text-[10px] font-medium bg-blue-950/60 text-blue-300 border border-blue-500/40">
                                    SQL Verified
                                </span>
                            )}
                        </div>
                        <button
                            type="button"
                            onClick={() => setAiReportOpen(false)}
                            className="text-slate-400 hover:text-white text-[11px]"
                        >
                            Collapse
                        </button>
                    </div>

                    <AIProposalReview proposal={aiReport}
                        stale={proposalBase !== JSON.stringify({config:initialConfig || {sources: []}, context:generationContext})}
                        onApply={(candidate) => {
                            onTablesChange?.(candidate.sources.map(source => source.table));
                            onChange?.(candidate);
                            setAiNotice('Proposal applied to draft. Start a Dry Run to evaluate it.');
                            setAiReport(null);
                        }} />

                    {aiReport.validation_report?.warnings && aiReport.validation_report.warnings.length > 0 && (
                        <div className="bg-amber-950/30 border border-amber-500/30 rounded-lg p-2.5 text-amber-200 space-y-1">
                            {aiReport.validation_report.warnings.map((w, idx) => (
                                <div key={idx}>⚠ {w}</div>
                            ))}
                        </div>
                    )}

                    {aiReport.profile_summary?.cross_table_overlaps && aiReport.profile_summary.cross_table_overlaps.length > 0 && (
                        <div className="space-y-1">
                            <div className="text-[11px] font-semibold text-slate-400 uppercase tracking-wider">Cross-Table Key Overlaps</div>
                            <div className="flex flex-wrap gap-1.5">
                                {aiReport.profile_summary.cross_table_overlaps.slice(0, 8).map((ov, idx) => (
                                    <span key={idx} className="px-2 py-1 rounded-md bg-slate-800/90 border border-slate-700 text-slate-200 text-[11px]">
                                        <span className="text-purple-300 font-semibold">{ov.identifier_type}</span>: {ov.table_a.split('.').pop()}.{ov.column_a} ↔ {ov.table_b.split('.').pop()}.{ov.column_b}{' '}
                                        <span className="text-emerald-300 font-mono">{Math.round(ov.overlap_a_in_b * 100)}% / {Math.round(ov.overlap_b_in_a * 100)}%</span>
                                    </span>
                                ))}
                            </div>
                        </div>
                    )}

                    {aiReport.explanations && (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                            {[
                                { title: 'Matching Rules', items: aiReport.explanations.rules || [] },
                                { title: 'Table Mappings', items: aiReport.explanations.tables || [] },
                                { title: 'Survivorship', items: aiReport.explanations.survivorship || [] },
                                { title: 'Identity Lenses', items: aiReport.explanations.lenses || [] },
                            ]
                                .filter(sec => sec.items.length > 0)
                                .map(sec => (
                                    <div key={sec.title} className="bg-slate-950/60 border border-slate-800 rounded-lg p-3 space-y-2">
                                        <div className="text-[11px] font-semibold text-slate-300 uppercase tracking-wider">{sec.title}</div>
                                        <div className="space-y-2 max-h-56 overflow-y-auto pr-1">
                                            {sec.items.map((item, idx) => (
                                                <div key={`${item.target}-${idx}`} className="border-b border-slate-800/70 last:border-0 pb-1.5 last:pb-0 space-y-0.5">
                                                    <div className="flex items-center justify-between gap-2">
                                                        <span className="font-mono font-semibold text-slate-100">{item.target}</span>
                                                        <div className="flex items-center gap-1">
                                                            {item.needs_review && (
                                                                <span className="px-1.5 py-0.5 rounded bg-amber-500/20 text-amber-300 border border-amber-500/40 text-[10px] font-semibold">
                                                                    Needs Review
                                                                </span>
                                                            )}
                                                            {item.confidence && (
                                                                <span className="px-1.5 py-0.5 rounded bg-slate-800 text-slate-400 text-[10px]">
                                                                    {item.confidence}
                                                                </span>
                                                            )}
                                                        </div>
                                                    </div>
                                                    <p className="text-slate-300 leading-relaxed">{item.summary}</p>
                                                    {item.evidence && item.evidence.length > 0 && (
                                                        <div className="text-[10px] text-slate-400 font-mono">
                                                            Evidence: {item.evidence.join(' · ')}
                                                        </div>
                                                    )}
                                                    {item.risks && item.risks.length > 0 && (
                                                        <div className="text-[10px] text-amber-300">
                                                            Risk: {item.risks.join(' ')}
                                                        </div>
                                                    )}
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                ))}
                        </div>
                    )}
                </div>
            )}

            {validationError && (
                <div className="bg-amber-950/50 border border-amber-500/40 rounded-xl p-3 text-xs text-amber-200 flex items-center gap-2">
                    <AlertCircle className="w-4 h-4 text-amber-400 shrink-0" />
                    <span>{validationError}</span>
                </div>
            )}

            {/* Integrated Multi-Schema Discovery Drawer */}
            {discoveryOpen && (
                <div className="bg-slate-900/90 border border-blue-500/30 rounded-xl p-4 space-y-3.5 shadow-lg">
                    <div className="flex flex-col md:flex-row md:items-center justify-between gap-3">
                        <div>
                            <h3 className="text-sm font-semibold text-white flex items-center gap-2">
                                <Search className="w-4 h-4 text-blue-400" />
                                Discover Warehouse Tables
                            </h3>
                            <p className="text-xs text-slate-400 mt-0.5">
                                Scan any schema/dataset sequentially — selected tables from previous schemas stay selected below.
                            </p>
                        </div>

                        <form
                            onSubmit={(e) => {
                                e.preventDefault();
                                runSchemaScan(schemaInput);
                            }}
                            className="flex flex-wrap items-center gap-2"
                        >
                            <input
                                type="text"
                                value={schemaInput}
                                onChange={(e) => setSchemaInput(e.target.value)}
                                placeholder="Schema / Dataset (e.g. raw)"
                                className="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-slate-100 placeholder-slate-500 w-48 focus:outline-none focus:border-blue-500"
                            />
                            <button
                                type="submit"
                                disabled={scanLoading}
                                className="px-3.5 py-1.5 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white rounded-lg text-xs font-semibold flex items-center gap-1.5 transition-colors"
                            >
                                {scanLoading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Search className="w-3.5 h-3.5" />}
                                Scan Schema
                            </button>
                        </form>
                    </div>

                    {scanError && (
                        <div className="p-2.5 rounded-lg bg-rose-950/50 border border-rose-500/40 text-xs text-rose-300">
                            {scanError}
                        </div>
                    )}

                    {/* Filter + Table Picker Grid */}
                    <div className="border border-slate-800 rounded-xl bg-slate-950/70 overflow-hidden">
                        <div className="px-3 py-2 border-b border-slate-800 flex flex-wrap items-center justify-between gap-2 bg-slate-900/60">
                            <div className="flex items-center gap-2 flex-1 min-w-[180px]">
                                <Search className="w-3.5 h-3.5 text-slate-500" />
                                <input
                                    type="text"
                                    value={tableFilter}
                                    onChange={(e) => setTableFilter(e.target.value)}
                                    placeholder="Filter scanned tables..."
                                    className="bg-transparent text-xs text-slate-200 placeholder-slate-500 focus:outline-none flex-1"
                                />
                            </div>
                            <div className="flex items-center gap-2">
                                <input
                                    type="text"
                                    value={customTableFqn}
                                    onChange={(e) => setCustomTableFqn(e.target.value)}
                                    onKeyDown={(e) => {
                                        if (e.key === 'Enter') {
                                            e.preventDefault();
                                            handleAddCustomTable();
                                        }
                                    }}
                                    placeholder="Or add FQN: schema.table"
                                    className="bg-slate-900 border border-slate-700 rounded-md px-2.5 py-1 text-xs text-slate-200 placeholder-slate-500 w-44 focus:outline-none focus:border-blue-500"
                                />
                                <button
                                    type="button"
                                    onClick={handleAddCustomTable}
                                    disabled={!customTableFqn.trim()}
                                    className="px-2.5 py-1 bg-slate-800 hover:bg-slate-700 disabled:opacity-40 text-slate-200 rounded-md text-xs border border-slate-700"
                                >
                                    Add FQN
                                </button>
                            </div>
                        </div>

                        <div className="max-h-52 overflow-y-auto p-2.5">
                            {filteredDiscovered.length === 0 && !scanLoading ? (
                                <div className="text-center text-xs text-slate-500 py-6">
                                    {discoveredTables.length > 0
                                        ? 'No scanned tables match your filter.'
                                        : 'Enter a schema/dataset name above and click Scan Schema (leave blank on DuckDB).'}
                                </div>
                            ) : (
                                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
                                    {filteredDiscovered.map(fqn => {
                                        const isSelected = tables.includes(fqn);
                                        return (
                                            <button
                                                key={fqn}
                                                type="button"
                                                onClick={() => handleToggleDiscoveredTable(fqn)}
                                                className={`flex items-center justify-between gap-2 px-3 py-2 rounded-lg border text-left text-xs transition-all ${
                                                    isSelected
                                                        ? 'bg-blue-600/15 border-blue-500/60 text-blue-200 font-medium'
                                                        : 'bg-slate-900/80 border-slate-800 text-slate-300 hover:border-slate-700 hover:bg-slate-800/80'
                                                }`}
                                            >
                                                <span className="flex items-center gap-2 truncate font-mono">
                                                    <TableIcon className={`w-3.5 h-3.5 shrink-0 ${isSelected ? 'text-blue-400' : 'text-slate-500'}`} />
                                                    <span className="truncate">{fqn}</span>
                                                </span>
                                                <span className={`w-4 h-4 rounded flex items-center justify-center border shrink-0 ${
                                                    isSelected ? 'bg-blue-600 border-blue-500 text-white' : 'border-slate-700 bg-slate-950'
                                                }`}>
                                                    {isSelected && <Check className="w-3 h-3" />}
                                                </span>
                                            </button>
                                        );
                                    })}
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            )}

            {/* Master-Detail Sources & Column Mapper */}
            {tables.length === 0 ? (
                <div className="border border-dashed border-slate-700 rounded-xl p-10 text-center space-y-2 bg-slate-900/30">
                    <TableIcon className="w-8 h-8 text-slate-600 mx-auto" />
                    <p className="text-sm font-medium text-slate-300">No source tables selected yet</p>
                    <p className="text-xs text-slate-500">
                        Use the table discovery browser above to scan a schema and select one or more source tables.
                    </p>
                </div>
            ) : loadingCols && Object.keys(columns).length === 0 ? (
                <div className="flex justify-center p-16">
                    <Loader2 className="animate-spin w-7 h-7 text-blue-500" />
                </div>
            ) : (
                <div className="grid grid-cols-1 lg:grid-cols-12 gap-5 items-start">
                    {/* Left Rail: Selected Source Tables */}
                    <div className="lg:col-span-4 space-y-2.5 bg-slate-900/60 border border-slate-800 rounded-xl p-3">
                        <div className="flex items-center justify-between px-1 pb-1 border-b border-slate-800/80">
                            <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
                                Source Tables ({tables.length})
                            </span>
                            {loadingCols && <Loader2 className="w-3.5 h-3.5 animate-spin text-blue-400" />}
                        </div>

                        <div className="space-y-1.5 max-h-[540px] overflow-y-auto pr-0.5">
                            {tables.map(table => {
                                const cfg = config[table] || {};
                                const idCount = (cfg.identifiers || []).length;
                                const attrCount = (cfg.attributes || []).length;
                                const isActive = activeTable === table;

                                return (
                                    <div
                                        key={table}
                                        onClick={() => setActiveTable(table)}
                                        className={`group p-3 rounded-xl border cursor-pointer transition-all space-y-2 ${
                                            isActive
                                                ? 'bg-blue-600/15 border-blue-500/60 shadow-sm'
                                                : 'bg-slate-950/60 border-slate-800/90 hover:border-slate-700'
                                        }`}
                                    >
                                        <div className="flex items-start justify-between gap-2">
                                            <div className="min-w-0">
                                                <div className={`text-xs font-mono font-semibold truncate ${isActive ? 'text-white' : 'text-slate-200'}`}>
                                                    {table}
                                                </div>
                                                <div className="text-[11px] text-slate-500 font-mono truncate mt-0.5">
                                                    id: {cfg.id || table.replace(/\./g, '_')}
                                                </div>
                                            </div>
                                            <button
                                                type="button"
                                                onClick={(e) => {
                                                    e.stopPropagation();
                                                    handleToggleDiscoveredTable(table);
                                                }}
                                                title="Remove source table"
                                                className="p-1 rounded text-slate-500 hover:text-rose-400 hover:bg-rose-950/40 opacity-70 group-hover:opacity-100 transition-colors"
                                            >
                                                <Trash2 className="w-3.5 h-3.5" />
                                            </button>
                                        </div>

                                        <div className="flex flex-wrap items-center gap-1.5 text-[11px]">
                                            {cfg.entity_key && (
                                                <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700 font-mono max-w-[120px] truncate">
                                                    <Key className="w-2.5 h-2.5 text-amber-400 shrink-0" />
                                                    {cfg.entity_key}
                                                </span>
                                            )}
                                            <span className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded border ${
                                                idCount > 0
                                                    ? 'bg-blue-950/60 text-blue-300 border-blue-800/60'
                                                    : 'bg-amber-950/50 text-amber-300 border-amber-700/60'
                                            }`}>
                                                <Fingerprint className="w-2.5 h-2.5" />
                                                {idCount} IDs
                                            </span>
                                            <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-emerald-950/50 text-emerald-300 border border-emerald-800/50">
                                                <Tag className="w-2.5 h-2.5" />
                                                {attrCount} Attrs
                                            </span>
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    </div>

                    {/* Right Detail Pane: Active Source Config & Columns */}
                    {activeTable && activeConfig && (
                        <div className="lg:col-span-8 bg-slate-900/60 border border-slate-800 rounded-xl p-4 sm:p-5 space-y-5">
                            <div className="flex flex-wrap items-center justify-between gap-3 pb-3 border-b border-slate-800">
                                <div>
                                    <span className="text-[11px] uppercase tracking-wider font-semibold text-blue-400">Active Source Table</span>
                                    <h3 className="text-base font-bold font-mono text-white">{activeTable}</h3>
                                </div>
                            </div>

                            {/* Source Extraction Settings */}
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                <div className="space-y-1.5">
                                    <label className="flex items-center gap-1.5 text-xs font-medium text-slate-300">
                                        <Key className="w-3.5 h-3.5 text-amber-400" /> Entity Key (Primary ID)
                                    </label>
                                    <CustomSelect
                                        value={activeConfig.entity_key || ''}
                                        onChange={(val) => updateSourceField(activeTable, 'entity_key', val)}
                                        options={[
                                            { value: 'AUTO', label: 'AUTO (Synthetic Hash Key)' },
                                            ...activeCols.map(c => ({ value: c.name, label: `${c.name} (${c.type})` })),
                                        ]}
                                        className="w-full"
                                        buttonClassName="text-xs py-2 bg-slate-950"
                                        menuClassName="left-0 right-0"
                                    />
                                </div>

                                <div className="space-y-1.5">
                                    <label className="flex items-center gap-1.5 text-xs font-medium text-slate-300">
                                        <Clock className="w-3.5 h-3.5 text-blue-400" /> Watermark Column
                                    </label>
                                    <CustomSelect
                                        value={activeConfig.watermark_column || ''}
                                        onChange={(val) => updateSourceField(activeTable, 'watermark_column', val)}
                                        options={[
                                            { value: '', label: '(None — Full Refresh / PK Diff)' },
                                            ...activeCols.map(c => ({ value: c.name, label: `${c.name} (${c.type})` })),
                                        ]}
                                        className="w-full"
                                        buttonClassName="text-xs py-2 bg-slate-950"
                                        menuClassName="left-0 right-0"
                                    />
                                </div>

                                <div className="space-y-1.5">
                                    <label className="flex items-center gap-1.5 text-xs font-medium text-slate-300">
                                        <Clock className="w-3.5 h-3.5 text-slate-400" /> Lookback Window (Minutes)
                                    </label>
                                    <input
                                        type="number"
                                        min="0"
                                        value={activeConfig.watermark_lookback_minutes || 0}
                                        onChange={e => updateSourceField(activeTable, 'watermark_lookback_minutes', parseInt(e.target.value, 10) || 0)}
                                        className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs text-slate-100 focus:outline-none focus:border-blue-500"
                                    />
                                </div>

                                <div className="space-y-1.5">
                                    <label className="flex items-center gap-2 text-xs font-medium text-slate-300 cursor-pointer pt-1">
                                        <input
                                            type="checkbox"
                                            checked={Boolean(activeConfig.deduplicate)}
                                            onChange={e => updateSourceField(activeTable, 'deduplicate', e.target.checked)}
                                            className="rounded border-slate-700 bg-slate-950 text-blue-500 focus:ring-blue-500"
                                        />
                                        Deduplicate duplicate entity_key rows in batch
                                    </label>
                                    {activeConfig.deduplicate && (
                                        <CustomSelect
                                            value={activeConfig.dedup_order_by || ''}
                                            onChange={(val) => updateSourceField(activeTable, 'dedup_order_by', val)}
                                            options={[
                                                { value: '', label: 'Default order (watermark DESC)' },
                                                ...activeCols.map(c => ({ value: `${c.name} DESC`, label: `${c.name} DESC` })),
                                            ]}
                                            className="w-full"
                                            buttonClassName="text-xs py-1.5 bg-slate-950"
                                            menuClassName="left-0 right-0"
                                        />
                                    )}
                                </div>
                            </div>

                            {/* Column Role Classification Table */}
                            <div className="space-y-2.5">
                                <div className="flex flex-wrap items-center justify-between gap-2">
                                    <label className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-slate-400">
                                        <Fingerprint className="w-3.5 h-3.5 text-purple-400" /> Column Classification ({activeCols.length})
                                    </label>
                                    <div className="relative w-56">
                                        <Search className="w-3.5 h-3.5 text-slate-500 absolute left-2.5 top-1/2 -translate-y-1/2" />
                                        <input
                                            type="text"
                                            value={colSearch}
                                            onChange={e => setColSearch(e.target.value)}
                                            placeholder="Filter columns..."
                                            className="w-full bg-slate-950 border border-slate-700 rounded-lg pl-8 pr-2.5 py-1 text-xs text-slate-200 placeholder-slate-500 focus:outline-none focus:border-blue-500"
                                        />
                                    </div>
                                </div>

                                <div className="border border-slate-800 rounded-xl bg-slate-950/80 max-h-[340px] overflow-y-auto">
                                    <p className="px-3.5 py-2 text-xs text-slate-400">A column can be both an Identifier and an Attribute. Select Attribute to make it available in Survivorship for the golden profile.</p>
                                    <div className="grid grid-cols-12 gap-2 px-3.5 py-2 border-b border-slate-800 text-[11px] font-semibold text-slate-500 uppercase sticky top-0 bg-slate-900/95 backdrop-blur z-10">
                                        <div className="col-span-4">Column</div>
                                        <div className="col-span-2">SQL Type</div>
                                        <div className="col-span-1 text-center" title="Include in matching rules">Match</div>
                                        <div className="col-span-5 text-right">Roles &amp; Types</div>
                                    </div>

                                    {filteredActiveCols.map(c => {
                                        const state = getColumnState(activeTable, c.name);
                                        const attrKey = (state.attrName || '').trim().toLowerCase();
                                        const collidingTables = state.isAttr && attrKey ? (sharedAttrTables[attrKey] || []) : [];
                                        const isHomonymCollision =
                                            state.isAttr &&
                                            collidingTables.length > 1 &&
                                            GENERIC_HOMONYM_ATTRS.has(attrKey);
                                        const suggestedAlias = isHomonymCollision
                                            ? suggestAliasedAttrName(activeTable, state.attrName || c.name)
                                            : '';
                                        return (
                                            <div key={c.name} className="grid grid-cols-12 gap-2 px-3.5 py-2 items-center hover:bg-slate-900/80 border-b border-slate-800/50 last:border-0">
                                                <div className="col-span-4 text-xs font-mono font-medium text-slate-200 truncate" title={c.name}>
                                                    {c.name}
                                                </div>
                                                <div className="col-span-2 text-[11px] font-mono text-slate-500 truncate" title={c.type}>
                                                    {c.type}
                                                </div>
                                                <div className="col-span-1 flex justify-center">
                                                    {state.isId && (
                                                        <button
                                                            type="button"
                                                            onClick={() => toggleMatchKey(activeTable, c.name)}
                                                            className={`p-1 rounded hover:bg-slate-800 ${state.isMatch ? 'text-blue-400' : 'text-slate-600'}`}
                                                            title={state.isMatch ? 'Included in matching rules' : 'Stored only (excluded from matching)'}
                                                        >
                                                            {state.isMatch ? <CheckSquare className="w-4 h-4" /> : <Square className="w-4 h-4" />}
                                                        </button>
                                                    )}
                                                </div>
                                                <div className="col-span-5 flex items-center justify-end gap-1.5 flex-wrap">
                                                    {state.isId && (
                                                        <input
                                                            type="text"
                                                            value={state.type || ''}
                                                            onChange={(e) => updateIdentifierType(activeTable, c.name, e.target.value)}
                                                            className="bg-slate-900 border border-slate-700 rounded-md px-2 py-1 text-xs font-mono text-blue-300 w-24 focus:outline-none focus:border-blue-500"
                                                            placeholder="id type"
                                                        />
                                                    )}
                                                    {state.isAttr && (
                                                        <div className="flex items-center gap-1">
                                                            <input
                                                                type="text"
                                                                value={state.attrName || ''}
                                                                onChange={(e) => updateAttributeName(activeTable, c.name, e.target.value)}
                                                                className={`bg-slate-900 border rounded-md px-2 py-1 text-xs font-mono w-28 focus:outline-none ${
                                                                    isHomonymCollision
                                                                        ? 'border-amber-500/70 text-amber-300 focus:border-amber-400'
                                                                        : 'border-slate-700 text-emerald-300 focus:border-emerald-500'
                                                                }`}
                                                                placeholder="attr name"
                                                                title="Target attribute name in golden_profile_current (rename to alias colliding columns)"
                                                            />
                                                            {isHomonymCollision && suggestedAlias && suggestedAlias !== attrKey && (
                                                                <button
                                                                    type="button"
                                                                    onClick={() => updateAttributeName(activeTable, c.name, suggestedAlias)}
                                                                    title={`'${attrKey}' is mapped on ${collidingTables.length} tables (${collidingTables.join(', ')}). Click to alias as '${suggestedAlias}'.`}
                                                                    className="px-1.5 py-1 text-[10px] font-mono rounded bg-amber-500/15 border border-amber-500/40 text-amber-300 hover:bg-amber-500/25 whitespace-nowrap"
                                                                >
                                                                    → {suggestedAlias}
                                                                </button>
                                                            )}
                                                        </div>
                                                    )}
                                                    <button
                                                        type="button"
                                                        onClick={() => toggleIdentifier(activeTable, c.name)}
                                                        aria-pressed={state.isId}
                                                        aria-label={`${c.name}: identifier`}
                                                        title="Use for identity matching; can also be a profile attribute"
                                                        className={`px-2.5 py-1 text-xs rounded-md border transition-colors ${
                                                            state.isId
                                                                ? 'bg-blue-600 border-blue-500 text-white font-medium'
                                                                : 'bg-slate-900 border-slate-700 text-slate-400 hover:border-slate-500'
                                                        }`}
                                                    >
                                                        Identifier
                                                    </button>
                                                    <button
                                                        type="button"
                                                        onClick={() => toggleAttribute(activeTable, c.name)}
                                                        aria-pressed={state.isAttr}
                                                        aria-label={`${c.name}: profile attribute`}
                                                        title="Make available in Survivorship and the golden profile; keeps identifier mapping"
                                                        className={`px-2.5 py-1 text-xs rounded-md border transition-colors ${
                                                            state.isAttr
                                                                ? 'bg-emerald-600/20 border-emerald-500/70 text-emerald-300 font-medium'
                                                                : 'bg-slate-900 border-slate-700 text-slate-400 hover:border-slate-500'
                                                        }`}
                                                    >
                                                        Attribute
                                                    </button>
                                                </div>
                                            </div>
                                        );
                                    })}
                                </div>
                            </div>
                        </div>
                    )}
                </div>
            )}

            {/* Bottom Navigation Helper */}
            <div className="flex items-center justify-between pt-2 border-t border-slate-800/80">
                {onBack ? (
                    <button
                        type="button"
                        onClick={onBack}
                        className="px-4 py-2 border border-slate-700 text-slate-300 rounded-lg text-xs font-medium hover:bg-slate-800"
                    >
                        Switch Warehouse
                    </button>
                ) : <div />}

                <button
                    type="button"
                    onClick={handleNext}
                    disabled={tables.length === 0}
                    className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed text-white rounded-lg text-xs font-semibold transition-colors"
                >
                    Continue to Matching Rules →
                </button>
            </div>
        </div>
    );
}
