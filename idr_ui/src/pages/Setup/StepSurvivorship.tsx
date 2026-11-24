import GroupedOutputEditor from '../../components/GroupedOutputEditor'
import ActivationDeltaEditor from '../../components/ActivationDeltaEditor'
import { useState, useEffect, useRef } from 'react';
import { Layers, ArrowUp, ArrowDown, Zap, Clock, Hash, Search } from "lucide-react";
import { IDRConfig, SurvivorshipRule, TableColumn } from '../../types';
import { CustomSelect } from '../../components/CustomSelect';
import { api } from '../../api/client';

interface StepSurvivorshipProps {
    config: IDRConfig;
    onChange?: (config: IDRConfig) => void;
    onNext: (config: IDRConfig) => void;
    onBack?: () => void;
}

type AttrCategory = 'boolean' | 'numeric' | 'datetime' | 'string' | 'unknown';

const classifySqlType = (sqlType?: string): AttrCategory => {
    if (!sqlType) return 'unknown';
    const t = sqlType.toLowerCase();
    if (t.includes('bool')) return 'boolean';
    if (/(int|float|double|decimal|numeric|real|number|byteint|short|long)/.test(t)) return 'numeric';
    if (/(date|time|timestamp)/.test(t)) return 'datetime';
    if (/(char|text|string|clob|varchar|nvarchar)/.test(t)) return 'string';
    return 'unknown';
};

export default function StepSurvivorship({ config, onChange, onNext, onBack }: StepSurvivorshipProps) {
    const [attributes, setAttributes] = useState<string[]>([]);
    const [sourcesByAttr, setSourcesByAttr] = useState<Record<string, string[]>>({});
    const [typeByAttr, setTypeByAttr] = useState<Record<string, string>>({});
    const [rules, setRules] = useState<Record<string, SurvivorshipRule>>({});
    const [attrSearch, setAttrSearch] = useState('');
    const [conflictsOnly, setConflictsOnly] = useState(false);

    const latestConfigRef = useRef(config);
    latestConfigRef.current = config;

    const lastEmittedSigRef = useRef<string>('');

    useEffect(() => {
        const allAttrs = new Set<string>();
        const attrSources: Record<string, string[]> = {};
        const attrTypes: Record<string, string> = {};
        const tablesMissingTypes = new Set<string>();

        (config.sources || []).forEach(src => {
            const srcKey = src.id || src.table;
            (src.attributes || []).forEach(a => {
                allAttrs.add(a.name);
                if (!attrSources[a.name]) attrSources[a.name] = [];
                if (!attrSources[a.name].includes(srcKey)) {
                    attrSources[a.name].push(srcKey);
                }
                if (a.data_type && !attrTypes[a.name]) {
                    attrTypes[a.name] = a.data_type;
                } else if (!a.data_type && src.table) {
                    tablesMissingTypes.add(src.table);
                }
            });
        });

        // Only retain survivorship rules for attributes still mapped on at least one active source.
        // Removing an attribute in Step 1 (Sources & Mappings) automatically prunes it here.
        const hasAnyMappedSources = (config.sources || []).length > 0;
        if (!hasAnyMappedSources) {
            (config.survivorship || []).forEach(r => {
                if (r.attribute) {
                    allAttrs.add(r.attribute);
                    if (!attrSources[r.attribute] && r.source_priority && r.source_priority.length > 0) {
                        attrSources[r.attribute] = [...r.source_priority];
                    }
                }
            });
        }

        const attrList = Array.from(allAttrs).sort();
        setAttributes(attrList);
        setSourcesByAttr(attrSources);
        setTypeByAttr(attrTypes);

        const incomingSig = JSON.stringify(config.survivorship || []);
        const existingByAttr = new Map<string, SurvivorshipRule>();
        for (const r of config.survivorship || []) {
            if (r.attribute) existingByAttr.set(r.attribute, r);
        }

        const initialRules: Record<string, SurvivorshipRule> = {};
        attrList.forEach(attr => {
            const validSources = attrSources[attr] || [];
            const validSet = new Set(validSources);
            const existing = existingByAttr.get(attr);

            let priority = validSources;
            if (existing?.source_priority && existing.source_priority.length > 0) {
                if (validSet.size > 0) {
                    const filtered = existing.source_priority.filter(s => validSet.has(s));
                    const missing = validSources.filter(s => !filtered.includes(s));
                    priority = [...filtered, ...missing];
                } else {
                    priority = existing.source_priority;
                }
            }

            initialRules[attr] = existing ? {
                ...existing,
                source_priority: priority,
            } : {
                attribute: attr,
                strategy: 'RECENCY',
                source_priority: priority,
            };
        });

        const missingFromConfig = attrList.some(attr => !existingByAttr.has(attr));
        const orphanedInConfig = (config.survivorship || []).some(r => r.attribute && !allAttrs.has(r.attribute));
        if (incomingSig !== lastEmittedSigRef.current || Object.keys(rules).length !== attrList.length || orphanedInConfig) {
            setRules(initialRules);
            if ((missingFromConfig || orphanedInConfig) && onChange) {
                const list = Object.values(initialRules);
                lastEmittedSigRef.current = JSON.stringify(list);
                onChange({
                    ...latestConfigRef.current,
                    survivorship: list,
                });
            }
        }

        if (tablesMissingTypes.size > 0) {
            let cancelled = false;
            Promise.all(
                Array.from(tablesMissingTypes).map(async (table) => {
                    try {
                        const res = await api.discoverColumns(table);
                        return { table, cols: (res.columns || []) as TableColumn[] };
                    } catch {
                        return { table, cols: [] as TableColumn[] };
                    }
                })
            ).then((results) => {
                if (cancelled) return;
                const colTypeByTable = new Map<string, Map<string, string>>();
                for (const { table, cols } of results) {
                    const m = new Map<string, string>();
                    cols.forEach(c => m.set(c.name.toLowerCase(), c.type));
                    colTypeByTable.set(table, m);
                }
                const resolved: Record<string, string> = { ...attrTypes };
                (config.sources || []).forEach(src => {
                    const m = colTypeByTable.get(src.table);
                    if (!m) return;
                    (src.attributes || []).forEach(a => {
                        if (!resolved[a.name]) {
                            const colName = (a.column || a.name).toLowerCase();
                            const t = m.get(colName);
                            if (t) resolved[a.name] = t;
                        }
                    });
                });
                setTypeByAttr(resolved);
            });
            return () => { cancelled = true; };
        }
    }, [config]);

    const commitRules = (nextRules: Record<string, SurvivorshipRule>) => {
        setRules(nextRules);
        const list = Object.values(nextRules);
        lastEmittedSigRef.current = JSON.stringify(list);
        onChange?.({
            ...latestConfigRef.current,
            survivorship: list,
        });
    };

    const updateRule = (attr: string, field: keyof SurvivorshipRule, value: any) => {
        commitRules({
            ...rules,
            [attr]: {
                ...rules[attr],
                [field]: value,
            },
        });
    };

    const applyBulkStrategy = (strategy: 'RECENCY' | 'PRIORITY' | 'FREQUENCY') => {
        const next: Record<string, SurvivorshipRule> = {};
        for (const attr of attributes) {
            next[attr] = {
                ...(rules[attr] || { attribute: attr, source_priority: sourcesByAttr[attr] || [] }),
                strategy,
            };
        }
        commitRules(next);
    };

    const movePriority = (attr: string, idx: number, direction: 'up' | 'down') => {
        const currentPriority = [...(rules[attr]?.source_priority || sourcesByAttr[attr] || [])];
        if (direction === 'up' && idx > 0) {
            [currentPriority[idx - 1], currentPriority[idx]] = [currentPriority[idx], currentPriority[idx - 1]];
        } else if (direction === 'down' && idx < currentPriority.length - 1) {
            [currentPriority[idx + 1], currentPriority[idx]] = [currentPriority[idx], currentPriority[idx + 1]];
        }
        updateRule(attr, 'source_priority', currentPriority);
    };

    const handleNext = () => {
        onNext({
            ...config,
            survivorship: Object.values(rules),
        });
    };

    const strategies = [
        { id: 'RECENCY', label: 'Recency (Newest Wins)', icon: Clock },
        { id: 'PRIORITY', label: 'Source Priority', icon: Layers },
        { id: 'PRIORITY_RECENCY', label: 'Source Priority + Recency', icon: Layers },
        { id: 'LATEST_FROM_SOURCE', label: 'Latest From Specific Source(s)', icon: Clock },
        { id: 'FREQUENCY', label: 'Frequency (Most Common)', icon: Zap },
        { id: 'ANY_TRUE', label: 'Consent: Any True (Opt-Out Safe)', icon: Zap },
        { id: 'ALL_TRUE', label: 'Consent: All True', icon: Zap },
        { id: 'EARLIEST', label: 'Earliest (First Seen)', icon: Clock },
        { id: 'AGG_MAX', label: 'Max Value', icon: ArrowUp },
        { id: 'AGG_MIN', label: 'Min Value', icon: ArrowUp },
        { id: 'AGG_SUM', label: 'Sum (LTV / Counts)', icon: Hash },
        { id: 'COLLECT_DISTINCT', label: 'Collect Distinct (List)', icon: Layers },
    ];

    return (
        <div className="space-y-4 flex flex-col">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                <div>
                    <h2 className="text-[15px] font-semibold text-ink">Golden Record Survivorship</h2>
                    <p className="text-xs text-ink-muted mt-0.5">
                        Choose conflict resolution strategies per unified profile attribute across competing sources.
                    </p>
                </div>

                {attributes.length > 0 && (
                    <div className="flex items-center gap-2 shrink-0">
                        <span className="text-[11px] text-ink-muted">Bulk apply:</span>
                        <button
                            type="button"
                            onClick={() => applyBulkStrategy('RECENCY')}
                            className="btn-secondary text-xs py-1 px-2.5 min-h-[28px]"
                        >
                            All Recency
                        </button>
                        <button
                            type="button"
                            onClick={() => applyBulkStrategy('PRIORITY')}
                            className="btn-secondary text-xs py-1 px-2.5 min-h-[28px]"
                        >
                            All Priority
                        </button>
                    </div>
                )}
            </div>

            {attributes.length > 0 && (
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 surface-subtle px-3.5 py-2.5">
                    <div className="relative flex-1 max-w-sm">
                        <Search className="w-3.5 h-3.5 text-ink-subtle absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none" />
                        <input
                            type="text"
                            value={attrSearch}
                            onChange={(e) => setAttrSearch(e.target.value)}
                            placeholder="Filter attributes..."
                            aria-label="Filter survivorship attributes"
                            className="input-field pl-8 pr-3 py-1 text-xs"
                        />
                    </div>
                    <div className="inline-flex rounded-md bg-surface border border-line-strong p-0.5 text-xs tabular-nums">
                        <button
                            type="button"
                            onClick={() => setConflictsOnly(false)}
                            className={`px-2.5 py-1 rounded font-medium transition-colors ${!conflictsOnly ? 'bg-accent text-accent-fg' : 'text-ink-muted hover:text-ink'}`}
                        >
                            All Attributes ({attributes.length})
                        </button>
                        <button
                            type="button"
                            onClick={() => setConflictsOnly(true)}
                            className={`px-2.5 py-1 rounded font-medium transition-colors ${conflictsOnly ? 'bg-accent text-accent-fg' : 'text-ink-muted hover:text-ink'}`}
                        >
                            Multi-Source Conflicts ({attributes.filter(a => (sourcesByAttr[a]?.length || 0) > 1).length})
                        </button>
                    </div>
                </div>
            )}

            <div className="space-y-2.5">
                {attributes.length === 0 && (
                    <div className="text-center p-10 text-xs text-ink-muted border border-dashed border-line-strong rounded-md">
                        No mapped profile attributes found. Map attribute columns in <strong>Sources &amp; Mappings</strong> to configure survivorship rules, or proceed directly to Run &amp; Verify.
                    </div>
                )}

                {attributes
                    .filter(attr => {
                        if (attrSearch.trim() && !attr.toLowerCase().includes(attrSearch.trim().toLowerCase())) {
                            return false;
                        }
                        if (conflictsOnly && (sourcesByAttr[attr]?.length || 0) <= 1) {
                            return false;
                        }
                        return true;
                    })
                    .map(attr => {
                    const rule = rules[attr] || {};
                    const currentStrategy = rule.strategy || 'RECENCY';
                    const validSources = sourcesByAttr[attr] || [];
                    const sqlType = typeByAttr[attr];
                    const category = classifySqlType(sqlType);

                    const applicableStrategies = strategies.filter(s => {
                        if (s.id === currentStrategy) return true;
                        if (category === 'unknown') return true;
                        if (s.id === 'ANY_TRUE' || s.id === 'ALL_TRUE') return category === 'boolean';
                        if (s.id === 'AGG_SUM') return category === 'numeric';
                        if (s.id === 'AGG_MAX' || s.id === 'AGG_MIN') {
                            return category === 'numeric' || category === 'datetime';
                        }
                        return true;
                    });

                    return (
                        <div key={attr} className="surface-panel p-3.5 space-y-3">
                            <div className="flex flex-wrap items-center justify-between gap-3">
                                <div className="flex items-center gap-2.5">
                                    <div className="p-1.5 bg-subtle border border-line rounded-md">
                                        <Hash className="w-4 h-4 text-accent" />
                                    </div>
                                    <div>
                                        <div className="flex items-center gap-2">
                                            <span className="font-semibold text-[13px] font-mono text-ink">{attr}</span>
                                            {sqlType && (
                                                <span className="badge-neutral font-mono uppercase">
                                                    {sqlType}
                                                </span>
                                            )}
                                        </div>
                                        <span className="text-xs text-ink-muted tabular-nums">
                                            Present in {validSources.length} {validSources.length === 1 ? 'source' : 'sources'}
                                        </span>
                                    </div>
                                </div>
                                <CustomSelect
                                    value={currentStrategy}
                                    onChange={(val) => updateRule(attr, 'strategy', val)}
                                    options={applicableStrategies.map(s => ({ value: s.id, label: s.label }))}
                                    ariaLabel={`Survivorship strategy for ${attr}`}
                                    buttonClassName="text-xs py-1.5 min-w-[240px]"
                                />
                            </div>

                            {(currentStrategy === 'PRIORITY' || currentStrategy === 'PRIORITY_RECENCY' || currentStrategy === 'LATEST_FROM_SOURCE') && (
                                <div className="space-y-2 surface-subtle p-3 text-xs">
                                    {validSources.length <= 1 ? (
                                        <div className="text-ink-muted">
                                            Only present in 1 source (<span className="font-mono text-ink">{validSources[0] || 'none'}</span>).
                                        </div>
                                    ) : (
                                        <>
                                            <label className="text-ink-muted text-[11px] uppercase tracking-[0.04em] font-semibold block">
                                                Source Priority Order (Highest to Lowest)
                                            </label>
                                            <div className="flex flex-col gap-1.5">
                                                {(rule.source_priority || validSources).map((src, idx, list) => (
                                                    <div key={src} className="flex items-center justify-between bg-surface px-3 py-1.5 rounded border border-line">
                                                        <span className="text-ink font-mono tabular-nums">{idx + 1}. {src}</span>
                                                        <div className="flex gap-1">
                                                            <button
                                                                type="button"
                                                                disabled={idx === 0}
                                                                onClick={() => movePriority(attr, idx, 'up')}
                                                                className="p-1 hover:bg-hover rounded disabled:opacity-25 text-ink-secondary"
                                                            >
                                                                <ArrowUp className="w-3.5 h-3.5" />
                                                            </button>
                                                            <button
                                                                type="button"
                                                                disabled={idx === list.length - 1}
                                                                onClick={() => movePriority(attr, idx, 'down')}
                                                                className="p-1 hover:bg-hover rounded disabled:opacity-25 text-ink-secondary"
                                                            >
                                                                <ArrowDown className="w-3.5 h-3.5" />
                                                            </button>
                                                        </div>
                                                    </div>
                                                ))}
                                            </div>
                                        </>
                                    )}
                                </div>
                            )}

                            {currentStrategy === 'RECENCY' && validSources.length > 1 && (
                                <div className="text-xs text-ink-muted">
                                    Resolves conflicting values using each source table&apos;s configured <strong className="text-ink-secondary">Watermark Column</strong>.
                                </div>
                            )}
                        </div>
                    );
                })}
            </div>

            <GroupedOutputEditor config={config} onChange={next => onChange?.(next)} />
            <ActivationDeltaEditor config={config} onChange={next => onChange?.(next)} />

            <div className="flex justify-between pt-3 border-t border-line">
                {onBack ? (
                    <button
                        type="button"
                        onClick={onBack}
                        className="btn-secondary text-xs"
                    >
                        ← Back to Rules
                    </button>
                ) : <div />}
                <button
                    type="button"
                    onClick={handleNext}
                    className="btn-primary text-xs"
                >
                    Continue to Run &amp; Verify →
                </button>
            </div>
        </div>
    );
}
