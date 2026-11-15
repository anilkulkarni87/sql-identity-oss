import { useState, useEffect } from 'react';
import { Layers, ArrowUp, Zap, Clock, Hash } from "lucide-react";
import { IDRConfig, SurvivorshipRule, TableColumn } from '../../types';
import { CustomSelect } from '../../components/CustomSelect';
import { api } from '../../api/client';

interface StepSurvivorshipProps {
    config: IDRConfig;
    onNext: (config: IDRConfig) => void;
    onBack: () => void;
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

export default function StepSurvivorship({ config, onNext, onBack }: StepSurvivorshipProps) {
    // Unique attributes across all sources
    const [attributes, setAttributes] = useState<string[]>([]);
    // Map of attribute -> list of source IDs that actually contain that attribute
    const [sourcesByAttr, setSourcesByAttr] = useState<Record<string, string[]>>({});
    // Map of attribute -> warehouse SQL data type
    const [typeByAttr, setTypeByAttr] = useState<Record<string, string>>({});
    // { attrName: { strategy: 'RECENCY', source_priority: ['table1', 'table2'] } }
    const [rules, setRules] = useState<Record<string, SurvivorshipRule>>({});

    useEffect(() => {
        // 1. Aggregate attributes, sources, and any already-known SQL types
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
        const attrList = Array.from(allAttrs).sort();
        setAttributes(attrList);
        setSourcesByAttr(attrSources);
        setTypeByAttr(attrTypes);

        // 2. Initialize rules (preserve existing survivorship rules when present)
        const existingByAttr = new Map<string, SurvivorshipRule>();
        for (const r of config.survivorship || []) {
            if (r.attribute) {
                existingByAttr.set(r.attribute, r);
            }
        }

        const initialRules: Record<string, SurvivorshipRule> = {};
        attrList.forEach(attr => {
            const validSources = attrSources[attr] || [];
            const validSet = new Set(validSources);
            const existing = existingByAttr.get(attr);

            let priority = validSources;
            if (existing?.source_priority && existing.source_priority.length > 0) {
                const filtered = existing.source_priority.filter(s => validSet.has(s));
                const missing = validSources.filter(s => !filtered.includes(s));
                priority = [...filtered, ...missing];
            }

            initialRules[attr] = existing ? {
                ...existing,
                source_priority: priority,
            } : {
                attribute: attr,
                strategy: 'RECENCY', // Default
                source_priority: priority,
            };
        });
        setRules(initialRules);

        // 3. Hydrate SQL types from warehouse schema if not already attached (e.g. imported/saved config)
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

    const updateRule = (attr: string, field: keyof SurvivorshipRule, value: any) => {
        setRules(prev => ({
            ...prev,
            [attr]: {
                ...prev[attr],
                [field]: value
            }
        }));
    };

    const handleNext = () => {
        // Merge survivorship rules into main config
        const finalConfig: IDRConfig = {
            ...config,
            survivorship: Object.values(rules)
        };
        onNext(finalConfig);
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

    const btnClass = "px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed";
    const btnOutlineClass = "px-4 py-2 border border-gray-600 text-gray-300 rounded-md text-sm font-medium hover:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-gray-500";

    return (
        <div className="space-y-6 h-full flex flex-col">
            <div>
                <h2 className="text-xl font-semibold text-white">Survivorship Rules</h2>
                <p className="text-sm text-gray-400">Decide how to resolve conflicting attribute values.</p>
            </div>

            <div className="flex-1 overflow-y-auto pr-2 space-y-4">
                {attributes.length === 0 && (
                    <div className="text-center p-10 text-gray-500 border border-dashed border-gray-700 rounded-lg">
                        No mapped attributes found. You can skip this step or go back to map some attributes.
                    </div>
                )}

                {attributes.map(attr => {
                    const rule = rules[attr] || {};
                    const currentStrategy = rule.strategy || 'RECENCY';
                    const validSources = sourcesByAttr[attr] || [];
                    const sqlType = typeByAttr[attr];
                    const category = classifySqlType(sqlType);

                    const applicableStrategies = strategies.filter(s => {
                        if (s.id === currentStrategy) return true; // Always preserve existing selection
                        if (category === 'unknown') return true; // No type metadata -> don't restrict
                        if (s.id === 'ANY_TRUE' || s.id === 'ALL_TRUE') return category === 'boolean';
                        if (s.id === 'AGG_SUM') return category === 'numeric';
                        if (s.id === 'AGG_MAX' || s.id === 'AGG_MIN') {
                            return category === 'numeric' || category === 'datetime';
                        }
                        return true;
                    });

                    return (
                        <div key={attr} className="border border-gray-700 rounded-lg bg-gray-800/50 p-4 space-y-4">
                            <div className="flex items-center justify-between">
                                <div className="flex items-center gap-3">
                                    <div className="p-2 bg-gray-700 rounded-md">
                                        <Hash className="w-4 h-4 text-blue-400" />
                                    </div>
                                    <div>
                                        <span className="font-semibold text-gray-200">{attr}</span>
                                        {sqlType && (
                                            <span className="ml-2 px-1.5 py-0.5 rounded bg-gray-700/80 text-[10px] font-mono text-gray-300 uppercase">
                                                {sqlType}
                                            </span>
                                        )}
                                        <span className="ml-2 text-xs text-gray-500">
                                            ({validSources.length} {validSources.length === 1 ? 'source' : 'sources'})
                                        </span>
                                    </div>
                                </div>
                                <CustomSelect
                                    value={currentStrategy}
                                    onChange={(val) => updateRule(attr, 'strategy', val)}
                                    options={applicableStrategies.map(s => ({ value: s.id, label: s.label }))}
                                    buttonClassName="text-sm min-w-[230px]"
                                />
                            </div>

                            {/* Strategy Specific Config */}
                            {(currentStrategy === 'PRIORITY' || currentStrategy === 'PRIORITY_RECENCY' || currentStrategy === 'LATEST_FROM_SOURCE') && (
                                <div className="space-y-2 bg-gray-900/50 p-3 rounded text-sm">
                                    {validSources.length <= 1 ? (
                                        <div className="text-xs text-gray-400">
                                            Only present in 1 source (<span className="font-mono text-gray-200">{validSources[0] || 'none'}</span>).
                                        </div>
                                    ) : (
                                        <>
                                            <label className="text-gray-400 text-xs uppercase tracking-wider font-semibold">Source Priority (High to Low)</label>
                                            <div className="flex flex-col gap-1">
                                                {(rule.source_priority || validSources).map((src, idx) => (
                                                    <div key={src} className="flex items-center justify-between bg-gray-800 px-3 py-2 rounded border border-gray-700">
                                                        <span className="text-gray-300">{idx + 1}. {src}</span>
                                                        <div className="flex gap-1">
                                                            <button
                                                                disabled={idx === 0}
                                                                onClick={() => {
                                                                    const newPriority = [...(rule.source_priority || validSources)];
                                                                    [newPriority[idx - 1], newPriority[idx]] = [newPriority[idx], newPriority[idx - 1]];
                                                                    updateRule(attr, 'source_priority', newPriority);
                                                                }}
                                                                className="p-1 hover:bg-gray-700 rounded disabled:opacity-20"
                                                            >
                                                                <ArrowUp className="w-3 h-3" />
                                                            </button>
                                                        </div>
                                                    </div>
                                                ))}
                                            </div>
                                        </>
                                    )}
                                </div>
                            )}

                            {currentStrategy === 'RECENCY' && (
                                <div className="text-xs text-gray-500 italic">
                                    Uses the <strong>Watermark Column</strong> configured for each source table.
                                </div>
                            )}
                        </div>
                    );
                })}
            </div>

            <div className="flex justify-between pt-4">
                <button className={btnOutlineClass} onClick={onBack}>Back</button>
                <button className={btnClass} onClick={handleNext}>Next: Review</button>
            </div>
        </div>
    );
}
