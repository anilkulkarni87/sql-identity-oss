import { useState, useEffect } from 'react';
import { Loader2, Key, Tag, ChevronDown, ChevronRight, Fingerprint, Clock, Info, CheckSquare, Square } from "lucide-react";
import { IDRConfig, SourceConfig, TableColumn, Identifier, Attribute } from '../../types';
import { api } from '../../api/client';

function HelpCard({ title, children }: { title: string, children: React.ReactNode }) {
    return (
        <div className="bg-blue-900/20 border border-blue-800 rounded p-3 mb-4 text-sm text-blue-200">
            <div className="flex items-center gap-2 font-semibold mb-1">
                <Info className="w-4 h-4" />
                {title}
            </div>
            <div className="text-blue-100/80 leading-relaxed text-xs">
                {children}
            </div>
        </div>
    );
}

interface StepMapProps {
    tables: string[];
    onNext: (config: IDRConfig) => void;
    onBack: () => void;
}

export default function StepMap({ tables, onNext, onBack }: StepMapProps) {
    const [columns, setColumns] = useState<Record<string, TableColumn[]>>({});
    const [config, setConfig] = useState<Record<string, SourceConfig>>({});
    const [loading, setLoading] = useState(false);
    const [openTable, setOpenTable] = useState<string | null>(tables[0] || null);

    useEffect(() => {
        loadColumns();
    }, [tables]);

    const loadColumns = async () => {
        setLoading(true);
        const newCols: Record<string, TableColumn[]> = {};
        const newConfig = { ...config };

        try {
            for (const table of tables) {
                if (columns[table]) {
                    newCols[table] = columns[table];
                    continue; // Already loaded
                }

                const data = await api.discoverColumns(table);
                const cols = data.columns as TableColumn[];
                newCols[table] = cols;

                // Initialize config for this table if missing
                if (!newConfig[table]) {
                    // Smart defaults
                    const likelyKey = cols.find(c => c.name.toLowerCase().includes('id') && !c.name.toLowerCase().includes('email'))?.name || cols[0]?.name;
                    const likelyWatermark = cols.find(c => ['updated_at', 'modified_at', 'last_modified', 'timestamp'].some(k => c.name.toLowerCase().includes(k)))?.name;

                    newConfig[table] = {
                        id: table.replace(/\./g, '_'),
                        table: table,
                        entity_key: likelyKey,
                        watermark_column: likelyWatermark || '',
                        // Initialize new fields
                        identifiers: [],
                        attributes: []
                    };
                    // Add lookback if it exists in SourceConfig type (we need to cast or add to type)
                    (newConfig[table] as any).watermark_lookback_minutes = 0;
                }
            }
            setColumns(newCols);
            setConfig(newConfig);
        } finally {
            setLoading(false);
        }
    };

    const updateConfig = (table: string, field: keyof SourceConfig, value: any) => {
        setConfig(prev => ({
            ...prev,
            [table]: {
                ...prev[table],
                [field]: value
            }
        }));
    };

    const toggleAttribute = (table: string, colName: string) => {
        setConfig(prev => {
            const currentAttrs = prev[table].attributes || [];
            const exists = currentAttrs.find(a => a.name === colName);

            let newAttrs: Attribute[];
            if (exists) {
                newAttrs = currentAttrs.filter(a => a.name !== colName);
            } else {
                newAttrs = [...currentAttrs, { name: colName, column: colName, expr: colName }];
                // Remove from identifiers if it was there
                prev[table].identifiers = (prev[table].identifiers || []).filter(i => i.column !== colName);
            }

            return {
                ...prev,
                [table]: {
                    ...prev[table],
                    attributes: newAttrs,
                    identifiers: prev[table].identifiers
                }
            };
        });
    };

    const toggleIdentifier = (table: string, colName: string) => {
        setConfig(prev => {
            const currentIds = prev[table].identifiers || [];
            const exists = currentIds.find(i => i.column === colName);

            let newIds: Identifier[];
            if (exists) {
                newIds = currentIds.filter(i => i.column !== colName);
            } else {
                // Default type guess
                let defaultType = colName.toLowerCase();
                if (defaultType.includes('email')) defaultType = 'email';
                else if (defaultType.includes('phone')) defaultType = 'phone';
                else if (defaultType.includes('ssn')) defaultType = 'ssn';
                else if (defaultType.includes('device')) defaultType = 'device_id';

                newIds = [...currentIds, { column: colName, type: defaultType, expr: colName, is_match_key: true }];
                // Remove from attributes if it was there
                prev[table].attributes = (prev[table].attributes || []).filter(a => a.name !== colName);
            }

            return {
                ...prev,
                [table]: {
                    ...prev[table],
                    identifiers: newIds,
                    attributes: prev[table].attributes
                }
            };
        });
    };

    const updateIdentifierType = (table: string, colName: string, newType: string) => {
        // Validation: Only allow alphanumeric and underscores
        let safeType = newType.replace(/[^a-zA-Z0-9_]/g, '').toLowerCase();

        // Prevent empty string - if usage clears it, maybe don't update? Or defaults?
        // Better: allow typing but validate on blur/save?
        // For now, let's just not allow empty rules generation if type is empty.
        // But here we just update state.

        setConfig(prev => {
            const currentIds = prev[table].identifiers || [];
            const newIds = currentIds.map(i =>
                i.column === colName ? { ...i, type: safeType } : i
            );
            return {
                ...prev,
                [table]: { ...prev[table], identifiers: newIds }
            };
        });
    };

    const toggleMatchKey = (table: string, colName: string) => {
        setConfig(prev => {
            const currentIds = prev[table].identifiers || [];
            const newIds = currentIds.map(i =>
                i.column === colName ? { ...i, is_match_key: !i.is_match_key } : i
            );
            return {
                ...prev,
                [table]: { ...prev[table], identifiers: newIds }
            };
        });
    };

    const getColumnState = (table: string, colName: string) => {
        const attrs = config[table]?.attributes || [];
        if (attrs.find(a => a.name === colName)) return { isAttr: true };

        const ids = config[table]?.identifiers || [];
        const idMatch = ids.find(i => i.column === colName);
        if (idMatch) return { isId: true, type: idMatch.type, isMatch: idMatch.is_match_key !== false };

        return {};
    };

    const handleNext = () => {
        // Validation Gate: Ensure at least one identifier is mapped across all tables
        const allIdentifiers = tables.flatMap(t => config[t]?.identifiers || []);
        if (allIdentifiers.length === 0) {
            alert("Please map at least one Identifier (e.g. Email, Phone) to enable matching.");
            return;
        }

        // Dynamically generate rules based on used identifier types
        // Create matching rules for each unique identifier type used THAT IS ENABLED for matching
        // AND has a valid type (not empty)
        const enabledIdentifiers = allIdentifiers.filter(i => i.is_match_key !== false && i.type && i.type.trim() !== '');

        const usedTypes = new Set(enabledIdentifiers.map(i => i.type));
        const dynamicRules = Array.from(usedTypes).map((type, idx) => ({
            id: 100 + idx,
            type: 'EXACT',
            match_keys: [type],
            priority: idx + 1,
            canonicalize: 'LOWERCASE' as const
        }));

        // Transform UI state to standard config format
        const finalConfig: IDRConfig = {
            sources: tables.map(t => ({
                id: config[t].id,
                table: t,
                entity_key: config[t].entity_key,
                watermark_column: config[t].watermark_column,
                // Ensure identifiers have 'expr' set
                identifiers: (config[t].identifiers || []).map(i => ({
                    type: i.type,
                    column: i.column,
                    expr: i.column // Use column name as expression by default
                })),
                attributes: config[t].attributes || []
            })),
            rules: dynamicRules
        };

        onNext(finalConfig);
    };

    const btnClass = "px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed";
    const btnOutlineClass = "px-4 py-2 border border-gray-600 text-gray-300 rounded-md text-sm font-medium hover:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-gray-500";

    if (loading && Object.keys(columns).length === 0) {
        return <div className="flex justify-center p-20"><Loader2 className="animate-spin w-8 h-8 text-blue-500" /></div>;
    }

    return (
        <div className="space-y-6 h-full flex flex-col">
            <div>
                <h2 className="text-xl font-semibold text-white">Map Columns</h2>
                <p className="text-sm text-gray-400">Define how IDR interprets your data schema.</p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                <HelpCard title="Entity Key">
                    Unique primary key for each row. Stable ID used to track records over time.
                </HelpCard>
                <HelpCard title="Identifiers">
                    Keys used for matching (Email, Phone). Use "Match Toggle" to include/exclude from matching rules.
                </HelpCard>
                <HelpCard title="Attributes">
                    Profile data (Name, City) attached to the unified ID. Used for Golden Record generation.
                </HelpCard>
            </div>

            <div className="flex-1 overflow-y-auto pr-2 space-y-2">
                {tables.map(table => {
                    const tableConfig = config[table] || {};
                    const tableCols = columns[table] || [];
                    const attrCount = (tableConfig.attributes || []).length;
                    const idCount = (tableConfig.identifiers || []).length;
                    const isOpen = openTable === table;

                    return (
                        <div key={table} className="border border-gray-700 rounded-lg bg-gray-800/50 overflow-hidden">
                            <button
                                className="w-full flex items-center justify-between p-4 bg-gray-800 hover:bg-gray-700 transition-colors"
                                onClick={() => setOpenTable(isOpen ? null : table)}
                            >
                                <div className="flex gap-4 items-center">
                                    {isOpen ? <ChevronDown className="w-4 h-4 text-gray-400" /> : <ChevronRight className="w-4 h-4 text-gray-400" />}
                                    <span className="font-semibold text-sm text-gray-200">{table}</span>
                                    {tableConfig.entity_key && (
                                        <span className="text-xs font-normal flex gap-1 items-center px-2 py-0.5 rounded border border-gray-600 text-gray-300 truncate max-w-[150px]">
                                            <Key className="w-3 h-3" /> {tableConfig.entity_key}
                                        </span>
                                    )}
                                    <span className="text-xs font-normal flex gap-1 items-center px-2 py-0.5 rounded bg-gray-700 text-gray-300">
                                        <Fingerprint className="w-3 h-3" /> {idCount} IDs
                                    </span>
                                    <span className="text-xs font-normal flex gap-1 items-center px-2 py-0.5 rounded bg-gray-700 text-gray-300">
                                        <Tag className="w-3 h-3" /> {attrCount} Attrs
                                    </span>
                                </div>
                            </button>

                            {isOpen && (
                                <div className="p-4 space-y-6 border-t border-gray-700 bg-gray-900/30">
                                    <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                                        {/* Configuration Section */}
                                        <div className="space-y-4">
                                            {/* Entity Key */}
                                            <div className="space-y-2">
                                                <label className="flex items-center gap-2 text-sm font-medium text-gray-300">
                                                    <Key className="w-4 h-4 text-orange-500" /> Entity Key (Primary ID)
                                                </label>
                                                <select
                                                    value={tableConfig.entity_key || ''}
                                                    onChange={e => updateConfig(table, 'entity_key', e.target.value)}
                                                    className="w-full bg-gray-900 border border-gray-700 rounded-md px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
                                                >
                                                    {tableCols.map(c => (
                                                        <option key={c.name} value={c.name}>
                                                            {c.name} ({c.type})
                                                        </option>
                                                    ))}
                                                </select>
                                                <p className="text-xs text-gray-500">The stable unique identifier for rows in this table.</p>
                                            </div>

                                            {/* Watermark Column */}
                                            <div className="space-y-2">
                                                <label className="flex items-center gap-2 text-sm font-medium text-gray-300">
                                                    <Clock className="w-4 h-4 text-blue-400" /> Watermark Column
                                                </label>
                                                <select
                                                    value={tableConfig.watermark_column || ''}
                                                    onChange={e => updateConfig(table, 'watermark_column', e.target.value)}
                                                    className="w-full bg-gray-900 border border-gray-700 rounded-md px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
                                                >
                                                    <option value="">(None - Full Refresh)</option>
                                                    {tableCols.map(c => (
                                                        <option key={c.name} value={c.name}>
                                                            {c.name} ({c.type})
                                                        </option>
                                                    ))}
                                                </select>
                                                <p className="text-xs text-gray-500">Used for incremental runs (e.g. updated_at).</p>
                                            </div>

                                            {/* Watermark Lookback - NEW */}
                                            <div className="space-y-2">
                                                <label className="flex items-center gap-2 text-sm font-medium text-gray-300">
                                                    <Clock className="w-4 h-4 text-gray-400" /> Lookback (Minutes)
                                                </label>
                                                <input
                                                    type="number"
                                                    value={(tableConfig as any).watermark_lookback_minutes || 0}
                                                    onChange={e => updateConfig(table, 'watermark_lookback_minutes' as any, parseInt(e.target.value) || 0)}
                                                    className="w-full bg-gray-900 border border-gray-700 rounded-md px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
                                                />
                                                <p className="text-xs text-gray-500">Buffer to handle late-arriving data (0 = strict).</p>
                                            </div>

                                        </div>

                                        {/* Attribute & Identifier Mapping Grid */}
                                        <div className="space-y-2 col-span-1 md:col-span-2">
                                            <label className="flex items-center gap-2 text-sm font-medium text-gray-300">
                                                <Fingerprint className="w-4 h-4 text-purple-500" /> Map Columns
                                            </label>
                                            <p className="text-xs text-gray-500 mb-2">Tag columns as Identifiers (matched against) or Attributes (profile data).</p>

                                            <div className="border border-gray-700 rounded bg-gray-900 h-[300px] overflow-y-auto">
                                                <div className="grid grid-cols-12 gap-2 p-2 px-4 border-b border-gray-800 text-xs font-semibold text-gray-500 uppercase sticky top-0 bg-gray-900 z-10">
                                                    <div className="col-span-3">Column</div>
                                                    <div className="col-span-2">Type</div>
                                                    <div className="col-span-1 text-center" title="Use for Matching">Match</div>
                                                    <div className="col-span-6 text-right">Mapping</div>
                                                </div>

                                                {tableCols.map(c => {
                                                    const state = getColumnState(table, c.name);

                                                    return (
                                                        <div key={c.name} className="grid grid-cols-12 gap-2 p-2 px-4 items-center hover:bg-gray-800 border-b border-gray-800/50 last:border-0">
                                                            <div className="col-span-3 text-sm font-medium text-gray-200 truncate" title={c.name}>{c.name}</div>
                                                            <div className="col-span-2 text-xs text-gray-500">{c.type}</div>

                                                            <div className="col-span-1 flex justify-center">
                                                                {state.isId && (
                                                                    <button
                                                                        onClick={() => toggleMatchKey(table, c.name)}
                                                                        className={`p-1 rounded hover:bg-gray-700 ${state.isMatch ? 'text-blue-400' : 'text-gray-600'}`}
                                                                        title={state.isMatch ? "Used for Matching" : "Excluded from Matching"}
                                                                    >
                                                                        {state.isMatch ? <CheckSquare className="w-4 h-4" /> : <Square className="w-4 h-4" />}
                                                                    </button>
                                                                )}
                                                            </div>

                                                            <div className="col-span-6 flex gap-2 justify-end items-center">
                                                                {state.isId ? (
                                                                    <div className="flex items-center gap-2">
                                                                        <input
                                                                            type="text"
                                                                            value={state.type}
                                                                            onChange={(e) => updateIdentifierType(table, c.name, e.target.value)}
                                                                            className="bg-gray-800 border border-gray-600 rounded px-2 py-1 text-xs text-white w-24 focus:outline-none focus:border-blue-500"
                                                                            placeholder="Type..."
                                                                        />
                                                                        <button
                                                                            onClick={() => toggleIdentifier(table, c.name)}
                                                                            className="px-2 py-1 text-xs rounded border bg-blue-600 border-blue-500 text-white transition-colors"
                                                                        >
                                                                            Identifier
                                                                        </button>
                                                                    </div>
                                                                ) : (
                                                                    <button
                                                                        onClick={() => toggleIdentifier(table, c.name)}
                                                                        className="px-2 py-1 text-xs rounded border bg-transparent border-gray-700 text-gray-400 hover:border-gray-500 transition-colors"
                                                                    >
                                                                        Identifier
                                                                    </button>
                                                                )}

                                                                <button
                                                                    onClick={() => toggleAttribute(table, c.name)}
                                                                    className={`px-2 py-1 text-xs rounded border transition-colors ${state.isAttr
                                                                        ? 'bg-green-600/20 border-green-500 text-green-400'
                                                                        : 'bg-transparent border-gray-700 text-gray-400 hover:border-gray-500'
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
                                </div>
                            )}
                        </div>
                    );
                })}
            </div>

            <div className="flex justify-between pt-4">
                <button className={btnOutlineClass} onClick={onBack}>Back</button>
                <button className={btnClass} onClick={handleNext}>Next: Rules</button>
            </div>
        </div >
    );
}
