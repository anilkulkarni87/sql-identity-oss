import { useState, useEffect } from 'react';
import { GitGraph, ArrowUp, ArrowDown, Trash2, Plus, Wand2, ShieldAlert, Link2 } from "lucide-react";
import { ExclusionRule, IDRConfig, MatchingRule, OverrideRule } from '../../types';
import { api } from '../../api/client';

const BUILTIN_CANONICALIZERS = [
    'EMAIL_STANDARD',
    'EMAIL_STRIP_PLUS',
    'PHONE_US_10',
    'PHONE_DIGITS',
    'ALPHANUMERIC_UPPER',
    'LOWERCASE',
    'UPPERCASE',
    'EXACT',
    'NONE',
];

interface StepRulesProps {
    config: IDRConfig;
    onNext: (config: IDRConfig) => void;
    onBack: () => void;
}

interface FuzzyTemplate {
    id: string;
    label: string;
    sql_template: string;
    default_threshold: number;
    description: string;
}

export default function StepRules({ config, onNext, onBack }: StepRulesProps) {
    const [rules, setRules] = useState<MatchingRule[]>([]);
    const [exclusions, setExclusions] = useState<ExclusionRule[]>(config.exclusions || []);
    const [useDefaultExclusions, setUseDefaultExclusions] = useState<boolean>(!!config.use_default_exclusions);
    const [newExclType, setNewExclType] = useState<string>('');
    const [newExclValue, setNewExclValue] = useState<string>('');
    const [newExclMatch, setNewExclMatch] = useState<'EXACT' | 'LIKE'>('EXACT');
    const [overrides, setOverrides] = useState<OverrideRule[]>(config.overrides || []);
    const [newOvType, setNewOvType] = useState<'FORCE_LINK' | 'DO_NOT_LINK' | 'ISOLATE'>('DO_NOT_LINK');
    const [newOvKeyA, setNewOvKeyA] = useState<string>('');
    const [newOvKeyB, setNewOvKeyB] = useState<string>('');
    const [newOvReason, setNewOvReason] = useState<string>('');
    const [availableTypes, setAvailableTypes] = useState<string[]>([]);
    const [availableAttributes, setAvailableAttributes] = useState<string[]>([]);
    const [fuzzyTemplates, setFuzzyTemplates] = useState<FuzzyTemplate[]>([]);

    useEffect(() => {
        // Initialize rules (combine exact and fuzzy)
        const combined: MatchingRule[] = [
            ...(config.rules || []),
            ...(config.fuzzy_rules || []).map(f => ({ ...f, type: 'FUZZY' }))
        ].sort((a, b) => a.priority - b.priority);

        if (combined.length > 0) {
            setRules(combined);
        }
        setExclusions(config.exclusions || []);
        setUseDefaultExclusions(!!config.use_default_exclusions);
        setOverrides(config.overrides || []);

        // Available match keys & attributes
        const typeSet = new Set<string>();
        const attrSet = new Set<string>();
        (config.sources || []).forEach(src => {
            (src.identifiers || []).forEach(id => {
                if (id.is_match_key !== false) typeSet.add(id.type);
            });
            (src.attributes || []).forEach(attr => {
                if (attr.name) attrSet.add(attr.name);
            });
        });
        const typesArr = Array.from(typeSet).sort();
        const attrsArr = Array.from(attrSet).sort();
        setAvailableTypes(typesArr);
        setAvailableAttributes(attrsArr.length > 0 ? attrsArr : typesArr);
        if (typesArr.length > 0 && !newExclType) {
            setNewExclType(typesArr[0]);
        }

        // Fetch fuzzy templates
        api.getFuzzyTemplates()
            .then(data => setFuzzyTemplates(data.templates || []))
            .catch(err => console.error("Failed to fetch templates", err));

    }, [config]);

    const resolveTemplateSql = (templateSql: string, colName: string) => {
        const safeCol = colName || availableAttributes[0] || availableTypes[0] || 'first_name';
        return templateSql.replace(/\{col\}/g, safeCol);
    };

    const updatePriorities = (newRules: MatchingRule[]) => {
        setRules(newRules.map((r, i) => ({ ...r, priority: i + 1 })));
    };

    const moveRule = (idx: number, direction: 'up' | 'down') => {
        const newRules = [...rules];
        if (direction === 'up' && idx > 0) {
            [newRules[idx], newRules[idx - 1]] = [newRules[idx - 1], newRules[idx]];
        } else if (direction === 'down' && idx < newRules.length - 1) {
            [newRules[idx], newRules[idx + 1]] = [newRules[idx + 1], newRules[idx]];
        }
        updatePriorities(newRules);
    };

    const deleteRule = (idx: number) => {
        updatePriorities(rules.filter((_, i) => i !== idx));
    };

    const addExactRule = (type: string) => {
        const lowerType = type.toLowerCase();
        const defaultCanon = lowerType.includes('email')
            ? 'EMAIL_STANDARD'
            : lowerType.includes('phone')
            ? 'PHONE_US_10'
            : 'LOWERCASE';
        const newRule: MatchingRule = {
            id: generateId(),
            type: 'EXACT',
            match_keys: [type],
            priority: rules.length + 1,
            canonicalize: defaultCanon,
            max_group_size: 10000,
        };
        updatePriorities([...rules, newRule]);
    };

    const addFuzzyRule = () => {
        const defaultAttr = availableAttributes[0] || availableTypes[0] || 'first_name';
        const tmpl = fuzzyTemplates[0] || { sql_template: 'jaro_winkler_similarity(<a>.{col}, <b>.{col})', default_threshold: 0.85 };
        const newRule: MatchingRule = {
            id: generateId(),
            type: 'FUZZY',
            match_keys: [],
            priority: rules.length + 1,
            blocking_key: availableTypes[0] || '',
            fuzzy_attribute: defaultAttr,
            score_expr: resolveTemplateSql(tmpl.sql_template, defaultAttr),
            threshold: tmpl.default_threshold
        };
        updatePriorities([...rules, newRule]);
    };

    const updateRuleField = (idx: number, field: keyof MatchingRule, value: any) => {
        const newRules = [...rules];
        const current = { ...newRules[idx], [field]: value };
        if (field === 'fuzzy_attribute' && current.score_expr) {
            // Re-substitute attribute name inside <a>.<col> and <b>.<col>
            current.score_expr = current.score_expr
                .replace(/\{col\}/g, value)
                .replace(/<a>\.[a-zA-Z0-9_]+/g, `<a>.${value}`)
                .replace(/<b>\.[a-zA-Z0-9_]+/g, `<b>.${value}`);
        }
        newRules[idx] = current;
        setRules(newRules);
    };

    const applyTemplate = (idx: number, tmplId: string) => {
        const tmpl = fuzzyTemplates.find(t => t.id === tmplId);
        if (tmpl) {
            const newRules = [...rules];
            const col = newRules[idx].fuzzy_attribute || availableAttributes[0] || availableTypes[0] || 'first_name';
            newRules[idx] = {
                ...newRules[idx],
                fuzzy_attribute: col,
                score_expr: resolveTemplateSql(tmpl.sql_template, col),
                threshold: tmpl.default_threshold
            };
            setRules(newRules);
        }
    };

    const addExclusion = () => {
        const trimmed = newExclValue.trim();
        if (!trimmed) return;
        const type = newExclType || availableTypes[0] || 'email';
        setExclusions(prev => [
            ...prev,
            { type, value: trimmed, match: newExclMatch, reason: 'UI exclusion' }
        ]);
        setNewExclValue('');
    };

    const removeExclusion = (idx: number) => {
        setExclusions(prev => prev.filter((_, i) => i !== idx));
    };

    const addOverride = () => {
        const ka = newOvKeyA.trim();
        const kb = newOvKeyB.trim();
        if (!ka) return;
        if (newOvType !== 'ISOLATE' && !kb) return;
        setOverrides(prev => [
            ...prev,
            {
                override_type: newOvType,
                entity_key_a: ka,
                entity_key_b: newOvType === 'ISOLATE' ? '__ISOLATE__' : kb,
                reason: newOvReason.trim() || 'UI stewardship override'
            }
        ]);
        setNewOvKeyA('');
        setNewOvKeyB('');
        setNewOvReason('');
    };

    const removeOverride = (idx: number) => {
        setOverrides(prev => prev.filter((_, i) => i !== idx));
    };

    const generateId = () => Math.floor(Math.random() * 1000000);

    const handleNext = () => {
        if (rules.length === 0) {
            alert("Define at least one rule.");
            return;
        }
        const exact = rules.filter(r => r.type === 'EXACT');
        const fuzzy = rules.filter(r => r.type === 'FUZZY').map(r => ({
            ...r,
            score_expr: resolveTemplateSql(r.score_expr || '', r.fuzzy_attribute || availableAttributes[0] || 'first_name')
        }));

        onNext({
            ...config,
            rules: exact,
            fuzzy_rules: fuzzy,
            exclusions,
            overrides,
            use_default_exclusions: useDefaultExclusions,
        });
    };

    const btnClass = "px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50";
    const btnOutlineClass = "px-4 py-2 border border-gray-600 text-gray-300 rounded-md text-sm font-medium hover:bg-gray-800";

    return (
        <div className="space-y-6 h-full flex flex-col">
            <div>
                <h2 className="text-xl font-semibold text-white">Matching Rules</h2>
                <p className="text-sm text-gray-400">Define Exact and Fuzzy rules for linking records.</p>
            </div>

            <div className="flex-1 overflow-y-auto pr-2 space-y-4">
                <div className="space-y-3">
                    {rules.map((rule, idx) => (
                        <div key={rule.id} className="flex gap-4 p-4 bg-gray-800 border border-gray-700 rounded-lg shadow-sm">
                            <div className="flex flex-col gap-1 pt-2">
                                <button onClick={() => moveRule(idx, 'up')} disabled={idx === 0} className="p-1 hover:bg-gray-700 rounded text-gray-400 disabled:opacity-30"><ArrowUp className="w-4 h-4" /></button>
                                <span className="text-xs font-bold text-center text-gray-500">{rule.priority}</span>
                                <button onClick={() => moveRule(idx, 'down')} disabled={idx === rules.length - 1} className="p-1 hover:bg-gray-700 rounded text-gray-400 disabled:opacity-30"><ArrowDown className="w-4 h-4" /></button>
                            </div>

                            <div className="flex-1 space-y-3">
                                <div className="flex items-center gap-2">
                                    <span className={`text-xs px-2 py-0.5 rounded-full border ${rule.type === 'EXACT' ? 'bg-blue-900/30 text-blue-300 border-blue-800' : 'bg-purple-900/30 text-purple-300 border-purple-800'}`}>
                                        {rule.type}
                                    </span>
                                    {rule.type === 'EXACT' ? (
                                        <div className="flex items-center gap-2">
                                            <GitGraph className="w-4 h-4 text-blue-400" />
                                            <span className="font-semibold text-gray-200">Match by {rule.match_keys.join(' + ')}</span>
                                        </div>
                                    ) : (
                                        <div className="flex items-center gap-2">
                                            <Wand2 className="w-4 h-4 text-purple-400" />
                                            <span className="font-semibold text-gray-200">Fuzzy Match</span>
                                        </div>
                                    )}
                                </div>

                                {rule.type === 'EXACT' ? (
                                    <div className="space-y-2">
                                        <div className="flex flex-wrap items-center gap-2">
                                            <label className="text-xs text-gray-500">Normalization:</label>
                                            <select
                                                value={
                                                    BUILTIN_CANONICALIZERS.includes((rule.canonicalize || 'LOWERCASE').toUpperCase())
                                                        ? (rule.canonicalize || 'LOWERCASE').toUpperCase()
                                                        : 'CUSTOM_SQL'
                                                }
                                                onChange={(e) => {
                                                    const val = e.target.value;
                                                    if (val === 'CUSTOM_SQL') {
                                                        updateRuleField(idx, 'canonicalize', 'LOWER(TRIM({val}))');
                                                    } else {
                                                        updateRuleField(idx, 'canonicalize', val);
                                                    }
                                                }}
                                                className="bg-gray-900 border border-gray-700 text-xs text-gray-300 rounded px-2 py-1 focus:outline-none focus:border-blue-500"
                                                onClick={(e) => e.stopPropagation()}
                                            >
                                                <option value="EMAIL_STANDARD">Email Standard (Trim + Lowercase, keeps +tag)</option>
                                                <option value="EMAIL_STRIP_PLUS">Email Strip +Tag (user+tag@domain → user@domain)</option>
                                                <option value="PHONE_US_10">Phone US 10-Digit (Strips +1 & punctuation)</option>
                                                <option value="PHONE_DIGITS">Phone Digits Only (Strips non-digits)</option>
                                                <option value="ALPHANUMERIC_UPPER">Alphanumeric Upper (Strips spaces/hyphens)</option>
                                                <option value="LOWERCASE">Lowercase</option>
                                                <option value="UPPERCASE">Uppercase</option>
                                                <option value="EXACT">Exact / None</option>
                                                <option value="CUSTOM_SQL">Custom Warehouse SQL (&#123;val&#125;)...</option>
                                            </select>
                                        </div>
                                        {!BUILTIN_CANONICALIZERS.includes((rule.canonicalize || 'LOWERCASE').toUpperCase()) && (
                                            <div className="flex items-center gap-2">
                                                <span className="text-[11px] font-mono text-blue-400">SQL Expression:</span>
                                                <input
                                                    type="text"
                                                    value={rule.canonicalize || ''}
                                                    onChange={(e) => updateRuleField(idx, 'canonicalize', e.target.value)}
                                                    placeholder="e.g. LPAD(LTRIM(TRIM({val}), '0'), 8, '0')"
                                                    className="flex-1 bg-gray-900 border border-blue-700/60 text-xs font-mono text-gray-200 rounded px-2.5 py-1 focus:outline-none focus:border-blue-400"
                                                />
                                            </div>
                                        )}
                                        <div className="flex flex-wrap items-center gap-2 pt-1 border-t border-gray-700/50">
                                            <label className="text-xs text-gray-400">Max Group Size:</label>
                                            <input
                                                type="number"
                                                min="2"
                                                value={rule.max_group_size ?? 10000}
                                                onChange={(e) => updateRuleField(idx, 'max_group_size', parseInt(e.target.value) || 10000)}
                                                className="w-24 bg-gray-900 border border-gray-700 text-xs font-mono text-gray-200 rounded px-2 py-1 focus:outline-none focus:border-blue-500"
                                            />
                                            <span className="text-[11px] text-gray-500">
                                                (Skips identifier values with more than this many records to prevent mega-clusters)
                                            </span>
                                        </div>
                                    </div>
                                ) : (
                                    <div className="grid grid-cols-3 gap-3 bg-gray-900/50 p-3 rounded border border-gray-700">
                                        <div>
                                            <label className="text-xs text-gray-500 block mb-1">Blocking Key</label>
                                            <select
                                                value={rule.blocking_key || ''}
                                                onChange={(e) => updateRuleField(idx, 'blocking_key', e.target.value)}
                                                className="w-full bg-gray-900 border border-gray-700 text-xs text-gray-300 rounded px-2 py-1 focus:outline-none focus:border-purple-500"
                                            >
                                                {Array.from(new Set([...availableTypes, ...availableAttributes])).map(t => (
                                                    <option key={t} value={t}>{t}</option>
                                                ))}
                                            </select>
                                        </div>
                                        <div>
                                            <label className="text-xs text-gray-500 block mb-1">Compare Attribute</label>
                                            <select
                                                value={rule.fuzzy_attribute || availableAttributes[0] || ''}
                                                onChange={(e) => updateRuleField(idx, 'fuzzy_attribute', e.target.value)}
                                                className="w-full bg-gray-900 border border-gray-700 text-xs text-gray-300 rounded px-2 py-1 focus:outline-none focus:border-purple-500"
                                            >
                                                {availableAttributes.map(a => <option key={a} value={a}>{a}</option>)}
                                            </select>
                                        </div>
                                        <div>
                                            <label className="text-xs text-gray-500 block mb-1">Method</label>
                                            <select
                                                onChange={(e) => applyTemplate(idx, e.target.value)}
                                                className="w-full bg-gray-900 border border-gray-700 text-xs text-gray-300 rounded px-2 py-1 focus:outline-none focus:border-purple-500"
                                                defaultValue=""
                                            >
                                                <option value="" disabled>Select template...</option>
                                                {fuzzyTemplates.map(t => <option key={t.id} value={t.id}>{t.label}</option>)}
                                            </select>
                                        </div>
                                        <div className="col-span-3">
                                            <div className="flex justify-between text-xs text-gray-500 mb-1">
                                                <span>Threshold</span>
                                                <span className="text-purple-300 font-mono">{rule.threshold}</span>
                                            </div>
                                            <input
                                                type="range" min="0.5" max="1.0" step="0.01"
                                                value={rule.threshold || 0.85}
                                                onChange={(e) => updateRuleField(idx, 'threshold', parseFloat(e.target.value))}
                                                className="w-full accent-purple-500"
                                            />
                                        </div>
                                        {rule.score_expr && (
                                            <div className="col-span-3">
                                                <span className="text-[11px] font-mono text-gray-500 block truncate" title={rule.score_expr}>
                                                    SQL: {rule.score_expr}
                                                </span>
                                            </div>
                                        )}
                                    </div>
                                )}
                            </div>
                            <button onClick={() => deleteRule(idx)} className="p-2 hover:bg-red-900/20 text-gray-500 hover:text-red-400 rounded transition-colors"><Trash2 className="w-4 h-4" /></button>
                        </div>
                    ))}

                    {rules.length === 0 && (
                        <div className="text-center p-8 border border-dashed border-gray-700 rounded-lg text-gray-500">
                            No rules defined. Add a rule to start.
                        </div>
                    )}
                </div>

                <div className="pt-4 border-t border-gray-800">
                    <label className="text-sm font-medium text-gray-300 mb-2 block">Add New Rule</label>
                    <div className="flex flex-wrap gap-2">
                        {availableTypes.map(type => (
                            <button key={type} onClick={() => addExactRule(type)} className="flex items-center gap-2 px-3 py-2 bg-gray-800 border border-gray-700 hover:border-blue-500 hover:text-blue-400 rounded-md text-sm text-gray-300 transition-all">
                                <Plus className="w-3 h-3" /> Exact: {type}
                            </button>
                        ))}
                        <div className="w-px bg-gray-700 mx-2 h-8"></div>
                        <button onClick={addFuzzyRule} className="flex items-center gap-2 px-3 py-2 bg-purple-900/30 border border-purple-800 hover:border-purple-500 hover:text-purple-300 rounded-md text-sm text-purple-300 transition-all">
                            <Wand2 className="w-3 h-3" /> Add Fuzzy Rule
                        </button>
                    </div>
                </div>


                {/* Interactive Identifier Exclusions Editor */}
                <div className="bg-gray-800/70 border border-gray-700 rounded-lg p-4 mt-4 space-y-3">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                        <div className="flex items-center gap-2">
                            <ShieldAlert className="w-4 h-4 text-amber-400" />
                            <h4 className="font-medium text-sm text-gray-200">Identifier Exclusions (Blocklist)</h4>
                        </div>
                        <label className="inline-flex items-center gap-2 text-xs text-amber-300 cursor-pointer bg-amber-950/40 border border-amber-800/60 px-2.5 py-1 rounded-md">
                            <input
                                type="checkbox"
                                checked={useDefaultExclusions}
                                onChange={(e) => setUseDefaultExclusions(e.target.checked)}
                                className="accent-amber-500 rounded"
                            />
                            <span>Include Curated Default Toxic Pack (33 rules: noreply@%, 0000000000, N/A, etc.)</span>
                        </label>
                    </div>
                    <p className="text-xs text-gray-400">
                        Prevent placeholder values (e.g. <code className="bg-gray-900 px-1 py-0.5 rounded">test@test.com</code>, <code className="bg-gray-900 px-1 py-0.5 rounded">0000000000</code>) from linking unrelated entities. Rows manually inserted into <code className="bg-gray-900 px-1 py-0.5 rounded">idr_meta.identifier_exclusion</code> via SQL are always preserved.
                    </p>
                    <div className="flex flex-wrap gap-2">
                        <select
                            value={newExclType || availableTypes[0] || 'ANY'}
                            onChange={(e) => setNewExclType(e.target.value)}
                            className="bg-gray-900 border border-gray-700 text-xs text-gray-200 rounded px-2 py-1.5"
                        >
                            <option value="ANY">ANY (All Types)</option>
                            {availableTypes.map(t => <option key={t} value={t}>{t}</option>)}
                        </select>
                        <select
                            value={newExclMatch}
                            onChange={(e) => setNewExclMatch(e.target.value as 'EXACT' | 'LIKE')}
                            className="bg-gray-900 border border-gray-700 text-xs text-gray-200 rounded px-2 py-1.5"
                        >
                            <option value="EXACT">EXACT</option>
                            <option value="LIKE">LIKE (%)</option>
                        </select>
                        <input
                            type="text"
                            value={newExclValue}
                            onChange={(e) => setNewExclValue(e.target.value)}
                            placeholder="Value or pattern (e.g. test@test.com)"
                            className="flex-1 min-w-[180px] bg-gray-900 border border-gray-700 text-xs text-gray-200 rounded px-3 py-1.5"
                        />
                        <button
                            type="button"
                            onClick={addExclusion}
                            className="px-3 py-1.5 bg-amber-600/30 border border-amber-600/60 hover:bg-amber-600/50 text-amber-200 rounded text-xs font-medium"
                        >
                            + Add Exclusion
                        </button>
                    </div>
                    {exclusions.length > 0 && (
                        <div className="flex flex-wrap gap-2 pt-2">
                            {exclusions.map((ex, i) => (
                                <span
                                    key={`${ex.type}-${ex.value}-${i}`}
                                    className="inline-flex items-center gap-2 px-2.5 py-1 bg-gray-900 border border-gray-700 rounded-full text-xs text-gray-300"
                                >
                                    <span className="text-amber-400 font-semibold">{ex.type}</span>
                                    <span className="text-gray-500">({ex.match || ex.match_type || 'EXACT'})</span>
                                    <span className="font-mono">{ex.value}</span>
                                    <button
                                        type="button"
                                        onClick={() => removeExclusion(i)}
                                        className="text-gray-500 hover:text-red-400"
                                    >
                                        ×
                                    </button>
                                </span>
                            ))}
                        </div>
                    )}
                </div>

                {/* Manual Stewardship Overrides Editor */}
                <div className="bg-gray-800/70 border border-gray-700 rounded-lg p-4 mt-4 space-y-3">
                    <div className="flex items-center gap-2">
                        <Link2 className="w-4 h-4 text-emerald-400" />
                        <h4 className="font-medium text-sm text-gray-200">Manual Stewardship Overrides (FORCE_LINK / DO_NOT_LINK / ISOLATE)</h4>
                    </div>
                    <p className="text-xs text-gray-400">
                        Force-merge specific entity keys (<code className="bg-gray-900 px-1 py-0.5 rounded">FORCE_LINK</code>), block false merges (<code className="bg-gray-900 px-1 py-0.5 rounded">DO_NOT_LINK</code>), or quarantine a single contaminated record (<code className="bg-gray-900 px-1 py-0.5 rounded">ISOLATE</code>).
                    </p>
                    <div className="flex flex-wrap gap-2">
                        <select
                            value={newOvType}
                            onChange={(e) => setNewOvType(e.target.value as 'FORCE_LINK' | 'DO_NOT_LINK' | 'ISOLATE')}
                            className="bg-gray-900 border border-gray-700 text-xs text-gray-200 rounded px-2 py-1.5"
                        >
                            <option value="DO_NOT_LINK">DO_NOT_LINK (Block Pair)</option>
                            <option value="FORCE_LINK">FORCE_LINK (Merge Pair)</option>
                            <option value="ISOLATE">ISOLATE (Quarantine Entity)</option>
                        </select>
                        <input
                            type="text"
                            value={newOvKeyA}
                            onChange={(e) => setNewOvKeyA(e.target.value)}
                            placeholder="Entity Key A (e.g. crm:101)"
                            className="min-w-[150px] bg-gray-900 border border-gray-700 text-xs text-gray-200 rounded px-3 py-1.5 font-mono"
                        />
                        {newOvType !== 'ISOLATE' && (
                            <input
                                type="text"
                                value={newOvKeyB}
                                onChange={(e) => setNewOvKeyB(e.target.value)}
                                placeholder="Entity Key B (e.g. orders:502)"
                                className="min-w-[150px] bg-gray-900 border border-gray-700 text-xs text-gray-200 rounded px-3 py-1.5 font-mono"
                            />
                        )}
                        <input
                            type="text"
                            value={newOvReason}
                            onChange={(e) => setNewOvReason(e.target.value)}
                            placeholder="Reason (optional)"
                            className="flex-1 min-w-[140px] bg-gray-900 border border-gray-700 text-xs text-gray-200 rounded px-3 py-1.5"
                        />
                        <button
                            type="button"
                            onClick={addOverride}
                            className="px-3 py-1.5 bg-emerald-600/30 border border-emerald-600/60 hover:bg-emerald-600/50 text-emerald-200 rounded text-xs font-medium"
                        >
                            + Add Override
                        </button>
                    </div>
                    {overrides.length > 0 && (
                        <div className="flex flex-wrap gap-2 pt-2">
                            {overrides.map((ov, i) => (
                                <span
                                    key={`${ov.override_type}-${ov.entity_key_a}-${ov.entity_key_b}-${i}`}
                                    className="inline-flex items-center gap-2 px-2.5 py-1 bg-gray-900 border border-gray-700 rounded-full text-xs text-gray-300"
                                >
                                    <span className="text-emerald-400 font-semibold">{ov.override_type}</span>
                                    <span className="font-mono">{ov.entity_key_a}</span>
                                    {ov.override_type !== 'ISOLATE' && ov.entity_key_b && (
                                        <>
                                            <span className="text-gray-500">↔</span>
                                            <span className="font-mono">{ov.entity_key_b}</span>
                                        </>
                                    )}
                                    <button
                                        type="button"
                                        onClick={() => removeOverride(i)}
                                        className="text-gray-500 hover:text-red-400"
                                    >
                                        ×
                                    </button>
                                </span>
                            ))}
                        </div>
                    )}
                </div>
            </div>

            <div className="flex justify-between pt-4">
                <button className={btnOutlineClass} onClick={onBack}>Back</button>
                <button className={btnClass} onClick={handleNext}>Next: Survivorship</button>
            </div>
        </div>
    );
}
