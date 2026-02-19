import { useState, useEffect } from 'react';
import { GitGraph, ArrowUp, ArrowDown, Trash2, Plus, Wand2, Info } from "lucide-react";
import { IDRConfig, MatchingRule } from '../../types';
import { api } from '../../api/client';

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
    const [availableTypes, setAvailableTypes] = useState<string[]>([]);
    const [fuzzyTemplates, setFuzzyTemplates] = useState<FuzzyTemplate[]>([]);

    useEffect(() => {
        // Initialize rules (combine exact and fuzzy)
        const combined: MatchingRule[] = [
            ...(config.rules || []),
            ...(config.fuzzy_rules || [])
        ].sort((a, b) => a.priority - b.priority);

        if (combined.length > 0) {
            setRules(combined);
        }

        // Available match keys
        const typeSet = new Set<string>();
        (config.sources || []).forEach(src => {
            (src.identifiers || []).forEach(id => {
                if (id.is_match_key !== false) typeSet.add(id.type);
            });
        });
        setAvailableTypes(Array.from(typeSet).sort());

        // Fetch fuzzy templates
        api.getFuzzyTemplates()
            .then(data => setFuzzyTemplates(data.templates || []))
            .catch(err => console.error("Failed to fetch templates", err));

    }, [config]);

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
        if (confirm("Remove this rule?")) {
            updatePriorities(rules.filter((_, i) => i !== idx));
        }
    };

    const addExactRule = (type: string) => {
        const newRule: MatchingRule = {
            id: generateId(),
            type: 'EXACT',
            match_keys: [type],
            priority: rules.length + 1,
            canonicalize: 'LOWERCASE'
        };
        updatePriorities([...rules, newRule]);
    };

    const addFuzzyRule = () => {
        if (fuzzyTemplates.length === 0 && availableTypes.length > 0) {
            console.warn("No templates available but adding fallback fuzzy rule");
        }

        const tmpl = fuzzyTemplates[0] || { sql_template: '', default_threshold: 0.85 };
        const newRule: MatchingRule = {
            id: generateId(),
            type: 'FUZZY',
            match_keys: [], // Unused for fuzzy
            priority: rules.length + 1,
            blocking_key: availableTypes[0] || '', // Default to first available
            score_expr: tmpl.sql_template,
            threshold: tmpl.default_threshold
        };
        updatePriorities([...rules, newRule]);
    };

    const updateRuleField = (idx: number, field: keyof MatchingRule, value: any) => {
        const newRules = [...rules];
        newRules[idx] = { ...newRules[idx], [field]: value };
        setRules(newRules);
    };

    const applyTemplate = (idx: number, tmplId: string) => {
        const tmpl = fuzzyTemplates.find(t => t.id === tmplId);
        if (tmpl) {
            const newRules = [...rules];
            newRules[idx] = {
                ...newRules[idx],
                score_expr: tmpl.sql_template,
                threshold: tmpl.default_threshold
            };
            setRules(newRules);
        }
    };

    const generateId = () => Math.floor(Math.random() * 1000000);

    const handleNext = () => {
        if (rules.length === 0) {
            alert("Define at least one rule.");
            return;
        }
        const exact = rules.filter(r => r.type === 'EXACT');
        const fuzzy = rules.filter(r => r.type === 'FUZZY');

        onNext({ ...config, rules: exact, fuzzy_rules: fuzzy });
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
                                    <div className="flex items-center gap-2">
                                        <label className="text-xs text-gray-500">Normalization:</label>
                                        <select
                                            value={rule.canonicalize || 'LOWERCASE'}
                                            onChange={(e) => updateRuleField(idx, 'canonicalize', e.target.value)}
                                            className="bg-gray-900 border border-gray-700 text-xs text-gray-300 rounded px-2 py-1 focus:outline-none focus:border-blue-500"
                                            onClick={(e) => e.stopPropagation()}
                                        >
                                            <option value="LOWERCASE">Lowercase</option>
                                            <option value="UPPERCASE">Uppercase</option>
                                            <option value="EXACT">None</option>
                                        </select>
                                    </div>
                                ) : (
                                    <div className="grid grid-cols-2 gap-4 bg-gray-900/50 p-3 rounded border border-gray-700">
                                        <div>
                                            <label className="text-xs text-gray-500 block mb-1">Blocking Key</label>
                                            <select
                                                value={rule.blocking_key || ''}
                                                onChange={(e) => updateRuleField(idx, 'blocking_key', e.target.value)}
                                                className="w-full bg-gray-900 border border-gray-700 text-xs text-gray-300 rounded px-2 py-1 focus:outline-none focus:border-purple-500"
                                            >
                                                {availableTypes.map(t => <option key={t} value={t}>{t}</option>)}
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
                                        <div className="col-span-2">
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


                <div className="bg-blue-900/10 border border-blue-900/50 rounded-lg p-3 mt-2">
                    <div className="flex gap-3">
                        <Info className="w-5 h-5 text-blue-400 mt-0.5" />
                        <div className="text-sm">
                            <h4 className="font-medium text-blue-300">Need to block specific values?</h4>
                            <p className="text-gray-400 mt-1">
                                To prevent common values (like <code className="bg-gray-800 px-1 py-0.5 rounded text-gray-300">test@test.com</code>) from linking users,
                                insert them into the <code className="bg-gray-800 px-1 py-0.5 rounded text-gray-300">idr_meta.identifier_exclusion</code> table.
                            </p>
                        </div>
                    </div>
                </div>
            </div>

            <div className="flex justify-between pt-4">
                <button className={btnOutlineClass} onClick={onBack}>Back</button>
                <button className={btnClass} onClick={handleNext}>Next: Survivorship</button>
            </div>
        </div>
    );
}
