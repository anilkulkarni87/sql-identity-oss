import { useState, useEffect } from 'react';
import { Layers, ArrowUp, Zap, Clock, Hash } from "lucide-react";
import { IDRConfig, SurvivorshipRule } from '../../types';

interface StepSurvivorshipProps {
    config: IDRConfig;
    onNext: (config: IDRConfig) => void;
    onBack: () => void;
}

export default function StepSurvivorship({ config, onNext, onBack }: StepSurvivorshipProps) {
    // Unique attributes across all sources
    const [attributes, setAttributes] = useState<string[]>([]);
    // { attrName: { strategy: 'RECENCY', source_priority: ['table1', 'table2'] } }
    const [rules, setRules] = useState<Record<string, SurvivorshipRule>>({});

    useEffect(() => {
        // 1. Aggregate attributes
        const allAttrs = new Set<string>();
        (config.sources || []).forEach(src => {
            (src.attributes || []).forEach(a => allAttrs.add(a.name));
        });
        const attrList = Array.from(allAttrs).sort();
        setAttributes(attrList);

        // 2. Initialize default rules
        const initialRules: Record<string, SurvivorshipRule> = {};
        attrList.forEach(attr => {
            initialRules[attr] = {
                attribute: attr,
                strategy: 'RECENCY', // Default
                source_priority: config.sources.map(s => s.table), // Default order
                // recency_field: 'updated_at' -- REMOVED: Uses source watermark by default
            };
        });
        setRules(initialRules);
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
        { id: 'FREQUENCY', label: 'Frequency (Most Common)', icon: Zap },
        { id: 'AGG_MAX', label: 'Max Value', icon: ArrowUp },
        // { id: 'AGG_SUM', label: 'Sum', icon: Hash },
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

                    return (
                        <div key={attr} className="border border-gray-700 rounded-lg bg-gray-800/50 p-4 space-y-4">
                            <div className="flex items-center justify-between">
                                <div className="flex items-center gap-3">
                                    <div className="p-2 bg-gray-700 rounded-md">
                                        <Hash className="w-4 h-4 text-blue-400" />
                                    </div>
                                    <span className="font-semibold text-gray-200">{attr}</span>
                                </div>
                                <select
                                    value={currentStrategy}
                                    onChange={(e) => updateRule(attr, 'strategy', e.target.value)}
                                    className="bg-gray-900 border border-gray-600 rounded px-3 py-1 text-sm text-white focus:outline-none focus:border-blue-500"
                                >
                                    {strategies.map(s => (
                                        <option key={s.id} value={s.id}>{s.label}</option>
                                    ))}
                                </select>
                            </div>

                            {/* Strategy Specific Config */}
                            {currentStrategy === 'PRIORITY' && (
                                <div className="space-y-2 bg-gray-900/50 p-3 rounded text-sm">
                                    <label className="text-gray-400 text-xs uppercase tracking-wider font-semibold">Source Priority (High to Low)</label>
                                    <div className="flex flex-col gap-1">
                                        {(rule.source_priority || []).map((src, idx) => (
                                            <div key={src} className="flex items-center justify-between bg-gray-800 px-3 py-2 rounded border border-gray-700">
                                                <span className="text-gray-300">{idx + 1}. {src}</span>
                                                <div className="flex gap-1">
                                                    <button
                                                        disabled={idx === 0}
                                                        onClick={() => {
                                                            const newPriority = [...(rule.source_priority || [])];
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
