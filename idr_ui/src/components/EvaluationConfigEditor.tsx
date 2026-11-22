import { useEffect, useState } from 'react'
import type { EvaluationOptions } from '../types'

export default function EvaluationConfigEditor({value, onChange}: {value?: EvaluationOptions; onChange?: (next?: EvaluationOptions) => void}) {
    const [mappingText, setMappingText] = useState(JSON.stringify(value?.source_mappings || [], null, 2))
    const [error, setError] = useState('')
    const mappingSignature = JSON.stringify(value?.source_mappings || [])
    useEffect(() => setMappingText(JSON.stringify(JSON.parse(mappingSignature), null, 2)), [mappingSignature])
    const edit = (patch: Partial<EvaluationOptions>) => onChange?.({...value, ground_truth_table: value?.ground_truth_table || '', ...patch})
    return (
        <div className="rounded-lg border border-gray-700 bg-gray-800/60 p-3 text-xs space-y-3">
            <label className="flex items-start gap-2.5 cursor-pointer">
                <input
                    type="checkbox"
                    checked={Boolean(value)}
                    disabled={!onChange}
                    onChange={e => onChange?.(e.target.checked ? { ground_truth_table: '' } : undefined)}
                    className="mt-0.5 w-4 h-4 rounded border-gray-700 bg-gray-900 text-blue-600 focus:ring-0"
                />
                <div>
                    <span className="font-medium text-gray-200 block">Benchmark against Ground Truth Labels (Optional)</span>
                    <span className="text-gray-400">Measure precision &amp; recall against a warehouse table of known-correct entity IDs</span>
                </div>
            </label>

            {value && (
                <fieldset className="space-y-3 pt-2 border-t border-gray-700/70" disabled={!onChange}>
                    <div className="grid grid-cols-1 sm:grid-cols-3 gap-2.5">
                        <label className="space-y-1">
                            <span className="text-gray-300 font-medium block">Label Table</span>
                            <input
                                value={value.ground_truth_table}
                                onChange={e => edit({ ground_truth_table: e.target.value })}
                                placeholder="schema.reviewed_labels"
                                className="w-full rounded bg-gray-900 border border-gray-700 px-2.5 py-1.5 text-gray-100"
                            />
                        </label>
                        <label className="space-y-1">
                            <span className="text-gray-300 font-medium block">Record Key Column</span>
                            <input
                                value={value.entity_key_col || 'entity_key'}
                                onChange={e => edit({ entity_key_col: e.target.value })}
                                className="w-full rounded bg-gray-900 border border-gray-700 px-2.5 py-1.5 text-gray-100"
                            />
                        </label>
                        <label className="space-y-1">
                            <span className="text-gray-300 font-medium block">Truth ID Column</span>
                            <input
                                value={value.truth_id_col || 'truth_id'}
                                onChange={e => edit({ truth_id_col: e.target.value })}
                                className="w-full rounded bg-gray-900 border border-gray-700 px-2.5 py-1.5 text-gray-100"
                            />
                        </label>
                    </div>

                    <details className="rounded border border-gray-700/80 bg-gray-900/50 px-3 py-2 space-y-2.5">
                        <summary className="cursor-pointer select-none text-gray-300 font-medium">
                            Optional: Pass/fail thresholds &amp; custom key mappings
                        </summary>
                        <div className="grid grid-cols-2 sm:grid-cols-5 gap-2 pt-2">
                            {([
                                ['min_precision', 'Min precision (0–1)', undefined, 0.01],
                                ['min_recall', 'Min recall (0–1)', undefined, 0.01],
                                ['min_evaluated_entities', 'Min records', 2, 1],
                                ['min_true_pairs', 'Min pairs', 1, 1],
                                ['min_label_overlap', 'Min overlap', 0, 0.01],
                            ] as const).map(([key, label, fallback, step]) => (
                                <label key={key} className="space-y-1">
                                    <span className="text-[11px] text-gray-400 block">{label}</span>
                                    <input
                                        type="number"
                                        min="0"
                                        step={step}
                                        value={value[key] ?? fallback ?? ''}
                                        onChange={e => edit({ [key]: e.target.value === '' ? undefined : Number(e.target.value) })}
                                        className="w-full rounded bg-gray-900 border border-gray-700 px-2 py-1 text-gray-100"
                                    />
                                </label>
                            ))}
                        </div>
                        <label className="block space-y-1 pt-1">
                            <span className="text-[11px] text-gray-400 block">Custom source key mappings (JSON array, only if label keys differ from entity_key)</span>
                            <textarea
                                aria-label="Evaluation source mappings"
                                rows={3}
                                value={mappingText}
                                onChange={e => setMappingText(e.target.value)}
                                placeholder={'[{"source_id":"source_a","key_columns":{"source_id_column":"label_record_column"}}]'}
                                className="block w-full rounded bg-gray-900 border border-gray-700 p-2 font-mono text-[11px] text-gray-200"
                            />
                        </label>
                        <button
                            type="button"
                            className="rounded border border-gray-600 hover:border-gray-500 px-2.5 py-1 text-[11px] text-gray-200"
                            onClick={() => {
                                try {
                                    const parsed = JSON.parse(mappingText)
                                    if (!Array.isArray(parsed)) throw new Error('Use a JSON array')
                                    edit({ source_mappings: parsed })
                                    setError('')
                                } catch (e) {
                                    setError(e instanceof Error ? e.message : String(e))
                                }
                            }}
                        >
                            Apply key mappings
                        </button>
                    </details>
                    {error && <p role="alert" className="text-red-300">{error}</p>}
                </fieldset>
            )}
        </div>
    )
}
