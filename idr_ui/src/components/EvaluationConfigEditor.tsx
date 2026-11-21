import { useEffect, useState } from 'react'
import type { EvaluationOptions } from '../types'

export default function EvaluationConfigEditor({value, onChange}: {value?: EvaluationOptions; onChange?: (next?: EvaluationOptions) => void}) {
    const [mappingText, setMappingText] = useState(JSON.stringify(value?.source_mappings || [], null, 2))
    const [error, setError] = useState('')
    const mappingSignature = JSON.stringify(value?.source_mappings || [])
    useEffect(() => setMappingText(JSON.stringify(JSON.parse(mappingSignature), null, 2)), [mappingSignature])
    const edit = (patch: Partial<EvaluationOptions>) => onChange?.({...value, ground_truth_table: value?.ground_truth_table || '', ...patch})
    return <details className="rounded border border-slate-600 p-3 text-xs space-y-3"><summary className="cursor-pointer font-semibold">Optional labeled evaluation and acceptance criteria</summary>
        <label className="flex items-center gap-2"><input type="checkbox" checked={Boolean(value)} disabled={!onChange} onChange={e => onChange?.(e.target.checked ? {ground_truth_table:''} : undefined)} />Evaluate against reviewed identity labels</label>
        {value && <fieldset className="space-y-3" disabled={!onChange}>
            <div className="flex flex-wrap gap-3"><label>Label table <input value={value.ground_truth_table} onChange={e => edit({ground_truth_table:e.target.value})} placeholder="schema.reviewed_labels" className="block bg-slate-800 p-2" /></label>
                <label>Record key column <input value={value.entity_key_col || 'entity_key'} onChange={e => edit({entity_key_col:e.target.value})} className="block bg-slate-800 p-2" /></label>
                <label>Reviewed identity column <input value={value.truth_id_col || 'truth_id'} onChange={e => edit({truth_id_col:e.target.value})} className="block bg-slate-800 p-2" /></label>
            </div>
            <p>For normalized labels, the record key is already an engine entity key. Otherwise map source key columns to label columns below. Composite and generated keys reuse the configured source key construction; label values stay in the warehouse.</p>
            <label className="block">Source mappings (JSON array)
                <textarea aria-label="Evaluation source mappings" rows={5} value={mappingText} onChange={e => setMappingText(e.target.value)} placeholder={'[{"source_id":"source_a","key_columns":{"source_id_column":"label_record_column"}}]'} className="block w-full bg-slate-800 p-2 font-mono" />
            </label>
            <button type="button" className="rounded border p-2" onClick={() => {try {const parsed = JSON.parse(mappingText); if (!Array.isArray(parsed)) throw new Error('Use a JSON array'); edit({source_mappings:parsed}); setError('')} catch(e) {setError(e instanceof Error ? e.message : String(e))}}}>Apply source mappings</button>
            <div className="flex flex-wrap gap-3">{([
                ['min_evaluated_entities','Minimum labeled records',2,1], ['min_true_pairs','Minimum same-identity pairs',1,1],
                ['min_label_overlap','Minimum label overlap (0–1)',0,0.01], ['min_precision','Minimum precision (0–1)',undefined,0.01], ['min_recall','Minimum recall (0–1)',undefined,0.01],
            ] as const).map(([key,label,fallback,step]) => <label key={key}>{label}<input type="number" min="0" step={step} value={value[key] ?? fallback ?? ''} onChange={e => edit({[key]:e.target.value === '' ? undefined : Number(e.target.value)})} className="block bg-slate-800 p-2 w-32" /></label>)}</div>
            <p>Results cover the labeled population only. Duplicate/conflicting labels are rejected; missing evidence is reported separately from an acceptance failure. Evaluation remains optional.</p>
            {error && <p role="alert" className="text-red-300">{error}</p>}
        </fieldset>}
    </details>
}
