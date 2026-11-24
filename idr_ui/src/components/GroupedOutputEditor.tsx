import { useEffect, useState } from 'react'
import type { IDRConfig } from '../types'

export default function GroupedOutputEditor({config,onChange}: {config:IDRConfig;onChange:(next:IDRConfig)=>void}) {
    const signature=JSON.stringify(config.grouped_outputs || [])
    const [text,setText]=useState(JSON.stringify(config.grouped_outputs || [],null,2))
    const [error,setError]=useState('')
    useEffect(()=>setText(JSON.stringify(JSON.parse(signature),null,2)),[signature])
    const defaultSourceId = config.sources?.[0]?.id || 'customer_consents'
    const exampleJson = JSON.stringify(
        [
            {
                id: 'current_preferences',
                dimensions: ['channel', 'purpose'],
                fields: ['opt_in_status'],
                strategy: 'RECENCY',
                conflict_policy: 'mark',
                null_dimensions: 'error',
                sources: [
                    {
                        source_id: defaultSourceId,
                        dimensions: { channel: 'channel', purpose: 'purpose' },
                        fields: { opt_in_status: 'consent_status' },
                        recency_field: 'updated_at',
                    },
                ],
            },
        ],
        null,
        2
    )
    return <details className="rounded border border-slate-600 p-3 text-xs space-y-3">
        <summary className="cursor-pointer font-semibold">Grouped state outputs (advanced)</summary>
        <p>Keep independent current states per resolved identity and configured dimensions. Map configured source IDs, dimension expressions, fields and effective timestamps. Conflicting equally authoritative states fail by default; explicit mark policy emits NULL state fields with a conflict flag.</p>
        <p>Outputs are written to idr_out.grouped_&lt;id&gt; during real pipeline execution. No domain-specific table or status names are assumed.</p>
        <textarea aria-label="Grouped output definitions" rows={10} value={text} onChange={e=>setText(e.target.value)} className="w-full bg-slate-800 p-2 font-mono" placeholder={'[{"id":"current_state","dimensions":["category"],"fields":["state"],"sources":[{"source_id":"source_a","dimensions":{"category":"category_column"},"fields":{"state":"state_column"},"recency_field":"updated_at"}]}]'} />
        <div className="flex flex-wrap gap-2">
            <button type="button" className="rounded border border-slate-500 hover:border-slate-400 px-2.5 py-1.5" onClick={()=>{try{const defs=JSON.parse(text);if(!Array.isArray(defs))throw new Error('Use a JSON array');onChange({...config,grouped_outputs:defs});setError('')}catch(e){setError(e instanceof Error?e.message:String(e))}}}>Apply grouped output definitions to draft</button>
            <button type="button" className="rounded border border-slate-600 hover:border-slate-500 px-2.5 py-1.5 text-slate-300" onClick={()=>{setText(exampleJson);setError('')}}>Insert Example</button>
        </div>
        <p>Definitions are validated on save and preview. Changing output definitions requires a FULL rebuild before incremental execution.</p>
        {error&&<p role="alert" className="text-red-300">{error}</p>}
    </details>
}
