import { useEffect, useState } from 'react'
import { Layers } from 'lucide-react'
import type { IDRConfig } from '../types'

export default function GroupedOutputEditor({config,onChange}: {config:IDRConfig;onChange:(next:IDRConfig)=>void}) {
    const signature=JSON.stringify(config.grouped_outputs || [])
    const [text,setText]=useState(JSON.stringify(config.grouped_outputs || [],null,2))
    const [error,setError]=useState('')
    useEffect(()=>setText(JSON.stringify(JSON.parse(signature),null,2)),[signature])
    const defaultSourceId = config.sources?.[0]?.id || 'customer_consents'
    const activeCount = (config.grouped_outputs || []).length
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
    return <details className="surface-subtle p-3.5 text-xs space-y-3">
        <summary className="cursor-pointer font-semibold text-ink flex flex-wrap items-center justify-between gap-2">
            <span className="inline-flex items-center gap-1.5">
                <Layers className="w-3.5 h-3.5 text-accent" />
                Grouped state outputs (advanced)
            </span>
            <span className={activeCount > 0 ? 'badge-accent font-mono tabular-nums' : 'badge-neutral font-mono'}>
                {activeCount > 0 ? `${activeCount} configured` : 'Optional'}
            </span>
        </summary>
        <p className="text-ink-secondary">Keep independent current states per resolved identity and configured dimensions. Map configured source IDs, dimension expressions, fields and effective timestamps. Conflicting equally authoritative states fail by default; explicit mark policy emits NULL state fields with a conflict flag.</p>
        <p className="text-ink-muted">Outputs are written to <code className="font-mono text-ink">idr_out.grouped_&lt;id&gt;</code> during real pipeline execution. No domain-specific table or status names are assumed.</p>
        <textarea aria-label="Grouped output definitions" rows={10} value={text} onChange={e=>setText(e.target.value)} className="input-field font-mono text-xs p-2.5" placeholder={'[{"id":"current_state","dimensions":["category"],"fields":["state"],"sources":[{"source_id":"source_a","dimensions":{"category":"category_column"},"fields":{"state":"state_column"},"recency_field":"updated_at"}]}]'} />
        <div className="flex flex-wrap gap-2">
            <button type="button" className="btn-secondary text-xs" onClick={()=>{try{const defs=JSON.parse(text);if(!Array.isArray(defs))throw new Error('Use a JSON array');onChange({...config,grouped_outputs:defs});setError('')}catch(e){setError(e instanceof Error?e.message:String(e))}}}>Apply grouped output definitions to draft</button>
            <button type="button" className="btn-ghost text-xs border border-line" onClick={()=>{setText(exampleJson);setError('')}}>Insert Example</button>
        </div>
        <p className="text-ink-muted">Definitions are validated on save and preview. Changing output definitions requires a FULL rebuild before incremental execution.</p>
        {error&&<p role="alert" className="text-status-danger font-medium">{error}</p>}
    </details>
}
