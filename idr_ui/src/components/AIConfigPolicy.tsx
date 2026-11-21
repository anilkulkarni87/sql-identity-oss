import { useEffect, useState } from 'react'
import type { AIConfigContext } from '../types'

const parseCsv = (raw: string) => raw.split(',').map(s => s.trim()).filter(Boolean)

export default function AIConfigPolicy({ context, tables, onChange, showBasics = false }: {
    context: AIConfigContext; tables: string[]; onChange: (next: AIConfigContext) => void; showBasics?: boolean
}) {
    const allowedSig = (context.allowed_matching_identifiers || []).join(', ')
    const lockedSig = context.locked_fields.join(', ')
    const [allowedText, setAllowedText] = useState(allowedSig)
    const [lockedText, setLockedText] = useState(lockedSig)

    useEffect(() => {
        if (parseCsv(allowedText).join(', ') !== allowedSig) setAllowedText(allowedSig)
    }, [allowedSig])
    useEffect(() => {
        if (parseCsv(lockedText).join(', ') !== lockedSig) setLockedText(lockedSig)
    }, [lockedSig])

    return <details className="rounded border border-gray-600 p-3 text-xs space-y-2">
        <summary className="cursor-pointer font-semibold">Matching permissions and locked decisions</summary>
        {showBasics && <div className="flex flex-wrap gap-2 mt-2">
            <label>Grain <select className="bg-gray-800 text-white p-2 rounded" value={context.grain} onChange={e => onChange({...context, grain:e.target.value as AIConfigContext['grain']})}>
                <option value="individual">Individual</option><option value="household">Household</option><option value="account">Account</option>
            </select></label>
            <label>Event handling <select className="bg-gray-800 text-white p-2 rounded" value={context.event_policy} onChange={e => onChange({...context, event_policy:e.target.value as AIConfigContext['event_policy']})}>
                <option value="bridge">Bridge; approve aggregates separately</option><option value="include">Include fully</option><option value="exclude">Exclude</option>
            </select></label>
            <label className="inline-flex items-center gap-2"><input type="checkbox" checked={context.fuzzy} onChange={e => onChange({...context, fuzzy:e.target.checked})} />Allow fuzzy proposals</label>
        </div>}
        <p className="mt-2">These constraints apply to generation, fallback, and review patches. Blank matching identifiers uses conservative defaults for the selected grain.</p>
        <p>Bridge sources contribute links. Approve individual Sum, Min or Max profile contributions in Survivorship.</p>
        <label className="block mt-2">Approved matching identifier types (comma separated)
            <input className="block w-full rounded bg-gray-800 text-white p-2 mt-1"
                value={allowedText}
                onChange={e => {
                    const raw = e.target.value
                    setAllowedText(raw)
                    const parsed = parseCsv(raw)
                    onChange({...context, allowed_matching_identifiers: parsed.length ? parsed : null})
                }} />
        </label>
        <label className="block mt-2">Locked draft fields (JSON pointers, comma separated)
            <input className="block w-full rounded bg-gray-800 text-white p-2 mt-1" placeholder="/sources/0/entity_key"
                value={lockedText}
                onChange={e => {
                    const raw = e.target.value
                    setLockedText(raw)
                    onChange({...context, locked_fields: parseCsv(raw)})
                }} />
        </label>
        <fieldset className="mt-2"><legend>Explicitly excluded sources</legend>
            {Array.from(new Set([...tables, ...context.excluded_tables])).map(table => <label key={table} className="flex items-center gap-2 mt-1">
                <input type="checkbox" checked={context.excluded_tables.includes(table)} onChange={e => onChange({...context,
                    excluded_tables: e.target.checked ? [...context.excluded_tables, table] : context.excluded_tables.filter(t => t !== table)})} />{table}
            </label>)}
        </fieldset>
    </details>
}
