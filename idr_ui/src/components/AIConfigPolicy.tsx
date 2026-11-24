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

    const activeCount =
        (context.allowed_matching_identifiers?.length ? 1 : 0) +
        (context.locked_fields.length ? 1 : 0) +
        (context.excluded_tables.length ? 1 : 0)
    const allTables = Array.from(new Set([...tables, ...context.excluded_tables]))

    return (
        <details className="surface-subtle px-3 py-2 text-xs">
            <summary className="cursor-pointer select-none flex items-center justify-between gap-2 text-ink-secondary hover:text-ink font-medium">
                <span>Advanced AI Constraints (Optional)</span>
                {activeCount > 0 ? (
                    <span className="badge-accent tabular-nums">
                        {activeCount} active
                    </span>
                ) : (
                    <span className="text-[11px] font-normal text-ink-muted">Allow-list keys or exclude tables</span>
                )}
            </summary>

            <div className="mt-3 pt-2.5 border-t border-line space-y-3">
                {showBasics && (
                    <div className="flex flex-wrap items-center gap-3">
                        <label className="inline-flex items-center gap-1.5 text-ink-muted">
                            <span className="font-medium">Grain:</span>
                            <select
                                className="bg-surface border border-line-strong text-ink px-2 py-1 rounded text-xs"
                                value={context.grain}
                                onChange={e => onChange({ ...context, grain: e.target.value as AIConfigContext['grain'] })}
                            >
                                <option value="individual">Individual</option>
                                <option value="household">Household</option>
                                <option value="account">Account</option>
                            </select>
                        </label>
                        <label className="inline-flex items-center gap-1.5 text-ink-muted">
                            <span className="font-medium">Event Tables:</span>
                            <select
                                className="bg-surface border border-line-strong text-ink px-2 py-1 rounded text-xs"
                                value={context.event_policy}
                                onChange={e => onChange({ ...context, event_policy: e.target.value as AIConfigContext['event_policy'] })}
                            >
                                <option value="bridge">Bridge Only</option>
                                <option value="include">Include Fully</option>
                                <option value="exclude">Exclude</option>
                            </select>
                        </label>
                        <label className="inline-flex items-center gap-1.5 text-ink-secondary cursor-pointer">
                            <input
                                type="checkbox"
                                checked={context.fuzzy}
                                onChange={e => onChange({ ...context, fuzzy: e.target.checked })}
                                className="rounded border-line-strong text-accent"
                            />
                            Allow fuzzy rules
                        </label>
                    </div>
                )}

                <label className="block space-y-1">
                    <span className="font-medium text-ink-secondary">Allowed Match Keys (optional)</span>
                    <input
                        className="input-field text-xs py-1"
                        placeholder="Leave blank for safe defaults (e.g. EMAIL, PHONE)"
                        value={allowedText}
                        onChange={e => {
                            const raw = e.target.value
                            setAllowedText(raw)
                            const parsed = parseCsv(raw)
                            onChange({ ...context, allowed_matching_identifiers: parsed.length ? parsed : null })
                        }}
                    />
                </label>

                {allTables.length > 0 && (
                    <div className="space-y-1.5">
                        <div className="font-medium text-ink-secondary">
                            Exclude Tables from AI
                            {context.excluded_tables.length > 0 && (
                                <span className="ml-1.5 text-[11px] font-normal text-ink-muted tabular-nums">
                                    ({context.excluded_tables.length} excluded)
                                </span>
                            )}
                        </div>
                        <div className="flex flex-wrap gap-1.5">
                            {allTables.map(table => {
                                const excluded = context.excluded_tables.includes(table)
                                const shortName = table.split('.').pop() || table
                                return (
                                    <button
                                        key={table}
                                        type="button"
                                        title={table}
                                        onClick={() =>
                                            onChange({
                                                ...context,
                                                excluded_tables: excluded
                                                    ? context.excluded_tables.filter(t => t !== table)
                                                    : [...context.excluded_tables, table],
                                            })
                                        }
                                        className={`px-2 py-0.5 rounded-full text-[11px] border transition-colors ${
                                            excluded
                                                ? 'bg-status-danger-soft border-status-danger-border text-status-danger line-through'
                                                : 'bg-surface border-line text-ink-secondary hover:border-line-strong'
                                        }`}
                                    >
                                        {shortName}
                                    </button>
                                )
                            })}
                        </div>
                    </div>
                )}

                <details className="text-[11px] text-ink-muted">
                    <summary className="cursor-pointer select-none hover:text-ink-secondary">
                        Expert: Lock specific config fields (JSON pointers)
                    </summary>
                    <input
                        className="input-field mt-1.5 text-xs font-mono py-1"
                        placeholder="/sources/0/entity_key"
                        value={lockedText}
                        onChange={e => {
                            const raw = e.target.value
                            setLockedText(raw)
                            onChange({ ...context, locked_fields: parseCsv(raw) })
                        }}
                    />
                </details>
            </div>
        </details>
    )
}
