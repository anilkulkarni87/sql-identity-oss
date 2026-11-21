import { useId } from 'react'
import type { IDRConfig } from '../types'

interface Proposal {
    status: 'ready_for_preview' | 'needs_decisions' | 'invalid'
    suggested_config: Record<string, unknown>
    diff: Array<{ path: string; before: unknown; after: unknown }>
    decisions: Array<{ target: string; reason: string; needs_review: boolean }>
    validation_report?: { valid: boolean; errors: string[] }
}

export default function AIProposalReview({ proposal, stale, onApply }: {
    proposal: Proposal; stale: boolean; onApply: (config: IDRConfig) => void
}) {
    const invalid = proposal.status === 'invalid' || !proposal.validation_report?.valid
    const blockedReasonId = useId()
    const blockedReason = invalid
        ? 'Cannot apply this proposal because validation failed. Resolve the errors above or generate a new proposal.'
        : stale ? 'The draft changed. Generate a fresh proposal.' : undefined
    return <section className="rounded-lg border border-purple-500/40 p-3 space-y-3 text-xs" aria-label="Review AI proposal">
        <p className="font-semibold">Proposal: {proposal.status.replace(/_/g, ' ')}</p>
        <p>Review these changes before applying them to your draft. Preview execution is a separate action.</p>
        {proposal.validation_report?.errors.map((error, i) => <p role="alert" key={i} className="text-red-400">{error}</p>)}
        {proposal.decisions.map((d, i) => <p key={`${d.target}:${i}`}>{d.target}: {d.reason}{d.needs_review ? ' — review required' : ''}</p>)}
        {proposal.diff.map(change => <details key={change.path} className="rounded border border-gray-700 p-2">
            <summary className="cursor-pointer font-medium">{change.path}</summary>
            <div className="grid gap-2 md:grid-cols-2 mt-2">
                <div><p>Current</p><pre className="max-h-64 overflow-auto whitespace-pre-wrap">{JSON.stringify(change.before ?? null, null, 2)}</pre></div>
                <div><p>Proposed</p><pre className="max-h-64 overflow-auto whitespace-pre-wrap">{JSON.stringify(change.after ?? null, null, 2)}</pre></div>
            </div>
        </details>)}
        {blockedReason && <p id={blockedReasonId} role="status" className="text-amber-400">{blockedReason}</p>}
        <button type="button" disabled={invalid || stale} onClick={() => onApply(proposal.suggested_config as unknown as IDRConfig)}
            aria-describedby={blockedReason ? blockedReasonId : undefined} title={blockedReason}
            className="rounded bg-purple-600 px-3 py-2 text-white disabled:opacity-40">Apply proposal to draft</button>
    </section>
}
