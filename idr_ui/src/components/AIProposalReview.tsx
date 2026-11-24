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
    return <section className="surface-panel border-accent-border p-3.5 space-y-2.5 text-xs" aria-label="Review AI proposal">
        <p className="font-semibold text-ink">Proposal: {proposal.status.replace(/_/g, ' ')}</p>
        <p className="text-ink-secondary">Review these changes before applying them to your draft. Preview execution is a separate action.</p>
        {proposal.validation_report?.errors.map((error, i) => <p role="alert" key={i} className="text-status-danger font-medium">{error}</p>)}
        {proposal.decisions.map((d, i) => <p key={`${d.target}:${i}`} className="text-ink-secondary"><span className="font-mono text-ink">{d.target}</span>: {d.reason}{d.needs_review ? ' — review required' : ''}</p>)}
        {proposal.diff.map(change => <details key={change.path} className="surface-subtle p-2.5">
            <summary className="cursor-pointer font-mono font-medium text-ink">{change.path}</summary>
            <div className="grid gap-2 md:grid-cols-2 mt-2">
                <div className="space-y-1"><p className="text-[11px] font-medium text-ink-muted">Current</p><pre className="max-h-64 overflow-auto whitespace-pre-wrap rounded bg-surface border border-line p-2 font-mono text-[11px] text-ink-secondary">{JSON.stringify(change.before ?? null, null, 2)}</pre></div>
                <div className="space-y-1"><p className="text-[11px] font-medium text-ink-muted">Proposed</p><pre className="max-h-64 overflow-auto whitespace-pre-wrap rounded bg-surface border border-line p-2 font-mono text-[11px] text-ink">{JSON.stringify(change.after ?? null, null, 2)}</pre></div>
            </div>
        </details>)}
        {blockedReason && <p id={blockedReasonId} role="status" className="text-status-warning">{blockedReason}</p>}
        <button type="button" disabled={invalid || stale} onClick={() => onApply(proposal.suggested_config as unknown as IDRConfig)}
            aria-describedby={blockedReason ? blockedReasonId : undefined} title={blockedReason}
            className="btn-primary text-xs">Apply proposal to draft</button>
    </section>
}
