import { useState } from 'react'
import { api } from '../api/client'
import type { AIReview, IDRConfig, ReviewPatch } from '../types'

export default function AIReviewPanel({ report, config, stale, onUpdate }: {
    report: AIReview; config: IDRConfig; stale: boolean;
    onUpdate?: (config: IDRConfig) => void; onPreview?: () => void;
}) {
    const [busy, setBusy] = useState(false)
    const [error, setError] = useState('')
    const [progress, setProgress] = useState('')
    const [comparison, setComparison] = useState<AIReview | null>(null)

    const compare = async (patch: ReviewPatch) => {
        setBusy(true); setError('')
        try { setComparison(await api.compareAiPatch(report.run_id, config, patch, report.lens, event => setProgress(String(event.stage || event.event || "comparison")))) }
        catch (err) { setError(err instanceof Error ? err.message : String(err)) }
        finally { setBusy(false) }
    }
    const apply = async (patch: ReviewPatch) => {
        setBusy(true); setError('')
        try { const result = await api.applyAiPatch(config, patch); onUpdate?.(result.config) }
        catch (err) { setError(err instanceof Error ? err.message : String(err)) }
        finally { setBusy(false) }
    }
    const isHealthyDeterministic = report.provider === 'deterministic' && !report.findings.length
    return <section className="surface-panel border-accent-border p-4 space-y-3 text-xs text-ink-secondary" aria-label="Draft graph review">
        <div className="flex flex-wrap items-center justify-between gap-2">
            <p className="font-semibold text-ink">{report.status.replace(/_/g, ' ')} · {report.lens} · {report.run_mode ?? 'preview'}</p>
            {isHealthyDeterministic ? (
                <span className="badge-success">
                    Healthy graph · deterministic review (no LLM refinement needed)
                </span>
            ) : report.provider ? (
                <span className="badge-accent font-mono">
                    Provider: {report.provider}
                </span>
            ) : null}
        </div>
        <p className="text-ink">{report.headline}</p>
        {report.execution_options && <p className="text-ink-muted">Evaluated settings: {report.execution_options.strict ? 'exact only' : 'fuzzy enabled when configured'} · group cap {report.execution_options.max_group_size || 'per rule'} · {report.execution_options.max_iters || 30} maximum iterations</p>}
        <p className="font-mono text-[10px] text-ink-muted break-all">Preview: {report.run_id}</p>
        <p className="tabular-nums">{report.telemetry.total_entities.toLocaleString()} entities · {report.telemetry.total_clusters.toLocaleString()} clusters · max {report.telemetry.max_cluster_size} · {report.telemetry.singleton_pct}% singleton clusters</p>
        <p className="text-ink-muted">Graph diagnostics do not establish identity accuracy. High singleton rates may be expected.</p>
        {report.telemetry.rule_contributions && <table className="w-full text-left border border-line rounded-md overflow-hidden"><caption className="text-left text-[11px] text-ink-muted mb-1">Rule evidence (causal effect requires a comparison)</caption><thead><tr><th className="table-th">Rule</th><th className="table-th">Edges</th></tr></thead><tbody>
            {report.telemetry.rule_contributions.map(rule => <tr key={rule.rule_id}><td className="table-td font-mono">{rule.rule_id}</td><td className="table-td font-mono tabular-nums">{rule.edge_count}</td></tr>)}
        </tbody></table>}
        {report.evaluation && <p>Labeled evaluation: precision {report.evaluation.precision ?? 'N/A'}, recall {report.evaluation.recall ?? 'N/A'}, evaluated records {report.evaluation.evaluated_entities}</p>}
        {report.evaluation_acceptance && <p>Labeled acceptance: {report.evaluation_acceptance.status.replace(/_/g,' ')}. {report.evaluation_acceptance.reasons.join('; ')} Scope: labeled population only.</p>}
        {report.limitations.map(text => <p key={text} className="text-ink-muted">{text}</p>)}
        {stale && <p role="status" className="text-status-warning font-medium">The draft or selected view changed. Run a new preview before comparing or applying recommendations.</p>}
        <h3 className="font-semibold text-[13px] text-ink">Recommended next steps</h3>
        {!report.findings.length && <p>Keep this draft for review. No rule change is supported by these diagnostics; use labeled evaluation to assess accuracy.</p>}
        {report.findings.map((finding, i) => <div key={i} className="surface-subtle p-3 space-y-2">
            <p className="font-semibold text-ink">{i + 1}. {finding.title}</p>
            <p><strong className="text-ink">Recommendation: </strong>{finding.recommendation || finding.patch?.label || 'Keep the current settings and inspect this evidence locally.'}</p>
            <p><strong className="text-ink">Evidence: </strong>{finding.detail} {finding.metric}</p>
            {finding.patch && <p className="font-mono text-ink">{finding.patch.target}: {finding.patch.action.replace(/_/g, ' ')}{finding.current_value !== undefined ? ` ${finding.current_value} →` : ''}{finding.patch.value !== null && finding.patch.value !== undefined ? ` ${JSON.stringify(finding.patch.value)}` : ''}</p>}
            {finding.expected_effect && <p><strong className="text-ink">Expected effect: </strong>{finding.expected_effect}</p>}
            {finding.tradeoff && <p><strong className="text-ink">Tradeoff: </strong>{finding.tradeoff}</p>}
            {finding.patch && <div className="flex gap-2 pt-1">
                <button type="button" disabled={busy || stale} onClick={() => compare(finding.patch!)} className="btn-primary text-xs">Preview Impact</button>
                {onUpdate && <button type="button" disabled={busy || stale} onClick={() => apply(finding.patch!)} className="btn-secondary text-xs">Apply to Draft</button>}
            </div>}
        </div>)}
        {busy && <p role="status" className="text-accent">Evaluating the selected change… {progress}</p>}
        {error && <p role="alert" className="text-status-danger">{error}</p>}
        {comparison?.comparison && <div className="rounded-md bg-status-success-soft border border-status-success-border p-3 space-y-2">
            <p className="font-semibold text-status-success">Measured impact on the same extracted records</p>
            <p className="text-ink">{comparison.comparison.patch.label}{comparison.comparison.patch.action !== 'remove_rule' ? ` · proposed value ${JSON.stringify(comparison.comparison.patch.value)}` : ''}</p>
            <p className="tabular-nums">{comparison.telemetry.total_clusters.toLocaleString()} clusters after change · max {comparison.telemetry.max_cluster_size}</p>
            <p className="tabular-nums">{comparison.comparison.changes.split_affected_entities.toLocaleString()} records in split components · {comparison.comparison.changes.merge_affected_entities.toLocaleString()} records in merged components</p>
            <p>{comparison.comparison.patch.tradeoff}</p>
            {onUpdate && <button type="button" disabled={busy || stale} onClick={() => apply(comparison.comparison!.patch)} className="btn-primary text-xs">Apply to Draft</button>}
        </div>}
    </section>
}
