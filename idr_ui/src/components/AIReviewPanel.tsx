import { useState } from 'react'
import { api } from '../api/client'
import type { AIReview, IDRConfig, ReviewPatch } from '../types'

export default function AIReviewPanel({ report, config, stale, onUpdate, onPreview }: {
    report: AIReview; config: IDRConfig; stale: boolean;
    onUpdate?: (config: IDRConfig) => void; onPreview: () => void;
}) {
    const [busy, setBusy] = useState(false)
    const [error, setError] = useState('')
    const [progress, setProgress] = useState('')
    const [comparison, setComparison] = useState<AIReview | null>(null)
    const [selectedRule, setSelectedRule] = useState('')
    const [action, setAction] = useState<ReviewPatch['action']>('remove_rule')
    const [value, setValue] = useState('25')

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
    const manualPatch = (): ReviewPatch => ({
        action,
        target: selectedRule,
        value: action === 'remove_rule'
            ? null
            : action === 'set_lens_types'
                ? value.split(',').map(s => s.trim()).filter(Boolean)
                : Number(value),
        label: `${action.replace(/_/g, ' ')}: ${selectedRule}`, base_draft_hash: report.draft_hash,
        measured: false, tradeoff: 'May remove legitimate links or introduce new links. Compare against the same extracted records.',
    })
    const targets = action === 'raise_fuzzy_threshold' ? config.fuzzy_rules || [] : action === 'set_lens_types' ? config.lenses || [] : config.rules || []
    const isHealthyDeterministic = report.provider === 'deterministic' && !report.findings.length
    return <section className="rounded-xl border border-purple-500/40 bg-gray-900 p-4 space-y-3 text-xs text-gray-200" aria-label="Draft graph review">
        <div className="flex flex-wrap items-center justify-between gap-2">
            <p className="font-semibold">{report.status.replace(/_/g, ' ')} · {report.lens} · {report.run_mode ?? 'preview'}</p>
            {isHealthyDeterministic ? (
                <span className="inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-medium bg-emerald-500/15 text-emerald-300 border border-emerald-500/30">
                    Healthy graph · deterministic review (no LLM refinement needed)
                </span>
            ) : report.provider ? (
                <span className="inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-medium bg-purple-500/15 text-purple-200 border border-purple-500/30">
                    Provider: {report.provider}
                </span>
            ) : null}
        </div>
        <p>{report.headline}</p>
        {report.execution_options && <p>Evaluated settings: {report.execution_options.strict ? 'exact only' : 'fuzzy enabled when configured'} · group cap {report.execution_options.max_group_size || 'per rule'} · {report.execution_options.max_iters || 30} maximum iterations</p>}
        <p className="font-mono text-[10px] break-all">Preview: {report.run_id}</p>
        <p>{report.telemetry.total_entities.toLocaleString()} entities · {report.telemetry.total_clusters.toLocaleString()} clusters · max {report.telemetry.max_cluster_size} · {report.telemetry.singleton_pct}% singleton clusters</p>
        <p>Graph diagnostics do not establish identity accuracy. High singleton rates may be expected.</p>
        {report.telemetry.rule_contributions && <table className="w-full text-left"><caption className="text-left">Rule evidence (causal effect requires a comparison)</caption><thead><tr><th>Rule</th><th>Edges</th></tr></thead><tbody>
            {report.telemetry.rule_contributions.map(rule => <tr key={rule.rule_id}><td>{rule.rule_id}</td><td>{rule.edge_count}</td></tr>)}
        </tbody></table>}
        {report.evaluation && <p>Labeled evaluation: precision {report.evaluation.precision ?? 'N/A'}, recall {report.evaluation.recall ?? 'N/A'}, evaluated records {report.evaluation.evaluated_entities}</p>}
        {report.evaluation_acceptance && <p>Labeled acceptance: {report.evaluation_acceptance.status.replace(/_/g,' ')}. {report.evaluation_acceptance.reasons.join('; ')} Scope: labeled population only.</p>}
        {report.limitations.map(text => <p key={text}>{text}</p>)}
        {stale && <p role="status" className="text-amber-300">The draft or selected view changed. Run a new preview before comparing or applying recommendations.</p>}
        <h3 className="font-semibold text-sm">Recommended next steps</h3>
        {!report.findings.length && <p>Keep this draft for review. No rule change is supported by these diagnostics; use labeled evaluation to assess accuracy.</p>}
        {report.findings.map((finding, i) => <div key={i} className="rounded border border-gray-700 p-3 space-y-2">
            <p className="font-semibold">{i + 1}. {finding.title}</p>
            <p><strong>Recommendation: </strong>{finding.recommendation || finding.patch?.label || 'Keep the current settings and inspect this evidence locally.'}</p>
            <p><strong>Evidence: </strong>{finding.detail} {finding.metric}</p>
            {finding.patch && <p className="font-mono">{finding.patch.target}: {finding.patch.action.replace(/_/g, ' ')}{finding.current_value !== undefined ? ` ${finding.current_value} →` : ''} {JSON.stringify(finding.patch.value)}</p>}
            {finding.expected_effect && <p><strong>Expected effect: </strong>{finding.expected_effect}</p>}
            {finding.tradeoff && <p><strong>Tradeoff: </strong>{finding.tradeoff}</p>}
            {finding.patch && <div className="flex gap-2">
                <button type="button" disabled={busy || stale} onClick={() => compare(finding.patch!)} className="rounded bg-purple-700 p-2 disabled:opacity-40">Compare this change</button>
                {onUpdate && <button type="button" disabled={busy || stale} onClick={() => apply(finding.patch!)} className="rounded border p-2 disabled:opacity-40">Apply unmeasured change to draft</button>}
            </div>}
        </div>)}
        <details className="space-y-2">
            <summary className="cursor-pointer">Compare a selected rule or lens change</summary>
            <div className="flex flex-wrap gap-2 mt-2">
                <select aria-label="Comparison action" value={action} onChange={e => {
                    const nextAction = e.target.value as ReviewPatch['action']
                    setAction(nextAction)
                    setSelectedRule('')
                    setComparison(null)
                    setValue(nextAction === 'set_group_limit' ? '25' : nextAction === 'raise_fuzzy_threshold' ? '0.9' : '')
                }} className="bg-gray-800 p-2 rounded">
                    <option value="remove_rule">Remove exact rule</option><option value="set_group_limit">Change group limit</option>
                    <option value="raise_fuzzy_threshold">Tighten fuzzy threshold</option><option value="set_lens_types">Change lens included types</option>
                </select>
                <select aria-label="Comparison target" value={selectedRule} onChange={e => {setSelectedRule(e.target.value); setComparison(null)}} className="bg-gray-800 p-2 rounded">
                    <option value="">Choose a target</option>{targets.map(t => <option key={t.id} value={String(t.id)}>{t.id}</option>)}
                </select>
                {action !== 'remove_rule' && <input aria-label="Proposed value" value={value} onChange={e => {setValue(e.target.value); setComparison(null)}} className="bg-gray-800 p-2 rounded" />}
                <button type="button" disabled={busy || stale || !selectedRule} onClick={() => compare(manualPatch())} className="rounded bg-purple-700 p-2 disabled:opacity-40">Run comparison</button>
            </div>
        </details>
        {busy && <p role="status">Evaluating the selected change… {progress}</p>}
        {error && <p role="alert" className="text-red-300">{error}</p>}
        {comparison?.comparison && <div className="rounded border border-emerald-600 p-3 space-y-2">
            <p>Measured comparison on the same extracted records</p>
            <p>{comparison.comparison.patch.label}{comparison.comparison.patch.action !== 'remove_rule' ? ` · proposed value ${JSON.stringify(comparison.comparison.patch.value)}` : ''}</p>
            <p>{comparison.telemetry.total_clusters} clusters after change · max {comparison.telemetry.max_cluster_size}</p>
            <p>{comparison.comparison.changes.split_affected_entities} records in split components · {comparison.comparison.changes.merge_affected_entities} records in merged components</p>
            <p>{comparison.comparison.patch.tradeoff}</p>
            {onUpdate && <button type="button" disabled={busy || stale} onClick={() => apply(comparison.comparison!.patch)} className="rounded bg-emerald-700 p-2 disabled:opacity-40">Apply compared change to draft</button>}
        </div>}
        <button type="button" onClick={onPreview} disabled={busy} className="rounded border border-purple-500 p-2">Run a fresh preview</button>
    </section>
}
