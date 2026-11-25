import { useState } from 'react'
import { Bot, Loader2, X } from 'lucide-react'
import { api } from '../../api/client'
import { useToast } from '../../components/Toast'

interface TriageQueuePanelProps {
    open: boolean
    onClose: () => void
    onInspectCluster: (resolvedId: string) => void
}

export default function TriageQueuePanel({
    open,
    onClose,
    onInspectCluster,
}: TriageQueuePanelProps) {
    const { toast } = useToast()
    const [triageLoading, setTriageLoading] = useState(false)
    const [triageMaxConf, setTriageMaxConf] = useState('0.75')
    const [triageEscalateProvider, setTriageEscalateProvider] = useState('')
    const [triageEscalateModel, setTriageEscalateModel] = useState('')
    const [triageResult, setTriageResult] = useState<Awaited<ReturnType<typeof api.runAiTriage>> | null>(null)
    const [triageError, setTriageError] = useState<string | null>(null)
    const [applyingOverrideKey, setApplyingOverrideKey] = useState<string | null>(null)
    const [appliedOverrides, setAppliedOverrides] = useState<Record<string, boolean>>({})

    const handleRunTriage = async () => {
        setTriageLoading(true)
        setTriageError(null)
        try {
            const parsedMax = Number(triageMaxConf)
            const hasEscalation = Boolean(triageEscalateProvider || triageEscalateModel.trim())
            const res = await api.runAiTriage(
                Number.isFinite(parsedMax) ? parsedMax : 0.75,
                8,
                hasEscalation
                    ? {
                          ...(triageEscalateProvider ? { escalateProvider: triageEscalateProvider } : {}),
                          ...(triageEscalateModel.trim() ? { escalateModel: triageEscalateModel.trim() } : {}),
                          escalationThreshold: 0.8,
                      }
                    : undefined
            )
            setTriageResult(res)
        } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : String(err)
            setTriageError(msg)
            toast('error', 'AI Triage failed', msg)
        } finally {
            setTriageLoading(false)
        }
    }

    const handleApplyTriageOverride = async (proposal: {
        resolved_id: string
        action: string
        entity_key_a?: string
        entity_key_b?: string
        reason: string
    }) => {
        const overrideType = proposal.action as 'DO_NOT_LINK' | 'ISOLATE' | 'FORCE_LINK'
        if (!proposal.entity_key_a || (overrideType !== 'ISOLATE' && !proposal.entity_key_b)) {
            toast('error', 'Missing entity key', 'Inspect the cluster to select specific entity keys for override.')
            return
        }
        const key = `${proposal.resolved_id}:${proposal.action}:${proposal.entity_key_a}`
        setApplyingOverrideKey(key)
        try {
            await api.manageOverride({
                action: 'add',
                override_type: overrideType,
                entity_key_a: proposal.entity_key_a,
                entity_key_b: proposal.entity_key_b,
                reason: `AI Triage (${proposal.resolved_id}): ${proposal.reason}`,
            })
            setAppliedOverrides((prev) => ({ ...prev, [key]: true }))
            toast(
                'success',
                'Override saved to idr_meta.identity_override',
                `${overrideType} recorded. Re-run the pipeline to apply it to the graph.`
            )
        } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : String(err)
            toast('error', 'Failed to save override', msg)
        } finally {
            setApplyingOverrideKey(null)
        }
    }

    if (!open) return null

    return (
        <div className="surface-panel p-4 space-y-3 text-xs">
            <div className="flex flex-wrap items-center justify-between gap-2">
                <div>
                    <div className="font-semibold text-ink flex items-center gap-1.5">
                        <Bot className="w-4 h-4 text-accent" />
                        3-Tier AI Triage Queue
                        <span className="badge-accent text-[10px]">SQL Tier 0 → LLM Judge</span>
                    </div>
                    <p className="text-ink-muted text-[11px] mt-0.5">
                        Filters healthy clusters in warehouse SQL ($0, zero PII) and judges only borderline or multi-subject clusters. Approved splits write to <code className="font-mono">idr_meta.identity_override</code>.
                    </p>
                </div>
                <button
                    type="button"
                    onClick={onClose}
                    aria-label="Close AI Triage Queue"
                    className="p-1 rounded text-ink-muted hover:text-ink hover:bg-hover"
                >
                    <X className="w-4 h-4" />
                </button>
            </div>

            <div className="flex flex-wrap items-end gap-2.5 pt-1">
                <label className="space-y-1">
                    <span className="text-[11px] text-ink-muted block">Tier-0 Max Confidence</span>
                    <input
                        type="number"
                        step="0.05"
                        min="0.1"
                        max="1.0"
                        value={triageMaxConf}
                        onChange={(e) => setTriageMaxConf(e.target.value)}
                        className="input-field text-xs py-1 w-28 font-mono"
                    />
                </label>
                <label className="space-y-1">
                    <span className="text-[11px] text-ink-muted block">Tier-2 Provider (optional)</span>
                    <select
                        aria-label="Tier-2 Escalation Provider"
                        value={triageEscalateProvider}
                        onChange={(e) => setTriageEscalateProvider(e.target.value)}
                        className="input-field text-xs py-1 w-40"
                    >
                        <option value="">Auto-detect</option>
                        <option value="anthropic">Anthropic</option>
                        <option value="openai">OpenAI</option>
                        <option value="gemini">Google Gemini</option>
                        <option value="warehouse">Warehouse Native</option>
                        <option value="ollama">Ollama (Local)</option>
                    </select>
                </label>
                <label className="space-y-1">
                    <span className="text-[11px] text-ink-muted block">Tier-2 Escalation Model (optional)</span>
                    <input
                        type="text"
                        value={triageEscalateModel}
                        onChange={(e) => setTriageEscalateModel(e.target.value)}
                        placeholder="e.g. claude-3-5-sonnet-latest or gpt-4o"
                        className="input-field text-xs py-1 w-60 font-mono"
                    />
                </label>
                <button
                    type="button"
                    onClick={handleRunTriage}
                    disabled={triageLoading}
                    className="btn-primary text-xs py-1.5"
                >
                    {triageLoading ? (
                        <>
                            <Loader2 className="w-3.5 h-3.5 animate-spin" />
                            Running Cascade…
                        </>
                    ) : (
                        <>
                            <Bot className="w-3.5 h-3.5" />
                            Run 3-Tier Triage
                        </>
                    )}
                </button>
            </div>

            {triageError && (
                <div className="rounded-md border border-status-danger/40 bg-status-danger/10 px-3 py-2 text-status-danger text-xs">
                    {triageError}
                </div>
            )}

            {triageResult?.cascade_summary && (
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 pt-1">
                    <div className="rounded-md border border-line bg-subtle px-3 py-2">
                        <div className="text-[11px] text-ink-muted">Multi-Entity Clusters</div>
                        <div className="text-sm font-mono font-semibold text-ink mt-0.5">
                            {triageResult.cascade_summary.total_multi_entity_clusters.toLocaleString()}
                        </div>
                    </div>
                    <div className="rounded-md border border-line bg-subtle px-3 py-2">
                        <div className="text-[11px] text-ink-muted">SQL Tier 0 Auto-Passed ($0)</div>
                        <div className="text-sm font-mono font-semibold text-status-success mt-0.5">
                            {triageResult.cascade_summary.tier0_auto_passed.toLocaleString()}{' '}
                            <span className="text-[11px] font-normal">
                                ({triageResult.cascade_summary.sql_filter_savings_pct}%)
                            </span>
                        </div>
                    </div>
                    <div className="rounded-md border border-line bg-subtle px-3 py-2">
                        <div className="text-[11px] text-ink-muted">Tier 1 Judged ({triageResult.model || 'LLM'})</div>
                        <div className="text-sm font-mono font-semibold text-ink mt-0.5">
                            {triageResult.cascade_summary.tier1_reviewed.toLocaleString()}
                        </div>
                    </div>
                    <div className="rounded-md border border-line bg-subtle px-3 py-2">
                        <div className="text-[11px] text-ink-muted">Tier 2 Escalated</div>
                        <div className="text-sm font-mono font-semibold text-accent mt-0.5">
                            {triageResult.cascade_summary.tier2_escalated.toLocaleString()}
                        </div>
                    </div>
                </div>
            )}

            {triageResult && triageResult.proposals.length === 0 && (
                <div className="rounded-md border border-line bg-subtle px-3 py-2.5 text-ink-secondary">
                    {triageResult.message || 'No borderline clusters flagged.'}
                </div>
            )}

            {triageResult && triageResult.proposals.length > 0 && (
                <div className="space-y-2 max-h-80 overflow-y-auto pr-1">
                    {triageResult.proposals.map((p, idx) => {
                        const key = `${p.resolved_id}:${p.action}:${p.entity_key_a || ''}`
                        const canOverride =
                            (p.action === 'ISOLATE' && Boolean(p.entity_key_a)) ||
                            ((p.action === 'DO_NOT_LINK' || p.action === 'FORCE_LINK') &&
                                Boolean(p.entity_key_a) &&
                                Boolean(p.entity_key_b))
                        const isApplied = Boolean(appliedOverrides[key])
                        return (
                            <div
                                key={`${p.resolved_id}-${idx}`}
                                className="rounded-md border border-line bg-subtle px-3 py-2.5 flex flex-col sm:flex-row sm:items-center justify-between gap-2"
                            >
                                <div className="space-y-1 min-w-0">
                                    <div className="flex flex-wrap items-center gap-1.5">
                                        <span className="font-mono font-semibold text-ink">{p.resolved_id}</span>
                                        <span
                                            className={
                                                p.action === 'KEEP'
                                                    ? 'badge-success'
                                                    : p.action === 'DO_NOT_LINK' || p.action === 'ISOLATE'
                                                    ? 'badge-danger'
                                                    : 'badge-warning'
                                            }
                                        >
                                            {p.action}
                                        </span>
                                        <span
                                            className="badge-neutral font-mono text-[10px]"
                                            title={
                                                p.escalation_reason === 'structural_disagreement'
                                                    ? 'Escalated to Tier 2: Tier-1 verdict conflicted with SQL structural fanout/identifier evidence'
                                                    : p.escalation_reason
                                                    ? `Escalated (${p.escalation_reason})`
                                                    : undefined
                                            }
                                        >
                                            {p.escalated ? 'TIER 2' : 'TIER 1'}
                                            {p.judge_confidence !== undefined ? ` · ${Math.round(p.judge_confidence * 100)}%` : ''}
                                            {p.escalation_reason === 'structural_disagreement' ? ' · Guardrail' : ''}
                                        </span>
                                        {p.subject_fanout !== null && p.subject_fanout !== undefined && (
                                            <span className="badge-neutral font-mono text-[10px]">
                                                fanout {p.subject_fanout}
                                            </span>
                                        )}
                                    </div>
                                    <p className="text-ink-secondary">{p.reason}</p>
                                </div>
                                <div className="flex items-center gap-1.5 shrink-0">
                                    {p.resolved_id && p.resolved_id !== 'parse_error' && (
                                        <button
                                            type="button"
                                            onClick={() => onInspectCluster(p.resolved_id)}
                                            className="btn-secondary text-[11px] py-1 px-2"
                                        >
                                            Inspect
                                        </button>
                                    )}
                                    {canOverride && (
                                        <button
                                            type="button"
                                            disabled={applyingOverrideKey === key || isApplied}
                                            onClick={() => handleApplyTriageOverride(p)}
                                            className="btn-primary text-[11px] py-1 px-2 disabled:opacity-50"
                                        >
                                            {isApplied ? 'Override Saved' : applyingOverrideKey === key ? 'Saving…' : 'Apply Override'}
                                        </button>
                                    )}
                                </div>
                            </div>
                        )
                    })}
                </div>
            )}
        </div>
    )
}
