import { useEffect, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import {
    X, Send, Play, ShieldCheck, Loader2,
    Clock, Settings as SettingsIcon, FlaskConical, Bot
} from 'lucide-react'
import { api } from '../api/client'
import { useToast } from './Toast'

interface CopilotMessage {
    role: 'user' | 'assistant'
    content: string
    intent?: string
    pii_masked?: boolean
    metrics?: Record<string, any> | null
    proposed_action?: {
        type: 'run_pipeline'
        label: string
        mode: 'INCR' | 'FULL'
        dry_run: boolean
        lens: string | null
    } | null
}

const QUICK_PROMPTS = [
    'Diagnose anomalies & toxic hubs',
    'Draft a strict login lens',
    'Compare identity lenses',
    'Run IDR incremental',
]

export default function AICopilotDrawer({
    open,
    onClose,
}: {
    open: boolean
    onClose: () => void
}) {
    const { toast } = useToast()
    const [input, setInput] = useState('')
    const [loading, setLoading] = useState(false)
    const [executingActionIdx, setExecutingActionIdx] = useState<number | null>(null)
    const [messages, setMessages] = useState<CopilotMessage[]>([])
    const bottomRef = useRef<HTMLDivElement>(null)

    const { data: aiCfg } = useQuery({
        queryKey: ['aiConfig'],
        queryFn: () => api.getAiConfig(),
        enabled: open,
    })

    useEffect(() => {
        if (!open) return
        const onKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') onClose()
        }
        window.addEventListener('keydown', onKeyDown)
        return () => window.removeEventListener('keydown', onKeyDown)
    }, [open, onClose])

    useEffect(() => {
        bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
    }, [messages, loading])

    const sendMessage = async (text?: string) => {
        const msg = (text ?? input).trim()
        if (!msg || loading) return
        if (!text) setInput('')

        const nextHistory = [...messages, { role: 'user' as const, content: msg }]
        setMessages(nextHistory)
        setLoading(true)

        try {
            const res = await api.copilotChat(
                msg,
                nextHistory.slice(-6).map((m) => ({ role: m.role, content: m.content }))
            )
            setMessages((prev) => [
                ...prev,
                {
                    role: 'assistant',
                    content: res.answer,
                    intent: res.intent,
                    pii_masked: res.pii_masked,
                    metrics: res.metrics,
                    proposed_action: res.proposed_action,
                },
            ])
        } catch (err: any) {
            setMessages((prev) => [
                ...prev,
                {
                    role: 'assistant',
                    content: `Unable to reach AI Copilot: ${err?.message || err}`,
                },
            ])
        } finally {
            setLoading(false)
        }
    }

    const executeProposedRun = async (
        idx: number,
        action: NonNullable<CopilotMessage['proposed_action']>,
        overrideDryRun?: boolean
    ) => {
        const dryRun = overrideDryRun ?? action.dry_run
        setExecutingActionIdx(idx)
        try {
            const res = await api.runSetup({
                mode: action.mode,
                strict: false,
                max_iterations: 30,
                dry_run: dryRun,
                ...(action.lens ? { lens: action.lens } : {}),
            })
            const runId = String(res?.run_id || 'completed')
            toast(
                'success',
                dryRun ? 'Dry Run Complete' : `IDR ${action.mode} Complete`,
                `Run ${runId} finished.`
            )
            setMessages((prev) => [
                ...prev,
                {
                    role: 'assistant',
                    content: `${dryRun ? 'Dry run' : `${action.mode} run`} finished (\`${runId}\`).`,
                },
            ])
        } catch (err: any) {
            toast('error', 'Pipeline run failed', String(err?.message || err))
        } finally {
            setExecutingActionIdx(null)
        }
    }

    if (!open) return null

    return (
        <div
            role="dialog"
            aria-modal="true"
            aria-label="IDR AI Copilot"
            className="fixed inset-0 z-50 flex justify-end"
        >
            <div
                className="fixed inset-0 bg-ink/25"
                onClick={onClose}
                aria-hidden="true"
            />
            <div className="relative z-10 w-full max-w-lg bg-surface border-l border-line h-full flex flex-col shadow-popover">
                {/* Header */}
                <div className="px-4 py-3 border-b border-line flex items-center justify-between bg-subtle">
                    <div>
                        <h2 className="text-[14px] font-semibold text-ink flex items-center gap-2">
                            <Bot className="w-4 h-4 text-accent" />
                            IDR AI Copilot
                        </h2>
                        <div className="flex flex-wrap items-center gap-2 mt-0.5 text-[11px] text-ink-muted">
                            <span className="font-mono text-ink-secondary">
                                {aiCfg?.provider || 'ollama'} · {aiCfg?.model || 'default'}
                            </span>
                            {aiCfg && (
                                <span className={aiCfg.mask_pii ? 'badge-success' : 'badge-accent'}>
                                    <ShieldCheck className="w-3 h-3" />
                                    {aiCfg.mask_pii ? 'PII Masked' : 'Local VPC'}
                                </span>
                            )}
                            <Link
                                to="/settings"
                                onClick={onClose}
                                className="inline-flex items-center gap-1 text-accent hover:underline"
                            >
                                <SettingsIcon className="w-3 h-3" />
                                Configure Model
                            </Link>
                        </div>
                    </div>
                    <button
                        type="button"
                        onClick={onClose}
                        aria-label="Close AI Copilot"
                        className="p-1.5 rounded-md text-ink-muted hover:text-ink hover:bg-hover"
                    >
                        <X className="w-4 h-4" />
                    </button>
                </div>

                {/* Messages */}
                <div className="flex-1 overflow-y-auto p-4 space-y-3">
                    {messages.length === 0 && (
                        <div className="space-y-3 pt-2">
                            <p className="text-xs text-ink-muted">
                                Ask about runtimes, compare lenses, diagnose megacluster hubs, or trigger incremental/full IDR runs.
                            </p>
                            <div className="flex flex-wrap gap-1.5">
                                {QUICK_PROMPTS.map((qp) => (
                                    <button
                                        key={qp}
                                        type="button"
                                        onClick={() => sendMessage(qp)}
                                        className="btn-secondary text-xs"
                                    >
                                        {qp}
                                    </button>
                                ))}
                            </div>
                        </div>
                    )}

                    {messages.map((m, idx) => (
                        <div
                            key={idx}
                            className={`flex flex-col gap-2 ${
                                m.role === 'user' ? 'items-end' : 'items-start'
                            }`}
                        >
                            <div
                                className={`max-w-[90%] rounded-md px-3 py-2 text-xs leading-relaxed whitespace-pre-wrap ${
                                    m.role === 'user'
                                        ? 'bg-accent text-accent-fg'
                                        : 'bg-subtle border border-line text-ink'
                                }`}
                            >
                                {m.content}
                            </div>

                            {/* Anomaly Triage Card */}
                            {m.intent === 'diagnose_anomalies' && m.metrics && (
                                <div className="w-full surface-panel p-3 space-y-2 text-xs">
                                    <div className="grid grid-cols-3 gap-2 font-mono tabular-nums">
                                        <div>
                                            <div className="text-[10px] font-sans text-ink-muted">Latest</div>
                                            <div className="font-semibold text-ink">
                                                {m.metrics.latest_duration_seconds != null ? `${m.metrics.latest_duration_seconds}s` : '—'}
                                            </div>
                                        </div>
                                        <div>
                                            <div className="text-[10px] font-sans text-ink-muted">Avg</div>
                                            <div className="font-semibold text-accent">
                                                {m.metrics.avg_duration_seconds != null ? `${m.metrics.avg_duration_seconds}s` : '—'}
                                            </div>
                                        </div>
                                        <div>
                                            <div className="text-[10px] font-sans text-ink-muted">Skipped Hubs</div>
                                            <div className="font-semibold text-status-warning">
                                                {(m.metrics.skipped_hubs || []).length}
                                            </div>
                                        </div>
                                    </div>
                                    {(m.metrics.recommended_exclusions || []).slice(0, 2).map((ex: any, i: number) => (
                                        <div key={i} className="flex items-center justify-between gap-2 pt-1.5 border-t border-line">
                                            <span className="font-mono text-[11px] text-ink-secondary truncate">
                                                {ex.identifier_type}: {ex.identifier_value_pattern} ({ex.group_size})
                                            </span>
                                            <button
                                                type="button"
                                                onClick={async () => {
                                                    try {
                                                        await api.manageExclusion({
                                                            action: 'add',
                                                            identifier_type: ex.identifier_type,
                                                            identifier_value_pattern: ex.identifier_value_pattern,
                                                            match_type: 'EXACT',
                                                            reason: 'Added from AI Anomaly Triage',
                                                        })
                                                        toast('success', 'Exclusion added', `${ex.identifier_type}: ${ex.identifier_value_pattern}`)
                                                    } catch (err: any) {
                                                        toast('error', 'Failed to add exclusion', String(err?.message || err))
                                                    }
                                                }}
                                                className="btn-secondary text-[11px] px-2 py-0.5 min-h-[26px]"
                                            >
                                                + Exclude Hub
                                            </button>
                                        </div>
                                    ))}
                                </div>
                            )}

                            {/* Runtime Trend Metric Card */}
                            {m.intent === 'get_runtime_trend' && m.metrics && (
                                <div className="w-full surface-panel p-3 grid grid-cols-3 gap-2 text-xs font-mono tabular-nums">
                                    <div>
                                        <div className="text-[10px] font-sans text-ink-muted flex items-center gap-1">
                                            <Clock className="w-3 h-3" /> Latest
                                        </div>
                                        <div className="font-semibold text-ink">
                                            {m.metrics.latest_run?.duration_seconds != null
                                                ? `${m.metrics.latest_run.duration_seconds}s`
                                                : '—'}
                                        </div>
                                    </div>
                                    <div>
                                        <div className="text-[10px] font-sans text-ink-muted">Recent Avg</div>
                                        <div className="font-semibold text-accent">
                                            {m.metrics.avg_duration_seconds != null
                                                ? `${m.metrics.avg_duration_seconds}s`
                                                : '—'}
                                        </div>
                                    </div>
                                    <div>
                                        <div className="text-[10px] font-sans text-ink-muted">Today Runs</div>
                                        <div className="font-semibold text-status-success">
                                            {(m.metrics.today_runs || []).length}
                                        </div>
                                    </div>
                                </div>
                            )}

                            {/* Action Confirmation Card */}
                            {m.proposed_action && (
                                <div className="w-full surface-panel p-3 space-y-2 border-accent-border bg-accent-soft/50">
                                    <div className="text-xs font-semibold text-ink">
                                        {m.proposed_action.label}
                                    </div>
                                    <div className="flex flex-wrap gap-2">
                                        <button
                                            type="button"
                                            disabled={executingActionIdx !== null}
                                            onClick={() => executeProposedRun(idx, m.proposed_action!, false)}
                                            className="btn-primary text-xs"
                                        >
                                            {executingActionIdx === idx ? (
                                                <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                            ) : (
                                                <Play className="w-3.5 h-3.5" />
                                            )}
                                            Confirm &amp; Run {m.proposed_action.mode}
                                        </button>
                                        <button
                                            type="button"
                                            disabled={executingActionIdx !== null}
                                            onClick={() => executeProposedRun(idx, m.proposed_action!, true)}
                                            className="btn-secondary text-xs"
                                        >
                                            <FlaskConical className="w-3.5 h-3.5" />
                                            Dry Run First
                                        </button>
                                    </div>
                                </div>
                            )}
                        </div>
                    ))}

                    {loading && (
                        <div className="flex items-center gap-2 text-xs text-ink-muted">
                            <Loader2 className="w-3.5 h-3.5 animate-spin text-accent" />
                            Thinking…
                        </div>
                    )}
                    <div ref={bottomRef} />
                </div>

                {/* Input */}
                <form
                    onSubmit={(e) => {
                        e.preventDefault()
                        sendMessage()
                    }}
                    className="p-3 border-t border-line bg-subtle flex gap-2"
                >
                    <input
                        type="text"
                        aria-label="Ask IDR AI Copilot"
                        value={input}
                        onChange={(e) => setInput(e.target.value)}
                        placeholder="Ask runtime trends or 'run IDR incremental'…"
                        className="input-field flex-1 text-xs"
                    />
                    <button
                        type="submit"
                        disabled={loading || !input.trim()}
                        aria-label="Send message"
                        className="btn-primary px-3"
                    >
                        <Send className="w-3.5 h-3.5" />
                    </button>
                </form>
            </div>
        </div>
    )
}
