import { useEffect, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import {
    Sparkles, X, Send, Play, ShieldCheck, Loader2,
    Clock, Settings as SettingsIcon, FlaskConical
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
            className="fixed inset-0 z-50 flex justify-end bg-black/60 backdrop-blur-sm"
        >
            <div className="w-full max-w-lg bg-slate-900 border-l border-slate-800 h-full flex flex-col shadow-2xl">
                {/* Header */}
                <div className="px-5 py-4 border-b border-slate-800 flex items-center justify-between">
                    <div>
                        <h2 className="text-base font-semibold text-white flex items-center gap-2">
                            <Sparkles className="w-4 h-4 text-blue-400" />
                            IDR AI Copilot
                        </h2>
                        <div className="flex items-center gap-2 mt-1 text-[11px] text-slate-400">
                            <span className="font-mono text-slate-300">
                                {aiCfg?.provider || 'ollama'} · {aiCfg?.model || 'default'}
                            </span>
                            {aiCfg && (
                                <span
                                    className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded border ${
                                        aiCfg.mask_pii
                                            ? 'bg-emerald-950/50 border-emerald-700/60 text-emerald-300'
                                            : 'bg-blue-950/50 border-blue-700/60 text-blue-300'
                                    }`}
                                >
                                    <ShieldCheck className="w-3 h-3" />
                                    {aiCfg.mask_pii ? 'PII Masked (BYOK)' : 'Local VPC'}
                                </span>
                            )}
                            <Link
                                to="/settings"
                                onClick={onClose}
                                className="inline-flex items-center gap-1 text-blue-400 hover:text-blue-300"
                            >
                                <SettingsIcon className="w-3 h-3" />
                                Model / BYOK
                            </Link>
                        </div>
                    </div>
                    <button
                        type="button"
                        onClick={onClose}
                        aria-label="Close AI Copilot"
                        className="p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800"
                    >
                        <X className="w-5 h-5" />
                    </button>
                </div>

                {/* Messages */}
                <div className="flex-1 overflow-y-auto p-4 space-y-4">
                    {messages.length === 0 && (
                        <div className="space-y-3 pt-4">
                            <p className="text-xs text-slate-400">
                                Ask about runtimes, compare lenses, or trigger incremental/full IDR runs with confirmation.
                            </p>
                            <div className="flex flex-wrap gap-2">
                                {QUICK_PROMPTS.map((qp) => (
                                    <button
                                        key={qp}
                                        type="button"
                                        onClick={() => sendMessage(qp)}
                                        className="text-left text-xs px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-200 transition"
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
                                className={`max-w-[90%] rounded-xl px-3.5 py-2.5 text-xs leading-relaxed whitespace-pre-wrap ${
                                    m.role === 'user'
                                        ? 'bg-blue-600 text-white'
                                        : 'bg-slate-800/90 border border-slate-700 text-slate-200'
                                }`}
                            >
                                {m.content}
                            </div>

                            {/* Anomaly Triage Card */}
                            {m.intent === 'diagnose_anomalies' && m.metrics && (
                                <div className="w-full bg-slate-950/80 border border-slate-800 rounded-xl p-3 space-y-2 text-xs">
                                    <div className="grid grid-cols-3 gap-2">
                                        <div>
                                            <div className="text-[10px] text-slate-400">Latest</div>
                                            <div className="font-mono font-bold text-white">
                                                {m.metrics.latest_duration_seconds != null ? `${m.metrics.latest_duration_seconds}s` : '—'}
                                            </div>
                                        </div>
                                        <div>
                                            <div className="text-[10px] text-slate-400">Avg</div>
                                            <div className="font-mono font-bold text-blue-300">
                                                {m.metrics.avg_duration_seconds != null ? `${m.metrics.avg_duration_seconds}s` : '—'}
                                            </div>
                                        </div>
                                        <div>
                                            <div className="text-[10px] text-slate-400">Skipped Hubs</div>
                                            <div className="font-mono font-bold text-amber-300">
                                                {(m.metrics.skipped_hubs || []).length}
                                            </div>
                                        </div>
                                    </div>
                                    {(m.metrics.recommended_exclusions || []).slice(0, 2).map((ex: any, i: number) => (
                                        <div key={i} className="flex items-center justify-between gap-2 pt-1.5 border-t border-slate-800">
                                            <span className="font-mono text-[11px] text-amber-200 truncate">
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
                                                className="px-2 py-1 rounded bg-amber-600/20 hover:bg-amber-600/30 border border-amber-500/40 text-[11px] text-amber-200 whitespace-nowrap"
                                            >
                                                + Exclude Hub
                                            </button>
                                        </div>
                                    ))}
                                </div>
                            )}

                            {/* Runtime Trend Metric Card */}
                            {m.intent === 'get_runtime_trend' && m.metrics && (
                                <div className="w-full bg-slate-950/80 border border-slate-800 rounded-xl p-3 grid grid-cols-3 gap-2 text-xs">
                                    <div>
                                        <div className="text-[10px] text-slate-400 flex items-center gap-1">
                                            <Clock className="w-3 h-3" /> Latest
                                        </div>
                                        <div className="font-mono font-bold text-white">
                                            {m.metrics.latest_run?.duration_seconds != null
                                                ? `${m.metrics.latest_run.duration_seconds}s`
                                                : '—'}
                                        </div>
                                    </div>
                                    <div>
                                        <div className="text-[10px] text-slate-400">Recent Avg</div>
                                        <div className="font-mono font-bold text-blue-300">
                                            {m.metrics.avg_duration_seconds != null
                                                ? `${m.metrics.avg_duration_seconds}s`
                                                : '—'}
                                        </div>
                                    </div>
                                    <div>
                                        <div className="text-[10px] text-slate-400">Today Runs</div>
                                        <div className="font-mono font-bold text-emerald-300">
                                            {(m.metrics.today_runs || []).length}
                                        </div>
                                    </div>
                                </div>
                            )}

                            {/* Action Confirmation Card */}
                            {m.proposed_action && (
                                <div className="w-full bg-slate-950 border border-blue-500/40 rounded-xl p-3.5 space-y-2.5">
                                    <div className="text-xs font-semibold text-blue-300">
                                        {m.proposed_action.label}
                                    </div>
                                    <div className="flex flex-wrap gap-2">
                                        <button
                                            type="button"
                                            disabled={executingActionIdx !== null}
                                            onClick={() => executeProposedRun(idx, m.proposed_action!, false)}
                                            className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-xs font-semibold text-white"
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
                                            className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 disabled:opacity-50 border border-slate-700 text-xs font-medium text-slate-200"
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
                        <div className="flex items-center gap-2 text-xs text-slate-400">
                            <Loader2 className="w-3.5 h-3.5 animate-spin" />
                            Thinking...
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
                    className="p-4 border-t border-slate-800 flex gap-2"
                >
                    <input
                        type="text"
                        value={input}
                        onChange={(e) => setInput(e.target.value)}
                        placeholder="Ask runtime trends or 'run IDR incremental'..."
                        className="flex-1 bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                    <button
                        type="submit"
                        disabled={loading || !input.trim()}
                        className="px-3.5 py-2 rounded-lg bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white"
                    >
                        <Send className="w-4 h-4" />
                    </button>
                </form>
            </div>
        </div>
    )
}
