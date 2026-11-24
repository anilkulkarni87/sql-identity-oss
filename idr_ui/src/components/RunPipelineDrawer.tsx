import { createContext, useContext, useEffect, useId, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQueryClient } from '@tanstack/react-query'
import {
    Play, X, CheckCircle2, AlertCircle, Loader2, ChevronDown,
    ChevronRight, Clock, ShieldCheck, ArrowRight, Eye
} from 'lucide-react'
import { api } from '../api/client'
import { useToast } from './Toast'

interface RunDrawerContextValue {
    openRunDrawer: () => void
    closeRunDrawer: () => void
}

const RunDrawerContext = createContext<RunDrawerContextValue>({
    openRunDrawer: () => {},
    closeRunDrawer: () => {},
})

export function useRunDrawer() {
    return useContext(RunDrawerContext)
}

interface StageProgress {
    name: string
    label: string
    order: number
    status: 'pending' | 'running' | 'completed' | 'skipped'
    duration_seconds?: number | null
    rows_affected?: number | null
    detail?: string | null
}

interface LiveProgress {
    run_id?: string | null
    status: string
    mode?: string
    dry_run?: boolean
    elapsed_seconds?: number
    current_stage?: string | null
    current_detail?: string | null
    stages: StageProgress[]
}

export function RunPipelineDrawerProvider({
    children,
    isConfigured,
}: {
    children: React.ReactNode
    isConfigured: boolean
}) {
    const [open, setOpen] = useState(false)
    const navigate = useNavigate()

    const openRunDrawer = () => {
        if (!isConfigured) {
            navigate('/setup')
            return
        }
        setOpen(true)
    }

    const closeRunDrawer = () => setOpen(false)

    return (
        <RunDrawerContext.Provider value={{ openRunDrawer, closeRunDrawer }}>
            {children}
            {open && <RunPipelineDrawer onClose={closeRunDrawer} />}
        </RunDrawerContext.Provider>
    )
}

function RunPipelineDrawer({ onClose }: { onClose: () => void }) {
    const navigate = useNavigate()
    const queryClient = useQueryClient()
    const { toast } = useToast()

    const titleId = useId()
    const maxIterId = useId()
    const maxGroupId = useId()
    const strictId = useId()
    const activationDeltaId = useId()

    const [mode, setMode] = useState<'INCR' | 'FULL'>('INCR')
    const [dryRun, setDryRun] = useState(false)
    const [showAdvanced, setShowAdvanced] = useState(false)
    const [maxIterations, setMaxIterations] = useState(30)
    const [maxGroupSize, setMaxGroupSize] = useState(10000)
    const [strictMode, setStrictMode] = useState(false)
    const [emitActivationDelta, setEmitActivationDelta] = useState(false)

    useEffect(() => {
        let cancelled = false
        api.getSetupConfig()
            .then((cfg) => {
                if (!cancelled && cfg?.activation_delta?.enabled) {
                    setEmitActivationDelta(true)
                }
            })
            .catch(() => {
                // ignore if not configured yet
            })
        return () => {
            cancelled = true
        }
    }, [])

    const [running, setRunning] = useState(false)
    const [progress, setProgress] = useState<LiveProgress | null>(null)
    const [result, setResult] = useState<Record<string, unknown> | null>(null)
    const [error, setError] = useState<string | null>(null)

    const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)
    const sseRef = useRef<EventSource | null>(null)

    const stopTracking = () => {
        if (sseRef.current) {
            sseRef.current.close()
            sseRef.current = null
        }
        if (pollRef.current) {
            clearInterval(pollRef.current)
            pollRef.current = null
        }
    }

    useEffect(() => {
        const onKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape' && !running) {
                onClose()
            }
        }
        window.addEventListener('keydown', onKeyDown)
        return () => {
            window.removeEventListener('keydown', onKeyDown)
            stopTracking()
        }
    }, [running, onClose])

    const startPollingFallback = () => {
        if (pollRef.current) return
        pollRef.current = setInterval(async () => {
            try {
                const p = await api.getRunProgress()
                if (p && p.stages && p.stages.length > 0) {
                    setProgress(p)
                }
            } catch {
                // ignore transient poll errors
            }
        }, 700)
    }

    const handleRun = async () => {
        setRunning(true)
        setError(null)
        setResult(null)
        setProgress(null)
        stopTracking()

        try {
            const rawBase = import.meta.env.VITE_API_BASE_URL?.trim()
            const base = (rawBase && rawBase.length > 0 ? rawBase : '/api').replace(/\/$/, '')
            const es = new EventSource(`${base}/setup/run/events`)
            sseRef.current = es
            es.onmessage = (evt) => {
                try {
                    const data = JSON.parse(evt.data) as LiveProgress
                    if (data && data.stages && data.stages.length > 0) {
                        setProgress(data)
                    }
                } catch {
                    // ignore keepalives
                }
            }
            es.onerror = () => {
                es.close()
                sseRef.current = null
                startPollingFallback()
            }
        } catch {
            startPollingFallback()
        }

        try {
            const res = await api.runSetup({
                mode,
                strict: strictMode,
                max_iterations: maxIterations,
                dry_run: dryRun,
                max_group_size: maxGroupSize,
                ...(!dryRun ? { emit_activation_delta: emitActivationDelta } : {}),
            })
            setResult(res)
            try {
                const finalP = await api.getRunProgress()
                if (finalP && finalP.stages?.length > 0) {
                    setProgress(finalP)
                }
            } catch {
                // ignore
            }
            toast(
                'success',
                dryRun ? 'Dry run completed' : 'Resolution run completed',
                res.run_id ? `Run ID: ${String(res.run_id)}` : undefined
            )
            queryClient.invalidateQueries({ queryKey: ['metrics'] })
            queryClient.invalidateQueries({ queryKey: ['runs'] })
            queryClient.invalidateQueries({ queryKey: ['distribution'] })
            queryClient.invalidateQueries({ queryKey: ['rules'] })
            queryClient.invalidateQueries({ queryKey: ['confidenceDist'] })
            queryClient.invalidateQueries({ queryKey: ['sourceContrib'] })
            queryClient.invalidateQueries({ queryKey: ['alerts'] })
            queryClient.invalidateQueries({ queryKey: ['lenses'] })
        } catch (err) {
            const msg = err instanceof Error ? err.message : 'Pipeline execution failed'
            setError(msg)
            toast('error', 'Pipeline run failed', msg)
        } finally {
            stopTracking()
            setRunning(false)
        }
    }

    const completedStages = progress?.stages.filter((s) => s.status === 'completed').length ?? 0
    const totalStages = progress?.stages.length ?? 0
    const progressRatio = totalStages > 0 ? completedStages / totalStages : 0

    return (
        <div className="fixed inset-0 z-50 flex justify-end">
            <div
                className="fixed inset-0 bg-ink/25 transition-opacity"
                onClick={() => !running && onClose()}
                aria-hidden="true"
            />
            <div
                role="dialog"
                aria-modal="true"
                aria-labelledby={titleId}
                className="relative z-10 w-full max-w-md bg-surface border-l border-line h-full flex flex-col shadow-popover"
            >
                <div className="h-12 px-4 border-b border-line flex items-center justify-between shrink-0 bg-subtle">
                    <h2 id={titleId} className="text-[14px] font-semibold text-ink">
                        Run Identity Resolution
                    </h2>
                    <button
                        type="button"
                        onClick={onClose}
                        disabled={running}
                        aria-label="Close run drawer"
                        className="p-1.5 rounded-md text-ink-muted hover:text-ink hover:bg-hover transition-colors disabled:opacity-40"
                    >
                        <X className="w-4 h-4" />
                    </button>
                </div>

                <div className="flex-1 overflow-y-auto p-4 space-y-4">
                    <div className="space-y-1.5">
                        <span className="block text-xs font-medium text-ink-secondary">Execution Mode</span>
                        <div className="grid grid-cols-2 gap-1.5 p-1 bg-subtle border border-line rounded-md">
                            {(['INCR', 'FULL'] as const).map((m) => (
                                <button
                                    key={m}
                                    type="button"
                                    disabled={running}
                                    onClick={() => setMode(m)}
                                    className={`px-3 py-1.5 text-xs font-medium rounded transition-colors ${
                                        mode === m
                                            ? 'bg-surface text-ink border border-line-strong'
                                            : 'text-ink-muted hover:text-ink border border-transparent'
                                    }`}
                                >
                                    {m === 'INCR' ? 'Incremental (INCR)' : 'Full Rebuild (FULL)'}
                                </button>
                            ))}
                        </div>
                    </div>

                    <label className="flex items-start gap-2.5 p-3 rounded-md bg-subtle border border-line cursor-pointer">
                        <input
                            type="checkbox"
                            checked={dryRun}
                            disabled={running}
                            onChange={(e) => setDryRun(e.target.checked)}
                            className="mt-0.5 rounded border-line-strong text-accent focus:ring-accent"
                        />
                        <div className="min-w-0">
                            <div className="text-[13px] font-medium text-ink flex items-center gap-1.5">
                                <Eye className="w-3.5 h-3.5 text-status-warning" />
                                Dry Run (Preview impact only)
                            </div>
                            <p className="text-[11px] text-ink-muted mt-0.5">
                                Computes proposed cluster merges and splits without writing to production tables.
                            </p>
                        </div>
                    </label>

                    <div className="border border-line rounded-md overflow-hidden">
                        <button
                            type="button"
                            onClick={() => setShowAdvanced((v) => !v)}
                            aria-expanded={showAdvanced}
                            className="w-full px-3 py-2 flex items-center justify-between text-xs font-medium text-ink-secondary bg-subtle hover:bg-hover transition-colors"
                        >
                            <span>Advanced Engine Guardrails</span>
                            {showAdvanced ? (
                                <ChevronDown className="w-3.5 h-3.5 text-ink-muted" />
                            ) : (
                                <ChevronRight className="w-3.5 h-3.5 text-ink-muted" />
                            )}
                        </button>
                        {showAdvanced && (
                            <div className="p-3 space-y-3 bg-surface border-t border-line">
                                <div className="grid grid-cols-2 gap-3">
                                    <div>
                                        <label htmlFor={maxIterId} className="block text-xs font-medium text-ink-secondary mb-1">
                                            Max LP Iterations
                                        </label>
                                        <input
                                            id={maxIterId}
                                            type="number"
                                            min={1}
                                            max={200}
                                            disabled={running}
                                            value={maxIterations}
                                            onChange={(e) => setMaxIterations(Number(e.target.value) || 30)}
                                            className="input-field font-mono tabular-nums"
                                        />
                                    </div>
                                    <div>
                                        <label htmlFor={maxGroupId} className="block text-xs font-medium text-ink-secondary mb-1">
                                            Max Group Size
                                        </label>
                                        <input
                                            id={maxGroupId}
                                            type="number"
                                            min={10}
                                            disabled={running}
                                            value={maxGroupSize}
                                            onChange={(e) => setMaxGroupSize(Number(e.target.value) || 10000)}
                                            className="input-field font-mono tabular-nums"
                                        />
                                    </div>
                                </div>
                                <label htmlFor={strictId} className="flex items-center gap-2 text-xs text-ink-secondary cursor-pointer">
                                    <input
                                        id={strictId}
                                        type="checkbox"
                                        checked={strictMode}
                                        disabled={running}
                                        onChange={(e) => setStrictMode(e.target.checked)}
                                        className="rounded border-line-strong text-accent focus:ring-accent"
                                    />
                                    <span>Strict mode (skip probabilistic fuzzy pass)</span>
                                </label>
                                <label
                                    htmlFor={activationDeltaId}
                                    className={`flex items-center gap-2 text-xs text-ink-secondary ${
                                        dryRun ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'
                                    }`}
                                >
                                    <input
                                        id={activationDeltaId}
                                        type="checkbox"
                                        checked={emitActivationDelta}
                                        disabled={running || dryRun}
                                        onChange={(e) => setEmitActivationDelta(e.target.checked)}
                                        className="rounded border-line-strong text-accent focus:ring-accent"
                                    />
                                    <span>Emit Activation Delta (Reverse-ETL changelog)</span>
                                </label>
                            </div>
                        )}
                    </div>

                    {error && (
                        <div role="alert" className="p-3 rounded-md bg-status-danger-soft border border-status-danger-border flex items-start gap-2">
                            <AlertCircle className="w-4 h-4 text-status-danger shrink-0 mt-0.5" />
                            <div className="text-xs text-status-danger break-words">{error}</div>
                        </div>
                    )}

                    {(running || progress) && (
                        <div className="surface-panel overflow-hidden">
                            <div className="px-3 py-2 bg-subtle border-b border-line flex items-center justify-between">
                                <span className="text-xs font-semibold text-ink">
                                    {running ? 'Running pipeline…' : 'Execution stages'}
                                </span>
                                <span className="text-[11px] font-mono tabular-nums text-ink-muted flex items-center gap-1">
                                    <Clock className="w-3 h-3" />
                                    {(progress?.elapsed_seconds ?? 0).toFixed(1)}s
                                </span>
                            </div>
                            <div className="h-1 bg-subtle overflow-hidden">
                                <div
                                    className="h-full bg-accent origin-left transition-transform duration-200"
                                    style={{ transform: `scaleX(${running && progressRatio === 0 ? 0.08 : progressRatio})` }}
                                />
                            </div>
                            <ul className="divide-y divide-line">
                                {(progress?.stages || []).map((stage) => (
                                    <li key={stage.name} className="px-3 py-2 flex items-center justify-between gap-2 text-xs">
                                        <div className="flex items-center gap-2 min-w-0">
                                            {stage.status === 'completed' ? (
                                                <CheckCircle2 className="w-3.5 h-3.5 text-status-success shrink-0" />
                                            ) : stage.status === 'running' ? (
                                                <Loader2 className="w-3.5 h-3.5 text-accent animate-spin shrink-0" />
                                            ) : (
                                                <span className="w-2 h-2 rounded-full bg-line-strong ml-0.5 mr-1 shrink-0" />
                                            )}
                                            <span className={stage.status === 'pending' ? 'text-ink-muted truncate' : 'text-ink font-medium truncate'}>
                                                {stage.label}
                                            </span>
                                        </div>
                                        <div className="flex items-center gap-2 font-mono text-[11px] tabular-nums text-ink-muted shrink-0">
                                            {stage.rows_affected != null && (
                                                <span>
                                                    {stage.name.includes('preflight')
                                                        ? stage.rows_affected > 0
                                                            ? `${stage.rows_affected.toLocaleString()} sources`
                                                            : 'Verified'
                                                        : stage.name.includes('attribute') && stage.rows_affected === 0
                                                        ? 'Skipped'
                                                        : stage.name.includes('dry_run')
                                                        ? stage.rows_affected > 0
                                                            ? `${stage.rows_affected.toLocaleString()} analyzed`
                                                            : 'Diff ready'
                                                        : stage.name.includes('label')
                                                        ? `${stage.rows_affected.toLocaleString()} clusters`
                                                        : stage.name.includes('edge')
                                                        ? `${stage.rows_affected.toLocaleString()} edges`
                                                        : `${stage.rows_affected.toLocaleString()} rows`}
                                                </span>
                                            )}
                                            {stage.duration_seconds != null && (
                                                <span>{stage.duration_seconds.toFixed(1)}s</span>
                                            )}
                                        </div>
                                    </li>
                                ))}
                            </ul>
                        </div>
                    )}

                    {result && (
                        <div className="p-3 rounded-md bg-status-success-soft border border-status-success-border space-y-2">
                            <div className="flex items-center justify-between">
                                <div className="flex items-center gap-1.5 text-xs font-semibold text-status-success">
                                    <ShieldCheck className="w-4 h-4" />
                                    {dryRun ? 'Dry Run Summary' : 'Run Succeeded'}
                                </div>
                                {Boolean(result.run_id) && (
                                    <span className="font-mono text-[11px] text-ink-muted">
                                        {String(result.run_id).slice(0, 12)}
                                    </span>
                                )}
                            </div>
                            {dryRun && typeof result.dry_run_summary === 'object' && result.dry_run_summary !== null && (
                                <div className="grid grid-cols-2 gap-2 pt-1">
                                    {Object.entries(result.dry_run_summary as Record<string, unknown>)
                                        .filter(([, v]) => typeof v === 'number')
                                        .slice(0, 4)
                                        .map(([k, v]) => (
                                            <div key={k} className="px-2 py-1.5 rounded bg-surface border border-line">
                                                <div className="text-[10px] uppercase tracking-wider text-ink-muted">
                                                    {k.replace(/_/g, ' ')}
                                                </div>
                                                <div className="text-sm font-semibold font-mono tabular-nums text-ink">
                                                    {(v as number).toLocaleString()}
                                                </div>
                                            </div>
                                        ))}
                                </div>
                            )}
                            {!dryRun &&
                                typeof result.activation_delta_summary === 'object' &&
                                result.activation_delta_summary !== null &&
                                Object.keys(result.activation_delta_summary as Record<string, number>).length > 0 && (
                                    <div className="flex flex-wrap items-center gap-1.5 pt-1">
                                        {Object.entries(result.activation_delta_summary as Record<string, number>).map(
                                            ([k, v]) => (
                                                <span
                                                    key={k}
                                                    className={
                                                        k.startsWith('RETIRED')
                                                            ? 'badge-danger font-mono text-[10px]'
                                                            : 'badge-neutral font-mono text-[10px]'
                                                    }
                                                >
                                                    {k}: {Number(v).toLocaleString()}
                                                </span>
                                            )
                                        )}
                                    </div>
                                )}
                            <div className="flex items-center gap-2 pt-1">
                                <button
                                    type="button"
                                    onClick={() => {
                                        onClose()
                                        navigate('/runs')
                                    }}
                                    className="btn-secondary text-xs flex-1"
                                >
                                    Run History
                                </button>
                                <button
                                    type="button"
                                    onClick={() => {
                                        onClose()
                                        navigate('/explorer')
                                    }}
                                    className="btn-secondary text-xs flex-1"
                                >
                                    Inspect Clusters
                                    <ArrowRight className="w-3 h-3" />
                                </button>
                            </div>
                        </div>
                    )}
                </div>

                <div className="px-4 py-3 border-t border-line bg-subtle flex items-center justify-between gap-2 shrink-0">
                    <button
                        type="button"
                        onClick={() => {
                            onClose()
                            navigate('/setup')
                        }}
                        disabled={running}
                        className="btn-ghost text-xs"
                    >
                        Identity Studio
                    </button>
                    <button
                        type="button"
                        onClick={handleRun}
                        disabled={running}
                        className="btn-primary"
                    >
                        {running ? (
                            <>
                                <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                Running…
                            </>
                        ) : (
                            <>
                                <Play className="w-3.5 h-3.5" />
                                {dryRun ? 'Run Dry Preview' : `Run ${mode}`}
                            </>
                        )}
                    </button>
                </div>
            </div>
        </div>
    )
}
