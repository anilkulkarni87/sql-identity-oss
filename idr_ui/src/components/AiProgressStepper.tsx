import { useEffect, useState } from 'react'
import { Bot, CheckCircle2, Circle, Loader2 } from 'lucide-react'

export interface AiProgressEvent {
    stage: 'profiling' | 'discovery' | 'pass_a' | 'pass_b' | 'validation' | 'complete' | string
    step: number
    total_steps: number
    message: string
}

const STAGES: Array<{ id: string; step: number; label: string }> = [
    { id: 'profiling', step: 1, label: 'Profile Tables' },
    { id: 'discovery', step: 2, label: 'Joins & Anomalies' },
    { id: 'pass_a', step: 3, label: 'Map Sources' },
    { id: 'pass_b', step: 4, label: 'Rules & Survivorship' },
    { id: 'validation', step: 5, label: 'SQL Verification' },
]

interface AiProgressStepperProps {
    progress: AiProgressEvent | null
    tableCount?: number
}

export function AiProgressStepper({ progress, tableCount }: AiProgressStepperProps) {
    const [elapsed, setElapsed] = useState(0)

    useEffect(() => {
        setElapsed(0)
        const id = window.setInterval(() => setElapsed((s) => s + 1), 1000)
        return () => window.clearInterval(id)
    }, [])

    const activeStep = progress?.step || 1
    const activeMessage =
        progress?.message ||
        `Starting Zero-PII profiling${tableCount ? ` across ${tableCount} warehouse table(s)` : ''}…`

    return (
        <div
            role="status"
            aria-live="polite"
            className="surface-panel border border-accent-border bg-accent-soft/40 p-4 space-y-3"
        >
            <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex items-center gap-2 min-w-0">
                    <div className="w-7 h-7 rounded-md bg-accent/15 border border-accent-border flex items-center justify-center shrink-0">
                        <Bot className="w-4 h-4 text-accent" />
                    </div>
                    <div className="min-w-0">
                        <div className="text-xs font-semibold text-ink">
                            AI Assisted Configuration in Progress
                        </div>
                        <div className="text-xs text-ink-secondary truncate" title={activeMessage}>
                            {activeMessage}
                        </div>
                    </div>
                </div>
                <div className="flex items-center gap-2 shrink-0 text-[11px] font-mono tabular-nums text-ink-muted">
                    <span className="px-2 py-0.5 rounded bg-surface border border-line">
                        Step {Math.min(activeStep, STAGES.length)} of {STAGES.length}
                    </span>
                    <span>{elapsed}s</span>
                </div>
            </div>

            <div className="grid grid-cols-2 sm:grid-cols-5 gap-2">
                {STAGES.map((st) => {
                    const isDone = activeStep > st.step
                    const isActive = activeStep === st.step
                    return (
                        <div
                            key={st.id}
                            className={`px-2.5 py-2 rounded-md border text-[11px] flex items-center gap-1.5 transition-colors ${
                                isDone
                                    ? 'bg-status-success-soft/60 border-status-success-border text-status-success font-medium'
                                    : isActive
                                    ? 'bg-surface border-accent text-accent font-semibold shadow-xs'
                                    : 'bg-subtle/60 border-line text-ink-subtle'
                            }`}
                        >
                            {isDone ? (
                                <CheckCircle2 className="w-3.5 h-3.5 shrink-0" />
                            ) : isActive ? (
                                <Loader2 className="w-3.5 h-3.5 shrink-0 animate-spin" />
                            ) : (
                                <Circle className="w-3.5 h-3.5 shrink-0 opacity-60" />
                            )}
                            <span className="truncate">
                                {st.step}. {st.label}
                            </span>
                        </div>
                    )
                })}
            </div>
        </div>
    )
}
