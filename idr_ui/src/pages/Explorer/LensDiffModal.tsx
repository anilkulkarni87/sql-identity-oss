import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { GitBranch, Loader2, X } from 'lucide-react'
import { api } from '../../api/client'
import { LensSummary } from './types'

interface LensDiffModalProps {
    open: boolean
    availableLenses: LensSummary[]
    initialLensA: string
    initialLensB: string
    onClose: () => void
}

export default function LensDiffModal({
    open,
    availableLenses,
    initialLensA,
    initialLensB,
    onClose,
}: LensDiffModalProps) {
    const [diffLensA, setDiffLensA] = useState(initialLensA)
    const [diffLensB, setDiffLensB] = useState(initialLensB)

    useEffect(() => {
        if (open) {
            setDiffLensA(initialLensA)
            setDiffLensB(initialLensB)
        }
    }, [open, initialLensA, initialLensB])

    const { data: lensDiffData, isLoading: diffLoading } = useQuery({
        queryKey: ['lens-diff', diffLensA, diffLensB],
        queryFn: () => api.diffLenses(diffLensA, diffLensB, 25),
        enabled: open && !!diffLensA && !!diffLensB && diffLensA !== diffLensB,
    })

    if (!open) return null

    return (
        <div role="dialog" aria-modal="true" aria-label="Compare Identity Lenses" className="fixed inset-0 z-50 flex items-center justify-center p-4">
            <div className="fixed inset-0 bg-ink/30" onClick={onClose} aria-hidden="true" />
            <div className="relative z-10 bg-surface border border-line-strong rounded-md w-full max-w-3xl max-h-[85vh] flex flex-col shadow-popover">
                <div className="flex items-center justify-between px-4 py-3 border-b border-line bg-subtle">
                    <h2 className="text-sm font-semibold text-ink flex items-center gap-2">
                        <GitBranch className="w-4 h-4 text-accent" />
                        Compare Identity Lenses
                    </h2>
                    <button
                        type="button"
                        onClick={onClose}
                        aria-label="Close lens diff modal"
                        className="p-1 rounded text-ink-muted hover:text-ink hover:bg-hover"
                    >
                        <X className="w-4 h-4" />
                    </button>
                </div>
                <div className="p-4 space-y-4 overflow-y-auto text-xs">
                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                        <div>
                            <label className="block text-ink-secondary font-medium mb-1">Lens A</label>
                            <select
                                value={diffLensA}
                                onChange={(e) => setDiffLensA(e.target.value)}
                                className="input-field text-xs"
                            >
                                <option value="default">Primary Graph (default)</option>
                                {availableLenses.map((l) => (
                                    <option key={l.id} value={l.id}>{l.id}</option>
                                ))}
                            </select>
                        </div>
                        <div>
                            <label className="block text-ink-secondary font-medium mb-1">Lens B</label>
                            <select
                                value={diffLensB}
                                onChange={(e) => setDiffLensB(e.target.value)}
                                className="input-field text-xs"
                            >
                                <option value="default">Primary Graph (default)</option>
                                {availableLenses.map((l) => (
                                    <option key={l.id} value={l.id}>{l.id}</option>
                                ))}
                            </select>
                        </div>
                    </div>

                    {diffLensA === diffLensB ? (
                        <p className="text-status-warning">Select two different lenses to compare.</p>
                    ) : diffLoading ? (
                        <div className="flex items-center gap-2 text-ink-muted py-6 justify-center">
                            <Loader2 className="w-4 h-4 animate-spin text-accent" />
                            Computing cross-lens diff…
                        </div>
                    ) : lensDiffData ? (
                        <div className="space-y-4">
                            <div className="grid grid-cols-3 gap-2 font-mono tabular-nums">
                                <div className="surface-subtle p-3">
                                    <div className="text-[11px] font-sans text-ink-muted">{lensDiffData.lens_a} Clusters</div>
                                    <div className="text-lg font-semibold text-ink mt-0.5">{lensDiffData.lens_a_clusters}</div>
                                </div>
                                <div className="surface-subtle p-3">
                                    <div className="text-[11px] font-sans text-ink-muted">{lensDiffData.lens_b} Clusters</div>
                                    <div className="text-lg font-semibold text-ink mt-0.5">{lensDiffData.lens_b_clusters}</div>
                                </div>
                                <div className="surface-subtle p-3">
                                    <div className="text-[11px] font-sans text-ink-muted">Entities Compared</div>
                                    <div className="text-lg font-semibold text-ink mt-0.5">{lensDiffData.entities_compared}</div>
                                </div>
                            </div>

                            <div>
                                <h3 className="text-[11px] font-semibold uppercase tracking-[0.04em] text-ink-muted mb-1.5">
                                    Merged in {lensDiffData.lens_b} only ({lensDiffData.merged_in_b_only.length})
                                </h3>
                                {lensDiffData.merged_in_b_only.length === 0 ? (
                                    <p className="text-ink-muted">None</p>
                                ) : (
                                    <div className="border border-line rounded-md divide-y divide-line max-h-40 overflow-y-auto">
                                        {lensDiffData.merged_in_b_only.map((row, i) => (
                                            <div key={i} className="px-3 py-1.5 flex justify-between gap-2 bg-surface">
                                                <span className="font-mono text-accent">{row.entity_key}</span>
                                                <span className="text-ink-muted font-mono truncate">
                                                    {row.resolved_a} → {row.resolved_b}
                                                </span>
                                            </div>
                                        ))}
                                    </div>
                                )}
                            </div>

                            <div>
                                <h3 className="text-[11px] font-semibold uppercase tracking-[0.04em] text-ink-muted mb-1.5">
                                    Merged in {lensDiffData.lens_a} only ({lensDiffData.merged_in_a_only.length})
                                </h3>
                                {lensDiffData.merged_in_a_only.length === 0 ? (
                                    <p className="text-ink-muted">None</p>
                                ) : (
                                    <div className="border border-line rounded-md divide-y divide-line max-h-40 overflow-y-auto">
                                        {lensDiffData.merged_in_a_only.map((row, i) => (
                                            <div key={i} className="px-3 py-1.5 flex justify-between gap-2 bg-surface">
                                                <span className="font-mono text-accent">{row.entity_key}</span>
                                                <span className="text-ink-muted font-mono truncate">
                                                    {row.resolved_a} ← {row.resolved_b}
                                                </span>
                                            </div>
                                        ))}
                                    </div>
                                )}
                            </div>
                        </div>
                    ) : null}
                </div>
            </div>
        </div>
    )
}
