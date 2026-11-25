import { useEffect, useState } from 'react'
import { ChevronDown, ChevronUp, GitBranch } from 'lucide-react'
import { formatSourceName } from '../../utils/colors'
import { CrosswalkEntry } from './types'

interface CrosswalkLineageStripProps {
    resolvedId: string
    history?: CrosswalkEntry[] | null
    onSelectCluster: (resolvedId: string) => void
}

export default function CrosswalkLineageStrip({
    resolvedId,
    history,
    onSelectCluster,
}: CrosswalkLineageStripProps) {
    const [crosswalkExpanded, setCrosswalkExpanded] = useState(false)

    useEffect(() => {
        setCrosswalkExpanded(false)
    }, [resolvedId])

    if (!history || history.length === 0) return null

    const mergedCnt = history.filter((c) => c.change_type === 'MERGED').length
    const splitCnt = history.filter((c) => c.change_type === 'SPLIT').length
    const totalMoved = history.reduce((acc, c) => acc + (Number(c.entities_moved) || 0), 0)
    const bySource: Record<string, number> = {}
    for (const cw of history) {
        const prev = String(cw.previous_resolved_id || '')
        const src = prev.includes(':') ? prev.split(':', 1)[0] : 'cluster'
        bySource[src] = (bySource[src] || 0) + 1
    }
    const sourceBreakdown = Object.entries(bySource)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)

    return (
        <div className="surface-panel overflow-hidden">
            <div className="px-3.5 py-2 bg-accent-soft flex flex-wrap items-center justify-between gap-2 text-xs">
                <div className="flex flex-wrap items-center gap-2 min-w-0">
                    <span className="inline-flex items-center gap-1.5 font-semibold text-accent">
                        <GitBranch className="w-3.5 h-3.5" />
                        Crosswalk Lineage
                    </span>
                    <span className="font-mono tabular-nums text-[11px] px-2 py-0.5 rounded bg-surface border border-line text-ink font-medium">
                        {history.length} {history.length === 1 ? 'transition' : 'transitions'} · {totalMoved.toLocaleString()} {totalMoved === 1 ? 'entity' : 'entities'} moved
                    </span>
                    {mergedCnt > 0 && (
                        <span className="badge-Exact font-mono tabular-nums">
                            {mergedCnt} MERGED
                        </span>
                    )}
                    {splitCnt > 0 && (
                        <span className="badge-warning font-mono tabular-nums">
                            {splitCnt} SPLIT
                        </span>
                    )}
                    {sourceBreakdown.map(([src, cnt]) => (
                        <span
                            key={src}
                            className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-surface border border-line text-[11px] text-ink-secondary"
                        >
                            <span>{formatSourceName(src)}</span>
                            <span className="font-mono tabular-nums text-ink-muted">×{cnt}</span>
                        </span>
                    ))}
                </div>
                <button
                    type="button"
                    onClick={() => setCrosswalkExpanded((v) => !v)}
                    aria-expanded={crosswalkExpanded}
                    className="btn-secondary text-xs px-2.5 py-1 min-h-[26px]"
                >
                    {crosswalkExpanded ? (
                        <>
                            <ChevronUp className="w-3.5 h-3.5" />
                            Hide Transitions
                        </>
                    ) : (
                        <>
                            <ChevronDown className="w-3.5 h-3.5" />
                            Inspect {history.length} {history.length === 1 ? 'Transition' : 'Transitions'}
                        </>
                    )}
                </button>
            </div>

            {crosswalkExpanded && (
                <div className="max-h-56 overflow-y-auto border-t border-line bg-surface">
                    <table className="data-table">
                        <thead className="sticky top-0 bg-subtle z-10">
                            <tr>
                                <th className="w-24">Type</th>
                                <th className="w-36">Origin Source</th>
                                <th>Previous Cluster ID</th>
                                <th>Target Cluster</th>
                                <th className="text-right w-24">Moved</th>
                            </tr>
                        </thead>
                        <tbody>
                            {history.map((cw, idx) => {
                                const prev = String(cw.previous_resolved_id || '')
                                const curr = String(cw.current_resolved_id || '')
                                const [prevSrc, prevKey] = prev.includes(':')
                                    ? prev.split(':', 2)
                                    : ['cluster', prev]
                                const isCurrentTarget = curr === resolvedId
                                return (
                                    <tr key={idx}>
                                        <td>
                                            <span className={cw.change_type === 'SPLIT' ? 'badge-warning font-mono' : 'badge-Exact font-mono'}>
                                                {cw.change_type}
                                            </span>
                                        </td>
                                        <td className="text-xs text-ink-secondary">
                                            {formatSourceName(prevSrc)}
                                        </td>
                                        <td className="font-mono text-xs text-ink" title={prev}>
                                            {prevKey}
                                        </td>
                                        <td className="font-mono text-xs">
                                            {isCurrentTarget ? (
                                                <span className="inline-flex items-center gap-1 text-accent font-medium">
                                                    <span>→</span>
                                                    <span>Current Cluster</span>
                                                </span>
                                            ) : (
                                                <button
                                                    type="button"
                                                    onClick={() => onSelectCluster(curr)}
                                                    className="text-accent hover:underline truncate max-w-[240px] inline-block"
                                                    title={curr}
                                                >
                                                    → {curr}
                                                </button>
                                            )}
                                        </td>
                                        <td className="text-right font-mono text-xs tabular-nums text-ink-secondary">
                                            {cw.entities_moved}
                                        </td>
                                    </tr>
                                )
                            })}
                        </tbody>
                    </table>
                </div>
            )}
        </div>
    )
}
