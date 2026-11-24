import { Fragment, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import {
    ChevronDown, ChevronRight, AlertTriangle, Network, ArrowRight,
    Search, RefreshCw, XCircle, Copy, Check, GitCompareArrows
} from 'lucide-react'
import { api } from '../api/client'

interface RunSummary {
    run_id: string
    run_mode: string
    status: string
    started_at: string
    duration_seconds: number | null
    entities_processed: number
    edges_created: number
    clusters_impacted: number
}

export default function Runs() {
    const navigate = useNavigate()
    const [selectedRunId, setSelectedRunId] = useState<string | null>(null)
    const [statusFilter, setStatusFilter] = useState<'ALL' | 'SUCCESS' | 'FAILED'>('ALL')
    const [searchQuery, setSearchQuery] = useState('')
    const [deltaTypeFilter, setDeltaTypeFilter] = useState<string>('ALL')
    const [copiedSyncRunId, setCopiedSyncRunId] = useState<string | null>(null)

    const { data: runs, isLoading, isError, refetch } = useQuery({
        queryKey: ['runs'],
        queryFn: () => api.getRuns(),
    })

    const { data: runDetail, isLoading: detailLoading } = useQuery({
        queryKey: ['runDetail', selectedRunId],
        queryFn: () => api.getRunDetail(selectedRunId!),
        enabled: !!selectedRunId,
    })

    const hasActivationDelta = Boolean(
        runDetail?.activation_delta_summary &&
            Object.keys(runDetail.activation_delta_summary).length > 0
    )

    const { data: activationDeltaData, isLoading: deltaLoading } = useQuery({
        queryKey: ['runActivationDelta', selectedRunId, deltaTypeFilter],
        queryFn: () => api.getRunActivationDelta(selectedRunId!, deltaTypeFilter, 'default', 50),
        enabled: Boolean(selectedRunId && hasActivationDelta),
    })

    const filteredRuns = useMemo(() => {
        return (runs || []).filter((r) => {
            const st = r.status.toLowerCase()
            if (statusFilter === 'SUCCESS' && !['success', 'completed', 'completed_with_warnings'].includes(st)) {
                return false
            }
            if (statusFilter === 'FAILED' && !['failed', 'error'].includes(st)) {
                return false
            }
            if (searchQuery.trim()) {
                const q = searchQuery.trim().toLowerCase()
                return r.run_id.toLowerCase().includes(q) || r.run_mode.toLowerCase().includes(q)
            }
            return true
        })
    }, [runs, statusFilter, searchQuery])

    const getStatusBadge = (status: string) => {
        const lower = status.toLowerCase()
        const label = lower.replace(/_/g, ' ')
        if (['success', 'completed'].includes(lower)) {
            return (
                <span className="badge-success capitalize">
                    <span className="w-1.5 h-1.5 rounded-full bg-status-success" />
                    {label}
                </span>
            )
        }
        if (lower === 'completed_with_warnings') {
            return (
                <span className="badge-warning capitalize">
                    <span className="w-1.5 h-1.5 rounded-full bg-status-warning" />
                    warnings
                </span>
            )
        }
        if (['failed', 'error'].includes(lower)) {
            return (
                <span className="badge-danger capitalize">
                    <span className="w-1.5 h-1.5 rounded-full bg-status-danger" />
                    {label}
                </span>
            )
        }
        if (lower === 'running') {
            return (
                <span className="badge-accent capitalize">
                    <span className="w-1.5 h-1.5 rounded-full bg-accent" />
                    running
                </span>
            )
        }
        return <span className="badge-neutral capitalize">{label}</span>
    }

    const formatDate = (dateStr: string) => {
        try {
            return new Date(dateStr).toLocaleString(undefined, {
                month: 'short',
                day: 'numeric',
                hour: '2-digit',
                minute: '2-digit',
            })
        } catch {
            return dateStr
        }
    }

    const toggleRun = (runId: string) => {
        setDeltaTypeFilter('ALL')
        setSelectedRunId((prev) => (prev === runId ? null : runId))
    }

    const handleCopyRunSyncSql = async (runId: string) => {
        const sql = `SELECT d.*, g.*\nFROM idr_out.identity_activation_delta d\nLEFT JOIN idr_out.golden_profiles g ON g.resolved_id = d.resolved_id\nWHERE d.run_id = '${runId}' AND d.lens_id = 'default';`
        try {
            await navigator.clipboard.writeText(sql)
            setCopiedSyncRunId(runId)
            setTimeout(() => setCopiedSyncRunId(null), 1800)
        } catch {
            // ignore clipboard errors
        }
    }

    return (
        <div className="space-y-4">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                <div>
                    <h1 className="text-[18px] font-semibold tracking-[-0.015em] text-ink">
                        Run History
                    </h1>
                    <p className="text-[13px] text-ink-muted mt-0.5">
                        Stage-by-stage execution telemetry, label propagation convergence, and cluster impact.
                    </p>
                </div>

                <button
                    type="button"
                    onClick={() => navigate('/explorer')}
                    className="btn-secondary self-start sm:self-auto"
                >
                    <Network className="w-3.5 h-3.5 text-ink-muted" />
                    Open Explorer
                    <ArrowRight className="w-3.5 h-3.5 text-ink-muted" />
                </button>
            </div>

            <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="inline-flex items-center gap-1 p-0.5 bg-subtle border border-line rounded-md">
                    {(['ALL', 'SUCCESS', 'FAILED'] as const).map((tab) => (
                        <button
                            key={tab}
                            type="button"
                            onClick={() => setStatusFilter(tab)}
                            className={`px-2.5 py-1 text-xs font-medium rounded transition-colors ${
                                statusFilter === tab
                                    ? 'bg-surface text-ink border border-line-strong'
                                    : 'text-ink-muted hover:text-ink border border-transparent'
                            }`}
                        >
                            {tab === 'ALL' ? 'All runs' : tab === 'SUCCESS' ? 'Succeeded' : 'Failed'}
                        </button>
                    ))}
                </div>

                <div className="relative w-full sm:w-64">
                    <Search className="w-3.5 h-3.5 text-ink-subtle absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none" />
                    <input
                        type="text"
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        placeholder="Filter by run ID or mode…"
                        aria-label="Filter runs by ID or mode"
                        className="input-field pl-8 font-mono text-xs"
                    />
                </div>
            </div>

            {isError && (
                <div role="alert" className="p-3 rounded-md bg-status-danger-soft border border-status-danger-border flex items-center justify-between gap-3">
                    <span className="text-xs text-status-danger">
                        Failed to load run history from the backend.
                    </span>
                    <button type="button" onClick={() => refetch()} className="btn-secondary text-xs">
                        <RefreshCw className="w-3 h-3" />
                        Retry
                    </button>
                </div>
            )}

            <div className="surface-panel overflow-hidden">
                <div className="overflow-x-auto">
                    <table className="w-full text-left border-collapse">
                        <thead>
                            <tr>
                                <th scope="col" className="table-th w-10">
                                    <span className="sr-only">Expand run details</span>
                                </th>
                                <th scope="col" className="table-th">Status</th>
                                <th scope="col" className="table-th">Run ID</th>
                                <th scope="col" className="table-th">Mode</th>
                                <th scope="col" className="table-th">Started</th>
                                <th scope="col" className="table-th text-right">Duration</th>
                                <th scope="col" className="table-th text-right">Entities</th>
                                <th scope="col" className="table-th text-right">Edges</th>
                                <th scope="col" className="table-th text-right">Clusters</th>
                            </tr>
                        </thead>
                        <tbody>
                            {isLoading ? (
                                <tr>
                                    <td colSpan={9} className="px-4 py-10 text-center text-ink-muted text-xs">
                                        Loading run telemetry…
                                    </td>
                                </tr>
                            ) : filteredRuns.length === 0 ? (
                                <tr>
                                    <td colSpan={9} className="px-4 py-10 text-center text-ink-muted text-xs">
                                        No pipeline runs match the current filter.
                                    </td>
                                </tr>
                            ) : (
                                filteredRuns.map((run: RunSummary) => {
                                    const isExpanded = selectedRunId === run.run_id
                                    return (
                                        <Fragment key={run.run_id}>
                                            <tr
                                                className={`transition-colors hover:bg-subtle ${
                                                    isExpanded ? 'bg-selected/60' : 'bg-surface'
                                                }`}
                                            >
                                                <td className="table-td w-10">
                                                    <button
                                                        type="button"
                                                        onClick={() => toggleRun(run.run_id)}
                                                        aria-expanded={isExpanded}
                                                        aria-label={`${isExpanded ? 'Collapse' : 'Expand'} details for run ${run.run_id}`}
                                                        className="inline-flex items-center justify-center w-7 h-7 rounded text-ink-muted hover:text-ink hover:bg-hover"
                                                    >
                                                        {isExpanded ? (
                                                            <ChevronDown className="w-4 h-4 text-accent" />
                                                        ) : (
                                                            <ChevronRight className="w-4 h-4" />
                                                        )}
                                                    </button>
                                                </td>
                                                <td className="table-td whitespace-nowrap">
                                                    {getStatusBadge(run.status)}
                                                </td>
                                                <td className="table-td whitespace-nowrap">
                                                    <button
                                                        type="button"
                                                        onClick={() => toggleRun(run.run_id)}
                                                        className="font-mono text-xs font-medium text-ink hover:text-accent"
                                                    >
                                                        {run.run_id}
                                                    </button>
                                                </td>
                                                <td className="table-td whitespace-nowrap">
                                                    <span className={run.run_mode === 'FULL' ? 'badge-accent font-mono' : 'badge-neutral font-mono'}>
                                                        {run.run_mode}
                                                    </span>
                                                </td>
                                                <td className="table-td whitespace-nowrap text-xs font-mono tabular-nums text-ink-muted">
                                                    {formatDate(run.started_at)}
                                                </td>
                                                <td className="table-td text-right font-mono text-xs tabular-nums text-ink">
                                                    {run.duration_seconds != null
                                                        ? run.duration_seconds < 0.01
                                                            ? '<0.01s'
                                                            : `${Number(run.duration_seconds.toFixed(2))}s`
                                                        : '—'}
                                                </td>
                                                <td className="table-td text-right font-mono text-xs tabular-nums text-ink">
                                                    {run.entities_processed.toLocaleString()}
                                                </td>
                                                <td className="table-td text-right font-mono text-xs tabular-nums text-ink">
                                                    {run.edges_created.toLocaleString()}
                                                </td>
                                                <td className="table-td text-right font-mono text-xs tabular-nums font-medium text-ink">
                                                    {run.clusters_impacted.toLocaleString()}
                                                </td>
                                            </tr>

                                            {isExpanded && (
                                                <tr className="bg-subtle/60">
                                                    <td colSpan={9} className="px-4 py-4 border-b border-line">
                                                        {detailLoading ? (
                                                            <p className="text-xs text-ink-muted">Loading stage breakdown…</p>
                                                        ) : runDetail ? (
                                                            <div className="space-y-3">
                                                                <div className="flex flex-wrap items-center justify-between gap-3 px-3 py-2 bg-surface border border-line rounded-md">
                                                                    <div className="flex flex-wrap items-center gap-3 text-xs">
                                                                        <span className="text-ink-muted">
                                                                            Status:{' '}
                                                                            <strong className="font-mono text-ink">{runDetail.status}</strong>
                                                                        </span>
                                                                        {runDetail.lp_iterations != null && (
                                                                            <span className="text-ink-muted">
                                                                                Convergence:{' '}
                                                                                <strong className="font-mono tabular-nums text-ink">
                                                                                    {runDetail.lp_iterations} LP iterations
                                                                                </strong>
                                                                            </span>
                                                                        )}
                                                                    </div>
                                                                    <button
                                                                        type="button"
                                                                        onClick={() => navigate('/explorer')}
                                                                        className="btn-secondary text-xs"
                                                                    >
                                                                        Inspect Clusters
                                                                        <ArrowRight className="w-3 h-3" />
                                                                    </button>
                                                                </div>

                                                                {runDetail.error_message && (
                                                                    <div role="alert" className="p-3 bg-status-danger-soft border border-status-danger-border rounded-md text-xs text-status-danger font-mono space-y-1">
                                                                        <div className="font-sans font-semibold flex items-center gap-1.5">
                                                                            <XCircle className="w-3.5 h-3.5" />
                                                                            Execution Error
                                                                        </div>
                                                                        <p className="break-words">{runDetail.error_message}</p>
                                                                    </div>
                                                                )}

                                                                {runDetail.warnings && runDetail.warnings.length > 0 && (
                                                                    <div className="p-3 bg-status-warning-soft border border-status-warning-border rounded-md space-y-1">
                                                                        <div className="flex items-center gap-1.5 text-xs font-semibold text-status-warning">
                                                                            <AlertTriangle className="w-3.5 h-3.5" />
                                                                            Warnings ({runDetail.warnings.length})
                                                                        </div>
                                                                        <ul className="list-disc list-inside text-xs text-ink-secondary space-y-0.5">
                                                                            {runDetail.warnings.map((w, i) => (
                                                                                <li key={i}>{w}</li>
                                                                            ))}
                                                                        </ul>
                                                                    </div>
                                                                )}

                                                                {runDetail.stages && runDetail.stages.length > 0 && (
                                                                    <div className="bg-surface border border-line rounded-md overflow-hidden">
                                                                        <div className="px-3 py-1.5 bg-subtle border-b border-line text-[11px] font-semibold uppercase tracking-[0.04em] text-ink-muted">
                                                                            Stage Breakdown ({runDetail.stages.length} stages)
                                                                        </div>
                                                                        <div className="divide-y divide-line">
                                                                            {(() => {
                                                                                const maxDuration = Math.max(
                                                                                    ...runDetail.stages.map((s) => s.duration_seconds || 0.1),
                                                                                    0.1
                                                                                )
                                                                                return runDetail.stages.map((st) => {
                                                                                    const duration = st.duration_seconds || 0
                                                                                    const ratio = Math.min(1, Math.max(0.04, duration / maxDuration))
                                                                                    return (
                                                                                        <div
                                                                                            key={`${st.stage_order}-${st.stage_name}`}
                                                                                            className="px-3 py-2 grid grid-cols-12 items-center gap-3 text-xs"
                                                                                        >
                                                                                            <div className="col-span-5 sm:col-span-4 flex items-center gap-2 min-w-0">
                                                                                                <span className="font-mono text-[11px] text-ink-muted tabular-nums w-5">
                                                                                                    {String(st.stage_order).padStart(2, '0')}
                                                                                                </span>
                                                                                                <span className="font-medium text-ink truncate">
                                                                                                    {st.stage_name}
                                                                                                </span>
                                                                                            </div>
                                                                                            <div className="col-span-3 sm:col-span-4">
                                                                                                <div className="h-1.5 bg-subtle rounded-sm overflow-hidden">
                                                                                                    <div
                                                                                                        className="h-full bg-accent origin-left transition-transform duration-200"
                                                                                                        style={{ transform: `scaleX(${ratio})` }}
                                                                                                    />
                                                                                                </div>
                                                                                            </div>
                                                                                            <div className="col-span-2 text-right font-mono tabular-nums text-ink-muted">
                                                                                                {st.rows_out != null ? `${st.rows_out.toLocaleString()} rows` : '0 rows'}
                                                                                            </div>
                                                                                            <div className="col-span-2 text-right font-mono tabular-nums font-medium text-ink">
                                                                                                {duration < 0.01 ? '<0.01s' : `${Number(duration.toFixed(2))}s`}
                                                                                            </div>
                                                                                        </div>
                                                                                    )
                                                                                })
                                                                            })()}
                                                                        </div>
                                                                    </div>
                                                                )}

                                                                {hasActivationDelta && runDetail.activation_delta_summary && (
                                                                    <div className="bg-surface border border-line rounded-md overflow-hidden">
                                                                        <div className="px-3 py-2 bg-subtle border-b border-line flex flex-wrap items-center justify-between gap-2">
                                                                            <div className="flex items-center gap-1.5 text-[11px] font-semibold uppercase tracking-[0.04em] text-ink-muted">
                                                                                <GitCompareArrows className="w-3.5 h-3.5 text-accent" />
                                                                                Activation Changelog (idr_out.identity_activation_delta)
                                                                            </div>
                                                                            <button
                                                                                type="button"
                                                                                onClick={() => handleCopyRunSyncSql(run.run_id)}
                                                                                className="btn-secondary text-xs py-1 px-2"
                                                                            >
                                                                                {copiedSyncRunId === run.run_id ? (
                                                                                    <>
                                                                                        <Check className="w-3 h-3 text-status-success" />
                                                                                        Copied SQL
                                                                                    </>
                                                                                ) : (
                                                                                    <>
                                                                                        <Copy className="w-3 h-3 text-ink-muted" />
                                                                                        Copy Sync SQL
                                                                                    </>
                                                                                )}
                                                                            </button>
                                                                        </div>

                                                                        <div className="px-3 py-2 border-b border-line flex flex-wrap items-center gap-1.5">
                                                                            {(() => {
                                                                                const summaryEntries = Object.entries(runDetail.activation_delta_summary)
                                                                                const totalDelta = summaryEntries.reduce((acc, [, v]) => acc + Number(v || 0), 0)
                                                                                return (
                                                                                    <>
                                                                                        <button
                                                                                            type="button"
                                                                                            onClick={() => setDeltaTypeFilter('ALL')}
                                                                                            className={`px-2 py-0.5 rounded text-xs font-mono transition-colors ${
                                                                                                deltaTypeFilter === 'ALL'
                                                                                                    ? 'bg-selected text-ink border border-line-strong font-semibold'
                                                                                                    : 'bg-subtle text-ink-muted hover:text-ink border border-transparent'
                                                                                            }`}
                                                                                        >
                                                                                            ALL ({totalDelta.toLocaleString()})
                                                                                        </button>
                                                                                        {summaryEntries.map(([ct, cnt]) => (
                                                                                            <button
                                                                                                key={ct}
                                                                                                type="button"
                                                                                                onClick={() => setDeltaTypeFilter(ct)}
                                                                                                className={`px-2 py-0.5 rounded text-xs font-mono transition-colors ${
                                                                                                    deltaTypeFilter === ct
                                                                                                        ? 'bg-selected text-ink border border-line-strong font-semibold'
                                                                                                        : 'bg-subtle text-ink-muted hover:text-ink border border-transparent'
                                                                                                }`}
                                                                                            >
                                                                                                {ct} ({Number(cnt).toLocaleString()})
                                                                                            </button>
                                                                                        ))}
                                                                                    </>
                                                                                )
                                                                            })()}
                                                                        </div>

                                                                        {deltaLoading ? (
                                                                            <p className="px-3 py-4 text-xs text-ink-muted">Loading activation delta rows…</p>
                                                                        ) : activationDeltaData?.rows && activationDeltaData.rows.length > 0 ? (
                                                                            <div className="overflow-x-auto max-h-72 overflow-y-auto">
                                                                                <table className="w-full text-left border-collapse text-xs">
                                                                                    <thead>
                                                                                        <tr className="bg-subtle/60 border-b border-line">
                                                                                            <th className="px-3 py-1.5 font-medium text-ink-muted">Status</th>
                                                                                            <th className="px-3 py-1.5 font-medium text-ink-muted">Change Type</th>
                                                                                            <th className="px-3 py-1.5 font-medium text-ink-muted">Resolved ID</th>
                                                                                            <th className="px-3 py-1.5 font-medium text-ink-muted">Lineage / Superseded By</th>
                                                                                            <th className="px-3 py-1.5 font-medium text-ink-muted text-right">Entities (+/-)</th>
                                                                                            <th className="px-3 py-1.5 font-medium text-ink-muted text-right">Profile</th>
                                                                                        </tr>
                                                                                    </thead>
                                                                                    <tbody className="divide-y divide-line">
                                                                                        {activationDeltaData.rows.map((drow) => {
                                                                                            let prevIds: string[] = []
                                                                                            try {
                                                                                                const parsed = JSON.parse(drow.previous_resolved_ids_json || '[]')
                                                                                                if (Array.isArray(parsed)) prevIds = parsed.map(String)
                                                                                            } catch {
                                                                                                prevIds = []
                                                                                            }
                                                                                            const isRetired = drow.activation_status === 'RETIRED'
                                                                                            return (
                                                                                                <tr key={`${drow.lens_id}:${drow.resolved_id}`} className="hover:bg-subtle/50">
                                                                                                    <td className="px-3 py-1.5 whitespace-nowrap">
                                                                                                        <span className={isRetired ? 'badge-danger font-mono' : 'badge-success font-mono'}>
                                                                                                            {drow.activation_status}
                                                                                                        </span>
                                                                                                    </td>
                                                                                                    <td className="px-3 py-1.5 whitespace-nowrap font-mono text-ink">
                                                                                                        {drow.change_type}
                                                                                                    </td>
                                                                                                    <td className="px-3 py-1.5 whitespace-nowrap font-mono">
                                                                                                        {isRetired ? (
                                                                                                            <span className="text-ink-muted line-through">
                                                                                                                {drow.resolved_id}
                                                                                                            </span>
                                                                                                        ) : (
                                                                                                            <button
                                                                                                                type="button"
                                                                                                                onClick={() => navigate(`/explorer?cluster=${encodeURIComponent(drow.resolved_id)}`)}
                                                                                                                className="text-accent hover:underline font-medium"
                                                                                                            >
                                                                                                                {drow.resolved_id}
                                                                                                            </button>
                                                                                                        )}
                                                                                                    </td>
                                                                                                    <td className="px-3 py-1.5 font-mono text-ink-secondary">
                                                                                                        {drow.superseded_by_resolved_id ? (
                                                                                                            <button
                                                                                                                type="button"
                                                                                                                onClick={() => navigate(`/explorer?cluster=${encodeURIComponent(drow.superseded_by_resolved_id!)}`)}
                                                                                                                className="text-accent hover:underline"
                                                                                                            >
                                                                                                                → {drow.superseded_by_resolved_id}
                                                                                                            </button>
                                                                                                        ) : prevIds.length > 0 ? (
                                                                                                            <span>← {prevIds.slice(0, 3).join(', ')}{prevIds.length > 3 ? ` (+${prevIds.length - 3})` : ''}</span>
                                                                                                        ) : (
                                                                                                            <span className="text-ink-muted">—</span>
                                                                                                        )}
                                                                                                    </td>
                                                                                                    <td className="px-3 py-1.5 text-right font-mono tabular-nums text-ink-secondary whitespace-nowrap">
                                                                                                        +{drow.entities_added} / -{drow.entities_removed} ({drow.cluster_size})
                                                                                                    </td>
                                                                                                    <td className="px-3 py-1.5 text-right font-mono text-ink-muted whitespace-nowrap">
                                                                                                        {drow.profile_changed ? 'Updated' : '—'}
                                                                                                    </td>
                                                                                                </tr>
                                                                                            )
                                                                                        })}
                                                                                    </tbody>
                                                                                </table>
                                                                            </div>
                                                                        ) : (
                                                                            <p className="px-3 py-4 text-xs text-ink-muted">No rows match the selected change type.</p>
                                                                        )}
                                                                    </div>
                                                                )}
                                                            </div>
                                                        ) : (
                                                            <p className="text-xs text-ink-muted">No stage details available.</p>
                                                        )}
                                                    </td>
                                                </tr>
                                            )}
                                        </Fragment>
                                    )
                                })
                            )}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    )
}
