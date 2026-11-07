import { Fragment, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import {
    CheckCircle, XCircle, Clock, AlertCircle, ChevronDown, ChevronRight,
    AlertTriangle, Network, ArrowRight, Activity, Layers, Database,
    GitMerge, Cpu, FileSpreadsheet, Sparkles
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

    const { data: runs, isLoading } = useQuery({
        queryKey: ['runs'],
        queryFn: () => api.getRuns()
    })

    const { data: runDetail, isLoading: detailLoading } = useQuery({
        queryKey: ['runDetail', selectedRunId],
        queryFn: () => api.getRunDetail(selectedRunId!),
        enabled: !!selectedRunId
    })

    const getStatusIcon = (status: string) => {
        switch (status.toLowerCase()) {
            case 'success':
            case 'completed':
            case 'completed_with_warnings':
                return <CheckCircle className="w-5 h-5 text-emerald-400" />
            case 'failed':
            case 'error':
                return <XCircle className="w-5 h-5 text-rose-400" />
            case 'running':
                return <Clock className="w-5 h-5 text-blue-400 animate-spin" />
            default:
                return <AlertCircle className="w-5 h-5 text-amber-400" />
        }
    }

    const formatDate = (dateStr: string) => {
        try {
            const date = new Date(dateStr)
            return date.toLocaleString()
        } catch {
            return dateStr
        }
    }

    const toggleRun = (runId: string) => {
        setSelectedRunId(prev => (prev === runId ? null : runId))
    }

    const getStageIcon = (stageName: string) => {
        const s = stageName.toLowerCase()
        if (s.includes('preflight')) return <Database className="w-4 h-4 text-gray-400" />
        if (s.includes('entity')) return <Layers className="w-4 h-4 text-cyan-400" />
        if (s.includes('identifier')) return <Cpu className="w-4 h-4 text-purple-400" />
        if (s.includes('attribute')) return <FileSpreadsheet className="w-4 h-4 text-amber-400" />
        if (s.includes('edge')) return <GitMerge className="w-4 h-4 text-blue-400" />
        if (s.includes('label') || s.includes('propagation')) return <Network className="w-4 h-4 text-emerald-400" />
        if (s.includes('fuzzy')) return <Sparkles className="w-4 h-4 text-pink-400" />
        return <Activity className="w-4 h-4 text-blue-400" />
    }

    return (
        <div className="space-y-6">
            <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                <div>
                    <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
                        <Activity className="w-6 h-6 text-blue-400" />
                        Run History
                    </h1>
                    <p className="text-sm text-gray-400 mt-1">
                        Inspect stage-by-stage pipeline executions, label propagation convergence, and cluster impact telemetry.
                    </p>
                </div>

                <button
                    onClick={() => navigate('/explorer')}
                    className="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white text-sm font-medium rounded-xl shadow-md transition-all self-start"
                >
                    <Network className="w-4 h-4" />
                    Open Graph Explorer
                    <ArrowRight className="w-4 h-4 ml-1" />
                </button>
            </div>

            <div className="bg-gray-800/90 rounded-xl overflow-hidden border border-gray-700 shadow-xl">
                <table className="w-full text-left border-collapse">
                    <thead className="bg-gray-900/80 border-b border-gray-700">
                        <tr>
                            <th className="px-4 py-3.5 text-xs font-semibold uppercase tracking-wider text-gray-400 w-10"></th>
                            <th className="px-4 py-3.5 text-xs font-semibold uppercase tracking-wider text-gray-400">Status</th>
                            <th className="px-4 py-3.5 text-xs font-semibold uppercase tracking-wider text-gray-400">Run ID</th>
                            <th className="px-4 py-3.5 text-xs font-semibold uppercase tracking-wider text-gray-400">Mode</th>
                            <th className="px-4 py-3.5 text-xs font-semibold uppercase tracking-wider text-gray-400">Started</th>
                            <th className="px-4 py-3.5 text-right text-xs font-semibold uppercase tracking-wider text-gray-400">Duration</th>
                            <th className="px-4 py-3.5 text-right text-xs font-semibold uppercase tracking-wider text-gray-400">Entities</th>
                            <th className="px-4 py-3.5 text-right text-xs font-semibold uppercase tracking-wider text-gray-400">Edges</th>
                            <th className="px-4 py-3.5 text-right text-xs font-semibold uppercase tracking-wider text-gray-400">Clusters</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-700/60">
                        {isLoading ? (
                            <tr>
                                <td colSpan={9} className="px-4 py-12 text-center text-gray-400 text-sm">
                                    Loading run telemetry...
                                </td>
                            </tr>
                        ) : runs?.length === 0 ? (
                            <tr>
                                <td colSpan={9} className="px-4 py-12 text-center text-gray-400 text-sm">
                                    No pipeline runs found.
                                </td>
                            </tr>
                        ) : (
                            runs?.map((run: RunSummary) => {
                                const isExpanded = selectedRunId === run.run_id
                                return (
                                    <Fragment key={run.run_id}>
                                        <tr
                                            onClick={() => toggleRun(run.run_id)}
                                            className={`hover:bg-gray-700/40 cursor-pointer transition-colors ${
                                                isExpanded ? 'bg-gray-700/30' : ''
                                            }`}
                                        >
                                            <td className="px-4 py-3.5 text-gray-400">
                                                {isExpanded ? (
                                                    <ChevronDown className="w-4 h-4 text-blue-400" />
                                                ) : (
                                                    <ChevronRight className="w-4 h-4" />
                                                )}
                                            </td>
                                            <td className="px-4 py-3.5">
                                                <div className="flex items-center gap-2">
                                                    {getStatusIcon(run.status)}
                                                    <span className="text-xs font-medium capitalize text-gray-300">
                                                        {run.status.toLowerCase().replace('_', ' ')}
                                                    </span>
                                                </div>
                                            </td>
                                            <td className="px-4 py-3.5">
                                                <span className="font-mono text-xs font-semibold text-white">
                                                    {run.run_id}
                                                </span>
                                            </td>
                                            <td className="px-4 py-3.5">
                                                <span
                                                    className={`px-2 py-0.5 rounded text-xs font-medium border ${
                                                        run.run_mode === 'FULL'
                                                            ? 'bg-purple-900/40 text-purple-300 border-purple-700/60'
                                                            : 'bg-cyan-900/40 text-cyan-300 border-cyan-700/60'
                                                    }`}
                                                >
                                                    {run.run_mode}
                                                </span>
                                            </td>
                                            <td className="px-4 py-3.5 text-xs text-gray-300">
                                                {formatDate(run.started_at)}
                                            </td>
                                            <td className="px-4 py-3.5 text-right font-mono text-xs text-gray-200">
                                                {run.duration_seconds !== null && run.duration_seconds !== undefined
                                                    ? `${run.duration_seconds}s`
                                                    : '—'}
                                            </td>
                                            <td className="px-4 py-3.5 text-right font-mono text-xs text-gray-200">
                                                {run.entities_processed.toLocaleString()}
                                            </td>
                                            <td className="px-4 py-3.5 text-right font-mono text-xs text-gray-200">
                                                {run.edges_created.toLocaleString()}
                                            </td>
                                            <td className="px-4 py-3.5 text-right font-mono text-xs text-emerald-400 font-semibold">
                                                {run.clusters_impacted.toLocaleString()}
                                            </td>
                                        </tr>

                                        {isExpanded && (
                                            <tr className="bg-gray-950/60">
                                                <td colSpan={9} className="px-6 py-5 space-y-5 border-t border-b border-gray-700/80">
                                                    {detailLoading ? (
                                                        <p className="text-xs text-gray-400">Loading stage breakdown...</p>
                                                    ) : runDetail ? (
                                                        <div className="space-y-5">
                                                            {/* Action & Stats Header */}
                                                            <div className="flex flex-wrap items-center justify-between gap-4 p-4 bg-gray-900/80 rounded-xl border border-gray-800 shadow-inner">
                                                                <div className="flex flex-wrap items-center gap-3 text-xs">
                                                                    <span className="px-3 py-1 bg-gray-800 rounded-lg border border-gray-700 text-gray-300">
                                                                        Status: <strong className="text-white ml-1">{runDetail.status}</strong>
                                                                    </span>
                                                                    {runDetail.lp_iterations !== null && runDetail.lp_iterations !== undefined && (
                                                                        <span className="px-3 py-1 bg-gray-800 rounded-lg border border-gray-700 text-gray-300">
                                                                            Label Propagation:{' '}
                                                                            <strong className="text-blue-400 ml-1">
                                                                                {runDetail.lp_iterations} iterations
                                                                            </strong>
                                                                        </span>
                                                                    )}
                                                                </div>

                                                                <button
                                                                    onClick={() => navigate('/explorer')}
                                                                    className="flex items-center gap-2 px-3 py-1.5 bg-blue-600/20 hover:bg-blue-600/30 text-blue-300 border border-blue-500/40 rounded-lg text-xs font-medium transition-all"
                                                                >
                                                                    <Network className="w-3.5 h-3.5" />
                                                                    Explore Generated Clusters
                                                                    <ArrowRight className="w-3.5 h-3.5" />
                                                                </button>
                                                            </div>

                                                            {/* Error Display */}
                                                            {runDetail.error_message && (
                                                                <div className="p-4 bg-rose-950/40 border border-rose-800/80 rounded-xl text-xs text-rose-300 font-mono space-y-1">
                                                                    <div className="font-bold flex items-center gap-1.5 text-rose-200">
                                                                        <XCircle className="w-4 h-4 text-rose-400" />
                                                                        Execution Error
                                                                    </div>
                                                                    <p>{runDetail.error_message}</p>
                                                                </div>
                                                            )}

                                                            {/* Warnings Display */}
                                                            {runDetail.warnings && runDetail.warnings.length > 0 && (
                                                                <div className="p-4 bg-amber-950/30 border border-amber-800/60 rounded-xl space-y-2">
                                                                    <div className="flex items-center gap-1.5 text-xs font-semibold text-amber-300">
                                                                        <AlertTriangle className="w-4 h-4" />
                                                                        Pipeline Warnings ({runDetail.warnings.length})
                                                                    </div>
                                                                    <ul className="list-disc list-inside text-xs text-amber-200/90 space-y-1 pl-1">
                                                                        {runDetail.warnings.map((w, i) => (
                                                                            <li key={i}>{w}</li>
                                                                        ))}
                                                                    </ul>
                                                                </div>
                                                            )}

                                                            {/* Stage Execution Breakdown */}
                                                            {runDetail.stages && runDetail.stages.length > 0 && (
                                                                <div className="space-y-3">
                                                                    <h4 className="text-xs font-semibold uppercase tracking-wider text-gray-400">
                                                                        Stage Execution Breakdown ({runDetail.stages.length} stages)
                                                                    </h4>
                                                                    <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-4 gap-3">
                                                                        {(() => {
                                                                            const maxDuration = Math.max(
                                                                                ...runDetail.stages.map(s => s.duration_seconds || 1)
                                                                            )

                                                                            return runDetail.stages.map(st => {
                                                                                const duration = st.duration_seconds || 0
                                                                                const percent = Math.min(
                                                                                    100,
                                                                                    Math.max(8, (duration / maxDuration) * 100)
                                                                                )

                                                                                return (
                                                                                    <div
                                                                                        key={`${st.stage_order}-${st.stage_name}`}
                                                                                        className="p-3 bg-gray-900/90 rounded-xl border border-gray-800 shadow-md space-y-2"
                                                                                    >
                                                                                        <div className="flex items-center justify-between">
                                                                                            <div className="flex items-center gap-1.5">
                                                                                                {getStageIcon(st.stage_name)}
                                                                                                <span className="text-xs font-semibold text-gray-200">
                                                                                                    {st.stage_name}
                                                                                                </span>
                                                                                            </div>
                                                                                            <span className="text-xs font-mono font-medium text-blue-400">
                                                                                                {duration}s
                                                                                            </span>
                                                                                        </div>

                                                                                        {/* Relative execution duration bar */}
                                                                                        <div className="w-full bg-gray-800 h-1.5 rounded-full overflow-hidden">
                                                                                            <div
                                                                                                className="h-full bg-blue-500 rounded-full transition-all duration-500"
                                                                                                style={{ width: `${percent}%` }}
                                                                                            />
                                                                                        </div>

                                                                                        <div className="flex justify-between items-center text-[11px] text-gray-400 font-mono pt-1">
                                                                                            <span>#{st.stage_order}</span>
                                                                                            <span>
                                                                                                {st.rows_out !== null && st.rows_out !== undefined
                                                                                                    ? `${st.rows_out.toLocaleString()} rows`
                                                                                                    : '0 rows'}
                                                                                            </span>
                                                                                        </div>
                                                                                    </div>
                                                                                )
                                                                            })
                                                                        })()}
                                                                    </div>
                                                                </div>
                                                            )}
                                                        </div>
                                                    ) : (
                                                        <p className="text-xs text-gray-400">No stage details available.</p>
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
    )
}
