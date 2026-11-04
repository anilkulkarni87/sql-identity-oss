import { Fragment, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { CheckCircle, XCircle, Clock, AlertCircle, ChevronDown, ChevronRight, AlertTriangle } from 'lucide-react'
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
                return <CheckCircle className="w-5 h-5 text-green-400" />
            case 'failed':
            case 'error':
                return <XCircle className="w-5 h-5 text-red-400" />
            case 'running':
                return <Clock className="w-5 h-5 text-blue-400 animate-spin" />
            default:
                return <AlertCircle className="w-5 h-5 text-yellow-400" />
        }
    }

    const formatDate = (dateStr: string) => {
        const date = new Date(dateStr)
        return date.toLocaleString()
    }

    const toggleRun = (runId: string) => {
        setSelectedRunId(prev => (prev === runId ? null : runId))
    }

    return (
        <div className="space-y-6">
            <div>
                <h1 className="text-2xl font-bold">Run History</h1>
                <p className="text-sm text-gray-400 mt-1">Click any run row to inspect stage-by-stage execution metrics, LP iterations, and diagnostics.</p>
            </div>

            <div className="bg-gray-800 rounded-xl overflow-hidden">
                <table className="w-full">
                    <thead className="bg-gray-700">
                        <tr>
                            <th className="px-4 py-3 text-left text-sm font-medium text-gray-300 w-10"></th>
                            <th className="px-4 py-3 text-left text-sm font-medium text-gray-300">Status</th>
                            <th className="px-4 py-3 text-left text-sm font-medium text-gray-300">Run ID</th>
                            <th className="px-4 py-3 text-left text-sm font-medium text-gray-300">Mode</th>
                            <th className="px-4 py-3 text-left text-sm font-medium text-gray-300">Started</th>
                            <th className="px-4 py-3 text-right text-sm font-medium text-gray-300">Duration</th>
                            <th className="px-4 py-3 text-right text-sm font-medium text-gray-300">Entities</th>
                            <th className="px-4 py-3 text-right text-sm font-medium text-gray-300">Edges</th>
                            <th className="px-4 py-3 text-right text-sm font-medium text-gray-300">Clusters</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-700">
                        {isLoading ? (
                            <tr>
                                <td colSpan={9} className="px-4 py-8 text-center text-gray-400">
                                    Loading...
                                </td>
                            </tr>
                        ) : runs?.length === 0 ? (
                            <tr>
                                <td colSpan={9} className="px-4 py-8 text-center text-gray-400">
                                    No runs found
                                </td>
                            </tr>
                        ) : (
                            runs?.map((run: RunSummary) => {
                                const isExpanded = selectedRunId === run.run_id
                                return (
                                    <Fragment key={run.run_id}>
                                        <tr
                                            onClick={() => toggleRun(run.run_id)}
                                            className={`hover:bg-gray-700/50 cursor-pointer transition-colors ${isExpanded ? 'bg-gray-700/40' : ''}`}
                                        >
                                            <td className="px-4 py-3 text-gray-400">
                                                {isExpanded ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
                                            </td>
                                            <td className="px-4 py-3">
                                                {getStatusIcon(run.status)}
                                            </td>
                                            <td className="px-4 py-3">
                                                <span className="font-mono text-sm">{run.run_id}</span>
                                            </td>
                                            <td className="px-4 py-3">
                                                <span className={`px-2 py-1 rounded text-xs ${run.run_mode === 'FULL'
                                                        ? 'bg-purple-600/30 text-purple-300'
                                                        : 'bg-cyan-600/30 text-cyan-300'
                                                    }`}>
                                                    {run.run_mode}
                                                </span>
                                            </td>
                                            <td className="px-4 py-3 text-sm text-gray-300">
                                                {formatDate(run.started_at)}
                                            </td>
                                            <td className="px-4 py-3 text-right text-sm">
                                                {run.duration_seconds !== null && run.duration_seconds !== undefined ? `${run.duration_seconds}s` : '—'}
                                            </td>
                                            <td className="px-4 py-3 text-right text-sm">
                                                {run.entities_processed.toLocaleString()}
                                            </td>
                                            <td className="px-4 py-3 text-right text-sm">
                                                {run.edges_created.toLocaleString()}
                                            </td>
                                            <td className="px-4 py-3 text-right text-sm">
                                                {run.clusters_impacted.toLocaleString()}
                                            </td>
                                        </tr>
                                        {isExpanded && (
                                            <tr className="bg-gray-900/60">
                                                <td colSpan={9} className="px-6 py-4 space-y-4">
                                                    {detailLoading ? (
                                                        <p className="text-sm text-gray-400">Loading stage telemetry...</p>
                                                    ) : runDetail ? (
                                                        <div className="space-y-4">
                                                            <div className="flex flex-wrap gap-4 text-xs text-gray-300">
                                                                <span className="px-2.5 py-1 bg-gray-800 rounded border border-gray-700">
                                                                    Status: <strong className="text-white">{runDetail.status}</strong>
                                                                </span>
                                                                {runDetail.lp_iterations !== null && runDetail.lp_iterations !== undefined && (
                                                                    <span className="px-2.5 py-1 bg-gray-800 rounded border border-gray-700">
                                                                        Label Propagation Iterations: <strong className="text-blue-400">{runDetail.lp_iterations}</strong>
                                                                    </span>
                                                                )}
                                                            </div>

                                                            {runDetail.error_message && (
                                                                <div className="p-3 bg-red-900/30 border border-red-700 rounded-lg text-xs text-red-300 font-mono">
                                                                    Error: {runDetail.error_message}
                                                                </div>
                                                            )}

                                                            {runDetail.warnings && runDetail.warnings.length > 0 && (
                                                                <div className="p-3 bg-amber-900/20 border border-amber-700/60 rounded-lg space-y-1">
                                                                    <div className="flex items-center gap-1.5 text-xs font-semibold text-amber-300">
                                                                        <AlertTriangle className="w-4 h-4" />
                                                                        Warnings ({runDetail.warnings.length})
                                                                    </div>
                                                                    <ul className="list-disc list-inside text-xs text-amber-200/90 space-y-0.5">
                                                                        {runDetail.warnings.map((w, i) => (
                                                                            <li key={i}>{w}</li>
                                                                        ))}
                                                                    </ul>
                                                                </div>
                                                            )}

                                                            {runDetail.stages && runDetail.stages.length > 0 && (
                                                                <div>
                                                                    <h4 className="text-xs font-semibold uppercase tracking-wider text-gray-400 mb-2">
                                                                        Stage Execution Breakdown
                                                                    </h4>
                                                                    <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
                                                                        {runDetail.stages.map((st) => (
                                                                            <div key={`${st.stage_order}-${st.stage_name}`} className="p-3 bg-gray-800 rounded-lg border border-gray-700 flex justify-between items-center">
                                                                                <div>
                                                                                    <span className="text-xs text-gray-400 mr-1.5">#{st.stage_order}</span>
                                                                                    <span className="text-sm font-medium text-gray-200">{st.stage_name}</span>
                                                                                    {st.rows_out !== null && st.rows_out !== undefined && (
                                                                                        <p className="text-xs text-gray-400 mt-0.5">
                                                                                            Rows: {st.rows_out.toLocaleString()}
                                                                                        </p>
                                                                                    )}
                                                                                </div>
                                                                                <span className="text-xs font-mono text-blue-300">
                                                                                    {st.duration_seconds !== null && st.duration_seconds !== undefined ? `${st.duration_seconds}s` : '—'}
                                                                                </span>
                                                                            </div>
                                                                        ))}
                                                                    </div>
                                                                </div>
                                                            )}
                                                        </div>
                                                    ) : (
                                                        <p className="text-sm text-gray-400">No stage details available.</p>
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
