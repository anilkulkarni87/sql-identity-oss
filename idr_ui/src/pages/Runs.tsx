import { useQuery } from '@tanstack/react-query'
import { CheckCircle, XCircle, Clock, AlertCircle } from 'lucide-react'
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
    const { data: runs, isLoading } = useQuery({
        queryKey: ['runs'],
        queryFn: () => api.getRuns()
    })

    const getStatusIcon = (status: string) => {
        switch (status.toLowerCase()) {
            case 'success':
            case 'completed':
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

    return (
        <div className="space-y-6">
            <h1 className="text-2xl font-bold">Run History</h1>

            <div className="bg-gray-800 rounded-xl overflow-hidden">
                <table className="w-full">
                    <thead className="bg-gray-700">
                        <tr>
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
                                <td colSpan={8} className="px-4 py-8 text-center text-gray-400">
                                    Loading...
                                </td>
                            </tr>
                        ) : runs?.length === 0 ? (
                            <tr>
                                <td colSpan={8} className="px-4 py-8 text-center text-gray-400">
                                    No runs found
                                </td>
                            </tr>
                        ) : (
                            runs?.map((run: RunSummary) => (
                                <tr key={run.run_id} className="hover:bg-gray-700/50">
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
                                        {run.duration_seconds ? `${run.duration_seconds}s` : '—'}
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
                            ))
                        )}
                    </tbody>
                </table>
            </div>
        </div>
    )
}
