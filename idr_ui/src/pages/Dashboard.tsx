import { useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { formatDistanceToNow } from 'date-fns'
import {
    BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
    PieChart, Pie, Cell
} from 'recharts'
import { Users, GitBranch, Activity, Clock, AlertTriangle, Play, Network, Sparkles, ArrowRight } from 'lucide-react'
import { api } from '../api/client'

export default function Dashboard() {
    const navigate = useNavigate()

    // Check setup status
    const { data: setupStatus } = useQuery({
        queryKey: ['setupStatus'],
        queryFn: () => api.getSetupStatus()
    })

    const isConfigured = Boolean(setupStatus?.configured)

    const { data: metrics, isLoading: metricsLoading } = useQuery({
        queryKey: ['metrics'],
        queryFn: () => api.getMetricsSummary(),
        enabled: isConfigured
    })

    const { data: distribution } = useQuery({
        queryKey: ['distribution'],
        queryFn: () => api.getClusterDistribution(),
        enabled: isConfigured
    })

    const { data: rules } = useQuery({
        queryKey: ['rules'],
        queryFn: () => api.getRuleStats(),
        enabled: isConfigured
    })

    const { data: alerts } = useQuery({
        queryKey: ['alerts'],
        queryFn: () => api.getAlerts(),
        enabled: isConfigured
    })

    const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4']

    return (
        <div className="space-y-6">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                <div>
                    <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
                        <Activity className="w-6 h-6 text-blue-400" />
                        Match Quality Dashboard
                    </h1>
                    <p className="text-sm text-gray-400 mt-1">
                        High-level identity resolution telemetry, cluster distribution, and rule metrics.
                    </p>
                </div>

                <div className="flex items-center gap-3">
                    <button
                        onClick={() => navigate('/explorer')}
                        className="flex items-center gap-2 bg-gray-800 hover:bg-gray-700 text-gray-200 border border-gray-700 px-4 py-2 rounded-xl text-sm font-medium transition-all shadow-sm"
                    >
                        <Network className="w-4 h-4 text-blue-400" />
                        Explore Graph
                    </button>
                    <button
                        onClick={() => navigate('/setup')}
                        className="flex items-center gap-2 bg-blue-600 hover:bg-blue-500 text-white px-4 py-2 rounded-xl text-sm font-medium transition-all shadow-md"
                    >
                        <Play className="w-4 h-4" />
                        {isConfigured ? 'Run Pipeline' : 'Setup Pipeline'}
                    </button>
                </div>
            </div>

            {/* Setup Callout Banner if not yet configured */}
            {setupStatus && !setupStatus.configured && (
                <div className="p-5 bg-gradient-to-r from-blue-950/60 to-purple-950/60 border border-blue-500/40 rounded-2xl shadow-xl flex flex-col md:flex-row items-start md:items-center justify-between gap-4">
                    <div className="space-y-1">
                        <div className="flex items-center gap-2 text-blue-300 font-semibold text-sm">
                            <Sparkles className="w-4 h-4" />
                            Configuration Required
                        </div>
                        <p className="text-xs text-gray-300 max-w-2xl">
                            Connect your warehouse (Databricks, Snowflake, BigQuery, Postgres), map your identity tables, and run your first identity resolution pipeline.
                        </p>
                    </div>
                    <button
                        onClick={() => navigate('/setup')}
                        className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white text-xs font-semibold rounded-xl shadow-lg transition-all flex-shrink-0"
                    >
                        Launch Setup Wizard
                        <ArrowRight className="w-3.5 h-3.5" />
                    </button>
                </div>
            )}

            {/* Metric Cards */}
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                <MetricCard
                    title="Total Clusters"
                    value={metrics?.total_clusters ? metrics.total_clusters.toLocaleString() : '—'}
                    icon={<Users className="w-5 h-5" />}
                    loading={metricsLoading}
                />
                <MetricCard
                    title="Total Edges"
                    value={metrics?.total_edges ? metrics.total_edges.toLocaleString() : '—'}
                    icon={<GitBranch className="w-5 h-5" />}
                    loading={metricsLoading}
                />
                <MetricCard
                    title="Avg Confidence"
                    value={metrics?.avg_confidence ? `${(metrics.avg_confidence * 100).toFixed(1)}%` : '—'}
                    icon={<Activity className="w-5 h-5" />}
                    loading={metricsLoading}
                />
                <MetricCard
                    title="Last Run"
                    value={
                        metrics?.last_run_started_at
                            ? formatDistanceToNow(new Date(metrics.last_run_started_at), { addSuffix: true })
                            : metrics?.last_run_duration
                            ? `${metrics.last_run_duration}s`
                            : '—'
                    }
                    icon={<Clock className="w-5 h-5" />}
                    loading={metricsLoading}
                />
            </div>

            {/* Charts Row */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Cluster Distribution */}
                <div className="bg-gray-800/90 rounded-xl p-6 border border-gray-700/80 shadow-xl">
                    <h3 className="text-base font-semibold text-white mb-4">Cluster Size Distribution</h3>
                    <ResponsiveContainer width="100%" height={250}>
                        <BarChart data={distribution || []}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                            <XAxis dataKey="bucket" stroke="#9ca3af" fontSize={11} />
                            <YAxis
                                stroke="#9ca3af"
                                fontSize={11}
                                width={60}
                                tickFormatter={(value: number) => {
                                    if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`
                                    if (value >= 1000) return `${(value / 1000).toFixed(1)}k`
                                    return value.toString()
                                }}
                            />
                            <Tooltip
                                contentStyle={{ backgroundColor: '#111827', border: '1px solid #374151', borderRadius: '8px' }}
                                labelStyle={{ color: '#fff', fontSize: '12px' }}
                                formatter={(value: number) => [value.toLocaleString(), 'Count']}
                            />
                            <Bar dataKey="count" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                        </BarChart>
                    </ResponsiveContainer>
                </div>

                {/* Rule Breakdown */}
                <div className="bg-gray-800/90 rounded-xl p-6 border border-gray-700/80 shadow-xl">
                    <h3 className="text-base font-semibold text-white mb-4">Match Rule Breakdown</h3>
                    <div className="flex flex-col sm:flex-row items-center gap-6">
                        <ResponsiveContainer width="100%" height={220} className="sm:w-1/2">
                            <PieChart>
                                <Pie
                                    data={rules || []}
                                    dataKey="edges_created"
                                    nameKey="rule_id"
                                    cx="50%"
                                    cy="50%"
                                    outerRadius={75}
                                >
                                    {(rules || []).map((_, index) => (
                                        <Cell key={index} fill={COLORS[index % COLORS.length]} />
                                    ))}
                                </Pie>
                                <Tooltip
                                    contentStyle={{ backgroundColor: '#111827', border: '1px solid #374151', borderRadius: '8px' }}
                                    formatter={(val: number) => [val.toLocaleString(), 'Edges Created']}
                                />
                            </PieChart>
                        </ResponsiveContainer>
                        <div className="flex-1 w-full space-y-2 max-h-56 overflow-y-auto pr-1">
                            {(rules || []).map((rule, i) => (
                                <div key={rule.rule_id} className="flex items-center justify-between p-2 rounded-lg bg-gray-900/50 border border-gray-700/50 text-xs">
                                    <div className="flex items-center gap-2">
                                        <div
                                            className="w-2.5 h-2.5 rounded-full flex-shrink-0"
                                            style={{ backgroundColor: COLORS[i % COLORS.length] }}
                                        />
                                        <span className="font-medium text-gray-200 truncate">{rule.identifier_type || rule.rule_id}</span>
                                    </div>
                                    <span className="font-mono text-gray-400 font-semibold">{rule.percentage}%</span>
                                </div>
                            ))}
                        </div>
                    </div>
                </div>
            </div>

            {/* Alerts Panel */}
            {alerts && alerts.length > 0 && (
                <div className="bg-amber-950/30 border border-amber-800/60 rounded-xl p-4 shadow-lg space-y-2">
                    <div className="flex items-center gap-2">
                        <AlertTriangle className="w-5 h-5 text-amber-400" />
                        <h3 className="font-semibold text-amber-300 text-sm">System Alerts ({alerts.length})</h3>
                    </div>
                    <ul className="space-y-1.5 pl-2">
                        {alerts.map((alert, i) => (
                            <li key={i} className="text-xs text-amber-200/90 list-disc list-inside">
                                {alert.message} {alert.count && `(${alert.count})`}
                            </li>
                        ))}
                    </ul>
                </div>
            )}
        </div>
    )
}

function MetricCard({
    title,
    value,
    icon,
    loading
}: {
    title: string
    value: string
    icon: React.ReactNode
    loading?: boolean
}) {
    return (
        <div className="bg-gray-800/90 rounded-xl p-5 border border-gray-700/80 shadow-md hover:border-gray-600 transition-all">
            <div className="flex items-center justify-between">
                <div>
                    <p className="text-xs font-medium text-gray-400">{title}</p>
                    <p className="text-2xl font-bold tracking-tight text-white mt-1">
                        {loading ? (
                            <span className="animate-pulse bg-gray-700 rounded w-20 h-7 block" />
                        ) : (
                            value
                        )}
                    </p>
                </div>
                <div className="p-3 bg-blue-600/15 border border-blue-500/20 rounded-xl text-blue-400">
                    {icon}
                </div>
            </div>
        </div>
    )
}
