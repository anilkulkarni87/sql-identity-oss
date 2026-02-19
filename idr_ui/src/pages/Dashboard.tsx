import { useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { formatDistanceToNow } from 'date-fns'
import {
    BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
    PieChart, Pie, Cell
} from 'recharts'
import { Users, GitBranch, Activity, Clock, AlertTriangle, Play } from 'lucide-react'
import { api } from '../api/client'

export default function Dashboard() {
    const navigate = useNavigate();

    // Check setup status
    const { data: setupStatus } = useQuery({
        queryKey: ['setupStatus'],
        queryFn: () => api.getSetupStatus()
    });

    useEffect(() => {
        if (setupStatus && !setupStatus.configured) {
            navigate('/setup');
        }
    }, [setupStatus, navigate]);

    const { data: metrics, isLoading: metricsLoading } = useQuery({
        queryKey: ['metrics'],
        queryFn: () => api.getMetricsSummary(),
        enabled: !!setupStatus?.configured // Only fetch metrics if configured
    })

    const { data: distribution } = useQuery({
        queryKey: ['distribution'],
        queryFn: () => api.getClusterDistribution()
    })

    const { data: rules } = useQuery({
        queryKey: ['rules'],
        queryFn: () => api.getRuleStats()
    })

    const { data: alerts } = useQuery({
        queryKey: ['alerts'],
        queryFn: () => api.getAlerts()
    })

    const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6']

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between">
                <h1 className="text-2xl font-bold">Match Quality Dashboard</h1>
                <button
                    onClick={() => navigate('/setup')}
                    className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
                >
                    <Play className="w-4 h-4" />
                    Run Pipeline
                </button>
            </div>

            {/* Metric Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                <MetricCard
                    title="Total Clusters"
                    value={metrics?.total_clusters?.toLocaleString() || '—'}
                    icon={<Users className="w-6 h-6" />}
                    loading={metricsLoading}
                />
                <MetricCard
                    title="Total Edges"
                    value={metrics?.total_edges?.toLocaleString() || '—'}
                    icon={<GitBranch className="w-6 h-6" />}
                    loading={metricsLoading}
                />
                <MetricCard
                    title="Avg Confidence"
                    value={metrics?.avg_confidence ? `${(metrics.avg_confidence * 100).toFixed(1)}%` : '—'}
                    icon={<Activity className="w-6 h-6" />}
                    loading={metricsLoading}
                />
                <MetricCard
                    title="Last Run"
                    value={metrics?.last_run_started_at ? formatDistanceToNow(new Date(metrics.last_run_started_at), { addSuffix: true }) : (metrics?.last_run_duration ? `${metrics.last_run_duration}s` : '—')}
                    icon={<Clock className="w-6 h-6" />}
                    loading={metricsLoading}
                />
            </div>

            {/* Charts Row */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Cluster Distribution */}
                <div className="bg-gray-800 rounded-xl p-6">
                    <h3 className="text-lg font-semibold mb-4">Cluster Size Distribution</h3>
                    <ResponsiveContainer width="100%" height={250}>
                        <BarChart data={distribution || []}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                            <XAxis dataKey="bucket" stroke="#9ca3af" />
                            <YAxis
                                stroke="#9ca3af"
                                width={60}
                                tickFormatter={(value: number) => {
                                    if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`
                                    if (value >= 1000) return `${(value / 1000).toFixed(1)}k`
                                    return value.toString()
                                }}
                            />
                            <Tooltip
                                contentStyle={{ backgroundColor: '#1f2937', border: 'none' }}
                                labelStyle={{ color: '#fff' }}
                                formatter={(value: number) => [value.toLocaleString(), 'Count']}
                            />
                            <Bar dataKey="count" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                        </BarChart>
                    </ResponsiveContainer>
                </div>

                {/* Rule Breakdown */}
                <div className="bg-gray-800 rounded-xl p-6">
                    <h3 className="text-lg font-semibold mb-4">Match Rule Breakdown</h3>
                    <div className="flex items-center gap-8">
                        <ResponsiveContainer width="50%" height={200}>
                            <PieChart>
                                <Pie
                                    data={rules || []}
                                    dataKey="edges_created"
                                    nameKey="rule_id"
                                    cx="50%"
                                    cy="50%"
                                    outerRadius={80}
                                    label={({ rule_id }) => rule_id}
                                >
                                    {(rules || []).map((_, index) => (
                                        <Cell key={index} fill={COLORS[index % COLORS.length]} />
                                    ))}
                                </Pie>
                                <Tooltip />
                            </PieChart>
                        </ResponsiveContainer>
                        <div className="flex-1 space-y-2">
                            {(rules || []).map((rule, i) => (
                                <div key={rule.rule_id} className="flex items-center justify-between">
                                    <div className="flex items-center gap-2">
                                        <div
                                            className="w-3 h-3 rounded-full"
                                            style={{ backgroundColor: COLORS[i % COLORS.length] }}
                                        />
                                        <span className="text-sm">{rule.identifier_type || rule.rule_id}</span>
                                    </div>
                                    <span className="text-sm text-gray-400">{rule.percentage}%</span>
                                </div>
                            ))}
                        </div>
                    </div>
                </div>
            </div>

            {/* Alerts Panel */}
            {alerts && alerts.length > 0 && (
                <div className="bg-yellow-900/30 border border-yellow-700 rounded-xl p-4">
                    <div className="flex items-center gap-2 mb-3">
                        <AlertTriangle className="w-5 h-5 text-yellow-400" />
                        <h3 className="font-semibold text-yellow-400">Warnings ({alerts.length})</h3>
                    </div>
                    <ul className="space-y-2">
                        {alerts.map((alert, i) => (
                            <li key={i} className="text-sm text-yellow-200">
                                • {alert.message} {alert.count && `(${alert.count})`}
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
        <div className="bg-gray-800 rounded-xl p-6">
            <div className="flex items-center justify-between">
                <div>
                    <p className="text-sm text-gray-400">{title}</p>
                    <p className="text-2xl font-bold mt-1">
                        {loading ? (
                            <span className="animate-pulse bg-gray-700 rounded w-20 h-8 block" />
                        ) : value}
                    </p>
                </div>
                <div className="p-3 bg-blue-600/20 rounded-lg text-blue-400">
                    {icon}
                </div>
            </div>
        </div>
    )
}
