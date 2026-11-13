import { useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { formatDistanceToNow } from 'date-fns'
import {
    BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
    PieChart, Pie, Cell, AreaChart, Area
} from 'recharts'
import { Users, GitBranch, Activity, Clock, AlertTriangle, Play, Network, Sparkles, ArrowRight, ShieldCheck, Layers } from 'lucide-react'
import { api } from '../api/client'
import { formatSourceName } from '../utils/colors'

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

    const { data: confidenceDist } = useQuery({
        queryKey: ['confidenceDist'],
        queryFn: () => api.getConfidenceDistribution(),
        enabled: isConfigured
    })

    const { data: sourceContrib } = useQuery({
        queryKey: ['sourceContrib'],
        queryFn: () => api.getSourceContribution(),
        enabled: isConfigured
    })

    const { data: recentRuns } = useQuery({
        queryKey: ['dashboardRuns'],
        queryFn: () => api.getRuns(10),
        enabled: isConfigured
    })

    const runTrendData = (recentRuns || [])
        .slice()
        .reverse()
        .map((r, idx) => ({
            label: r.started_at ? new Date(r.started_at).toLocaleDateString(undefined, { month: 'short', day: 'numeric' }) : `#${idx + 1}`,
            duration: r.duration_seconds ?? 0,
            entities: r.entities_processed ?? 0,
            mode: r.run_mode,
        }))

    const sourceChartData = (sourceContrib || []).map((s) => ({
        ...s,
        label: formatSourceName(s.source_id),
    }))

    const formatCompact = (value: number) => {
        if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`
        if (value >= 1000) return `${(value / 1000).toFixed(1)}k`
        return value.toString()
    }

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
                    onClick={() => navigate('/explorer')}
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
                    onClick={() => navigate('/runs')}
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
                        <div className="w-full sm:w-48 h-[220px] shrink-0">
                            <ResponsiveContainer width="100%" height="100%">
                                <PieChart>
                                    <Pie
                                        data={rules || []}
                                        dataKey="edges_created"
                                        nameKey="rule_id"
                                        cx="50%"
                                        cy="50%"
                                        innerRadius={42}
                                        outerRadius={78}
                                        paddingAngle={2}
                                    >
                                        {(rules || []).map((_, index) => (
                                            <Cell key={index} fill={COLORS[index % COLORS.length]} />
                                        ))}
                                    </Pie>
                                    <Tooltip
                                        contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '8px' }}
                                        formatter={(val: number) => [val.toLocaleString(), 'Edges Created']}
                                    />
                                </PieChart>
                            </ResponsiveContainer>
                        </div>
                        <div className="flex-1 w-full min-w-0 space-y-2 max-h-56 overflow-y-auto pr-1">
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

            {/* Row 2: Quality, Source Coverage & Run Trend */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Multi-Entity Confidence Tiers */}
                <div className="bg-slate-900/90 rounded-xl p-6 border border-slate-800 shadow-xl">
                    <div className="flex items-center justify-between mb-1">
                        <h3 className="text-base font-semibold text-white flex items-center gap-2">
                            <ShieldCheck className="w-4 h-4 text-emerald-400" />
                            Multi-Entity Confidence
                        </h3>
                    </div>
                    <p className="text-xs text-slate-400 mb-4">
                        Excludes singletons so risky or over-merged clusters stand out.
                    </p>
                    <ResponsiveContainer width="100%" height={220}>
                        <BarChart data={confidenceDist || []} margin={{ left: -10, right: 8 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                            <XAxis
                                dataKey="bucket"
                                stroke="#94a3b8"
                                fontSize={10}
                                tickFormatter={(v: string) => v.split(' (')[0]}
                            />
                            <YAxis stroke="#94a3b8" fontSize={11} width={50} tickFormatter={formatCompact} />
                            <Tooltip
                                contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '8px' }}
                                labelStyle={{ color: '#fff', fontSize: '12px' }}
                                formatter={(value: number) => [value.toLocaleString(), 'Clusters']}
                            />
                            <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                                {(confidenceDist || []).map((entry, idx) => (
                                    <Cell key={idx} fill={entry.color || '#3b82f6'} />
                                ))}
                            </Bar>
                        </BarChart>
                    </ResponsiveContainer>
                </div>

                {/* Source System Contribution */}
                <div className="bg-slate-900/90 rounded-xl p-6 border border-slate-800 shadow-xl">
                    <h3 className="text-base font-semibold text-white flex items-center gap-2 mb-1">
                        <Layers className="w-4 h-4 text-blue-400" />
                        Source System Coverage
                    </h3>
                    <p className="text-xs text-slate-400 mb-4">
                        Entities contributed by each connected source table.
                    </p>
                    <ResponsiveContainer width="100%" height={220}>
                        <BarChart data={sourceChartData} layout="vertical" margin={{ left: 10, right: 12 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                            <XAxis type="number" stroke="#94a3b8" fontSize={11} tickFormatter={formatCompact} />
                            <YAxis type="category" dataKey="label" stroke="#94a3b8" fontSize={10} width={96} />
                            <Tooltip
                                contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '8px' }}
                                labelStyle={{ color: '#fff', fontSize: '12px' }}
                                formatter={(value: number) => [value.toLocaleString(), 'Entities']}
                            />
                            <Bar dataKey="entities" fill="#3b82f6" radius={[0, 4, 4, 0]} />
                        </BarChart>
                    </ResponsiveContainer>
                </div>

                {/* Run Duration Trend */}
                <div className="bg-slate-900/90 rounded-xl p-6 border border-slate-800 shadow-xl">
                    <div className="flex items-center justify-between mb-1">
                        <h3 className="text-base font-semibold text-white flex items-center gap-2">
                            <Clock className="w-4 h-4 text-purple-400" />
                            Pipeline Latency Trend
                        </h3>
                        <button
                            onClick={() => navigate('/runs')}
                            className="text-xs text-blue-400 hover:text-blue-300 transition-colors"
                        >
                            View all →
                        </button>
                    </div>
                    <p className="text-xs text-slate-400 mb-4">
                        Execution time (seconds) across recent pipeline runs.
                    </p>
                    <ResponsiveContainer width="100%" height={220}>
                        <AreaChart data={runTrendData} margin={{ left: -10, right: 8 }}>
                            <defs>
                                <linearGradient id="durationGrad" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.4} />
                                    <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0.0} />
                                </linearGradient>
                            </defs>
                            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                            <XAxis dataKey="label" stroke="#94a3b8" fontSize={10} />
                            <YAxis stroke="#94a3b8" fontSize={11} width={44} tickFormatter={(v: number) => `${v}s`} />
                            <Tooltip
                                contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '8px' }}
                                labelStyle={{ color: '#fff', fontSize: '12px' }}
                                formatter={(value: number) => [`${value}s`, 'Duration']}
                            />
                            <Area
                                type="monotone"
                                dataKey="duration"
                                stroke="#8b5cf6"
                                strokeWidth={2}
                                fillOpacity={1}
                                fill="url(#durationGrad)"
                            />
                        </AreaChart>
                    </ResponsiveContainer>
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
    loading,
    onClick
}: {
    title: string
    value: string
    icon: React.ReactNode
    loading?: boolean
    onClick?: () => void
}) {
    return (
        <div
            onClick={onClick}
            className={`bg-slate-900/90 rounded-xl p-5 border border-slate-800 shadow-md hover:border-slate-700 transition-all ${
                onClick ? 'cursor-pointer hover:bg-slate-900' : ''
            }`}
        >
            <div className="flex items-center justify-between">
                <div>
                    <p className="text-xs font-medium text-slate-400">{title}</p>
                    <p className="text-2xl font-bold tracking-tight tabular-nums text-white mt-1">
                        {loading ? (
                            <span className="animate-pulse bg-slate-800 rounded w-20 h-7 block" />
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
