import { useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { formatDistanceToNow } from 'date-fns'
import {
    BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
    PieChart, Pie, Cell, AreaChart, Area
} from 'recharts'
import { AlertTriangle, Play, Network, ArrowRight, RefreshCw } from 'lucide-react'
import { api } from '../api/client'
import { ENTITY_PALETTE, formatSourceName } from '../utils/colors'
import { useTheme } from '../utils/theme'
import { useRunDrawer } from '../components/RunPipelineDrawer'

export default function Dashboard() {
    const navigate = useNavigate()
    const { theme } = useTheme()
    const { openRunDrawer } = useRunDrawer()

    const { data: setupStatus } = useQuery({
        queryKey: ['setupStatus'],
        queryFn: () => api.getSetupStatus(),
    })

    const isConfigured = Boolean(setupStatus?.configured)

    const { data: metrics, isLoading: metricsLoading, isError: metricsError, refetch: refetchMetrics } = useQuery({
        queryKey: ['metrics'],
        queryFn: () => api.getMetricsSummary(),
        enabled: isConfigured,
    })

    const { data: distribution } = useQuery({
        queryKey: ['distribution'],
        queryFn: () => api.getClusterDistribution(),
        enabled: isConfigured,
    })

    const { data: rules } = useQuery({
        queryKey: ['rules'],
        queryFn: () => api.getRuleStats(),
        enabled: isConfigured,
    })

    const { data: alerts } = useQuery({
        queryKey: ['alerts'],
        queryFn: () => api.getAlerts(),
        enabled: isConfigured,
    })

    const { data: confidenceDist } = useQuery({
        queryKey: ['confidenceDist'],
        queryFn: () => api.getConfidenceDistribution(),
        enabled: isConfigured,
    })

    const { data: sourceContrib } = useQuery({
        queryKey: ['sourceContrib'],
        queryFn: () => api.getSourceContribution(),
        enabled: isConfigured,
    })

    const { data: recentRuns } = useQuery({
        queryKey: ['dashboardRuns'],
        queryFn: () => api.getRuns(10),
        enabled: isConfigured,
    })

    const formatDuration = (sec: number) =>
        sec < 0.05 ? '<0.1s' : sec < 10 ? `${Number(sec.toFixed(1))}s` : `${Math.round(sec)}s`

    const runTrendData = useMemo(
        () =>
            (recentRuns || [])
                .slice()
                .reverse()
                .map((r, idx) => ({
                    label: r.started_at
                        ? new Date(r.started_at).toLocaleString(undefined, {
                              month: 'short',
                              day: 'numeric',
                              hour: '2-digit',
                              minute: '2-digit',
                          })
                        : `#${idx + 1}`,
                    duration: Number((r.duration_seconds ?? 0).toFixed(2)),
                    entities: r.entities_processed ?? 0,
                    mode: r.run_mode,
                })),
        [recentRuns]
    )

    const sourceChartData = useMemo(
        () =>
            (sourceContrib || []).map((s) => ({
                ...s,
                label: formatSourceName(s.source_id),
            })),
        [sourceContrib]
    )

    const formatCompact = (value: number) => {
        if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`
        if (value >= 1000) return `${(value / 1000).toFixed(1)}k`
        return value.toString()
    }

    const chartTokens = useMemo(
        () =>
            theme === 'dark'
                ? {
                      grid: '#1e293b',
                      axis: '#94a3b8',
                      bar: '#60a5fa',
                      areaStroke: '#60a5fa',
                      tooltipBg: '#111620',
                      tooltipBorder: '#334155',
                      tooltipInk: '#f1f5f9',
                  }
                : {
                      grid: '#e2e8f0',
                      axis: '#64748b',
                      bar: '#1d4ed8',
                      areaStroke: '#1d4ed8',
                      tooltipBg: '#ffffff',
                      tooltipBorder: '#cbd5e1',
                      tooltipInk: '#0f172a',
                  },
        [theme]
    )

    const tooltipStyle = {
        backgroundColor: chartTokens.tooltipBg,
        border: `1px solid ${chartTokens.tooltipBorder}`,
        borderRadius: '6px',
        fontSize: '12px',
        color: chartTokens.tooltipInk,
        boxShadow: 'var(--shadow-popover)',
    }

    return (
        <div className="space-y-5">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                <div>
                    <h1 className="text-[18px] font-semibold tracking-[-0.015em] text-ink">
                        Match Quality Dashboard
                    </h1>
                    <p className="text-[13px] text-ink-muted mt-0.5">
                        Cluster distribution, match rule attribution, confidence tiers, and run latency.
                    </p>
                </div>

                <div className="flex items-center gap-2 shrink-0">
                    <button
                        type="button"
                        onClick={() => navigate('/explorer')}
                        className="btn-secondary"
                    >
                        <Network className="w-3.5 h-3.5 text-ink-muted" />
                        Explore Clusters
                    </button>
                    {!isConfigured && (
                        <button
                            type="button"
                            onClick={openRunDrawer}
                            className="btn-primary"
                        >
                            <Play className="w-3.5 h-3.5" />
                            Configure Pipeline
                        </button>
                    )}
                </div>
            </div>

            {setupStatus && !setupStatus.configured ? (
                <div className="surface-panel p-6 flex flex-col md:flex-row items-start md:items-center justify-between gap-4">
                    <div className="space-y-1 max-w-2xl">
                        <h2 className="text-[14px] font-semibold text-ink">
                            Connect a warehouse to start resolving identities
                        </h2>
                        <p className="text-[13px] text-ink-muted">
                            Configure your warehouse connection (DuckDB, Snowflake, BigQuery, Databricks, or Postgres),
                            map source tables and identifier rules, and execute your first resolution run.
                        </p>
                    </div>
                    <button
                        type="button"
                        onClick={() => navigate('/setup')}
                        className="btn-primary shrink-0"
                    >
                        Open Identity Studio
                        <ArrowRight className="w-3.5 h-3.5" />
                    </button>
                </div>
            ) : (
                <>
                    {metricsError && (
                        <div
                            role="alert"
                            className="p-3 rounded-md bg-status-danger-soft border border-status-danger-border flex items-center justify-between gap-3"
                        >
                            <span className="text-xs text-status-danger">
                                Unable to load dashboard metrics from the warehouse. Verify your connection and retry.
                            </span>
                            <button
                                type="button"
                                onClick={() => refetchMetrics()}
                                className="btn-secondary text-xs shrink-0"
                            >
                                <RefreshCw className="w-3 h-3" />
                                Retry
                            </button>
                        </div>
                    )}

                    {alerts && alerts.length > 0 && (
                        <div className="p-3 rounded-md bg-status-warning-soft border border-status-warning-border space-y-1.5">
                            <div className="flex items-center gap-1.5 text-xs font-semibold text-status-warning">
                                <AlertTriangle className="w-3.5 h-3.5" />
                                <span>System Alerts ({alerts.length})</span>
                            </div>
                            <ul className="space-y-1 pl-5 list-disc text-xs text-ink-secondary">
                                {alerts.map((alert, i) => (
                                    <li key={i}>
                                        {alert.message}{' '}
                                        {alert.count != null && (
                                            <span className="font-mono tabular-nums font-medium">
                                                ({alert.count.toLocaleString()})
                                            </span>
                                        )}
                                    </li>
                                ))}
                            </ul>
                        </div>
                    )}

                    <div className="surface-panel grid grid-cols-2 lg:grid-cols-4 divide-y lg:divide-y-0 sm:divide-x divide-line overflow-hidden">
                        <MetricCell
                            label="Resolved Clusters"
                            value={metrics?.total_clusters != null ? metrics.total_clusters.toLocaleString() : '—'}
                            sub={metrics?.total_entities != null ? `${metrics.total_entities.toLocaleString()} entities` : undefined}
                            loading={metricsLoading}
                            onClick={() => navigate('/explorer')}
                        />
                        <MetricCell
                            label="Match Edges"
                            value={metrics?.total_edges != null ? metrics.total_edges.toLocaleString() : '—'}
                            sub={rules ? `${rules.length} active rules` : undefined}
                            loading={metricsLoading}
                        />
                        <MetricCell
                            label="Avg Confidence"
                            value={metrics?.avg_confidence != null ? `${(metrics.avg_confidence * 100).toFixed(1)}%` : '—'}
                            sub="Multi-entity mean"
                            loading={metricsLoading}
                        />
                        <MetricCell
                            label="Last Run"
                            value={
                                metrics?.last_run_started_at
                                    ? formatDistanceToNow(new Date(metrics.last_run_started_at), { addSuffix: true })
                                    : metrics?.last_run_duration != null
                                    ? `${metrics.last_run_duration}s`
                                    : '—'
                            }
                            sub={
                                metrics?.last_run_duration != null
                                    ? `Duration ${formatDuration(metrics.last_run_duration)}`
                                    : undefined
                            }
                            loading={metricsLoading}
                            onClick={() => navigate('/runs')}
                        />
                    </div>

                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                        <section className="surface-panel p-4">
                            <div className="flex items-center justify-between mb-3">
                                <div>
                                    <h2 className="text-[13px] font-semibold text-ink">Cluster Size Distribution</h2>
                                    <p className="text-xs text-ink-muted">Entity count per resolved cluster bucket</p>
                                </div>
                            </div>
                            <ResponsiveContainer width="100%" height={220}>
                                <BarChart data={distribution || []} margin={{ top: 4, right: 8, left: -12, bottom: 0 }}>
                                    <CartesianGrid strokeDasharray="3 3" stroke={chartTokens.grid} vertical={false} />
                                    <XAxis dataKey="bucket" stroke={chartTokens.axis} fontSize={11} tickLine={false} />
                                    <YAxis
                                        stroke={chartTokens.axis}
                                        fontSize={11}
                                        tickLine={false}
                                        axisLine={false}
                                        width={56}
                                        tickFormatter={formatCompact}
                                    />
                                    <Tooltip
                                        contentStyle={tooltipStyle}
                                        formatter={(value: number) => [value.toLocaleString(), 'Clusters']}
                                    />
                                    <Bar dataKey="count" fill={chartTokens.bar} radius={[3, 3, 0, 0]} />
                                </BarChart>
                            </ResponsiveContainer>
                        </section>

                        <section className="surface-panel p-4">
                            <div className="mb-3">
                                <h2 className="text-[13px] font-semibold text-ink">Match Rule Attribution</h2>
                                <p className="text-xs text-ink-muted">Share of edges created by each deterministic or fuzzy rule</p>
                            </div>
                            <div className="flex flex-col sm:flex-row items-center gap-4">
                                <div className="w-40 h-[196px] shrink-0">
                                    <ResponsiveContainer width="100%" height="100%">
                                        <PieChart>
                                            <Pie
                                                data={rules || []}
                                                dataKey="edges_created"
                                                nameKey="rule_id"
                                                cx="50%"
                                                cy="50%"
                                                innerRadius={44}
                                                outerRadius={72}
                                                paddingAngle={1.5}
                                                stroke="none"
                                            >
                                                {(rules || []).map((_, index) => (
                                                    <Cell
                                                        key={index}
                                                        fill={ENTITY_PALETTE[index % ENTITY_PALETTE.length].bg}
                                                    />
                                                ))}
                                            </Pie>
                                            <Tooltip
                                                contentStyle={tooltipStyle}
                                                formatter={(val: number) => [val.toLocaleString(), 'Edges']}
                                            />
                                        </PieChart>
                                    </ResponsiveContainer>
                                </div>
                                <div className="flex-1 w-full min-w-0 max-h-[196px] overflow-y-auto border border-line rounded-md divide-y divide-line">
                                    {(rules || []).length === 0 ? (
                                        <div className="px-3 py-6 text-center text-xs text-ink-muted">
                                            No match rule edges recorded yet.
                                        </div>
                                    ) : (
                                        (rules || []).map((rule, i) => (
                                            <div
                                                key={rule.rule_id}
                                                className="flex items-center justify-between gap-2 px-3 py-1.5 text-xs bg-surface hover:bg-subtle transition-colors"
                                            >
                                                <div className="flex items-center gap-2 min-w-0">
                                                    <span
                                                        className="w-2 h-2 rounded-full shrink-0"
                                                        style={{ backgroundColor: ENTITY_PALETTE[i % ENTITY_PALETTE.length].bg }}
                                                    />
                                                    <span className="font-mono text-ink truncate">
                                                        {rule.identifier_type || rule.rule_id}
                                                    </span>
                                                </div>
                                                <div className="flex items-center gap-2.5 font-mono tabular-nums shrink-0">
                                                    <span className="text-ink-muted">{rule.edges_created.toLocaleString()}</span>
                                                    <span className="text-ink font-medium w-10 text-right">{rule.percentage}%</span>
                                                </div>
                                            </div>
                                        ))
                                    )}
                                </div>
                            </div>
                        </section>
                    </div>

                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                        <section className="surface-panel p-4">
                            <div className="mb-3">
                                <h2 className="text-[13px] font-semibold text-ink">Multi-Entity Confidence</h2>
                                <p className="text-xs text-ink-muted">Excludes singletons to highlight low-confidence merges</p>
                            </div>
                            <ResponsiveContainer width="100%" height={200}>
                                <BarChart data={confidenceDist || []} margin={{ top: 4, right: 8, left: -14, bottom: 0 }}>
                                    <CartesianGrid strokeDasharray="3 3" stroke={chartTokens.grid} vertical={false} />
                                    <XAxis
                                        dataKey="bucket"
                                        stroke={chartTokens.axis}
                                        fontSize={10}
                                        tickLine={false}
                                        tickFormatter={(v: string) => v.split(' (')[0]}
                                    />
                                    <YAxis
                                        stroke={chartTokens.axis}
                                        fontSize={11}
                                        tickLine={false}
                                        axisLine={false}
                                        width={48}
                                        tickFormatter={formatCompact}
                                    />
                                    <Tooltip
                                        contentStyle={tooltipStyle}
                                        formatter={(value: number) => [value.toLocaleString(), 'Clusters']}
                                    />
                                    <Bar dataKey="count" radius={[3, 3, 0, 0]}>
                                        {(confidenceDist || []).map((entry, idx) => (
                                            <Cell key={idx} fill={entry.color || chartTokens.bar} />
                                        ))}
                                    </Bar>
                                </BarChart>
                            </ResponsiveContainer>
                        </section>

                        <section className="surface-panel p-4">
                            <div className="mb-3">
                                <h2 className="text-[13px] font-semibold text-ink">Source System Coverage</h2>
                                <p className="text-xs text-ink-muted">Entities contributed by each connected source table</p>
                            </div>
                            <ResponsiveContainer width="100%" height={Math.max(200, sourceChartData.length * 26)}>
                                <BarChart data={sourceChartData} layout="vertical" margin={{ top: 4, right: 12, left: 4, bottom: 0 }}>
                                    <CartesianGrid strokeDasharray="3 3" stroke={chartTokens.grid} horizontal={false} />
                                    <XAxis type="number" stroke={chartTokens.axis} fontSize={11} tickLine={false} tickFormatter={formatCompact} />
                                    <YAxis type="category" dataKey="label" stroke={chartTokens.axis} fontSize={11} tickLine={false} axisLine={false} width={120} interval={0} />
                                    <Tooltip
                                        contentStyle={tooltipStyle}
                                        formatter={(value: number) => [value.toLocaleString(), 'Entities']}
                                    />
                                    <Bar dataKey="entities" fill={chartTokens.bar} radius={[0, 3, 3, 0]} />
                                </BarChart>
                            </ResponsiveContainer>
                        </section>

                        <section className="surface-panel p-4">
                            <div className="flex items-start justify-between gap-2 mb-3">
                                <div>
                                    <h2 className="text-[13px] font-semibold text-ink">Pipeline Latency Trend</h2>
                                    <p className="text-xs text-ink-muted">Execution duration (s) across recent runs</p>
                                </div>
                                <button
                                    type="button"
                                    onClick={() => navigate('/runs')}
                                    className="text-xs font-medium text-accent hover:underline"
                                >
                                    All runs →
                                </button>
                            </div>
                            <ResponsiveContainer width="100%" height={200}>
                                <AreaChart data={runTrendData} margin={{ top: 4, right: 8, left: -14, bottom: 0 }}>
                                    <CartesianGrid strokeDasharray="3 3" stroke={chartTokens.grid} vertical={false} />
                                    <XAxis dataKey="label" stroke={chartTokens.axis} fontSize={10} tickLine={false} />
                                    <YAxis
                                        stroke={chartTokens.axis}
                                        fontSize={11}
                                        tickLine={false}
                                        axisLine={false}
                                        width={44}
                                        tickFormatter={(v: number) => `${v}s`}
                                    />
                                    <Tooltip
                                        contentStyle={tooltipStyle}
                                        formatter={(value: number) => [`${value}s`, 'Duration']}
                                    />
                                    <Area
                                        type="monotone"
                                        dataKey="duration"
                                        stroke={chartTokens.areaStroke}
                                        strokeWidth={1.75}
                                        fill={chartTokens.areaStroke}
                                        fillOpacity={theme === 'dark' ? 0.16 : 0.08}
                                    />
                                </AreaChart>
                            </ResponsiveContainer>
                        </section>
                    </div>
                </>
            )}
        </div>
    )
}

function MetricCell({
    label,
    value,
    sub,
    loading,
    onClick,
}: {
    label: string
    value: string
    sub?: string
    loading?: boolean
    onClick?: () => void
}) {
    const content = (
        <>
            <div className="flex items-center justify-between gap-2">
                <span className="text-xs font-medium text-ink-muted">{label}</span>
                {onClick && <ArrowRight className="w-3 h-3 text-ink-subtle" />}
            </div>
            <div className="text-[22px] leading-7 font-semibold tracking-[-0.02em] tabular-nums text-ink mt-1">
                {loading ? (
                    <span className="inline-block h-6 w-20 rounded bg-subtle animate-pulse" />
                ) : (
                    value
                )}
            </div>
            {sub && <div className="text-[11px] font-mono text-ink-muted mt-0.5 truncate">{sub}</div>}
        </>
    )

    if (onClick) {
        return (
            <button
                type="button"
                onClick={onClick}
                className="text-left px-4 py-3.5 bg-surface hover:bg-subtle transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-accent"
            >
                {content}
            </button>
        )
    }

    return <div className="px-4 py-3.5 bg-surface">{content}</div>
}
