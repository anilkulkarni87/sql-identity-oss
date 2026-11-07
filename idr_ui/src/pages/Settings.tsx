import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
    Database, Settings as SettingsIcon, CheckCircle, XCircle,
    RefreshCw, Wand2, Shield, Server, FileCode, ArrowRight,
    Layers, AlertCircle, Sparkles, Loader2
} from 'lucide-react'
import { api } from '../api/client'
import { formatSourceName } from '../utils/colors'

export default function Settings() {
    const navigate = useNavigate()
    const queryClient = useQueryClient()
    const [testing, setTesting] = useState(false)
    const [connectingId, setConnectingId] = useState<string | null>(null)
    const [testMessage, setTestMessage] = useState<string | null>(null)

    // Setup status (connection & configured flags)
    const { data: status, isLoading: statusLoading } = useQuery({
        queryKey: ['setupStatus'],
        queryFn: () => api.getSetupStatus()
    })

    // Auto-discovered local warehouse connections
    const { data: discoveredData } = useQuery({
        queryKey: ['discoveredConnections'],
        queryFn: () => api.getDiscoveredConnections(),
        staleTime: 60000,
    })
    const discoveredConnections = discoveredData?.connections || []

    const handleQuickConnect = async (conn: any) => {
        setConnectingId(conn.id)
        setTestMessage(null)
        try {
            await api.setupConnect(conn.platform, conn.params || {})
            await queryClient.invalidateQueries({ queryKey: ['setupStatus'] })
            await queryClient.invalidateQueries({ queryKey: ['health'] })
            setTestMessage(`Successfully connected to ${conn.name}!`)
        } catch (err: any) {
            setTestMessage(`Connection failed: ${err.message || String(err)}`)
        } finally {
            setConnectingId(null)
        }
    }

    // Active pipeline configuration (sources, rules, survivorship)
    const { data: config, isLoading: configLoading } = useQuery({
        queryKey: ['setupConfig'],
        queryFn: () => api.getSetupConfig(),
        enabled: !!status?.configured
    })

    const handleTestConnection = async () => {
        setTesting(true)
        setTestMessage(null)
        try {
            await queryClient.invalidateQueries({ queryKey: ['setupStatus'] })
            await queryClient.invalidateQueries({ queryKey: ['health'] })
            const freshStatus = await api.getSetupStatus()
            if (freshStatus.connected) {
                setTestMessage(`Connection active. Successfully communicating with ${freshStatus.platform || 'warehouse'}.`)
            } else {
                setTestMessage('Warehouse connection is currently offline or unconfigured.')
            }
        } catch (err: any) {
            setTestMessage(`Connection test failed: ${err.message || String(err)}`)
        } finally {
            setTesting(false)
        }
    }

    const sources = config?.sources || []
    const rules = config?.rules || []
    const fuzzyRules = config?.fuzzy_rules || []
    const survivorship = config?.survivorship || []

    return (
        <div className="space-y-8 max-w-4xl mx-auto pb-12">
            <div>
                <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
                    <SettingsIcon className="w-6 h-6 text-blue-400" />
                    System Settings & Configuration
                </h1>
                <p className="text-sm text-gray-400 mt-1">
                    Manage your warehouse connection, view active resolution pipeline definitions, inspect security mode, and review engine telemetry.
                </p>
            </div>

            {/* 1. Warehouse Connection Card */}
            <div className="bg-gray-800/90 rounded-xl p-6 border border-gray-700/80 shadow-xl space-y-5">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-4 border-b border-gray-700/60">
                    <div className="flex items-center gap-3">
                        <div className="p-2.5 bg-blue-600/15 border border-blue-500/20 rounded-xl text-blue-400">
                            <Database className="w-5 h-5" />
                        </div>
                        <div>
                            <h2 className="text-base font-semibold text-white">Warehouse Connection</h2>
                            <p className="text-xs text-gray-400">Target execution backend for IDR SQL identity workloads</p>
                        </div>
                    </div>

                    <div className="flex items-center gap-2">
                        <button
                            onClick={handleTestConnection}
                            disabled={testing || statusLoading}
                            className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-700 hover:bg-gray-600 disabled:opacity-50 text-gray-200 text-xs font-medium rounded-lg border border-gray-600 transition-colors"
                        >
                            <RefreshCw className={`w-3.5 h-3.5 ${testing ? 'animate-spin' : ''}`} />
                            Test Connection
                        </button>
                        <button
                            onClick={() => navigate('/setup')}
                            className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 hover:bg-blue-500 text-white text-xs font-semibold rounded-lg shadow-sm transition-all"
                        >
                            <Wand2 className="w-3.5 h-3.5" />
                            Reconfigure in Wizard
                        </button>
                    </div>
                </div>

                {/* Connection Status Details */}
                <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 text-xs">
                    <div className="p-4 bg-gray-900/60 rounded-xl border border-gray-700/60 space-y-1">
                        <span className="text-gray-400 font-medium">Status</span>
                        <div className="flex items-center gap-2 pt-0.5">
                            {status?.connected ? (
                                <>
                                    <CheckCircle className="w-4 h-4 text-emerald-400" />
                                    <span className="font-semibold text-emerald-400">Online & Connected</span>
                                </>
                            ) : (
                                <>
                                    <XCircle className="w-4 h-4 text-rose-400" />
                                    <span className="font-semibold text-rose-400">Not Connected</span>
                                </>
                            )}
                        </div>
                    </div>

                    <div className="p-4 bg-gray-900/60 rounded-xl border border-gray-700/60 space-y-1">
                        <span className="text-gray-400 font-medium">Platform Backend</span>
                        <p className="font-semibold text-gray-100 capitalize pt-0.5">
                            {status?.platform ? status.platform : 'None Configured'}
                        </p>
                    </div>

                    <div className="p-4 bg-gray-900/60 rounded-xl border border-gray-700/60 space-y-1">
                        <span className="text-gray-400 font-medium">Pipeline Configuration</span>
                        <p className="font-semibold text-gray-100 pt-0.5">
                            {status?.configured ? (
                                <span className="text-blue-400">Configured & Active</span>
                            ) : (
                                <span className="text-amber-400">Pending Setup</span>
                            )}
                        </p>
                    </div>
                </div>

                {/* Auto-Discovered Connections in Environment */}
                {discoveredConnections.length > 0 && (
                    <div className="pt-3 border-t border-gray-700/60 space-y-2.5">
                        <div className="flex items-center justify-between">
                            <span className="text-xs font-semibold text-gray-300 flex items-center gap-1.5">
                                <Sparkles className="w-3.5 h-3.5 text-blue-400" />
                                Detected Warehouse Connections ({discoveredConnections.length})
                            </span>
                            <span className="text-[11px] text-gray-500">1-click switch</span>
                        </div>
                        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                            {discoveredConnections.map(conn => (
                                <div
                                    key={conn.id}
                                    className="p-3 bg-gray-900/60 border border-gray-700/60 hover:border-blue-500/50 rounded-lg flex items-center justify-between gap-2 transition-all"
                                >
                                    <div className="min-w-0 space-y-0.5">
                                        <div className="flex items-center gap-1.5">
                                            <span className="text-xs font-semibold text-white truncate">{conn.name}</span>
                                            <span className="text-[10px] uppercase font-mono px-1 py-0.5 rounded bg-gray-800 text-blue-300 border border-gray-700">
                                                {conn.platform}
                                            </span>
                                        </div>
                                        <p className="text-[11px] text-gray-400 truncate font-mono">{conn.description}</p>
                                    </div>
                                    <button
                                        onClick={() => handleQuickConnect(conn)}
                                        disabled={connectingId !== null}
                                        className="px-2.5 py-1 bg-blue-600/80 hover:bg-blue-600 disabled:opacity-50 text-white text-xs font-medium rounded transition-colors whitespace-nowrap flex items-center gap-1"
                                    >
                                        {connectingId === conn.id ? (
                                            <>
                                                <Loader2 className="w-3 h-3 animate-spin" />
                                                <span>Connecting...</span>
                                            </>
                                        ) : (
                                            'Connect'
                                        )}
                                    </button>
                                </div>
                            ))}
                        </div>
                    </div>
                )}

                {testMessage && (
                    <div className="p-3 bg-gray-900/80 rounded-lg border border-gray-700 text-xs text-gray-300 font-mono flex items-center gap-2">
                        <AlertCircle className="w-4 h-4 text-blue-400 flex-shrink-0" />
                        <span>{testMessage}</span>
                    </div>
                )}
            </div>

            {/* 2. Pipeline Configuration Card */}
            <div className="bg-gray-800/90 rounded-xl p-6 border border-gray-700/80 shadow-xl space-y-5">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-4 border-b border-gray-700/60">
                    <div className="flex items-center gap-3">
                        <div className="p-2.5 bg-purple-600/15 border border-purple-500/20 rounded-xl text-purple-400">
                            <FileCode className="w-5 h-5" />
                        </div>
                        <div>
                            <h2 className="text-base font-semibold text-white">Active Pipeline Definition</h2>
                            <p className="text-xs text-gray-400">Resolved entities, matching rules, and survivorship strategies</p>
                        </div>
                    </div>

                    <button
                        onClick={() => navigate('/setup')}
                        className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-700 hover:bg-gray-600 text-gray-200 text-xs font-medium rounded-lg border border-gray-600 transition-colors self-start sm:self-auto"
                    >
                        <span>Edit Configuration</span>
                        <ArrowRight className="w-3.5 h-3.5" />
                    </button>
                </div>

                {configLoading ? (
                    <p className="text-xs text-gray-400">Loading pipeline metadata...</p>
                ) : status?.configured && sources.length > 0 ? (
                    <div className="space-y-4">
                        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 text-xs">
                            <div className="p-3 bg-gray-900/60 rounded-xl border border-gray-700/60">
                                <span className="text-gray-400 block">Sources / Entity Types</span>
                                <span className="text-lg font-bold text-white mt-1 block">
                                    {sources.length} tables
                                </span>
                            </div>
                            <div className="p-3 bg-gray-900/60 rounded-xl border border-gray-700/60">
                                <span className="text-gray-400 block">Resolution Rules</span>
                                <span className="text-lg font-bold text-white mt-1 block">
                                    {rules.length + fuzzyRules.length} rules
                                </span>
                            </div>
                            <div className="p-3 bg-gray-900/60 rounded-xl border border-gray-700/60">
                                <span className="text-gray-400 block">Survivorship Rules</span>
                                <span className="text-lg font-bold text-white mt-1 block">
                                    {survivorship.length} attributes
                                </span>
                            </div>
                        </div>

                        {/* Sources breakdown */}
                        <div className="space-y-2">
                            <span className="text-xs font-semibold uppercase tracking-wider text-gray-400">
                                Mapped Source Tables:
                            </span>
                            <div className="flex flex-wrap gap-2">
                                {sources.map((s: any, idx: number) => {
                                    const tableName = s.table || s.id || `Source ${idx + 1}`
                                    return (
                                        <div
                                            key={idx}
                                            className="px-3 py-1.5 bg-gray-900/80 rounded-lg border border-gray-700/70 text-xs flex items-center gap-2"
                                        >
                                            <Layers className="w-3.5 h-3.5 text-blue-400" />
                                            <span className="font-semibold text-gray-200">
                                                {formatSourceName(s.id || tableName)}
                                            </span>
                                            <span className="font-mono text-[11px] text-gray-500">
                                                ({tableName})
                                            </span>
                                        </div>
                                    )
                                })}
                            </div>
                        </div>
                    </div>
                ) : (
                    <div className="p-6 text-center text-gray-500 bg-gray-900/40 rounded-xl border border-dashed border-gray-700">
                        <Layers className="w-8 h-8 mx-auto mb-2 opacity-50 text-blue-400" />
                        <p className="text-sm font-medium text-gray-300">No identity pipeline configured yet</p>
                        <p className="text-xs text-gray-500 mt-1 max-w-sm mx-auto">
                            Run through the Setup Wizard to select tables, configure entity keys, and define match rules.
                        </p>
                        <button
                            onClick={() => navigate('/setup')}
                            className="mt-4 px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white text-xs font-semibold rounded-lg shadow-md transition-all inline-flex items-center gap-1.5"
                        >
                            <Wand2 className="w-3.5 h-3.5" />
                            Start Setup Wizard
                        </button>
                    </div>
                )}
            </div>

            {/* 3. Authentication & Security Card */}
            <div className="bg-gray-800/90 rounded-xl p-6 border border-gray-700/80 shadow-xl space-y-4">
                <div className="flex items-center gap-3 pb-3 border-b border-gray-700/60">
                    <div className="p-2.5 bg-emerald-600/15 border border-emerald-500/20 rounded-xl text-emerald-400">
                        <Shield className="w-5 h-5" />
                    </div>
                    <div>
                        <h2 className="text-base font-semibold text-white">Authentication & Access Control</h2>
                        <p className="text-xs text-gray-400">Security posture and API authentication enforcement</p>
                    </div>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 text-xs">
                    <div className="p-4 bg-gray-900/60 rounded-xl border border-gray-700/60 space-y-1">
                        <span className="text-gray-400 font-medium">Auth Provider Mode</span>
                        <div className="pt-0.5">
                            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-emerald-950/60 text-emerald-300 border border-emerald-800/60">
                                Insecure Dev Auth Mode (Active)
                            </span>
                        </div>
                        <p className="text-[11px] text-gray-500 mt-1">
                            Local development mode enabled with <code className="text-gray-400 font-mono">IDR_ALLOW_INSECURE_DEV_AUTH=true</code>.
                        </p>
                    </div>

                    <div className="p-4 bg-gray-900/60 rounded-xl border border-gray-700/60 space-y-1">
                        <span className="text-gray-400 font-medium">Enterprise SSO / OIDC</span>
                        <p className="text-gray-300 font-medium pt-0.5">Optional Production Integration</p>
                        <p className="text-[11px] text-gray-500 mt-1">
                            Set <code className="text-gray-400 font-mono">IDR_AUTH_ISSUER</code> and <code className="text-gray-400 font-mono">IDR_AUTH_CLIENT_ID</code> to enforce bearer JWT validation.
                        </p>
                    </div>
                </div>
            </div>

            {/* 4. System Telemetry & Engine Information */}
            <div className="bg-gray-800/90 rounded-xl p-6 border border-gray-700/80 shadow-xl space-y-4">
                <div className="flex items-center gap-3 pb-3 border-b border-gray-700/60">
                    <div className="p-2.5 bg-cyan-600/15 border border-cyan-500/20 rounded-xl text-cyan-400">
                        <Server className="w-5 h-5" />
                    </div>
                    <div>
                        <h2 className="text-base font-semibold text-white">System Information</h2>
                        <p className="text-xs text-gray-400">Build environment, API endpoints, and engine capabilities</p>
                    </div>
                </div>

                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
                    <div className="p-3 bg-gray-900/60 rounded-lg border border-gray-700/60">
                        <span className="text-gray-400 block text-[11px]">IDR Engine</span>
                        <span className="font-mono text-gray-200 font-semibold mt-0.5 block">v0.5.0</span>
                    </div>
                    <div className="p-3 bg-gray-900/60 rounded-lg border border-gray-700/60">
                        <span className="text-gray-400 block text-[11px]">License</span>
                        <span className="font-mono text-gray-200 font-semibold mt-0.5 block">Apache 2.0 (OSS)</span>
                    </div>
                    <div className="p-3 bg-gray-900/60 rounded-lg border border-gray-700/60">
                        <span className="text-gray-400 block text-[11px]">API Endpoint</span>
                        <span className="font-mono text-gray-200 font-semibold mt-0.5 block">/api</span>
                    </div>
                    <div className="p-3 bg-gray-900/60 rounded-lg border border-gray-700/60">
                        <span className="text-gray-400 block text-[11px]">Graph Algorithm</span>
                        <span className="font-mono text-blue-400 font-semibold mt-0.5 block">Label Propagation</span>
                    </div>
                </div>
            </div>
        </div>
    )
}
