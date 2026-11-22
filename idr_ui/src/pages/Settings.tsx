import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
    RefreshCw, ArrowRight, Loader2, SlidersHorizontal, Bot, Shield
} from 'lucide-react'
import { api } from '../api/client'
import { formatSourceName } from '../utils/colors'
import { useIDRAuth } from '../auth/IDRAuthProvider'
import { useToast } from '../components/Toast'
import { CustomSelect } from '../components/CustomSelect'

export default function Settings() {
    const navigate = useNavigate()
    const queryClient = useQueryClient()
    const auth = useIDRAuth()
    const { toast } = useToast()

    const [testing, setTesting] = useState(false)
    const [connectingId, setConnectingId] = useState<string | null>(null)

    // AI Assistant (Local Ollama / BYOK) state
    const [aiProvider, setAiProvider] = useState('ollama')
    const [aiModel, setAiModel] = useState('llama3.2')
    const [aiBaseUrl, setAiBaseUrl] = useState('http://localhost:11434/v1')
    const [aiApiKey, setAiApiKey] = useState('')
    const [aiMaskPii, setAiMaskPii] = useState<boolean>(false)
    const [aiStatusMsg, setAiStatusMsg] = useState<string | null>(null)
    const [aiTesting, setAiTesting] = useState(false)
    const [ollamaModels, setOllamaModels] = useState<string[]>([])

    const { data: aiConfig } = useQuery({
        queryKey: ['aiConfig'],
        queryFn: () => api.getAiConfig(),
    })

    useEffect(() => {
        if (aiConfig && aiConfig.enabled) {
            setAiProvider(aiConfig.provider || 'ollama')
            setAiModel(aiConfig.model || 'llama3.2')
            setAiBaseUrl(aiConfig.base_url || 'http://localhost:11434/v1')
            setAiMaskPii(Boolean(aiConfig.mask_pii))
        }
    }, [aiConfig])

    useEffect(() => {
        if (aiProvider !== 'ollama') return
        let cancelled = false
        api.getOllamaModels(aiBaseUrl || 'http://localhost:11434/v1')
            .then((res) => {
                if (cancelled) return
                const models = res.models || []
                setOllamaModels(models)
                if (models.length > 0) {
                    setAiModel((prev) => {
                        if (models.includes(prev)) return prev
                        const match = models.find((m) => m.startsWith(prev))
                        return match || models[0]
                    })
                }
            })
            .catch(() => {})
        return () => {
            cancelled = true
        }
    }, [aiProvider, aiBaseUrl])

    const { data: status, isLoading: statusLoading } = useQuery({
        queryKey: ['setupStatus'],
        queryFn: () => api.getSetupStatus(),
    })

    const { data: discoveredData } = useQuery({
        queryKey: ['discoveredConnections'],
        queryFn: () => api.getDiscoveredConnections(),
        staleTime: 60000,
    })
    const discoveredConnections = discoveredData?.connections || []

    const { data: config, isLoading: configLoading } = useQuery({
        queryKey: ['setupConfig'],
        queryFn: () => api.getSetupConfig(),
        enabled: !!status?.configured,
    })

    const handleQuickConnect = async (conn: any) => {
        setConnectingId(conn.id)
        try {
            await api.setupConnect(conn.platform, conn.params || {})
            await queryClient.invalidateQueries({ queryKey: ['setupStatus'] })
            await queryClient.invalidateQueries({ queryKey: ['health'] })
            toast('success', `Connected to ${conn.name}`)
        } catch (err: any) {
            toast('error', 'Connection failed', err?.message || String(err))
        } finally {
            setConnectingId(null)
        }
    }

    const handleTestConnection = async () => {
        setTesting(true)
        try {
            await queryClient.invalidateQueries({ queryKey: ['setupStatus'] })
            await queryClient.invalidateQueries({ queryKey: ['health'] })
            const freshStatus = await api.getSetupStatus()
            if (freshStatus.connected) {
                toast('success', 'Warehouse connection verified', `Active platform: ${freshStatus.platform || 'warehouse'}`)
            } else {
                toast('error', 'Warehouse offline', 'Connection is currently offline or unconfigured.')
            }
        } catch (err: any) {
            toast('error', 'Connection test failed', err?.message || String(err))
        } finally {
            setTesting(false)
        }
    }

    const sources = config?.sources || []
    const rules = config?.rules || []
    const fuzzyRules = config?.fuzzy_rules || []
    const survivorship = config?.survivorship || []

    const oidcConfigured = Boolean(
        import.meta.env.VITE_AUTH_AUTHORITY && import.meta.env.VITE_AUTH_CLIENT_ID
    )

    return (
        <div className="space-y-5 max-w-4xl pb-10">
            <div>
                <h1 className="text-[18px] font-semibold tracking-[-0.015em] text-ink">
                    Settings &amp; AI Configuration
                </h1>
                <p className="text-[13px] text-ink-muted mt-0.5">
                    Warehouse backend connection, local Ollama / BYOK AI model setup, active pipeline definition, and runtime metadata.
                </p>
            </div>

            {/* 1. Warehouse Connection */}
            <section className="surface-panel overflow-hidden">
                <div className="px-4 py-3 bg-subtle border-b border-line flex flex-wrap items-center justify-between gap-2">
                    <div>
                        <h2 className="text-[13px] font-semibold text-ink">Warehouse Connection</h2>
                        <p className="text-xs text-ink-muted">Target execution engine for SQL identity workloads</p>
                    </div>
                    <div className="flex items-center gap-2">
                        <button
                            type="button"
                            onClick={handleTestConnection}
                            disabled={testing || statusLoading}
                            className="btn-secondary text-xs"
                        >
                            <RefreshCw className={`w-3.5 h-3.5 ${testing ? 'animate-spin' : ''}`} />
                            Test Connection
                        </button>
                        <button
                            type="button"
                            onClick={() => navigate('/setup?step=connect')}
                            className="btn-primary text-xs"
                        >
                            <SlidersHorizontal className="w-3.5 h-3.5" />
                            Manage / Switch Connection
                        </button>
                    </div>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-3 divide-y sm:divide-y-0 sm:divide-x divide-line text-xs">
                    <div className="px-4 py-3">
                        <div className="text-ink-muted">Status</div>
                        <div className="mt-1">
                            {status?.connected ? (
                                <span className="badge-success">
                                    <span className="w-1.5 h-1.5 rounded-full bg-status-success" />
                                    Connected
                                </span>
                            ) : (
                                <span className="badge-danger">
                                    <span className="w-1.5 h-1.5 rounded-full bg-status-danger" />
                                    Disconnected
                                </span>
                            )}
                        </div>
                    </div>
                    <div className="px-4 py-3">
                        <div className="text-ink-muted">Platform Backend</div>
                        <div className="mt-1 font-mono font-semibold text-ink uppercase">
                            {status?.platform || 'None'}
                        </div>
                    </div>
                    <div className="px-4 py-3">
                        <div className="text-ink-muted">Pipeline State</div>
                        <div className="mt-1">
                            {status?.configured ? (
                                <span className="badge-accent">Active</span>
                            ) : (
                                <span className="badge-warning">Pending Setup</span>
                            )}
                        </div>
                    </div>
                </div>

                {discoveredConnections.length > 0 && (
                    <div className="px-4 py-3 border-t border-line space-y-2">
                        <div className="flex items-center justify-between text-xs">
                            <span className="font-semibold text-ink">
                                Detected Local Connections ({discoveredConnections.length})
                            </span>
                            <span className="text-ink-muted">1-click switch</span>
                        </div>
                        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                            {discoveredConnections.map((conn) => (
                                <div
                                    key={conn.id}
                                    className="px-3 py-2 bg-subtle border border-line rounded-md flex items-center justify-between gap-2"
                                >
                                    <div className="min-w-0">
                                        <div className="flex items-center gap-1.5">
                                            <span className="text-xs font-semibold text-ink truncate">{conn.name}</span>
                                            <span className="badge-neutral font-mono uppercase">{conn.platform}</span>
                                        </div>
                                        <p className="text-[11px] font-mono text-ink-muted truncate mt-0.5">
                                            {conn.description}
                                        </p>
                                    </div>
                                    <button
                                        type="button"
                                        onClick={() => handleQuickConnect(conn)}
                                        disabled={connectingId !== null}
                                        className="btn-secondary text-xs shrink-0"
                                    >
                                        {connectingId === conn.id ? (
                                            <>
                                                <Loader2 className="w-3 h-3 animate-spin" />
                                                Connecting…
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
            </section>

            {/* 2. AI Assistant (Local Ollama / BYOK) */}
            <section className="surface-panel overflow-hidden">
                <div className="px-4 py-3 bg-subtle border-b border-line flex flex-wrap items-center justify-between gap-2">
                    <div>
                        <h2 className="text-[13px] font-semibold text-ink flex items-center gap-1.5">
                            <Bot className="w-4 h-4 text-accent" />
                            AI Assistant (Local Ollama &amp; BYOK)
                        </h2>
                        <p className="text-xs text-ink-muted">
                            Run locally with Ollama for zero data movement, or bring any model key with automatic PII masking
                        </p>
                    </div>
                    <span
                        className={
                            aiProvider === 'ollama'
                                ? 'badge-success'
                                : aiProvider === 'disabled'
                                ? 'badge-neutral'
                                : 'badge-warning'
                        }
                    >
                        <Shield className="w-3 h-3" />
                        {aiProvider === 'ollama'
                            ? aiMaskPii
                                ? 'Local + PII Masked'
                                : 'Local Mode (Zero Egress)'
                            : aiProvider === 'disabled'
                            ? 'Disabled'
                            : 'Cloud + Auto PII Masking'}
                    </span>
                </div>

                <div className="p-4 space-y-4 text-xs">
                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
                        <div className="space-y-1">
                            <label className="text-ink-secondary font-medium block">AI Provider</label>
                            <CustomSelect
                                value={aiProvider}
                                onChange={(val) => {
                                    setAiProvider(val)
                                    if (val === 'ollama') {
                                        setAiBaseUrl('http://localhost:11434/v1')
                                        setAiModel('llama3.2')
                                        setAiMaskPii(false)
                                    } else if (val === 'openai') {
                                        setAiBaseUrl('https://api.openai.com/v1')
                                        setAiModel('gpt-4o-mini')
                                        setAiMaskPii(true)
                                    } else if (val === 'anthropic') {
                                        setAiBaseUrl('https://api.anthropic.com/v1')
                                        setAiModel('claude-3-5-sonnet-latest')
                                        setAiMaskPii(true)
                                    } else if (val === 'gemini') {
                                        setAiBaseUrl('https://generativelanguage.googleapis.com/v1beta')
                                        setAiModel('gemini-2.0-flash')
                                        setAiMaskPii(true)
                                    } else if (val === 'openai_compatible') {
                                        setAiBaseUrl('')
                                        setAiMaskPii(true)
                                    }
                                }}
                                options={[
                                    { value: 'ollama', label: 'Ollama (Local / MacBook)' },
                                    { value: 'openai', label: 'OpenAI' },
                                    { value: 'anthropic', label: 'Anthropic Claude' },
                                    { value: 'gemini', label: 'Google Gemini' },
                                    { value: 'openai_compatible', label: 'Custom OpenAI-Compatible' },
                                    { value: 'disabled', label: 'Disabled' },
                                ]}
                                className="w-full"
                                buttonClassName="py-1.5"
                                menuClassName="left-0 right-auto w-full"
                            />
                        </div>

                        <div className="space-y-1">
                            <div className="flex items-center justify-between">
                                <label htmlFor="ai-model-input" className="text-ink-secondary font-medium">
                                    Model
                                </label>
                                {aiProvider === 'ollama' && (
                                    <button
                                        type="button"
                                        onClick={async () => {
                                            try {
                                                const res = await api.getOllamaModels(aiBaseUrl)
                                                setOllamaModels(res.models || [])
                                                if (res.models?.length) {
                                                    setAiModel(res.models[0])
                                                    setAiStatusMsg(`Found ${res.models.length} local Ollama model(s).`)
                                                } else {
                                                    setAiStatusMsg('No Ollama models found. Run `ollama pull llama3.2`.')
                                                }
                                            } catch (err: any) {
                                                setAiStatusMsg(`Ollama lookup failed: ${err.message}`)
                                            }
                                        }}
                                        className="text-[11px] font-medium text-accent hover:underline"
                                    >
                                        Refresh Local Models
                                    </button>
                                )}
                            </div>
                            {aiProvider === 'ollama' && ollamaModels.length > 0 ? (
                                <CustomSelect
                                    value={aiModel}
                                    onChange={setAiModel}
                                    options={ollamaModels.map((m) => ({ value: m, label: m }))}
                                    className="w-full"
                                    buttonClassName="py-1.5"
                                />
                            ) : (
                                <input
                                    id="ai-model-input"
                                    type="text"
                                    value={aiModel}
                                    onChange={(e) => setAiModel(e.target.value)}
                                    placeholder="e.g. llama3.2, gpt-4o-mini"
                                    disabled={aiProvider === 'disabled'}
                                    className="input-field font-mono text-xs"
                                />
                            )}
                        </div>

                        <div className="space-y-1">
                            <label htmlFor="ai-base-url-input" className="text-ink-secondary font-medium block">
                                Base URL
                            </label>
                            <input
                                id="ai-base-url-input"
                                type="text"
                                value={aiBaseUrl}
                                onChange={(e) => setAiBaseUrl(e.target.value)}
                                placeholder="http://localhost:11434/v1"
                                disabled={aiProvider === 'disabled'}
                                className="input-field font-mono text-xs"
                            />
                        </div>

                        {aiProvider !== 'ollama' && aiProvider !== 'disabled' && (
                        <div className="space-y-1">
                            <label htmlFor="ai-api-key-input" className="text-ink-secondary font-medium block">
                                API Key
                            </label>
                            <input
                                id="ai-api-key-input"
                                type="password"
                                value={aiApiKey}
                                onChange={(e) => setAiApiKey(e.target.value)}
                                placeholder="sk-... (stored in memory only)"
                                className="input-field font-mono text-xs"
                            />
                        </div>
                        )}
                    </div>

                    <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pt-2 border-t border-line">
                        <label className="inline-flex items-center gap-2 text-xs text-ink-secondary cursor-pointer">
                            <input
                                type="checkbox"
                                checked={aiProvider !== 'ollama' && aiProvider !== 'disabled' ? true : aiMaskPii}
                                onChange={(e) => setAiMaskPii(e.target.checked)}
                                disabled={aiProvider !== 'ollama'}
                                className="rounded border-line-strong text-accent focus:ring-accent"
                            />
                            <span>
                                Mask PII before sending to AI
                                {aiProvider !== 'ollama' && aiProvider !== 'disabled' && (
                                    <span className="text-status-warning ml-1">
                                        (enforced for non-local models)
                                    </span>
                                )}
                            </span>
                        </label>

                        <button
                            type="button"
                            disabled={aiTesting}
                            onClick={async () => {
                                setAiTesting(true)
                                setAiStatusMsg(null)
                                try {
                                    await api.updateAiConfig({
                                        provider: aiProvider,
                                        model: aiModel.trim(),
                                        base_url: aiBaseUrl.trim(),
                                        api_key: aiApiKey.trim() || undefined,
                                        mask_pii: aiProvider === 'ollama' ? aiMaskPii : true,
                                    })
                                    await queryClient.invalidateQueries({ queryKey: ['aiConfig'] })
                                    if (aiProvider === 'disabled') {
                                        setAiStatusMsg('AI Assistant disabled.')
                                        toast('info', 'AI Assistant disabled')
                                    } else {
                                        const res = await api.testAiConnection()
                                        const msg = `Connected to ${res.provider} (${res.model}) in ${res.latency_ms}ms — PII masking: ${
                                            res.mask_pii ? 'ON' : 'OFF (local)'
                                        }`
                                        setAiStatusMsg(msg)
                                        toast('success', 'AI Assistant ready', msg)
                                    }
                                } catch (err: any) {
                                    const errMsg = `AI test failed: ${err.message || String(err)}`
                                    setAiStatusMsg(errMsg)
                                    toast('error', 'AI test failed', err.message || String(err))
                                } finally {
                                    setAiTesting(false)
                                }
                            }}
                            className="btn-primary text-xs"
                        >
                            {aiTesting ? (
                                <Loader2 className="w-3.5 h-3.5 animate-spin" />
                            ) : (
                                <Bot className="w-3.5 h-3.5" />
                            )}
                            <span>Save &amp; Test AI</span>
                        </button>
                    </div>

                    {aiStatusMsg && (
                        <div className="p-2.5 rounded-md bg-subtle border border-line text-xs font-mono text-ink-secondary">
                            {aiStatusMsg}
                        </div>
                    )}
                </div>
            </section>

            {/* 3. Active Pipeline Definition */}
            <section className="surface-panel overflow-hidden">
                <div className="px-4 py-3 bg-subtle border-b border-line flex flex-wrap items-center justify-between gap-2">
                    <div>
                        <h2 className="text-[13px] font-semibold text-ink">Active Pipeline Definition</h2>
                        <p className="text-xs text-ink-muted">Mapped source tables, match rules, and survivorship policies</p>
                    </div>
                    <button
                        type="button"
                        onClick={() => navigate('/setup')}
                        className="btn-secondary text-xs"
                    >
                        Edit in Identity Studio
                        <ArrowRight className="w-3.5 h-3.5" />
                    </button>
                </div>

                {configLoading ? (
                    <div className="px-4 py-6 text-xs text-ink-muted">Loading pipeline definition…</div>
                ) : status?.configured && sources.length > 0 ? (
                    <div className="divide-y divide-line">
                        <div className="grid grid-cols-3 divide-x divide-line text-xs">
                            <div className="px-4 py-3">
                                <div className="text-ink-muted">Source Tables</div>
                                <div className="text-base font-semibold font-mono tabular-nums text-ink mt-0.5">
                                    {sources.length}
                                </div>
                            </div>
                            <div className="px-4 py-3">
                                <div className="text-ink-muted">Match Rules</div>
                                <div className="text-base font-semibold font-mono tabular-nums text-ink mt-0.5">
                                    {rules.length + fuzzyRules.length}
                                </div>
                            </div>
                            <div className="px-4 py-3">
                                <div className="text-ink-muted">Survivorship Attributes</div>
                                <div className="text-base font-semibold font-mono tabular-nums text-ink mt-0.5">
                                    {survivorship.length}
                                </div>
                            </div>
                        </div>
                        <div className="px-4 py-3 space-y-2">
                            <div className="text-[11px] font-semibold uppercase tracking-[0.04em] text-ink-muted">
                                Mapped Sources
                            </div>
                            <div className="flex flex-wrap gap-1.5">
                                {sources.map((s: any, idx: number) => {
                                    const tableName = s.table || s.id || `Source ${idx + 1}`
                                    return (
                                        <span key={idx} className="badge-neutral py-1 px-2">
                                            <span className="font-medium text-ink">{formatSourceName(s.id || tableName)}</span>
                                            <span className="font-mono text-ink-muted">{tableName}</span>
                                        </span>
                                    )
                                })}
                            </div>
                        </div>
                    </div>
                ) : (
                    <div className="px-4 py-8 text-center space-y-2">
                        <p className="text-xs font-medium text-ink">No pipeline configured yet</p>
                        <p className="text-xs text-ink-muted max-w-sm mx-auto">
                            Open Identity Studio to connect tables, map entity keys, and define match rules.
                        </p>
                        <button type="button" onClick={() => navigate('/setup')} className="btn-primary text-xs mt-1">
                            Open Identity Studio
                        </button>
                    </div>
                )}
            </section>

            {/* 4. Authentication & Access Control */}
            <section className="surface-panel overflow-hidden">
                <div className="px-4 py-3 bg-subtle border-b border-line">
                    <h2 className="text-[13px] font-semibold text-ink">Authentication &amp; Access Control</h2>
                    <p className="text-xs text-ink-muted">Runtime OIDC enforcement and session status</p>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-2 divide-y sm:divide-y-0 sm:divide-x divide-line text-xs">
                    <div className="px-4 py-3 space-y-1">
                        <div className="text-ink-muted">Active Auth Posture</div>
                        <div>
                            {oidcConfigured ? (
                                <span className="badge-success">
                                    OIDC Enforced ({auth.isAuthenticated ? 'Signed In' : 'Unauthenticated'})
                                </span>
                            ) : (
                                <span className="badge-warning">Local Dev Auth (OIDC Not Configured)</span>
                            )}
                        </div>
                        <p className="text-[11px] text-ink-muted pt-0.5">
                            {oidcConfigured
                                ? `Issuer: ${import.meta.env.VITE_AUTH_AUTHORITY}`
                                : 'Set VITE_AUTH_AUTHORITY and VITE_AUTH_CLIENT_ID to enable OIDC bearer JWT enforcement.'}
                        </p>
                    </div>

                    <div className="px-4 py-3 space-y-1">
                        <div className="text-ink-muted">Backend Verification</div>
                        <div className="font-medium text-ink">Bearer JWT Validation</div>
                        <p className="text-[11px] text-ink-muted pt-0.5">
                            Configure <code className="font-mono text-ink-secondary">IDR_AUTH_ISSUER</code> and{' '}
                            <code className="font-mono text-ink-secondary">IDR_AUTH_CLIENT_ID</code> on the API server for production deployments.
                        </p>
                    </div>
                </div>
            </section>

            {/* 5. Runtime Metadata */}
            <section className="surface-panel overflow-hidden">
                <div className="px-4 py-3 bg-subtle border-b border-line">
                    <h2 className="text-[13px] font-semibold text-ink">Runtime Metadata</h2>
                </div>
                <div className="grid grid-cols-2 sm:grid-cols-4 divide-y sm:divide-y-0 sm:divide-x divide-line text-xs">
                    <div className="px-4 py-3">
                        <div className="text-ink-muted">UI / Engine Version</div>
                        <div className="font-mono font-semibold text-ink mt-0.5">v0.7.0</div>
                    </div>
                    <div className="px-4 py-3">
                        <div className="text-ink-muted">License</div>
                        <div className="font-mono font-semibold text-ink mt-0.5">Apache 2.0</div>
                    </div>
                    <div className="px-4 py-3">
                        <div className="text-ink-muted">API Base</div>
                        <div className="font-mono font-semibold text-ink mt-0.5">
                            {import.meta.env.VITE_API_BASE_URL || '/api'}
                        </div>
                    </div>
                    <div className="px-4 py-3">
                        <div className="text-ink-muted">Graph Algorithm</div>
                        <div className="font-mono font-semibold text-accent mt-0.5">Label Propagation</div>
                    </div>
                </div>
            </section>
        </div>
    )
}
