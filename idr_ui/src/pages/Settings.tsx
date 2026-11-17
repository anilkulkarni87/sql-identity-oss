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
import { CustomSelect } from '../components/CustomSelect'

export default function Settings() {
    const navigate = useNavigate()
    const queryClient = useQueryClient()
    const [testing, setTesting] = useState(false)
    const [testMessage, setTestMessage] = useState<string | null>(null)

    // AI Assistant config state
    const [aiProvider, setAiProvider] = useState('ollama')
    const [aiModel, setAiModel] = useState('llama3.2')
    const [aiBaseUrl, setAiBaseUrl] = useState('http://localhost:11434/v1')
    const [aiApiKey, setAiApiKey] = useState('')
    const [aiMaskPii, setAiMaskPii] = useState<boolean>(false)
    const [aiStatusMsg, setAiStatusMsg] = useState<string | null>(null)
    const [aiTesting, setAiTesting] = useState(false)
    const [ollamaModels, setOllamaModels] = useState<string[]>([])

    // Setup status (connection & configured flags)
    const { data: status, isLoading: statusLoading } = useQuery({
        queryKey: ['setupStatus'],
        queryFn: () => api.getSetupStatus()
    })

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
                            onClick={() => navigate('/setup?step=connect')}
                            className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 hover:bg-blue-500 text-white text-xs font-semibold rounded-lg shadow-sm transition-all"
                        >
                            <Wand2 className="w-3.5 h-3.5" />
                            Manage / Switch Connection
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

            {/* 4. AI Assistant (Local Ollama / BYOK) */}
            <div className="bg-gray-800/90 rounded-xl p-6 border border-gray-700/80 shadow-xl space-y-5">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-3 border-b border-gray-700/60">
                    <div className="flex items-center gap-3">
                        <div className="p-2.5 bg-purple-600/15 border border-purple-500/20 rounded-xl text-purple-400">
                            <Sparkles className="w-5 h-5" />
                        </div>
                        <div>
                            <h2 className="text-base font-semibold text-white">AI Assistant (Ollama &amp; BYOK)</h2>
                            <p className="text-xs text-gray-400">
                                Run locally with Ollama for zero data movement, or bring any model key with automatic PII masking
                            </p>
                        </div>
                    </div>
                    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border ${
                        aiProvider === 'ollama'
                            ? 'bg-emerald-950/60 text-emerald-300 border-emerald-700/60'
                            : aiProvider === 'disabled'
                            ? 'bg-gray-900 text-gray-400 border-gray-700'
                            : 'bg-amber-950/60 text-amber-300 border-amber-700/60'
                    }`}>
                        <Shield className="w-3.5 h-3.5" />
                        {aiProvider === 'ollama'
                            ? (aiMaskPii ? 'Local + PII Masked' : 'Local Mode (Zero Egress)')
                            : aiProvider === 'disabled'
                            ? 'Disabled'
                            : 'Cloud + Auto PII Masking'}
                    </span>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 text-xs">
                    <div className="space-y-1.5">
                        <label className="text-gray-400 font-medium block">AI Provider</label>
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
                            buttonClassName="text-xs py-2"
                            menuClassName="left-0 right-auto w-full"
                        />
                    </div>

                    <div className="space-y-1.5">
                        <div className="flex items-center justify-between">
                            <label className="text-gray-400 font-medium">Model</label>
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
                                    className="text-[11px] text-purple-400 hover:text-purple-300"
                                >
                                    Refresh Local Models
                                </button>
                            )}
                        </div>
                        {aiProvider === 'ollama' && ollamaModels.length > 0 ? (
                            <CustomSelect
                                value={aiModel}
                                onChange={setAiModel}
                                options={ollamaModels.map(m => ({ value: m, label: m }))}
                                className="w-full"
                                buttonClassName="text-xs py-2"
                            />
                        ) : (
                            <input
                                type="text"
                                value={aiModel}
                                onChange={(e) => setAiModel(e.target.value)}
                                placeholder="e.g. llama3.2, gpt-4o-mini"
                                disabled={aiProvider === 'disabled'}
                                className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-xs text-slate-200 focus:outline-none focus:border-purple-500 disabled:opacity-50"
                            />
                        )}
                    </div>

                    <div className="space-y-1.5">
                        <label className="text-gray-400 font-medium block">Base URL</label>
                        <input
                            type="text"
                            value={aiBaseUrl}
                            onChange={(e) => setAiBaseUrl(e.target.value)}
                            placeholder="http://localhost:11434/v1"
                            disabled={aiProvider === 'disabled'}
                            className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-xs text-slate-200 font-mono focus:outline-none focus:border-purple-500 disabled:opacity-50"
                        />
                    </div>

                    <div className="space-y-1.5">
                        <label className="text-gray-400 font-medium block">
                            API Key {aiProvider === 'ollama' && <span className="text-gray-500">(optional for Ollama)</span>}
                        </label>
                        <input
                            type="password"
                            value={aiApiKey}
                            onChange={(e) => setAiApiKey(e.target.value)}
                            placeholder={aiProvider === 'ollama' ? 'Not required for local Ollama' : 'sk-... (stored in memory only)'}
                            disabled={aiProvider === 'disabled'}
                            className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-xs text-slate-200 font-mono focus:outline-none focus:border-purple-500 disabled:opacity-50"
                        />
                    </div>
                </div>

                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pt-2">
                    <label className="inline-flex items-center gap-2 text-xs text-gray-300 cursor-pointer">
                        <input
                            type="checkbox"
                            checked={aiProvider !== 'ollama' && aiProvider !== 'disabled' ? true : aiMaskPii}
                            onChange={(e) => setAiMaskPii(e.target.checked)}
                            disabled={aiProvider !== 'ollama'}
                            className="rounded border-gray-600 bg-gray-900 text-purple-500 focus:ring-purple-500"
                        />
                        <span>
                            Mask PII before sending to AI
                            {aiProvider !== 'ollama' && aiProvider !== 'disabled' && (
                                <span className="text-amber-400 ml-1">(enforced for non-local models)</span>
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
                                    model: aiModel,
                                    base_url: aiBaseUrl,
                                    api_key: aiApiKey || undefined,
                                    mask_pii: aiProvider === 'ollama' ? aiMaskPii : true,
                                })
                                if (aiProvider === 'disabled') {
                                    setAiStatusMsg('AI Assistant disabled.')
                                } else {
                                    const res = await api.testAiConnection()
                                    setAiStatusMsg(
                                        `Connected to ${res.provider} (${res.model}) in ${res.latency_ms}ms — PII masking: ${res.mask_pii ? 'ON' : 'OFF (local)'}`
                                    )
                                }
                            } catch (err: any) {
                                setAiStatusMsg(`AI test failed: ${err.message || String(err)}`)
                            } finally {
                                setAiTesting(false)
                            }
                        }}
                        className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-purple-600 hover:bg-purple-500 text-white text-xs font-medium transition-colors disabled:opacity-50"
                    >
                        {aiTesting ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Sparkles className="w-3.5 h-3.5" />}
                        <span>Save &amp; Test AI</span>
                    </button>
                </div>

                {aiStatusMsg && (
                    <div className="p-3 rounded-lg bg-gray-900/80 border border-gray-700 text-xs text-gray-200">
                        {aiStatusMsg}
                    </div>
                )}
            </div>

            {/* 5. System Telemetry & Engine Information */}
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
                        <span className="font-mono text-gray-200 font-semibold mt-0.5 block">v0.6.1</span>
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
