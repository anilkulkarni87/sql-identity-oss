import { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Loader2, RefreshCw, Sparkles, Check } from "lucide-react";
import { api } from '../../api/client';

const PLATFORMS = [
    { id: 'duckdb', name: 'DuckDB (Local)', description: 'Fast local embedded database.' },
    { id: 'snowflake', name: 'Snowflake', description: 'Cloud data warehouse.' },
    { id: 'bigquery', name: 'Google BigQuery', description: 'Serverless data warehouse.' },
    { id: 'databricks', name: 'Databricks', description: 'Unified data platform.' },
];

interface StepConnectProps {
    onNext: (data: any) => void;
    isConnected?: boolean;
}

export default function StepConnect({ onNext, isConnected = false }: StepConnectProps) {
    const [platform, setPlatform] = useState('duckdb');
    const [params, setParams] = useState<any>({});
    const [loading, setLoading] = useState(false);
    const [connectingId, setConnectingId] = useState<string | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [warning, setWarning] = useState<string | null>(null);
    const [pendingData, setPendingData] = useState<any>(null);
    const [hasUserSelectedPlatform, setHasUserSelectedPlatform] = useState(false);

    // Auto-discover credentials from local environment, dbt profiles, CLI configs, ADC
    const { data: discoveredData } = useQuery({
        queryKey: ['discoveredConnections'],
        queryFn: () => api.getDiscoveredConnections(),
        staleTime: 60000,
    });

    const discoveredConnections = discoveredData?.connections || [];

    // Automatically detect and pre-select discovered cloud connection on initial load
    useEffect(() => {
        if (!hasUserSelectedPlatform && discoveredConnections.length > 0 && Object.keys(params).length === 0) {
            // Prioritize Databricks if configured, then any non-duckdb cloud connection
            const primaryConn = discoveredConnections.find(c => c.platform === 'databricks')
                || discoveredConnections.find(c => c.platform !== 'duckdb')
                || discoveredConnections[0];
            if (primaryConn) {
                setPlatform(primaryConn.platform);
                setParams({ ...(primaryConn.params || {}) });
            }
        }
    }, [discoveredConnections, hasUserSelectedPlatform, params]);

    const handlePlatformChange = (newPlatform: string) => {
        setHasUserSelectedPlatform(true);
        setPlatform(newPlatform);
        setError(null);
        setWarning(null);

        // Auto-fill parameters if credentials were discovered for this platform
        const matched = discoveredConnections.find(c => c.platform === newPlatform);
        if (matched) {
            setParams({ ...(matched.params || {}) });
        } else {
            setParams(newPlatform === 'databricks' ? { catalog: 'main', schema: 'default' } : {});
        }
    };

    const onSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        submitConnection(platform, params);
    };

    const submitConnection = async (selectedPlatform: string, selectedParams: any, connId?: string) => {
        if (warning && pendingData) {
            onNext(pendingData);
            return;
        }

        setLoading(true);
        if (connId) setConnectingId(connId);
        setError(null);
        setWarning(null);

        try {
            const data = await api.setupConnect(selectedPlatform, selectedParams);

            if (data.warning) {
                setWarning(data.warning);
                setPendingData(data);
                setLoading(false);
                setConnectingId(null);
                return;
            }

            onNext(data);
        } catch (err: any) {
            setError(err.message || String(err));
        } finally {
            setLoading(false);
            setConnectingId(null);
        }
    };

    const handleSelectDiscovered = (conn: any, autoConnect = false) => {
        setPlatform(conn.platform);
        setParams({ ...(conn.params || {}) });
        setError(null);
        setWarning(null);

        if (autoConnect) {
            submitConnection(conn.platform, conn.params || {}, conn.id);
        }
    };

    const updateParam = (key: string, value: any) => {
        setParams((prev: any) => ({ ...prev, [key]: value }));
    };

    const inputClass = "w-full bg-gray-900 border border-gray-700 rounded-md px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500";
    const labelClass = "block text-xs font-medium text-gray-400 mb-1";

    return (
        <div className="space-y-6 max-w-2xl mx-auto">
            <div className="text-center pb-4 border-b border-gray-800">
                <h2 className="text-xl font-semibold text-white">Connect Data Warehouse</h2>
                <p className="text-sm text-gray-400 mt-1">Select your platform or use auto-detected credentials from your environment.</p>
                {isConnected && (
                    <div className="mt-2 bg-emerald-900/30 text-emerald-400 border border-emerald-700/50 text-xs py-1 px-3 rounded-full inline-flex items-center gap-2">
                        <RefreshCw className="w-3 h-3" />
                        API Connected
                    </div>
                )}
            </div>

            {/* 1. Auto-Discovered Credentials Section */}
            {discoveredConnections.length > 0 && (
                <div className="bg-gradient-to-r from-blue-950/40 via-indigo-950/30 to-purple-950/40 border border-blue-500/30 rounded-xl p-4 space-y-3">
                    <div className="flex items-center justify-between">
                        <span className="text-xs font-semibold text-blue-300 uppercase tracking-wider flex items-center gap-1.5">
                            <Sparkles className="w-3.5 h-3.5 text-blue-400" />
                            Discovered Local Credentials ({discoveredConnections.length})
                        </span>
                        <span className="text-[11px] text-gray-400">Auto-detected from CLI & dbt profiles</span>
                    </div>

                    <div className="grid grid-cols-1 gap-2">
                        {discoveredConnections.map(conn => {
                            const isConnectingThis = loading && connectingId === conn.id;
                            return (
                                <div
                                    key={conn.id}
                                    className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 p-3 bg-gray-900/80 hover:bg-gray-900 border border-gray-700/80 hover:border-blue-500/50 rounded-lg transition-all"
                                >
                                    <div className="space-y-0.5">
                                        <div className="flex items-center gap-2">
                                            <span className="text-xs font-bold text-white">{conn.name}</span>
                                            <span className="text-[10px] uppercase font-mono px-1.5 py-0.5 rounded bg-gray-800 text-blue-300 border border-gray-700">
                                                {conn.platform}
                                            </span>
                                            <span className="text-[10px] text-gray-400 font-mono">
                                                source: {conn.source}
                                            </span>
                                        </div>
                                        <p className="text-xs text-gray-400 font-mono text-[11px]">{conn.description}</p>
                                    </div>

                                    <div className="flex items-center gap-2 self-end sm:self-auto">
                                        <button
                                            type="button"
                                            onClick={() => handleSelectDiscovered(conn, false)}
                                            className="px-2.5 py-1 text-gray-300 hover:text-white bg-gray-800 hover:bg-gray-700 text-xs rounded border border-gray-600 transition-colors"
                                            title="Populate form below to inspect or edit"
                                        >
                                            Review
                                        </button>
                                        <button
                                            type="button"
                                            disabled={loading}
                                            onClick={() => handleSelectDiscovered(conn, true)}
                                            className="px-3 py-1 bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white text-xs font-medium rounded shadow-sm transition-all flex items-center gap-1.5"
                                        >
                                            {isConnectingThis ? (
                                                <>
                                                    <Loader2 className="w-3 h-3 animate-spin" />
                                                    Connecting...
                                                </>
                                            ) : (
                                                <>
                                                    <Check className="w-3 h-3" />
                                                    1-Click Connect
                                                </>
                                            )}
                                        </button>
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                    <div className="text-center pt-1">
                        <span className="text-[11px] text-gray-500">or select a warehouse platform below to configure manually</span>
                    </div>
                </div>
            )}

            {/* 2. Platform Selector */}
            <form onSubmit={onSubmit} className="space-y-6">
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                    {PLATFORMS.map(p => {
                        const isDiscovered = discoveredConnections.some(c => c.platform === p.id);
                        return (
                            <button
                                key={p.id}
                                type="button"
                                onClick={() => handlePlatformChange(p.id)}
                                className={`relative p-3 rounded-lg border text-left transition-all ${platform === p.id
                                    ? 'bg-blue-600/10 border-blue-500 ring-1 ring-blue-500'
                                    : 'bg-gray-800 border-gray-700 hover:border-gray-600'
                                    }`}
                            >
                                {isDiscovered && (
                                    <span className="absolute top-2 right-2 px-1.5 py-0.5 rounded text-[9px] font-medium bg-emerald-950 text-emerald-400 border border-emerald-800/60 flex items-center gap-1">
                                        <Sparkles className="w-2.5 h-2.5" />
                                        Detected
                                    </span>
                                )}
                                <div className={`font-semibold text-sm ${platform === p.id ? 'text-blue-400' : 'text-gray-200'}`}>
                                    {p.name}
                                </div>
                                <div className="text-xs text-gray-500 mt-1 line-clamp-1">{p.description}</div>
                            </button>
                        );
                    })}
                </div>

                {/* 3. Parameter Inputs */}
                <div className="space-y-4 bg-gray-800/50 p-4 rounded-lg border border-gray-700/50">
                    {platform === 'duckdb' && (
                        <div>
                            <label className={labelClass}>Database Path</label>
                            <input
                                type="text"
                                value={params.path || ''}
                                placeholder="data/identity_graph.db"
                                className={inputClass}
                                onChange={e => updateParam('path', e.target.value)}
                            />
                            <p className="text-xs text-gray-500 mt-1">Leave empty for in-memory (data lost on restart).</p>
                        </div>
                    )}

                    {platform === 'snowflake' && (
                        <>
                            <div>
                                <label className={labelClass}>Account</label>
                                <input
                                    type="text"
                                    required
                                    value={params.account || ''}
                                    placeholder="xy12345.us-east-1"
                                    className={inputClass}
                                    onChange={e => updateParam('account', e.target.value)}
                                />
                            </div>
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <label className={labelClass}>User</label>
                                    <input
                                        type="text"
                                        required
                                        value={params.user || ''}
                                        className={inputClass}
                                        onChange={e => updateParam('user', e.target.value)}
                                    />
                                </div>
                                <div>
                                    <label className={labelClass}>Password</label>
                                    <input
                                        type="password"
                                        required
                                        value={params.password || ''}
                                        className={inputClass}
                                        onChange={e => updateParam('password', e.target.value)}
                                    />
                                </div>
                            </div>
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <label className={labelClass}>Database</label>
                                    <input
                                        type="text"
                                        required
                                        value={params.database || ''}
                                        className={inputClass}
                                        onChange={e => updateParam('database', e.target.value)}
                                    />
                                </div>
                                <div>
                                    <label className={labelClass}>Schema</label>
                                    <input
                                        type="text"
                                        required
                                        value={params.schema || 'PUBLIC'}
                                        className={inputClass}
                                        onChange={e => updateParam('schema', e.target.value)}
                                    />
                                </div>
                            </div>
                            <div>
                                <label className={labelClass}>Warehouse</label>
                                <input
                                    type="text"
                                    required
                                    value={params.warehouse || ''}
                                    className={inputClass}
                                    onChange={e => updateParam('warehouse', e.target.value)}
                                />
                            </div>
                        </>
                    )}

                    {platform === 'bigquery' && (
                        <>
                            <div>
                                <label className={labelClass}>Project ID</label>
                                <input
                                    type="text"
                                    required
                                    value={params.project || ''}
                                    placeholder="my-gcp-project"
                                    className={inputClass}
                                    onChange={e => updateParam('project', e.target.value)}
                                />
                            </div>
                            <div>
                                <label className={labelClass}>Dataset / Schema</label>
                                <input
                                    type="text"
                                    value={params.dataset || params.schema || ''}
                                    placeholder="default"
                                    className={inputClass}
                                    onChange={e => updateParam('dataset', e.target.value)}
                                />
                            </div>
                            <div>
                                <label className={labelClass}>Credentials JSON (Optional if gcloud ADC is active)</label>
                                <textarea
                                    className={`${inputClass} font-mono text-xs`}
                                    rows={3}
                                    value={params.credentials_json || ''}
                                    placeholder="{ ... } (leave empty to use default Google Cloud ADC)"
                                    onChange={e => updateParam('credentials_json', e.target.value)}
                                />
                            </div>
                        </>
                    )}

                    {platform === 'databricks' && (
                        <>
                            {/* Discovered Profiles Banner / Selector */}
                            {discoveredConnections.filter(c => c.platform === 'databricks').length > 0 && (
                                <div className="p-3 bg-blue-950/40 border border-blue-500/30 rounded-lg text-xs space-y-2">
                                    <div className="flex items-center justify-between">
                                        <span className="font-semibold text-blue-300 flex items-center gap-1.5">
                                            <Sparkles className="w-3.5 h-3.5 text-blue-400" />
                                            Auto-detected Databricks Configuration
                                        </span>
                                        <span className="text-[10px] text-gray-400">
                                            CLI & Environment
                                        </span>
                                    </div>
                                    <div className="flex flex-wrap gap-1.5 pt-1">
                                        {discoveredConnections.filter(c => c.platform === 'databricks').map(dc => {
                                            const isSelected = params.server_hostname === dc.params?.server_hostname &&
                                                params.http_path === dc.params?.http_path;
                                            return (
                                                <button
                                                    key={dc.id}
                                                    type="button"
                                                    onClick={() => {
                                                        setParams({ ...(dc.params || {}) });
                                                        setError(null);
                                                    }}
                                                    className={`px-2.5 py-1 rounded text-[11px] font-mono transition-colors border ${
                                                        isSelected
                                                            ? 'bg-blue-600 text-white border-blue-400 shadow-sm'
                                                            : 'bg-gray-800 hover:bg-gray-700 text-gray-300 border-gray-600'
                                                    }`}
                                                >
                                                    {dc.name}
                                                </button>
                                            );
                                        })}
                                    </div>
                                </div>
                            )}

                            <div>
                                <label className={labelClass}>Server Hostname</label>
                                <input
                                    type="text"
                                    required
                                    value={params.server_hostname || ''}
                                    placeholder="dbc-xxxx.cloud.databricks.com"
                                    className={inputClass}
                                    onChange={e => {
                                        let val = e.target.value.trim();
                                        if (val.startsWith("https://")) val = val.slice("https://".length);
                                        if (val.startsWith("http://")) val = val.slice("http://".length);
                                        val = val.split("/")[0].split("?")[0];
                                        updateParam('server_hostname', val);
                                    }}
                                />
                                <p className="text-[11px] text-gray-500 mt-1">Databricks workspace hostname (e.g. dbc-xxxx.cloud.databricks.com, no https://).</p>
                            </div>
                            <div>
                                <label className={labelClass}>HTTP Path</label>
                                <input
                                    type="text"
                                    required
                                    value={params.http_path || ''}
                                    placeholder="/sql/1.0/warehouses/xxxx"
                                    className={inputClass}
                                    onChange={e => updateParam('http_path', e.target.value.trim())}
                                />
                                <p className="text-[11px] text-gray-500 mt-1">SQL Warehouse endpoint HTTP path (e.g. /sql/1.0/warehouses/...)</p>
                            </div>
                            <div>
                                <label className={labelClass}>Access Token</label>
                                <input
                                    type="password"
                                    required
                                    value={params.access_token || ''}
                                    placeholder="dapi..."
                                    className={inputClass}
                                    onChange={e => updateParam('access_token', e.target.value.trim())}
                                />
                            </div>
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <label className={labelClass}>Catalog</label>
                                    <input
                                        type="text"
                                        required
                                        value={params.catalog || 'main'}
                                        placeholder="main"
                                        className={inputClass}
                                        onChange={e => updateParam('catalog', e.target.value.trim())}
                                    />
                                </div>
                                <div>
                                    <label className={labelClass}>Schema</label>
                                    <input
                                        type="text"
                                        value={params.schema || 'default'}
                                        placeholder="default"
                                        className={inputClass}
                                        onChange={e => updateParam('schema', e.target.value.trim())}
                                    />
                                </div>
                            </div>
                        </>
                    )}
                </div>

                {error && (
                    <div className="bg-red-500/10 border border-red-500/50 rounded-md p-3 text-sm text-red-400">
                        {error}
                    </div>
                )}

                {warning && (
                    <div className="bg-yellow-500/10 border border-yellow-500/50 rounded-md p-3 text-sm text-yellow-400">
                        <p className="font-bold mb-1">Warning</p>
                        {warning}
                        <p className="text-xs mt-2 text-yellow-500/70">Click Connect again to proceed anyway.</p>
                    </div>
                )}

                <button
                    type="submit"
                    disabled={loading}
                    className="w-full py-3 px-4 bg-blue-600 hover:bg-blue-700 text-white rounded-lg font-medium transition-colors flex items-center justify-center disabled:opacity-50 disabled:cursor-not-allowed"
                >
                    {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : (warning ? 'Confirm & Proceed' : 'Connect to Warehouse')}
                </button>
            </form>
        </div>
    );
}
