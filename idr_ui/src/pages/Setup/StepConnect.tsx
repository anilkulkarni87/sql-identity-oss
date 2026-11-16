import { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
    Loader2, Sparkles, Check, Database, ShieldCheck,
    Server, KeyRound, ArrowLeft
} from "lucide-react";
import { api } from '../../api/client';

const PLATFORMS = [
    { id: 'duckdb', name: 'DuckDB', subtitle: 'Local / Embedded', description: 'Fast local embedded database.' },
    { id: 'snowflake', name: 'Snowflake', subtitle: 'Cloud DW', description: 'Cloud data warehouse.' },
    { id: 'bigquery', name: 'BigQuery', subtitle: 'Google Cloud', description: 'Serverless data warehouse.' },
    { id: 'databricks', name: 'Databricks', subtitle: 'Lakehouse SQL', description: 'Unified data platform.' },
];

interface StepConnectProps {
    onNext: (data: any) => void;
    isConnected?: boolean;
    onCancel?: () => void;
}

export default function StepConnect({ onNext, isConnected = false, onCancel }: StepConnectProps) {
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
        setPendingData(null);

        const matched = discoveredConnections.find(c => c.platform === newPlatform);
        if (matched) {
            setParams({ ...(matched.params || {}) });
        } else {
            setParams(newPlatform === 'databricks' ? { catalog: 'main', schema: 'default' } : {});
        }
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

            onNext({ ...data, params: selectedParams });
        } catch (err: any) {
            setError(err.message || String(err));
        } finally {
            setLoading(false);
            setConnectingId(null);
        }
    };

    const onSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        submitConnection(platform, params);
    };

    const handleSelectDiscovered = (conn: any, autoConnect = false) => {
        setHasUserSelectedPlatform(true);
        setPlatform(conn.platform);
        setParams({ ...(conn.params || {}) });
        setError(null);
        setWarning(null);
        setPendingData(null);

        if (autoConnect) {
            submitConnection(conn.platform, conn.params || {}, conn.id);
        }
    };

    const updateParam = (key: string, value: any) => {
        setParams((prev: any) => ({ ...prev, [key]: value }));
    };

    const snowflakeAuthMode = params.auth_method || ((params.private_key_path || params.private_key) ? 'keypair' : 'password');
    const databricksAuthMode = params.dbx_auth_method || ((params.client_id && !params.access_token) ? 'oauth_m2m' : 'pat');

    const inputClass = "w-full bg-slate-950/90 border border-slate-700/90 rounded-lg px-3 py-2 text-sm text-slate-100 placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500 transition-colors";
    const labelClass = "block text-xs font-medium text-slate-400 mb-1.5";
    const subCardClass = "bg-slate-900/70 border border-slate-800 rounded-xl p-4 space-y-4";

    const hasDiscovered = discoveredConnections.length > 0;

    return (
        <div className="space-y-6 max-w-5xl mx-auto">
            {/* Header */}
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-4 border-b border-slate-800">
                <div>
                    <div className="flex items-center gap-2.5">
                        <h2 className="text-xl font-bold tracking-tight text-white">Connect Data Warehouse</h2>
                        {isConnected && (
                            <span className="bg-emerald-950/70 text-emerald-400 border border-emerald-700/50 text-[11px] font-medium py-0.5 px-2.5 rounded-full inline-flex items-center gap-1.5">
                                <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
                                Active Session
                            </span>
                        )}
                    </div>
                    <p className="text-sm text-slate-400 mt-1">
                        Choose an auto-detected profile or configure target endpoint and authentication credentials.
                    </p>
                </div>

                {onCancel && (
                    <button
                        type="button"
                        onClick={onCancel}
                        className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-700 bg-slate-800/80 hover:bg-slate-700 text-xs font-medium text-slate-200 transition-colors self-start sm:self-auto"
                    >
                        <ArrowLeft className="w-3.5 h-3.5" />
                        Back to Studio
                    </button>
                )}
            </div>

            <div className={`grid grid-cols-1 ${hasDiscovered ? 'lg:grid-cols-12 gap-6 items-start' : 'max-w-2xl mx-auto'}`}>
                {/* Left Column: Auto-Discovered Profiles */}
                {hasDiscovered && (
                    <div className="lg:col-span-5 bg-gradient-to-b from-blue-950/35 via-indigo-950/20 to-slate-900/70 border border-blue-500/30 rounded-xl p-4 space-y-3.5">
                        <div className="flex items-center justify-between">
                            <span className="text-xs font-semibold text-blue-300 uppercase tracking-wider flex items-center gap-1.5">
                                <Sparkles className="w-3.5 h-3.5 text-blue-400" />
                                Auto-Detected Profiles ({discoveredConnections.length})
                            </span>
                            <span className="text-[11px] text-slate-400">CLI &amp; dbt</span>
                        </div>

                        <div className="space-y-2.5">
                            {discoveredConnections.map(conn => {
                                const isConnectingThis = loading && connectingId === conn.id;
                                const isActiveSelection =
                                    platform === conn.platform &&
                                    ((conn.platform === 'snowflake' && params.account === conn.params?.account && params.database === conn.params?.database) ||
                                        (conn.platform === 'databricks' && params.server_hostname === conn.params?.server_hostname) ||
                                        (conn.platform === 'bigquery' && params.project === conn.params?.project));

                                return (
                                    <div
                                        key={conn.id}
                                        className={`p-3 rounded-xl border transition-all space-y-2.5 ${
                                            isActiveSelection
                                                ? 'bg-slate-900/95 border-blue-500/60 shadow-md shadow-blue-950/30'
                                                : 'bg-slate-900/75 hover:bg-slate-900 border-slate-700/80 hover:border-blue-500/40'
                                        }`}
                                    >
                                        <div className="space-y-1">
                                            <div className="flex items-center justify-between gap-2">
                                                <span className="text-xs font-semibold text-white truncate">{conn.name}</span>
                                                <span className="text-[10px] uppercase font-mono px-1.5 py-0.5 rounded bg-slate-800 text-blue-300 border border-slate-700 shrink-0">
                                                    {conn.platform}
                                                </span>
                                            </div>
                                            <p className="text-[11px] text-slate-400 font-mono leading-relaxed break-all">
                                                {conn.description}
                                            </p>
                                        </div>

                                        <div className="flex items-center justify-between gap-2 pt-1 border-t border-slate-800/80">
                                            <span className="text-[10px] text-slate-500 font-mono truncate">
                                                via {conn.source}
                                            </span>
                                            <div className="flex items-center gap-1.5 shrink-0">
                                                <button
                                                    type="button"
                                                    onClick={() => handleSelectDiscovered(conn, false)}
                                                    className="px-2.5 py-1 text-slate-300 hover:text-white bg-slate-800 hover:bg-slate-700 text-xs rounded-md border border-slate-700 transition-colors"
                                                    title="Inspect or modify credentials in form"
                                                >
                                                    Review
                                                </button>
                                                <button
                                                    type="button"
                                                    disabled={loading}
                                                    onClick={() => handleSelectDiscovered(conn, true)}
                                                    className="px-3 py-1 bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white text-xs font-medium rounded-md shadow-sm transition-all flex items-center gap-1.5"
                                                >
                                                    {isConnectingThis ? (
                                                        <>
                                                            <Loader2 className="w-3 h-3 animate-spin" />
                                                            Connecting
                                                        </>
                                                    ) : (
                                                        <>
                                                            <Check className="w-3 h-3" />
                                                            Connect
                                                        </>
                                                    )}
                                                </button>
                                            </div>
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    </div>
                )}

                {/* Right Column: Platform Cards & Connection Form */}
                <form onSubmit={onSubmit} className={`${hasDiscovered ? 'lg:col-span-7' : ''} space-y-5`}>
                    {/* Platform Selector Tiles */}
                    <div>
                        <label className={labelClass}>Target Warehouse Platform</label>
                        <div className="grid grid-cols-2 sm:grid-cols-4 gap-2.5">
                            {PLATFORMS.map(p => {
                                const isDiscovered = discoveredConnections.some(c => c.platform === p.id);
                                const selected = platform === p.id;
                                return (
                                    <button
                                        key={p.id}
                                        type="button"
                                        onClick={() => handlePlatformChange(p.id)}
                                        className={`relative p-3 rounded-xl border text-left transition-all ${
                                            selected
                                                ? 'bg-blue-600/15 border-blue-500 ring-1 ring-blue-500/70 shadow-sm'
                                                : 'bg-slate-900/70 border-slate-700/80 hover:border-slate-600'
                                        }`}
                                    >
                                        {isDiscovered && (
                                            <span
                                                className="absolute top-2 right-2 w-2 h-2 rounded-full bg-emerald-400"
                                                title="Local credentials detected"
                                            />
                                        )}
                                        <div className={`font-semibold text-sm ${selected ? 'text-blue-300' : 'text-slate-200'}`}>
                                            {p.name}
                                        </div>
                                        <div className="text-[11px] text-slate-400 mt-0.5 truncate">{p.subtitle}</div>
                                    </button>
                                );
                            })}
                        </div>
                    </div>

                    {/* Platform-Specific Structured Sections */}
                    {platform === 'duckdb' && (
                        <div className={subCardClass}>
                            <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-slate-300">
                                <Database className="w-3.5 h-3.5 text-blue-400" />
                                Local DuckDB Engine
                            </div>
                            <div>
                                <label className={labelClass}>Database File Path</label>
                                <input
                                    type="text"
                                    value={params.path || ''}
                                    placeholder="data/identity_graph.db"
                                    className={inputClass}
                                    onChange={e => updateParam('path', e.target.value)}
                                />
                                <p className="text-[11px] text-slate-500 mt-1.5">
                                    Leave blank to run in-memory (<code>:memory:</code>), or point to a persistent <code>.duckdb</code> file.
                                </p>
                            </div>
                        </div>
                    )}

                    {platform === 'snowflake' && (
                        <div className="space-y-4">
                            {/* Section 1: Endpoint & Scope */}
                            <div className={subCardClass}>
                                <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-slate-300">
                                    <Server className="w-3.5 h-3.5 text-blue-400" />
                                    Target Endpoint &amp; Scope
                                </div>

                                <div>
                                    <label className={labelClass}>Account Identifier</label>
                                    <input
                                        type="text"
                                        required
                                        value={params.account || ''}
                                        placeholder="xy12345.us-east-1 or ORG-ACCOUNT"
                                        className={inputClass}
                                        onChange={e => updateParam('account', e.target.value)}
                                    />
                                    <p className="text-[11px] text-slate-500 mt-1">
                                        Use account locator (<code>xy12345.us-east-1</code>) or <code>ORGNAME-ACCOUNTNAME</code>.
                                    </p>
                                </div>

                                <div className="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
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
                                        <label className={labelClass}>Default Schema</label>
                                        <input
                                            type="text"
                                            required
                                            value={params.schema || 'PUBLIC'}
                                            className={inputClass}
                                            onChange={e => updateParam('schema', e.target.value)}
                                        />
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
                                    <div>
                                        <label className={labelClass}>Role (Optional)</label>
                                        <input
                                            type="text"
                                            value={params.role || ''}
                                            placeholder="SYSADMIN"
                                            className={inputClass}
                                            onChange={e => updateParam('role', e.target.value.trim())}
                                        />
                                    </div>
                                </div>
                            </div>

                            {/* Section 2: Authentication */}
                            <div className={subCardClass}>
                                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2">
                                    <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-slate-300">
                                        <KeyRound className="w-3.5 h-3.5 text-emerald-400" />
                                        Authentication
                                    </div>
                                    {/* Dark-mode segmented toggle */}
                                    <div className="inline-flex rounded-lg bg-slate-950 p-1 border border-slate-800">
                                        {[
                                            { id: 'password', label: 'Password' },
                                            { id: 'keypair', label: 'RSA Key-Pair (JWT)' },
                                        ].map(opt => {
                                            const active = snowflakeAuthMode === opt.id;
                                            return (
                                                <button
                                                    key={opt.id}
                                                    type="button"
                                                    onClick={() => {
                                                        updateParam('auth_method', opt.id);
                                                        if (opt.id === 'keypair') {
                                                            updateParam('authenticator', 'SNOWFLAKE_JWT');
                                                            updateParam('password', '');
                                                        } else {
                                                            updateParam('authenticator', '');
                                                        }
                                                    }}
                                                    className={`px-3 py-1 rounded-md text-xs font-medium transition-colors ${
                                                        active
                                                            ? 'bg-blue-600 text-white shadow-sm'
                                                            : 'text-slate-400 hover:text-slate-200'
                                                    }`}
                                                >
                                                    {opt.label}
                                                </button>
                                            );
                                        })}
                                    </div>
                                </div>

                                <div>
                                    <label className={labelClass}>User / Service Account</label>
                                    <input
                                        type="text"
                                        required
                                        value={params.user || ''}
                                        className={inputClass}
                                        onChange={e => updateParam('user', e.target.value)}
                                    />
                                </div>

                                {snowflakeAuthMode === 'keypair' ? (
                                    <div className="space-y-3">
                                        <div>
                                            <label className={labelClass}>Private Key File Path (.p8)</label>
                                            <input
                                                type="text"
                                                value={params.private_key_path || ''}
                                                placeholder="/path/to/rsa_key.p8 (or paste PEM below)"
                                                className={inputClass}
                                                onChange={e => updateParam('private_key_path', e.target.value.trim())}
                                            />
                                        </div>
                                        <div>
                                            <label className={labelClass}>Or Private Key PEM (Optional if path set)</label>
                                            <textarea
                                                rows={2}
                                                value={params.private_key || ''}
                                                placeholder="-----BEGIN PRIVATE KEY-----"
                                                className={`${inputClass} font-mono text-xs`}
                                                onChange={e => updateParam('private_key', e.target.value)}
                                            />
                                        </div>
                                        <div>
                                            <label className={labelClass}>Key Passphrase (Optional)</label>
                                            <input
                                                type="password"
                                                value={params.private_key_passphrase || ''}
                                                className={inputClass}
                                                onChange={e => updateParam('private_key_passphrase', e.target.value)}
                                            />
                                        </div>
                                    </div>
                                ) : (
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
                                )}
                            </div>
                        </div>
                    )}

                    {platform === 'bigquery' && (
                        <div className={subCardClass}>
                            <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-slate-300">
                                <ShieldCheck className="w-3.5 h-3.5 text-blue-400" />
                                Google BigQuery Project &amp; Credentials
                            </div>
                            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
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
                                    <label className={labelClass}>Default Dataset / Schema</label>
                                    <input
                                        type="text"
                                        value={params.dataset || params.schema || ''}
                                        placeholder="default"
                                        className={inputClass}
                                        onChange={e => updateParam('dataset', e.target.value)}
                                    />
                                </div>
                            </div>
                            <div>
                                <label className={labelClass}>Service Account JSON (Optional if gcloud ADC is active)</label>
                                <textarea
                                    className={`${inputClass} font-mono text-xs`}
                                    rows={3}
                                    value={params.credentials_json || ''}
                                    placeholder="{ ... } (leave empty to use Google Cloud ADC)"
                                    onChange={e => updateParam('credentials_json', e.target.value)}
                                />
                            </div>
                        </div>
                    )}

                    {platform === 'databricks' && (
                        <div className="space-y-4">
                            <div className={subCardClass}>
                                <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-slate-300">
                                    <Server className="w-3.5 h-3.5 text-blue-400" />
                                    SQL Warehouse &amp; Unity Catalog
                                </div>
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
                                </div>
                                <div className="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
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
                                        <label className={labelClass}>Default Schema</label>
                                        <input
                                            type="text"
                                            value={params.schema || 'default'}
                                            placeholder="default"
                                            className={inputClass}
                                            onChange={e => updateParam('schema', e.target.value.trim())}
                                        />
                                    </div>
                                </div>
                            </div>

                            <div className={subCardClass}>
                                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2">
                                    <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-slate-300">
                                        <KeyRound className="w-3.5 h-3.5 text-emerald-400" />
                                        Authentication
                                    </div>
                                    <div className="inline-flex rounded-lg bg-slate-950 p-1 border border-slate-800">
                                        {[
                                            { id: 'pat', label: 'Personal Token (PAT)' },
                                            { id: 'oauth_m2m', label: 'OAuth M2M (SP)' },
                                        ].map(opt => {
                                            const active = databricksAuthMode === opt.id;
                                            return (
                                                <button
                                                    key={opt.id}
                                                    type="button"
                                                    onClick={() => {
                                                        updateParam('dbx_auth_method', opt.id);
                                                        if (opt.id === 'oauth_m2m') {
                                                            updateParam('access_token', '');
                                                        }
                                                    }}
                                                    className={`px-3 py-1 rounded-md text-xs font-medium transition-colors ${
                                                        active
                                                            ? 'bg-blue-600 text-white shadow-sm'
                                                            : 'text-slate-400 hover:text-slate-200'
                                                    }`}
                                                >
                                                    {opt.label}
                                                </button>
                                            );
                                        })}
                                    </div>
                                </div>

                                {databricksAuthMode === 'oauth_m2m' ? (
                                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
                                        <div>
                                            <label className={labelClass}>OAuth Client ID</label>
                                            <input
                                                type="text"
                                                required
                                                value={params.client_id || ''}
                                                placeholder="Service Principal UUID"
                                                className={inputClass}
                                                onChange={e => updateParam('client_id', e.target.value.trim())}
                                            />
                                        </div>
                                        <div>
                                            <label className={labelClass}>OAuth Client Secret</label>
                                            <input
                                                type="password"
                                                required
                                                value={params.client_secret || ''}
                                                placeholder="dose..."
                                                className={inputClass}
                                                onChange={e => updateParam('client_secret', e.target.value.trim())}
                                            />
                                        </div>
                                    </div>
                                ) : (
                                    <div>
                                        <label className={labelClass}>Personal Access Token</label>
                                        <input
                                            type="password"
                                            required
                                            value={params.access_token || ''}
                                            placeholder="dapi..."
                                            className={inputClass}
                                            onChange={e => updateParam('access_token', e.target.value.trim())}
                                        />
                                    </div>
                                )}
                            </div>
                        </div>
                    )}

                    {error && (
                        <div className="bg-rose-950/50 border border-rose-500/40 rounded-xl p-3.5 text-sm text-rose-300">
                            {error}
                        </div>
                    )}

                    {warning && (
                        <div className="bg-amber-950/40 border border-amber-500/40 rounded-xl p-3.5 text-sm text-amber-300">
                            <p className="font-semibold mb-1">Permissions Warning</p>
                            <p className="text-xs text-amber-200/90">{warning}</p>
                            <p className="text-[11px] mt-2 text-amber-400/80">Click Confirm &amp; Proceed to continue in read-only review mode.</p>
                        </div>
                    )}

                    <button
                        type="submit"
                        disabled={loading}
                        className="w-full py-2.5 px-4 bg-blue-600 hover:bg-blue-500 text-white rounded-xl font-semibold text-sm shadow-lg shadow-blue-950/30 transition-all flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                        {loading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" />
                                Connecting to {PLATFORMS.find(p => p.id === platform)?.name}...
                            </>
                        ) : (
                            warning ? 'Confirm & Proceed' : `Connect to ${PLATFORMS.find(p => p.id === platform)?.name}`
                        )}
                    </button>
                </form>
            </div>
        </div>
    );
}
