import { useState } from 'react';
import { Loader2, RefreshCw } from "lucide-react";
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
    const [error, setError] = useState<string | null>(null);
    const [warning, setWarning] = useState<string | null>(null);
    const [pendingData, setPendingData] = useState<any>(null);

    const onSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        submitConnection(platform, params);
    };

    const submitConnection = async (selectedPlatform: string, selectedParams: any) => {
        // If user acknowledges warning
        if (warning && pendingData) {
            onNext(pendingData);
            return;
        }

        setLoading(true);
        setError(null);
        setWarning(null);

        try {
            const data = await api.setupConnect(selectedPlatform, selectedParams);

            // If connection successful but has warnings (e.g. existing tables)
            if (data.warning) {
                setWarning(data.warning);
                setPendingData(data);
                setLoading(false);
                return;
            }

            onNext(data);
        } catch (err: any) {
            setError(err.message || String(err));
        } finally {
            setLoading(false);
        }
    };

    const updateParam = (key: string, value: any) => {
        setParams((prev: any) => ({ ...prev, [key]: value }));
    };

    const inputClass = "w-full bg-gray-900 border border-gray-700 rounded-md px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500";
    const labelClass = "block text-xs font-medium text-gray-400 mb-1";

    return (
        <div className="space-y-6">
            <div className="text-center pb-4 border-b border-gray-800">
                <h2 className="text-xl font-semibold text-white">Connect Data Warehouse</h2>
                <p className="text-sm text-gray-400 mt-1">Select your platform and provide connection details.</p>
                {isConnected && (
                    <div className="mt-2 bg-green-900/20 text-green-400 text-xs py-1 px-3 rounded-full inline-flex items-center gap-2">
                        <RefreshCw className="w-3 h-3" />
                        API Connected
                    </div>
                )}
            </div>

            <form onSubmit={onSubmit} className="space-y-6 max-w-lg mx-auto">
                <div className="grid grid-cols-2 gap-3">
                    {PLATFORMS.map(p => (
                        <button
                            key={p.id}
                            type="button"
                            onClick={() => { setPlatform(p.id); setParams({}); setError(null); }}
                            className={`p-3 rounded-lg border text-left transition-all ${platform === p.id
                                ? 'bg-blue-600/10 border-blue-500 ring-1 ring-blue-500'
                                : 'bg-gray-800 border-gray-700 hover:border-gray-600'
                                }`}
                        >
                            <div className={`font-semibold text-sm ${platform === p.id ? 'text-blue-400' : 'text-gray-200'}`}>
                                {p.name}
                            </div>
                            <div className="text-xs text-gray-500 mt-1">{p.description}</div>
                        </button>
                    ))}
                </div>

                <div className="space-y-4 bg-gray-800/50 p-4 rounded-lg border border-gray-700/50">
                    {platform === 'duckdb' && (
                        <div>
                            <label className={labelClass}>Database Path</label>
                            <input
                                type="text"
                                placeholder="data/identity_graph.db"
                                className={inputClass}
                                onChange={e => updateParam('path', e.target.value)}
                            />
                            <p className="text-xs text-gray-600 mt-1">Leave empty for in-memory (data lost on restart).</p>
                        </div>
                    )}

                    {platform === 'snowflake' && (
                        <>
                            <div>
                                <label className={labelClass}>Account</label>
                                <input type="text" required className={inputClass} onChange={e => updateParam('account', e.target.value)} />
                            </div>
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <label className={labelClass}>User</label>
                                    <input type="text" required className={inputClass} onChange={e => updateParam('user', e.target.value)} />
                                </div>
                                <div>
                                    <label className={labelClass}>Password</label>
                                    <input type="password" required className={inputClass} onChange={e => updateParam('password', e.target.value)} />
                                </div>
                            </div>
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <label className={labelClass}>Database</label>
                                    <input type="text" required className={inputClass} onChange={e => updateParam('database', e.target.value)} />
                                </div>
                                <div>
                                    <label className={labelClass}>Schema</label>
                                    <input type="text" required className={inputClass} onChange={e => updateParam('schema', e.target.value)} />
                                </div>
                            </div>
                            <div>
                                <label className={labelClass}>Warehouse</label>
                                <input type="text" required className={inputClass} onChange={e => updateParam('warehouse', e.target.value)} />
                            </div>
                        </>
                    )}

                    {platform === 'bigquery' && (
                        <>
                            <div>
                                <label className={labelClass}>Project ID</label>
                                <input type="text" required className={inputClass} onChange={e => updateParam('project', e.target.value)} />
                            </div>
                            <div>
                                <label className={labelClass}>Dataset</label>
                                <input type="text" required className={inputClass} onChange={e => updateParam('dataset', e.target.value)} />
                            </div>
                            <div>
                                <label className={labelClass}>Credentials JSON (Path or Content)</label>
                                <textarea
                                    className={`${inputClass} font-mono text-xs`}
                                    rows={3}
                                    placeholder="{ ... }"
                                    onChange={e => updateParam('credentials_json', e.target.value)}
                                />
                            </div>
                        </>
                    )}

                    {platform === 'databricks' && (
                        <>
                            <div>
                                <label className={labelClass}>Server Hostname</label>
                                <input type="text" required className={inputClass} onChange={e => updateParam('server_hostname', e.target.value)} />
                            </div>
                            <div>
                                <label className={labelClass}>HTTP Path</label>
                                <input type="text" required className={inputClass} onChange={e => updateParam('http_path', e.target.value)} />
                            </div>
                            <div>
                                <label className={labelClass}>Access Token</label>
                                <input type="password" required className={inputClass} onChange={e => updateParam('access_token', e.target.value)} />
                            </div>
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <label className={labelClass}>Catalog</label>
                                    <input type="text" required className={inputClass} onChange={e => updateParam('catalog', e.target.value)} />
                                </div>
                                <div>
                                    <label className={labelClass}>Schema</label>
                                    <input type="text" required className={inputClass} onChange={e => updateParam('schema', e.target.value)} />
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
                    {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : (warning ? 'Confirm & Proceed' : 'Connect')}
                </button>
            </form>
        </div>
    );
}
