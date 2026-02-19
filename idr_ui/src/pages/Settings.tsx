import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Database, Cloud, CheckCircle, XCircle, Loader2 } from 'lucide-react'
import { api } from '../api/client'

interface HealthStatus {
    connected: boolean
    configured: boolean
    platform: string | null
}

export default function Settings() {
    const queryClient = useQueryClient()
    const [platform, setPlatform] = useState('duckdb')

    // DuckDB
    const [database, setDatabase] = useState('')

    // BigQuery
    const [projectId, setProjectId] = useState('')

    // Snowflake
    const [sfAccount, setSfAccount] = useState('')
    const [sfUser, setSfUser] = useState('')
    const [sfPassword, setSfPassword] = useState('')
    const [sfWarehouse, setSfWarehouse] = useState('')
    const [sfDatabase, setSfDatabase] = useState('')

    // Databricks
    const [dbHostname, setDbHostname] = useState('')
    const [dbHttpPath, setDbHttpPath] = useState('')
    const [dbToken, setDbToken] = useState('')
    const [dbCatalog, setDbCatalog] = useState('')

    const { data: health } = useQuery<HealthStatus>({
        queryKey: ['setupStatus'],
        queryFn: () => api.getSetupStatus(),
        refetchInterval: 5000
    })

    const connectMutation = useMutation({
        mutationFn: async () => {
            const body: Record<string, string> = { platform }

            if (platform === 'duckdb') {
                body.database = database
            } else if (platform === 'bigquery') {
                body.project_id = projectId
            } else if (platform === 'snowflake') {
                body.account = sfAccount
                body.user = sfUser
                body.password = sfPassword
                body.warehouse = sfWarehouse
                body.sf_database = sfDatabase
            } else if (platform === 'databricks') {
                body.server_hostname = dbHostname
                body.http_path = dbHttpPath
                body.access_token = dbToken
                body.catalog = dbCatalog
            }

            return api.connect(body)
        },
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['setupStatus'] })
            queryClient.invalidateQueries({ queryKey: ['metrics'] })
        }
    })

    return (
        <div className="space-y-6 max-w-2xl">
            <h1 className="text-2xl font-bold">Settings</h1>

            {/* Connection Status */}
            <div className="bg-gray-800 rounded-xl p-6">
                <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
                    <Database className="w-5 h-5" />
                    Connection Status
                </h2>

                <div className="flex items-center gap-3">
                    {health?.connected ? (
                        <>
                            <CheckCircle className="w-6 h-6 text-green-400" />
                            <div>
                                <p className="font-medium text-green-400">Connected</p>
                                <p className="text-sm text-gray-400">
                                    Platform: {health.platform}
                                </p>
                            </div>
                        </>
                    ) : (
                        <>
                            <XCircle className="w-6 h-6 text-red-400" />
                            <div>
                                <p className="font-medium text-red-400">Not Connected</p>
                                <p className="text-sm text-gray-400">
                                    Configure a connection below
                                </p>
                            </div>
                        </>
                    )}
                </div>
            </div>

            {/* Connection Form */}
            <div className="bg-gray-800 rounded-xl p-6">
                <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
                    <Cloud className="w-5 h-5" />
                    Connect to Database
                </h2>

                {/* Platform Selection */}
                <div className="mb-4">
                    <label className="block text-sm text-gray-400 mb-2">Platform</label>
                    <div className="grid grid-cols-4 gap-2">
                        {[
                            { id: 'duckdb', label: 'DuckDB', icon: '🦆' },
                            { id: 'bigquery', label: 'BigQuery', icon: '☁️' },
                            { id: 'snowflake', label: 'Snowflake', icon: '❄️' },
                            { id: 'databricks', label: 'Databricks', icon: '🧱' },
                        ].map((p) => (
                            <button
                                key={p.id}
                                onClick={() => setPlatform(p.id)}
                                className={`p-3 rounded-lg border transition-colors ${platform === p.id
                                    ? 'border-blue-500 bg-blue-600/20'
                                    : 'border-gray-600 hover:border-gray-500'
                                    }`}
                            >
                                <span className="text-2xl">{p.icon}</span>
                                <p className="text-sm mt-1">{p.label}</p>
                            </button>
                        ))}
                    </div>
                </div>

                {/* DuckDB fields */}
                {platform === 'duckdb' && (
                    <div className="space-y-3">
                        <InputField
                            label="Database Path"
                            value={database}
                            onChange={setDatabase}
                            placeholder="/path/to/database.duckdb"
                            hint="Path relative to the API server, or absolute path"
                        />
                    </div>
                )}

                {/* BigQuery fields */}
                {platform === 'bigquery' && (
                    <div className="space-y-3">
                        <InputField
                            label="GCP Project ID"
                            value={projectId}
                            onChange={setProjectId}
                            placeholder="my-gcp-project"
                            hint="Requires GOOGLE_APPLICATION_CREDENTIALS env var on server"
                        />
                    </div>
                )}

                {/* Snowflake fields */}
                {platform === 'snowflake' && (
                    <div className="space-y-3">
                        <InputField
                            label="Account"
                            value={sfAccount}
                            onChange={setSfAccount}
                            placeholder="xyz12345.us-east-1"
                        />
                        <div className="grid grid-cols-2 gap-3">
                            <InputField
                                label="User"
                                value={sfUser}
                                onChange={setSfUser}
                                placeholder="username"
                            />
                            <InputField
                                label="Password"
                                value={sfPassword}
                                onChange={setSfPassword}
                                placeholder="••••••••"
                                type="password"
                            />
                        </div>
                        <div className="grid grid-cols-2 gap-3">
                            <InputField
                                label="Warehouse"
                                value={sfWarehouse}
                                onChange={setSfWarehouse}
                                placeholder="COMPUTE_WH"
                            />
                            <InputField
                                label="Database"
                                value={sfDatabase}
                                onChange={setSfDatabase}
                                placeholder="IDR_DB"
                            />
                        </div>
                    </div>
                )}

                {/* Databricks fields */}
                {platform === 'databricks' && (
                    <div className="space-y-3">
                        <InputField
                            label="Server Hostname"
                            value={dbHostname}
                            onChange={setDbHostname}
                            placeholder="adb-xxxxx.azuredatabricks.net"
                        />
                        <InputField
                            label="HTTP Path"
                            value={dbHttpPath}
                            onChange={setDbHttpPath}
                            placeholder="/sql/1.0/warehouses/xxxxx"
                        />
                        <InputField
                            label="Access Token"
                            value={dbToken}
                            onChange={setDbToken}
                            placeholder="dapi..."
                            type="password"
                        />
                        <InputField
                            label="Catalog"
                            value={dbCatalog}
                            onChange={setDbCatalog}
                            placeholder="main"
                        />
                    </div>
                )}

                {/* Connect Button */}
                <button
                    onClick={() => connectMutation.mutate()}
                    disabled={connectMutation.isPending}
                    className="w-full mt-4 py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-600 disabled:cursor-not-allowed rounded-lg font-medium transition-colors flex items-center justify-center gap-2"
                >
                    {connectMutation.isPending ? (
                        <>
                            <Loader2 className="w-5 h-5 animate-spin" />
                            Connecting...
                        </>
                    ) : (
                        'Connect'
                    )}
                </button>

                {/* Error/Success Messages */}
                {connectMutation.isError && (
                    <p className="mt-3 text-sm text-red-400">
                        ❌ {(connectMutation.error as Error).message}
                    </p>
                )}
                {connectMutation.isSuccess && (
                    <p className="mt-3 text-sm text-green-400">
                        ✓ Connected successfully!
                    </p>
                )}
            </div>
        </div>
    )
}

function InputField({
    label,
    value,
    onChange,
    placeholder,
    hint,
    type = 'text'
}: {
    label: string
    value: string
    onChange: (v: string) => void
    placeholder: string
    hint?: string
    type?: string
}) {
    return (
        <div>
            <label className="block text-sm text-gray-400 mb-1">{label}</label>
            <input
                type={type}
                value={value}
                onChange={(e) => onChange(e.target.value)}
                placeholder={placeholder}
                className="w-full px-4 py-2 bg-gray-700 rounded-lg border border-gray-600 focus:border-blue-500 focus:outline-none"
            />
            {hint && <p className="text-xs text-gray-500 mt-1">{hint}</p>}
        </div>
    )
}
