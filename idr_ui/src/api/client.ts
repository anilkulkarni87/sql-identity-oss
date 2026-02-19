/**
 * API Client for IDR Backend
 */

const rawBaseUrl = import.meta.env.VITE_API_BASE_URL?.trim()
const BASE_URL = (rawBaseUrl && rawBaseUrl.length > 0 ? rawBaseUrl : '/api').replace(/\/$/, '')


let getToken: (() => string | undefined) | null = null;

export const setTokenGetter = (fn: () => string | undefined) => {
    getToken = fn;
}

export class APIError extends Error {
    status: number
    detail: string

    constructor(status: number, detail: string) {
        super(detail)
        this.name = 'APIError'
        this.status = status
        this.detail = detail
    }
}

function buildUrl(path: string): string {
    return `${BASE_URL}${path.startsWith('/') ? path : `/${path}`}`
}

function buildHeaders(options: RequestInit): Headers {
    const headers = new Headers(options.headers || {})
    if (options.body !== undefined && !headers.has('Content-Type')) {
        headers.set('Content-Type', 'application/json')
    }

    if (getToken) {
        const token = getToken()
        if (token) {
            headers.set('Authorization', `Bearer ${token}`)
        }
    }

    return headers
}

async function parseError(response: Response): Promise<APIError> {
    let detail = `API error: ${response.status}`
    try {
        const data = await response.json()
        if (data && typeof data.detail === 'string') {
            detail = data.detail
        }
    } catch {
        // Keep default detail when response is not JSON.
    }
    return new APIError(response.status, detail)
}

export async function apiRequest(path: string, options: RequestInit = {}): Promise<Response> {
    return fetch(buildUrl(path), {
        ...options,
        headers: buildHeaders(options),
    })
}

async function fetchJson<T>(path: string, options: RequestInit = {}): Promise<T> {
    const response = await apiRequest(path, options)
    if (!response.ok) {
        throw await parseError(response)
    }

    if (response.status === 204) {
        return undefined as T
    }

    const text = await response.text()
    if (!text) {
        return undefined as T
    }
    return JSON.parse(text) as T
}

export const api = {
    connect: (payload: Record<string, string>) => fetchJson<{
        status: string
        platform: string
        message: string
    }>('/connect', {
        method: 'POST',
        body: JSON.stringify(payload),
    }),

    // Dashboard Metrics
    getMetricsSummary: () => fetchJson<{
        total_clusters: number
        total_entities: number
        total_edges: number
        avg_confidence: number
        last_run_id: string | null
        last_run_duration: number | null
        last_run_started_at: string | null
    }>('/metrics/summary'),

    getClusterDistribution: () => fetchJson<Array<{
        bucket: string
        count: number
    }>>('/metrics/distribution'),

    getRuleStats: () => fetchJson<Array<{
        rule_id: string
        identifier_type: string | null
        edges_created: number
        percentage: number
    }>>('/metrics/rules'),

    getAlerts: () => fetchJson<Array<{
        severity: string
        message: string
        count: number | null
    }>>('/alerts'),

    // Entity/Cluster Search
    searchEntities: (query: string) => fetchJson<Array<{
        resolved_id: string
        cluster_size: number
        confidence_score: number | null
    }>>(`/entities/search?q=${encodeURIComponent(query)}`),

    getCluster: (clusterId: string) => fetchJson<{
        resolved_id: string
        cluster_size: number
        confidence_score: number | null
        entities: Array<{
            entity_key: string
            source_id: string
            source_key: string
        }>
        edges: Array<{
            left_entity_key: string
            right_entity_key: string
            identifier_type: string
            identifier_value: string
            rule_id: string
        }>
    }>(`/clusters/${encodeURIComponent(clusterId)}`),

    // Run History
    getRuns: (limit = 20) => fetchJson<Array<{
        run_id: string
        run_mode: string
        status: string
        started_at: string
        duration_seconds: number | null
        entities_processed: number
        edges_created: number
        clusters_impacted: number
    }>>(`/runs?limit=${limit}`),

    // System Health
    getHealth: () => fetchJson<{
        status: string
        connected: boolean
        platform: string | null
    }>('/health'),

    // Setup Status
    getSetupStatus: () => fetchJson<{
        connected: boolean
        configured: boolean
        platform: string | null
    }>('/setup/status'),

    getSetupConfig: () => fetchJson<{
        sources: Array<Record<string, unknown>>
        rules?: Array<Record<string, unknown>>
        fuzzy_rules?: Array<Record<string, unknown>>
        survivorship?: Array<Record<string, unknown>>
    }>('/setup/config'),

    setupConnect: (platform: string, params: Record<string, unknown>) => fetchJson<{
        status: string
        platform: string
        warning?: string
    }>('/setup/connect', {
        method: 'POST',
        body: JSON.stringify({ platform, params }),
    }),

    discoverTables: (schema?: string) => fetchJson<{
        tables: string[]
    }>(`/setup/discover/tables${schema ? `?schema=${encodeURIComponent(schema)}` : ''}`),

    discoverColumns: (table: string) => fetchJson<{
        columns: Array<{ name: string; type: string }>
    }>(`/setup/discover/columns?table=${encodeURIComponent(table)}`),

    getFuzzyTemplates: () => fetchJson<{
        templates: Array<{
            id: string
            label: string
            sql_template: string
            default_threshold: number
            description: string
        }>
    }>('/setup/fuzzy-templates'),

    saveSetupConfig: (config: unknown) => fetchJson<{
        status: string
        message: string
    }>('/setup/config/save', {
        method: 'POST',
        body: JSON.stringify({ config }),
    }),

    runSetup: (payload: {
        mode: 'INCR' | 'FULL'
        strict: boolean
        max_iterations: number
        dry_run: boolean
    }) => fetchJson<Record<string, unknown>>('/setup/run', {
        method: 'POST',
        body: JSON.stringify(payload),
    }),

    // Schema Docs
    getSchema: () => fetchJson<Array<{
        schema_name: string
        table_name: string
        fqn: string
        description: string | null
        columns: Array<{
            name: string
            type: string
            is_pk: boolean
            description: string | null
        }>
    }>>('/schema')
}
