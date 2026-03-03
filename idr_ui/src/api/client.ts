/**
 * API Client for IDR Backend
 */

import type { IDRConfig, SetupConnectionData, SetupRunResult, TableColumn, WarehousePlatform } from '../types'
import { redactSensitiveText } from '../security/redaction'

const rawBaseUrl = import.meta.env.VITE_API_BASE_URL?.trim()
const BASE_URL = (rawBaseUrl && rawBaseUrl.length > 0 ? rawBaseUrl : '/api').replace(/\/$/, '')


let getToken: (() => string | undefined) | null = null;
type AuthFailureStatus = 401 | 403

export interface APIAuthFailureEvent {
    status: AuthFailureStatus
    path: string
    error: APIError
}

type APIAuthFailureListener = (event: APIAuthFailureEvent) => void
const authFailureListeners = new Set<APIAuthFailureListener>()

export const setTokenGetter = (fn: () => string | undefined) => {
    getToken = fn;
}

export const onAPIAuthFailure = (listener: APIAuthFailureListener): (() => void) => {
    authFailureListeners.add(listener)
    return () => authFailureListeners.delete(listener)
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

export interface WhoAmIResponse {
    sub?: string | null
    auth_type?: string
    roles?: string[]
    permissions?: string[]
    scope?: string | null
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

function resolveErrorDetail(payload: unknown): string | null {
    if (!payload || typeof payload !== 'object') {
        return null
    }

    const data = payload as Record<string, unknown>
    const candidates = [data.detail, data.message, data.error]

    for (const candidate of candidates) {
        if (typeof candidate === 'string' && candidate.trim()) {
            return redactSensitiveText(candidate.trim())
        }
        if (Array.isArray(candidate)) {
            const joined = candidate
                .filter((item): item is string => typeof item === 'string')
                .map((item) => item.trim())
                .filter(Boolean)
                .join('; ')
            if (joined) {
                return redactSensitiveText(joined)
            }
        }
    }

    return null
}

function emitAuthFailure(status: AuthFailureStatus, path: string, error: APIError): void {
    authFailureListeners.forEach((listener) => {
        try {
            listener({ status, path, error })
        } catch {
            // Keep failures isolated to listener code.
        }
    })
}

async function parseError(response: Response): Promise<APIError> {
    let detail = `API error: ${response.status}`
    try {
        const contentType = response.headers.get('content-type') || ''
        if (contentType.includes('application/json')) {
            const data = await response.json()
            detail = resolveErrorDetail(data) || detail
        } else {
            const text = await response.text()
            if (text.trim()) {
                detail = redactSensitiveText(text.trim())
            }
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
        const error = await parseError(response)
        if (error.status === 401 || error.status === 403) {
            emitAuthFailure(error.status as AuthFailureStatus, path, error)
        }
        throw error
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
    getWhoAmI: () => fetchJson<WhoAmIResponse>('/auth/whoami'),

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

    getSetupConfig: () => fetchJson<IDRConfig>('/setup/config'),

    setupConnect: (platform: WarehousePlatform, params: Record<string, string>) => fetchJson<SetupConnectionData>('/setup/connect', {
        method: 'POST',
        body: JSON.stringify({ platform, params }),
    }),

    discoverTables: (schema?: string) => fetchJson<{
        tables: string[]
    }>(`/setup/discover/tables${schema ? `?schema=${encodeURIComponent(schema)}` : ''}`),

    discoverColumns: (table: string) => fetchJson<{ columns: TableColumn[] }>(`/setup/discover/columns?table=${encodeURIComponent(table)}`),

    getFuzzyTemplates: () => fetchJson<{
        templates: Array<{
            id: string
            label: string
            sql_template: string
            default_threshold: number
            description: string
        }>
    }>('/setup/fuzzy-templates'),

    saveSetupConfig: (config: IDRConfig) => fetchJson<{
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
    }) => fetchJson<SetupRunResult>('/setup/run', {
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
