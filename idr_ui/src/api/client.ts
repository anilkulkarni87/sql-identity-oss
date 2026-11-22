import type { AIConfigContext, AIReview, IDRConfig, ReviewPatch, EvaluationOptions, ProfileValidation, ProfilePreviewResult } from "../types";

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

async function readAIStream<T>(response: Response, onProgress?: (event: Record<string, unknown>) => void): Promise<T> {
    if (!response.body) throw new APIError(502, 'Progress stream unavailable; retry explicitly.')
    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''
    let result: T | undefined
    const consume = (frame: string) => {
        const lines = frame.split('\n')
        const kind = lines.find(line => line.startsWith('event:'))?.slice(6).trim()
        const data = lines.filter(line => line.startsWith('data:')).map(line => line.slice(5).trim()).join('\n')
        if (!data) return
        const value = JSON.parse(data)
        if (kind === 'progress') onProgress?.(value)
        if (kind === 'result') result = value as T
        if (kind === 'error') throw new APIError(422, value.detail || 'AI operation failed')
    }
    try {
        while (true) {
            const {value, done} = await reader.read()
            buffer += done ? decoder.decode() : decoder.decode(value, {stream:true})
            const frames = buffer.split(/\r?\n\r?\n/)
            buffer = frames.pop() || ''
            frames.forEach(consume)
            if (done) { if (buffer.trim()) consume(buffer); break }
        }
        if (!result) throw new APIError(502, 'Stream ended before returning a result.')
        return result
    } finally { reader.releaseLock() }
}

export interface AIGenerationOptions {
    mode: 'config' | 'rules';
    current_config: IDRConfig;
    context: AIConfigContext;
    base_draft_hash?: string;
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

    getConfidenceDistribution: () => fetchJson<Array<{
        bucket: string
        count: number
        color: string
    }>>('/metrics/confidence'),

    getSourceContribution: () => fetchJson<Array<{
        source_id: string
        entities: number
        clusters: number
    }>>('/metrics/sources'),

    // Entity/Cluster Search
    searchEntities: (query: string) => fetchJson<Array<{
        resolved_id: string
        cluster_size: number
        confidence_score: number | null
    }>>(`/entities/search?q=${encodeURIComponent(query)}`),

    getCluster: (clusterId: string) => fetchJson<{
        resolved_id: string
        super_cluster_id?: string | null
        cluster_size: number
        confidence_score: number | null
        confidence_breakdown?: {
            verdict: string
            confidence_score: number | null
            subject_count: number
            child_count: number
            subject_fanout: number
            max_fanout: number
            edge_diversity: number
            edge_count: number
            match_density?: number | null
            has_fuzzy_bridge: boolean
            reason: string
        } | null
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
        golden_profile?: Record<string, unknown> | null
        crosswalk_history?: Array<{
            previous_resolved_id: string
            current_resolved_id: string
            change_type: string
            entities_moved: number
            run_id: string
            updated_at: string
        }> | null
        truncated?: boolean
    }>(`/clusters/${encodeURIComponent(clusterId)}`),

    manageOverride: (payload: {
        action: 'add' | 'remove' | 'list'
        override_type?: 'FORCE_LINK' | 'DO_NOT_LINK' | 'ISOLATE'
        entity_key_a?: string
        entity_key_b?: string
        reason?: string
    }) => fetchJson<{
        status: string
        action?: string
        overrides?: Array<Record<string, unknown>>
    }>('/overrides', {
        method: 'POST',
        body: JSON.stringify(payload),
    }),

    manageExclusion: (payload: {
        action?: 'add' | 'remove' | 'list'
        identifier_type?: string
        identifier_value_pattern?: string
        match_type?: 'EXACT' | 'LIKE'
        reason?: string
    }) => fetchJson<{
        status: string
        action?: string
        exclusions?: Array<Record<string, unknown>>
    }>('/exclusions', {
        method: 'POST',
        body: JSON.stringify(payload),
    }),

    privacyForget: (payload: {
        identifier_value?: string
        entity_key?: string
        resolved_id?: string
        dry_run?: boolean
        reason?: string
    }) => fetchJson<{
        request_id: string
        status: string
        dry_run: boolean
        target_hash: string
        resolved_id_hash: string
        resolved_ids: string[]
        entity_keys: string[]
        entities_purged: number
        edges_purged: number
        profiles_purged: number
        tables_cleaned: string[]
        executed_at: string
    }>('/privacy/forget', {
        method: 'POST',
        body: JSON.stringify(payload),
    }),

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

    getRunDetail: (runId: string) => fetchJson<{
        run_id: string
        run_mode: string
        status: string
        started_at: string
        ended_at?: string | null
        duration_seconds: number | null
        entities_processed: number
        edges_created: number
        clusters_impacted: number
        lp_iterations?: number | null
        error_message?: string | null
        warnings: string[]
        stages: Array<{
            stage_name: string
            stage_order: number
            rows_in?: number | null
            rows_out?: number | null
            duration_seconds?: number | null
            notes?: string | null
        }>
    }>(`/runs/${encodeURIComponent(runId)}`),

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

    getDiscoveredConnections: () => fetchJson<{
        connections: Array<{
            id: string
            source: string
            platform: string
            name: string
            description: string
            params: Record<string, string>
            masked_summary?: string
        }>
    }>('/setup/discovered-connections'),

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

    saveSetupConfigAsync: (config: unknown) => fetchJson<{
        status: string
        message: string
    }>('/setup/config/save/async', {
        method: 'POST',
        body: JSON.stringify({ config }),
    }),

    getSaveConfigProgress: () => fetchJson<{
        status: 'IDLE' | 'SAVING' | 'SUCCESS' | 'ERROR'
        step?: string
        label?: string
        percent: number
        detail?: string
        elapsed_seconds?: number
        message?: string
        error?: string
    }>('/setup/config/save/progress'),

    getConfigHistory: (limit = 15) => fetchJson<{
        snapshots: Array<{
            config_hash: string
            created_at: string
            sources_count: number
            rules_count: number
            exclusions_count: number
            config: Record<string, unknown>
        }>
    }>(`/setup/config/history?limit=${limit}`),

    runSetup: (payload: {
        mode: 'INCR' | 'FULL'
        strict: boolean
        max_iterations: number
        dry_run: boolean
        max_group_size?: number
        max_moved_entities_pct?: number
        max_cluster_delta_pct?: number
        lens?: string
        skip_lenses?: boolean
        config?: unknown
        ground_truth?: EvaluationOptions | Record<string, never>
    }) => fetchJson<Record<string, unknown>>('/setup/run', {
        method: 'POST',
        body: JSON.stringify(payload),
    }),

    previewProfiles: (payload: {config:IDRConfig; run_id?:string; draft_hash?:string; lens?:string; options:Record<string,unknown>}) => fetchJson<ProfileValidation | ProfilePreviewResult>('/setup/profile-preview', {method:'POST', body:JSON.stringify(payload)}),

    getRunProgress: () => fetchJson<{
        run_id?: string | null
        status: string
        mode?: string
        dry_run?: boolean
        elapsed_seconds?: number
        current_stage?: string | null
        current_detail?: string | null
        stages: Array<{
            name: string
            label: string
            order: number
            status: 'pending' | 'running' | 'completed' | 'skipped'
            duration_seconds?: number | null
            rows_affected?: number | null
            detail?: string | null
        }>
    }>('/setup/run/progress'),

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
    }>>('/schema'),

    // AI Assistant (Ollama-first + BYOK)
    getAiConfig: () => fetchJson<{
        provider: string
        enabled: boolean
        model: string
        base_url: string
        has_api_key: boolean
        is_local: boolean
        mask_pii: boolean
    }>('/ai/config'),

    updateAiConfig: (payload: {
        provider: string
        model?: string
        base_url?: string
        api_key?: string
        mask_pii?: boolean | null
    }) => fetchJson<{
        provider: string
        enabled: boolean
        model: string
        base_url: string
        has_api_key: boolean
        is_local: boolean
        mask_pii: boolean
    }>('/ai/config', {
        method: 'POST',
        body: JSON.stringify(payload)
    }),

    getOllamaModels: (baseUrl?: string) =>
        fetchJson<{ models: string[] }>(
            `/ai/ollama/models${baseUrl ? `?base_url=${encodeURIComponent(baseUrl)}` : ''}`
        ),

    testAiConnection: () => fetchJson<{
        status: string
        provider: string
        model: string
        is_local: boolean
        mask_pii: boolean
        latency_ms: number
        reply: string
    }>('/ai/test', { method: 'POST', body: '{}' }),

    explainClusterAi: (resolvedId: string, prompt?: string) => fetchJson<{
        resolved_id: string
        pii_masked: boolean
        provider: string
        model: string
        explanation: string
    }>('/ai/explain', {
        method: 'POST',
        body: JSON.stringify({
            resolved_id: resolvedId,
            ...(prompt?.trim() ? { prompt: prompt.trim() } : {})
        })
    }),

    runAiTriage: (maxConfidence = 0.65, limit = 8) => fetchJson<{
        status: string
        pii_masked: boolean
        proposals: Array<{
            resolved_id: string
            action: string
            entity_key_a?: string
            entity_key_b?: string
            reason: string
        }>
    }>('/ai/triage', {
        method: 'POST',
        body: JSON.stringify({ max_confidence: maxConfidence, limit })
    }),

    askAi: (question: string) => fetchJson<{
        question: string
        pii_masked: boolean
        provider: string
        model: string
        answer: string
    }>('/ai/ask', {
        method: 'POST',
        body: JSON.stringify({ question })
    }),

    copilotChat: (message: string, history?: Array<{ role: string; content: string }>) => fetchJson<{
        intent: string
        answer: string
        proposed_action: {
            type: 'run_pipeline'
            label: string
            mode: 'INCR' | 'FULL'
            dry_run: boolean
            lens: string | null
        } | null
        metrics: Record<string, any> | null
        pii_masked: boolean
        provider: string
        model: string
    }>('/ai/copilot', {
        method: 'POST',
        body: JSON.stringify({ message, history })
    }),

    suggestConfigAi: (tables?: string[], prompt = '', options?: AIGenerationOptions) => fetchJson<{
        proposal_id: string
        base_draft_hash: string
        candidate_draft_hash: string
        status: 'ready_for_preview' | 'needs_decisions' | 'invalid'
        decisions: Array<{ target: string; origin: string; reason: string; needs_review: boolean }>
        diff: Array<{ path: string; before: unknown; after: unknown }>
        pii_masked: boolean
        provider: string
        model: string
        generation_mode?: 'ai_validated' | 'ai_repaired' | 'deterministic_fallback'
        suggested_config: Record<string, unknown>
        explanations?: {
            tables: Array<{
                target: string
                summary: string
                evidence?: string[]
                risks?: string[]
                confidence?: 'high' | 'medium' | 'low'
                needs_review?: boolean
            }>
            rules: Array<{
                target: string
                summary: string
                evidence?: string[]
                risks?: string[]
                confidence?: 'high' | 'medium' | 'low'
                needs_review?: boolean
            }>
            survivorship: Array<{
                target: string
                summary: string
                evidence?: string[]
                risks?: string[]
                confidence?: 'high' | 'medium' | 'low'
                needs_review?: boolean
            }>
            lenses: Array<{
                target: string
                summary: string
                evidence?: string[]
                risks?: string[]
                confidence?: 'high' | 'medium' | 'low'
                needs_review?: boolean
            }>
        }
        validation_report?: {
            valid: boolean
            errors: string[]
            warnings: string[]
            repair_attempts: number
            sql_verified: boolean
        }
        profile_summary?: {
            dialect?: string
            tables_profiled?: number
            tables_with_identifiers?: number
            cross_table_overlaps?: Array<{
                table_a: string
                column_a: string
                table_b: string
                column_b: string
                identifier_type: string
                overlap_a_in_b: number
                overlap_b_in_a: number
                shared_distinct_estimate: number
            }>
        }
    }>('/ai/suggest-config', {
        method: 'POST',
        body: JSON.stringify({
            ...(tables && tables.length > 0 ? { tables } : {}),
            prompt,
            goal: prompt,
            ...options,
        })
    }),

    suggestConfigAiStream: async (
        tables?: string[],
        prompt = 'Suggest entity keys, identifier mappings, matching rules, survivorship rules, and multi-lens views.',
        onProgress?: (evt: { stage: string; step: number; total_steps: number; message: string }) => void,
        options?: AIGenerationOptions
    ) => {
        const res = await apiRequest('/ai/suggest-config/stream', {
            method: 'POST',
            body: JSON.stringify({
                ...(tables && tables.length > 0 ? { tables } : {}),
                prompt,
                goal: prompt,
                ...options,
            }),
        })
        return readAIStream<Awaited<ReturnType<typeof api.suggestConfigAi>>>(res, event =>
            onProgress?.(event as {stage: string; step: number; total_steps: number; message: string}))
    },

    critiqueDryRunAi: (config: Record<string, unknown>, runId: string, fingerprint: string, lens = 'default') =>
        fetchJson<AIReview>('/ai/critique-dry-run', {
            method: 'POST', body: JSON.stringify({config, run_id: runId, draft_hash: fingerprint, lens}),
        }),

    compareAiPatch: async (runId: string, config: IDRConfig, patch: ReviewPatch, lens = 'default', onProgress?: (event: Record<string, unknown>) => void) => {
        const response = await apiRequest('/ai/compare/stream', {
            method: 'POST', body: JSON.stringify({run_id: runId, config, patch, lens}),
        })
        return readAIStream<AIReview>(response, onProgress)
    },

    applyAiPatch: (config: IDRConfig, patch: ReviewPatch) =>
        fetchJson<{ config: IDRConfig; draft_hash: string; review_stale: boolean }>('/ai/patch', {
            method: 'POST', body: JSON.stringify({config, patch}),
        }),

    getAiReview: (runId: string) => fetchJson<AIReview>(`/ai/reviews/${encodeURIComponent(runId)}`),

    // Multi-Lens Identity
    listLenses: () => fetchJson<{
        lenses: Array<{
            id: string
            include_types: string[]
            exclude_types: string[]
            source_filter: string[]
            max_group_size: number | null
            cluster_count: number
            entity_count: number
        }>
    }>('/lenses'),

    diffLenses: (lensA: string, lensB: string, limit = 25) => fetchJson<{
        lens_a: string
        lens_b: string
        lens_a_clusters: number
        lens_b_clusters: number
        entities_compared: number
        merged_in_b_only: Array<{ entity_key: string; resolved_a: string; resolved_b: string }>
        merged_in_a_only: Array<{ entity_key: string; resolved_a: string; resolved_b: string }>
    }>(`/lenses/diff?lens_a=${encodeURIComponent(lensA)}&lens_b=${encodeURIComponent(lensB)}&limit=${limit}`),

    getLensClusters: (lensId: string, q?: string, limit = 50) => {
        const params = new URLSearchParams({ limit: String(limit) })
        if (q && q.trim()) params.set('q', q.trim())
        return fetchJson<Array<{
            resolved_id: string
            cluster_size: number
            confidence_score: number
        }>>(`/lenses/${encodeURIComponent(lensId)}/clusters?${params.toString()}`)
    },

    getLensClusterDetail: (lensId: string, resolvedId: string) => fetchJson<{
        resolved_id: string
        super_cluster_id?: string | null
        cluster_size: number
        confidence_score: number | null
        confidence_breakdown?: {
            verdict: string
            confidence_score: number | null
            subject_count: number
            child_count: number
            subject_fanout: number
            max_fanout: number
            edge_diversity: number
            edge_count: number
            match_density?: number | null
            has_fuzzy_bridge: boolean
            reason: string
        } | null
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
        golden_profile?: Record<string, unknown> | null
        crosswalk_history?: Array<{
            previous_resolved_id: string
            current_resolved_id: string
            change_type: string
            entities_moved: number
            run_id: string
            updated_at: string
        }> | null
        truncated?: boolean
    }>(`/lenses/${encodeURIComponent(lensId)}/clusters/${encodeURIComponent(resolvedId)}`)
}
