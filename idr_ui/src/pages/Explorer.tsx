import { useEffect, useMemo, useRef, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import IdentityGraph, { GraphNodeData, GraphEdgeData, GraphLayoutName } from '../components/IdentityGraph'
import {
    Search, X, Check, Copy, Eye, EyeOff, Hash, ShieldAlert,
    Trash2, GitBranch, Loader2, RefreshCw, Bot, ChevronDown, ChevronUp
} from 'lucide-react'
import { api } from '../api/client'
import { useToast } from '../components/Toast'
import { useDebounce } from '../utils/useDebounce'
import {
    getEntityColor,
    formatSourceName,
    getIdentifierBadge,
    formatAttributeKey,
} from '../utils/colors'

interface ClusterSummary {
    resolved_id: string
    cluster_size: number
    confidence_score: number | null
}

type InspectorTab = 'golden' | 'edges' | 'entity'

export default function Explorer() {
    const { toast } = useToast()
    const queryClient = useQueryClient()
    const [searchParams, setSearchParams] = useSearchParams()
    const searchInputRef = useRef<HTMLInputElement>(null)

    const [searchQuery, setSearchQuery] = useState('')
    const debouncedSearchQuery = useDebounce(searchQuery, 250)

    const selectedCluster = searchParams.get('cluster')
    const setSelectedCluster = (clusterId: string | null) => {
        if (clusterId) {
            setSearchParams({ cluster: clusterId })
        } else {
            setSearchParams({})
        }
    }

    const [selectedLens, setSelectedLens] = useState<string>('')
    const [diffModalOpen, setDiffModalOpen] = useState(false)
    const [diffLensA, setDiffLensA] = useState<string>('')
    const [diffLensB, setDiffLensB] = useState<string>('')
    const [aiExplanation, setAiExplanation] = useState<string | null>(null)
    const [aiExplaining, setAiExplaining] = useState(false)
    const [aiCustomPrompt, setAiCustomPrompt] = useState('')

    const [selectedNode, setSelectedNode] = useState<GraphNodeData | null>(null)
    const [activeTab, setActiveTab] = useState<InspectorTab>('golden')
    const [layoutName, setLayoutName] = useState<GraphLayoutName>('concentric')
    const [hiddenSources, setHiddenSources] = useState<Set<string>>(new Set())
    const [groupHighVolume, setGroupHighVolume] = useState(true)
    const [expandedGraphGroups, setExpandedGraphGroups] = useState<Set<string>>(new Set())
    const [expandedEntityGroups, setExpandedEntityGroups] = useState<Set<string>>(new Set())
    const [hideEmptyAttrs, setHideEmptyAttrs] = useState(true)
    const [attrFilter, setAttrFilter] = useState('')
    const [edgeSearch, setEdgeSearch] = useState('')
    const [copiedKey, setCopiedKey] = useState<string | null>(null)
    const [confirmOverrideKey, setConfirmOverrideKey] = useState<string | null>(null)
    const [crosswalkExpanded, setCrosswalkExpanded] = useState(false)

    const [rtbfPreview, setRtbfPreview] = useState<any | null>(null)
    const [rtbfReceipt, setRtbfReceipt] = useState<any | null>(null)
    const [rtbfLoading, setRtbfLoading] = useState<'preview' | 'purge' | null>(null)
    const rtbfInFlightRef = useRef(false)

    useEffect(() => {
        const onKeyDown = (e: KeyboardEvent) => {
            const tag = (e.target as HTMLElement)?.tagName
            if (e.key === '/' && tag !== 'INPUT' && tag !== 'TEXTAREA' && tag !== 'SELECT') {
                e.preventDefault()
                searchInputRef.current?.focus()
            } else if (e.key === 'Escape') {
                if (diffModalOpen) {
                    setDiffModalOpen(false)
                } else if (confirmOverrideKey) {
                    setConfirmOverrideKey(null)
                } else if (selectedNode) {
                    setSelectedNode(null)
                }
            }
        }
        window.addEventListener('keydown', onKeyDown)
        return () => window.removeEventListener('keydown', onKeyDown)
    }, [diffModalOpen, confirmOverrideKey, selectedNode])

    const { data: setupStatus } = useQuery({
        queryKey: ['setupStatus'],
        queryFn: () => api.getSetupStatus(),
    })
    const isConnected = Boolean(setupStatus?.connected)

    const { data: lensesData } = useQuery({
        queryKey: ['lenses'],
        queryFn: () => api.listLenses(),
        enabled: isConnected,
    })
    const availableLenses = lensesData?.lenses || []

    const { data: lensDiffData, isLoading: diffLoading } = useQuery({
        queryKey: ['lens-diff', diffLensA, diffLensB],
        queryFn: () => api.diffLenses(diffLensA, diffLensB, 25),
        enabled: diffModalOpen && !!diffLensA && !!diffLensB && diffLensA !== diffLensB,
    })

    const { data: searchResults, isLoading: searching } = useQuery({
        queryKey: ['search', selectedLens, debouncedSearchQuery],
        queryFn: () =>
            selectedLens
                ? api.getLensClusters(selectedLens, debouncedSearchQuery)
                : api.searchEntities(debouncedSearchQuery),
        enabled: isConnected && (selectedLens ? true : debouncedSearchQuery.trim().length >= 3),
    })

    const {
        data: clusterDetail,
        isLoading: clusterLoading,
        isError: clusterError,
        refetch: refetchCluster,
    } = useQuery({
        queryKey: ['cluster', selectedLens, selectedCluster],
        queryFn: () =>
            selectedLens
                ? api.getLensClusterDetail(selectedLens, selectedCluster!)
                : api.getCluster(selectedCluster!),
        enabled: !!selectedCluster,
    })

    useEffect(() => {
        setExpandedGraphGroups(new Set())
        setExpandedEntityGroups(new Set())
    }, [selectedCluster])

    const handleSelectNode = (node: GraphNodeData | null) => {
        if (node?.isGroup) {
            setExpandedGraphGroups((prev) => {
                const next = new Set(prev)
                next.add(node.source_id)
                return next
            })
            return
        }
        setSelectedNode(node)
        setConfirmOverrideKey(null)
        if (node) {
            setActiveTab('entity')
        }
    }

    const GROUP_THRESHOLD = 12

    const uniqueSources = useMemo(() => {
        if (!clusterDetail?.entities) return []
        return Array.from(new Set(clusterDetail.entities.map((e) => e.source_id))).sort()
    }, [clusterDetail])

    const sourceCounts = useMemo(() => {
        const counts: Record<string, number> = {}
        if (!clusterDetail?.entities) return counts
        for (const e of clusterDetail.entities) {
            counts[e.source_id] = (counts[e.source_id] || 0) + 1
        }
        return counts
    }, [clusterDetail])

    const hasHighVolumeSources = useMemo(
        () => Object.values(sourceCounts).some((cnt) => cnt > GROUP_THRESHOLD),
        [sourceCounts]
    )

    const collapsedSources = useMemo(() => {
        const set = new Set<string>()
        if (!groupHighVolume) return set
        for (const [src, cnt] of Object.entries(sourceCounts)) {
            if (cnt > GROUP_THRESHOLD && !hiddenSources.has(src) && !expandedGraphGroups.has(src)) {
                set.add(src)
            }
        }
        return set
    }, [groupHighVolume, sourceCounts, hiddenSources, expandedGraphGroups])

    const visibleEntities: GraphNodeData[] = useMemo(() => {
        if (!clusterDetail?.entities) return []
        return clusterDetail.entities
            .filter((e) => !hiddenSources.has(e.source_id))
            .map((e) => {
                const colorScheme = getEntityColor(e.source_id, uniqueSources)
                const formattedSource = formatSourceName(e.source_id)
                const shortKey = e.source_key.length > 12 ? `${e.source_key.slice(0, 10)}…` : e.source_key
                return {
                    id: e.entity_key,
                    source_id: e.source_id,
                    source_key: e.source_key,
                    label: `${formattedSource} (${shortKey})`,
                    color: colorScheme.bg,
                    borderColor: colorScheme.border,
                    size: 24,
                }
            })
    }, [clusterDetail, uniqueSources, hiddenSources])

    const { graphNodes, graphEdges } = useMemo(() => {
        if (!clusterDetail?.entities) return { graphNodes: [] as GraphNodeData[], graphEdges: [] as GraphEdgeData[] }

        const entityToGroupId = new Map<string, string>()
        const nodes: GraphNodeData[] = []

        for (const src of collapsedSources) {
            const colorScheme = getEntityColor(src, uniqueSources)
            const count = sourceCounts[src] || 0
            const groupId = `__group__:${src}`
            nodes.push({
                id: groupId,
                source_id: src,
                source_key: `${count} entities`,
                label: `${formatSourceName(src)} ×${count}`,
                color: colorScheme.bg,
                borderColor: colorScheme.border,
                size: 34,
                isGroup: true,
                groupCount: count,
            })
        }

        for (const n of visibleEntities) {
            if (collapsedSources.has(n.source_id)) {
                entityToGroupId.set(n.id, `__group__:${n.source_id}`)
            } else {
                nodes.push(n)
            }
        }

        const activeNodeIds = new Set(nodes.map((n) => n.id))
        const seenEdges = new Set<string>()
        const edges: GraphEdgeData[] = []

        for (let idx = 0; idx < (clusterDetail.edges || []).length; idx++) {
            const e = clusterDetail.edges[idx]
            const srcId = entityToGroupId.get(e.left_entity_key) || e.left_entity_key
            const tgtId = entityToGroupId.get(e.right_entity_key) || e.right_entity_key
            if (srcId === tgtId) continue
            if (!activeNodeIds.has(srcId) || !activeNodeIds.has(tgtId)) continue

            const isGroupEdge = srcId.startsWith('__group__:') || tgtId.startsWith('__group__:')
            if (isGroupEdge) {
                const pairKey = [srcId, tgtId].sort().join('|') + `|${e.identifier_type}`
                if (seenEdges.has(pairKey)) continue
                seenEdges.add(pairKey)
            }

            const isFuzzy =
                (e.rule_id || '').toLowerCase().includes('fuzzy') ||
                (e.identifier_type || '').toLowerCase().includes('fuzzy')
            edges.push({
                id: `edge-${idx}`,
                source: srcId,
                target: tgtId,
                label: e.identifier_type,
                identifier_type: e.identifier_type,
                identifier_value: e.identifier_value,
                rule_id: e.rule_id,
                isFuzzy,
            })
        }

        return { graphNodes: nodes, graphEdges: edges }
    }, [clusterDetail, visibleEntities, collapsedSources, uniqueSources, sourceCounts])

    const displayedEdges = useMemo(() => {
        if (!clusterDetail?.edges) return []
        let list = clusterDetail.edges
        if (selectedNode) {
            list = list.filter(
                (e) => e.left_entity_key === selectedNode.id || e.right_entity_key === selectedNode.id
            )
        }
        if (edgeSearch.trim()) {
            const q = edgeSearch.toLowerCase()
            list = list.filter(
                (e) =>
                    e.identifier_value.toLowerCase().includes(q) ||
                    e.identifier_type.toLowerCase().includes(q) ||
                    (e.rule_id || '').toLowerCase().includes(q) ||
                    e.left_entity_key.toLowerCase().includes(q) ||
                    e.right_entity_key.toLowerCase().includes(q)
            )
        }
        return list
    }, [clusterDetail, selectedNode, edgeSearch])

    const goldenProfileCategories = useMemo(() => {
        if (!clusterDetail?.golden_profile) return null

        const categories: Record<string, Array<{ key: string; label: string; value: unknown }>> = {
            'Identity & Contact': [],
            'Commerce & Transactions': [],
            'Engagement & Activity': [],
            'Location & Address': [],
            'Additional Attributes': [],
        }

        const filterLower = attrFilter.trim().toLowerCase()
        let totalEmpty = 0

        for (const [rawKey, rawVal] of Object.entries(clusterDetail.golden_profile)) {
            const isEmpty =
                rawVal === null || rawVal === undefined || rawVal === '' || rawVal === '-' || rawVal === '—'
            if (isEmpty) totalEmpty++
            if (hideEmptyAttrs && isEmpty) continue

            const label = formatAttributeKey(rawKey)
            if (
                filterLower &&
                !rawKey.toLowerCase().includes(filterLower) &&
                !label.toLowerCase().includes(filterLower) &&
                !String(rawVal).toLowerCase().includes(filterLower)
            ) {
                continue
            }

            const k = rawKey.toLowerCase()
            if (/name|email|phone|gender|birth|dob|segment|tier|customer|account/.test(k)) {
                categories['Identity & Contact'].push({ key: rawKey, label, value: rawVal })
            } else if (/amount|spend|value|price|subtotal|discount|cost|points|payment|order|store|balance/.test(k)) {
                categories['Commerce & Transactions'].push({ key: rawKey, label, value: rawVal })
            } else if (/channel|visit|timestamp|date|event|device|browser|os|ip|session|fraud|score|verified/.test(k)) {
                categories['Engagement & Activity'].push({ key: rawKey, label, value: rawVal })
            } else if (/address|street|city|state|zip|postal|country/.test(k)) {
                categories['Location & Address'].push({ key: rawKey, label, value: rawVal })
            } else {
                categories['Additional Attributes'].push({ key: rawKey, label, value: rawVal })
            }
        }

        return { categories, totalEmpty }
    }, [clusterDetail?.golden_profile, hideEmptyAttrs, attrFilter])

    const toggleSourceFilter = (sourceId: string) => {
        setHiddenSources((prev) => {
            const next = new Set(prev)
            if (next.has(sourceId)) next.delete(sourceId)
            else next.add(sourceId)
            return next
        })
    }

    const copyToClipboard = (text: string, id: string) => {
        navigator.clipboard.writeText(text)
        setCopiedKey(id)
        setTimeout(() => setCopiedKey(null), 1800)
    }

    const handleStewardshipOverride = async (
        overrideType: 'DO_NOT_LINK' | 'ISOLATE',
        entityKeyA: string,
        entityKeyB?: string
    ) => {
        try {
            await api.manageOverride({
                action: 'add',
                override_type: overrideType,
                entity_key_a: entityKeyA,
                entity_key_b: entityKeyB || '__ISOLATE__',
                reason: 'Added from Identity Graph Explorer',
            })
            setConfirmOverrideKey(null)
            toast(
                'success',
                overrideType === 'ISOLATE' ? 'Entity quarantine queued' : 'DO_NOT_LINK override saved',
                overrideType === 'ISOLATE'
                    ? `${entityKeyA} will be isolated on the next pipeline run.`
                    : `${entityKeyA} ↔ ${entityKeyB} will be unlinked on the next pipeline run.`
            )
        } catch (err: any) {
            toast('error', 'Failed to save override', String(err?.message || err))
        }
    }

    const handleExcludeIdentifier = async (identifierType: string, identifierValue: string) => {
        if (!identifierValue) return
        try {
            await api.manageExclusion({
                action: 'add',
                identifier_type: identifierType,
                identifier_value_pattern: identifierValue,
                match_type: 'EXACT',
                reason: `Excluded from cluster ${selectedCluster || 'explorer'}`,
            })
            toast(
                'success',
                `Excluded ${identifierType}: ${identifierValue}`,
                'Re-run in INCR mode to split affected clusters.'
            )
        } catch (err: any) {
            toast('error', 'Failed to save exclusion', String(err?.message || err))
        }
    }

    const handlePreviewRtbf = async (resolvedId: string) => {
        if (rtbfInFlightRef.current || rtbfLoading !== null) return
        rtbfInFlightRef.current = true
        setRtbfLoading('preview')
        try {
            const res = await api.privacyForget({
                resolved_id: resolvedId,
                dry_run: true,
                reason: 'GDPR_CCPA_RTBF',
            })
            setRtbfPreview(res)
            setRtbfReceipt(null)
        } catch (err: any) {
            toast('error', 'GDPR preview failed', String(err?.message || err))
        } finally {
            rtbfInFlightRef.current = false
            setRtbfLoading(null)
        }
    }

    const handleConfirmRtbf = async (resolvedId: string) => {
        if (rtbfInFlightRef.current || rtbfLoading !== null) return
        rtbfInFlightRef.current = true
        setRtbfLoading('purge')
        try {
            const res = await api.privacyForget({
                resolved_id: resolvedId,
                dry_run: false,
                reason: 'GDPR_CCPA_RTBF',
            })
            setRtbfPreview(null)
            setRtbfReceipt(res)
            setSelectedCluster(null)
            setSelectedNode(null)
            await queryClient.invalidateQueries({ queryKey: ['search'] })
            await queryClient.invalidateQueries({ queryKey: ['metrics'] })
            toast(
                'success',
                'GDPR purge & suppression complete',
                `${res.entities_purged} entities purged and suppressed from future runs.`
            )
        } catch (err: any) {
            toast('error', 'GDPR purge failed', String(err?.message || err))
        } finally {
            rtbfInFlightRef.current = false
            setRtbfLoading(null)
        }
    }

    return (
        <div className="space-y-4">
            {/* Compact Header + Lens Picker + Search */}
            <div className="flex flex-col lg:flex-row lg:items-end justify-between gap-3">
                <div>
                    <h1 className="text-[18px] font-semibold tracking-[-0.015em] text-ink">
                        Identity Graph Explorer
                    </h1>
                    <p className="text-[13px] text-ink-muted mt-0.5">
                        Inspect entity clusters, deterministic vs. fuzzy edge provenance, Multi-Lens diffs, and Golden Records.
                    </p>
                </div>

                <div className="flex flex-wrap items-center gap-2">
                    {availableLenses.length > 0 && (
                        <>
                            <select
                                aria-label="Identity lens"
                                value={selectedLens}
                                onChange={(e) => {
                                    setSelectedLens(e.target.value)
                                    setSelectedCluster(null)
                                    setSelectedNode(null)
                                }}
                                className="input-field w-auto text-xs py-1.5"
                            >
                                <option value="">Primary Graph (Default)</option>
                                {availableLenses.map((l) => (
                                    <option key={l.id} value={l.id}>
                                        Lens: {l.id} ({l.cluster_count})
                                    </option>
                                ))}
                            </select>

                            {availableLenses.length >= 2 && (
                                <button
                                    type="button"
                                    onClick={() => {
                                        setDiffLensA(availableLenses[0].id)
                                        setDiffLensB(availableLenses[1].id)
                                        setDiffModalOpen(true)
                                    }}
                                    className="btn-secondary text-xs"
                                >
                                    <GitBranch className="w-3.5 h-3.5 text-accent" />
                                    Compare Lenses
                                </button>
                            )}
                        </>
                    )}

                    <div className="relative w-full sm:w-80">
                        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-ink-subtle pointer-events-none" />
                        <input
                            ref={searchInputRef}
                            type="text"
                            aria-label="Search identity clusters by email, phone, customer ID, or hash"
                            placeholder="Search email, phone, ID, or hash… (/)"
                            value={searchQuery}
                            onChange={(e) => setSearchQuery(e.target.value)}
                            className="input-field pl-8 pr-8 text-xs"
                        />
                        {searchQuery && (
                            <button
                                type="button"
                                onClick={() => {
                                    setSearchQuery('')
                                    setSelectedNode(null)
                                }}
                                aria-label="Clear search"
                                className="absolute right-2 top-1/2 -translate-y-1/2 p-1 rounded text-ink-muted hover:text-ink hover:bg-hover"
                            >
                                <X className="w-3.5 h-3.5" />
                            </button>
                        )}
                    </div>
                </div>
            </div>

            {!selectedLens && searchQuery.trim().length > 0 && searchQuery.trim().length < 3 && (
                <p className="text-xs text-ink-muted">
                    Type at least 3 characters to query the identity index.
                </p>
            )}

            {/* Search Results Table */}
            {(selectedLens || searchQuery.trim().length >= 3) && (
                <div className="surface-panel overflow-hidden">
                    <div className="px-3 py-2 bg-subtle border-b border-line flex items-center justify-between text-xs text-ink-muted">
                        <span>
                            {setupStatus && !isConnected
                                ? 'Warehouse disconnected — connect in Identity Studio to search.'
                                : searching
                                    ? 'Querying identity index…'
                                    : `${searchResults?.length || 0} matching clusters`}
                        </span>
                        {searchResults && searchResults.length > 0 && (
                            <span>Select a cluster to inspect</span>
                        )}
                    </div>
                    <div className="max-h-52 overflow-y-auto divide-y divide-line">
                        {searchResults && searchResults.length === 0 && !searching ? (
                            <div className="px-3 py-5 text-center text-xs text-ink-muted">
                                No clusters matched &ldquo;{searchQuery}&rdquo;.
                            </div>
                        ) : (
                            searchResults?.map((result: ClusterSummary) => {
                                const isSelected = selectedCluster === result.resolved_id
                                return (
                                    <button
                                        key={result.resolved_id}
                                        type="button"
                                        onClick={() => {
                                            setSelectedCluster(result.resolved_id)
                                            setSelectedNode(null)
                                            setHiddenSources(new Set())
                                            setAiExplanation(null)
                                            setActiveTab('golden')
                                        }}
                                        className={`w-full text-left px-3 py-2 flex items-center justify-between gap-3 transition-colors ${isSelected
                                                ? 'bg-selected text-ink'
                                                : 'bg-surface hover:bg-subtle text-ink-secondary'
                                            }`}
                                    >
                                        <div className="flex items-center gap-2 min-w-0">
                                            <Hash className="w-3.5 h-3.5 text-accent shrink-0" />
                                            <span className="font-mono text-xs font-medium text-ink truncate">
                                                {result.resolved_id}
                                            </span>
                                        </div>
                                        <div className="flex items-center gap-2 shrink-0 font-mono tabular-nums">
                                            {result.confidence_score != null && (
                                                <span className="badge-success">
                                                    {(result.confidence_score * 100).toFixed(1)}%
                                                </span>
                                            )}
                                            <span className="badge-neutral">
                                                {result.cluster_size.toLocaleString()} entities
                                            </span>
                                        </div>
                                    </button>
                                )
                            })
                        )}
                    </div>
                </div>
            )}

            {/* GDPR RTBF Audit Receipt */}
            {rtbfReceipt && (
                <div className="p-3.5 rounded-md bg-status-success-soft border border-status-success-border text-xs space-y-1.5">
                    <div className="flex items-center justify-between font-semibold text-status-success">
                        <span>GDPR / CCPA Purge Complete ({rtbfReceipt.request_id})</span>
                        <button
                            type="button"
                            onClick={() => setRtbfReceipt(null)}
                            aria-label="Dismiss purge receipt"
                            className="p-1 rounded hover:bg-surface/50"
                        >
                            <X className="w-3.5 h-3.5" />
                        </button>
                    </div>
                    <p className="text-ink-secondary">
                        Purged <strong className="font-mono tabular-nums text-ink">{rtbfReceipt.entities_purged}</strong> entities and{' '}
                        <strong className="font-mono tabular-nums text-ink">{rtbfReceipt.edges_purged}</strong> edges across{' '}
                        <code className="font-mono">idr_out.*</code> and <code className="font-mono">idr_work.*</code>.
                    </p>
                    <p className="font-mono text-[11px] text-ink-muted break-all">
                        SHA-256 Audit Receipt: {rtbfReceipt.target_hash}
                    </p>
                </div>
            )}

            {/* Cluster Loading / Error States */}
            {selectedCluster && clusterLoading && (
                <div className="grid grid-cols-1 lg:grid-cols-12 gap-4" aria-busy="true">
                    <div className="lg:col-span-7 h-[560px] surface-panel animate-pulse bg-subtle" />
                    <div className="lg:col-span-5 h-[560px] surface-panel animate-pulse bg-subtle" />
                </div>
            )}

            {selectedCluster && clusterError && (
                <div role="alert" className="p-4 rounded-md bg-status-danger-soft border border-status-danger-border flex items-center justify-between gap-3">
                    <span className="text-xs text-status-danger">
                        Unable to load cluster <code className="font-mono">{selectedCluster}</code>.
                    </span>
                    <button type="button" onClick={() => refetchCluster()} className="btn-secondary text-xs">
                        <RefreshCw className="w-3 h-3" />
                        Retry
                    </button>
                </div>
            )}

            {/* Split Workbench: Graph Left (7 cols) + Tabbed Inspector Right (5 cols) */}
            {selectedCluster && clusterDetail && (
                <div className="space-y-3">
                    {/* AI Cluster Explanation Bar */}
                    <div className="surface-panel px-3.5 py-2 flex flex-wrap items-center justify-between gap-2">
                        <div className="flex items-center gap-2 text-xs text-ink-secondary">
                            <Bot className="w-4 h-4 text-accent" />
                            <span>AI Cluster Forensics</span>
                        </div>
                        <div className="flex flex-1 sm:flex-initial items-center gap-2 min-w-[260px] max-w-xl">
                            <input
                                type="text"
                                aria-label="Ask AI about this cluster"
                                value={aiCustomPrompt}
                                onChange={(e) => setAiCustomPrompt(e.target.value)}
                                onKeyDown={(e) => {
                                    if (e.key === 'Enter' && !aiExplaining) {
                                        e.preventDefault()
                                        document.getElementById('btn-explain-cluster-ai')?.click()
                                    }
                                }}
                                placeholder="Ask a question about this cluster (or click Explain)…"
                                className="input-field py-1 text-xs flex-1"
                            />
                            <button
                                id="btn-explain-cluster-ai"
                                type="button"
                                disabled={aiExplaining}
                                onClick={async () => {
                                    setAiExplaining(true)
                                    setAiExplanation(null)
                                    try {
                                        const res = await api.explainClusterAi(selectedCluster, aiCustomPrompt)
                                        setAiExplanation(
                                            `[${res.provider}:${res.model} · PII Masking: ${res.pii_masked ? 'ON' : 'OFF (local)'
                                            }]\n\n${res.explanation}`
                                        )
                                    } catch (err: any) {
                                        setAiExplanation(
                                            `AI Explain unavailable: ${err.message || String(err)} (Configure Ollama or a BYOK key in Settings.)`
                                        )
                                    } finally {
                                        setAiExplaining(false)
                                    }
                                }}
                                className="btn-secondary text-xs shrink-0"
                            >
                                {aiExplaining ? (
                                    <>
                                        <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                        Analyzing…
                                    </>
                                ) : aiCustomPrompt.trim() ? (
                                    'Ask AI'
                                ) : (
                                    'Explain with AI'
                                )}
                            </button>
                        </div>
                    </div>

                    {aiExplanation && (
                        <div className="p-3.5 rounded-md bg-subtle border border-line text-xs text-ink whitespace-pre-wrap relative">
                            <button
                                type="button"
                                onClick={() => setAiExplanation(null)}
                                aria-label="Close AI explanation"
                                className="absolute top-2 right-2 p-1 rounded text-ink-muted hover:text-ink hover:bg-hover"
                            >
                                <X className="w-3.5 h-3.5" />
                            </button>
                            {aiExplanation}
                        </div>
                    )}

                    {/* Crosswalk Lineage Strip (Compact Summary + Expandable Scrollable Table) */}
                    {clusterDetail.crosswalk_history && clusterDetail.crosswalk_history.length > 0 && (() => {
                        const history = clusterDetail.crosswalk_history
                        const mergedCnt = history.filter((c) => c.change_type === 'MERGED').length
                        const splitCnt = history.filter((c) => c.change_type === 'SPLIT').length
                        const totalMoved = history.reduce((acc, c) => acc + (Number(c.entities_moved) || 0), 0)
                        const bySource: Record<string, number> = {}
                        for (const cw of history) {
                            const prev = String(cw.previous_resolved_id || '')
                            const src = prev.includes(':') ? prev.split(':', 1)[0] : 'cluster'
                            bySource[src] = (bySource[src] || 0) + 1
                        }
                        const sourceBreakdown = Object.entries(bySource)
                            .sort((a, b) => b[1] - a[1])
                            .slice(0, 5)

                        return (
                            <div className="surface-panel overflow-hidden">
                                <div className="px-3.5 py-2 bg-accent-soft flex flex-wrap items-center justify-between gap-2 text-xs">
                                    <div className="flex flex-wrap items-center gap-2 min-w-0">
                                        <span className="inline-flex items-center gap-1.5 font-semibold text-accent">
                                            <GitBranch className="w-3.5 h-3.5" />
                                            Crosswalk Lineage
                                        </span>
                                        <span className="font-mono tabular-nums text-[11px] px-2 py-0.5 rounded bg-surface border border-line text-ink font-medium">
                                            {history.length} {history.length === 1 ? 'transition' : 'transitions'} · {totalMoved.toLocaleString()} {totalMoved === 1 ? 'entity' : 'entities'} moved
                                        </span>
                                        {mergedCnt > 0 && (
                                            <span className="badge-Exact font-mono tabular-nums">
                                                {mergedCnt} MERGED
                                            </span>
                                        )}
                                        {splitCnt > 0 && (
                                            <span className="badge-warning font-mono tabular-nums">
                                                {splitCnt} SPLIT
                                            </span>
                                        )}
                                        {sourceBreakdown.map(([src, cnt]) => (
                                            <span
                                                key={src}
                                                className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-surface border border-line text-[11px] text-ink-secondary"
                                            >
                                                <span>{formatSourceName(src)}</span>
                                                <span className="font-mono tabular-nums text-ink-muted">×{cnt}</span>
                                            </span>
                                        ))}
                                    </div>
                                    <button
                                        type="button"
                                        onClick={() => setCrosswalkExpanded((v) => !v)}
                                        aria-expanded={crosswalkExpanded}
                                        className="btn-secondary text-xs px-2.5 py-1 min-h-[26px]"
                                    >
                                        {crosswalkExpanded ? (
                                            <>
                                                <ChevronUp className="w-3.5 h-3.5" />
                                                Hide Transitions
                                            </>
                                        ) : (
                                            <>
                                                <ChevronDown className="w-3.5 h-3.5" />
                                                Inspect {history.length} {history.length === 1 ? 'Transition' : 'Transitions'}
                                            </>
                                        )}
                                    </button>
                                </div>

                                {crosswalkExpanded && (
                                    <div className="max-h-56 overflow-y-auto border-t border-line bg-surface">
                                        <table className="data-table">
                                            <thead className="sticky top-0 bg-subtle z-10">
                                                <tr>
                                                    <th className="w-24">Type</th>
                                                    <th className="w-36">Origin Source</th>
                                                    <th>Previous Cluster ID</th>
                                                    <th>Target Cluster</th>
                                                    <th className="text-right w-24">Moved</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {history.map((cw, idx) => {
                                                    const prev = String(cw.previous_resolved_id || '')
                                                    const curr = String(cw.current_resolved_id || '')
                                                    const [prevSrc, prevKey] = prev.includes(':')
                                                        ? prev.split(':', 2)
                                                        : ['cluster', prev]
                                                    const isCurrentTarget = curr === clusterDetail.resolved_id
                                                    return (
                                                        <tr key={idx}>
                                                            <td>
                                                                <span className={cw.change_type === 'SPLIT' ? 'badge-warning font-mono' : 'badge-Exact font-mono'}>
                                                                    {cw.change_type}
                                                                </span>
                                                            </td>
                                                            <td className="text-xs text-ink-secondary">
                                                                {formatSourceName(prevSrc)}
                                                            </td>
                                                            <td className="font-mono text-xs text-ink" title={prev}>
                                                                {prevKey}
                                                            </td>
                                                            <td className="font-mono text-xs">
                                                                {isCurrentTarget ? (
                                                                    <span className="inline-flex items-center gap-1 text-accent font-medium">
                                                                        <span>→</span>
                                                                        <span>Current Cluster</span>
                                                                    </span>
                                                                ) : (
                                                                    <button
                                                                        type="button"
                                                                        onClick={() => setSelectedCluster(curr)}
                                                                        className="text-accent hover:underline truncate max-w-[240px] inline-block"
                                                                        title={curr}
                                                                    >
                                                                        → {curr}
                                                                    </button>
                                                                )}
                                                            </td>
                                                            <td className="text-right font-mono text-xs tabular-nums text-ink-secondary">
                                                                {cw.entities_moved}
                                                            </td>
                                                        </tr>
                                                    )
                                                })}
                                            </tbody>
                                        </table>
                                    </div>
                                )}
                            </div>
                        )
                    })()}

                    {/* RTBF Dry-Run Confirmation Banner */}
                    {rtbfPreview && (
                        <div className="p-4 rounded-md bg-status-danger-soft border border-status-danger-border text-xs space-y-2.5">
                            <div className="flex items-center justify-between gap-2">
                                <span className="font-semibold text-status-danger flex items-center gap-1.5">
                                    <Trash2 className="w-4 h-4" />
                                    Confirm GDPR / CCPA Cascading Purge
                                </span>
                                <button
                                    type="button"
                                    onClick={() => setRtbfPreview(null)}
                                    className="text-ink-muted hover:text-ink font-medium"
                                >
                                    Cancel
                                </button>
                            </div>
                            <p className="text-ink-secondary leading-relaxed">
                                Permanently purges cluster <code className="font-mono text-ink">{selectedCluster}</code> (
                                <strong className="font-mono tabular-nums text-ink">{rtbfPreview.entities_purged}</strong> entities,{' '}
                                <strong className="font-mono tabular-nums text-ink">{rtbfPreview.edges_purged}</strong> edges),
                                writes SHA-256 suppression hashes to <code className="font-mono">idr_meta.privacy_suppression</code>,
                                and logs an audit receipt.
                            </p>
                            {rtbfPreview.entity_keys && rtbfPreview.entity_keys.length > 0 && (
                                <div className="flex flex-wrap gap-1.5 p-2 rounded bg-surface border border-line">
                                    {rtbfPreview.entity_keys.slice(0, 6).map((ek: string) => (
                                        <span key={ek} className="font-mono text-[11px] text-ink-secondary px-1.5 py-0.5 rounded bg-subtle border border-line">
                                            {ek}
                                        </span>
                                    ))}
                                    {rtbfPreview.entity_keys.length > 6 && (
                                        <span className="font-mono text-[11px] text-ink-muted px-1 py-0.5">
                                            +{rtbfPreview.entity_keys.length - 6} more
                                        </span>
                                    )}
                                </div>
                            )}
                            <div className="flex items-center gap-2 pt-0.5">
                                <button
                                    type="button"
                                    onClick={() => handleConfirmRtbf(selectedCluster)}
                                    disabled={rtbfLoading !== null}
                                    className="btn-danger"
                                >
                                    {rtbfLoading === 'purge' && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
                                    {rtbfLoading === 'purge' ? 'Purging…' : 'Confirm & Purge Cluster'}
                                </button>
                                <button
                                    type="button"
                                    onClick={() => setRtbfPreview(null)}
                                    disabled={rtbfLoading !== null}
                                    className="btn-secondary"
                                >
                                    Cancel
                                </button>
                            </div>
                        </div>
                    )}

                    {/* Workbench Grid */}
                    <div className="grid grid-cols-1 lg:grid-cols-12 gap-4 items-stretch">
                        {/* LEFT PANE: Graph Canvas + Source Filter Strip */}
                        <section className="lg:col-span-7 surface-panel overflow-hidden flex flex-col">
                            <div className="px-3.5 py-2.5 border-b border-line flex flex-wrap items-center justify-between gap-2 bg-surface">
                                <div className="flex items-center gap-2 min-w-0">
                                    <span className="font-mono text-xs font-semibold text-ink truncate max-w-[240px]" title={clusterDetail.resolved_id}>
                                        {clusterDetail.resolved_id}
                                    </span>
                                    <button
                                        type="button"
                                        onClick={() => copyToClipboard(clusterDetail.resolved_id, 'cluster')}
                                        aria-label="Copy cluster resolved ID"
                                        className="p-1 rounded text-ink-muted hover:text-ink hover:bg-hover"
                                    >
                                        {copiedKey === 'cluster' ? (
                                            <Check className="w-3.5 h-3.5 text-status-success" />
                                        ) : (
                                            <Copy className="w-3.5 h-3.5" />
                                        )}
                                    </button>
                                    {clusterDetail.confidence_score != null && (
                                        <span className="badge-success font-mono tabular-nums">
                                            {(clusterDetail.confidence_score * 100).toFixed(1)}%
                                        </span>
                                    )}
                                    {clusterDetail.super_cluster_id &&
                                        clusterDetail.super_cluster_id !== clusterDetail.resolved_id && (
                                            <span
                                                className="badge-neutral font-mono"
                                                title={`Super-cluster ${clusterDetail.super_cluster_id}`}
                                            >
                                                Super: …{clusterDetail.super_cluster_id.slice(-8)}
                                            </span>
                                        )}
                                </div>

                                <div className="flex items-center gap-2 shrink-0">
                                    <span className="text-xs font-mono tabular-nums text-ink-muted">
                                        {visibleEntities.length}/{clusterDetail.entities.length} entities · {graphEdges.length} edges
                                    </span>
                                    <button
                                        type="button"
                                        onClick={() => handlePreviewRtbf(clusterDetail.resolved_id)}
                                        disabled={rtbfLoading !== null}
                                        className="btn-danger text-xs px-2 py-1 min-h-[28px]"
                                        title="GDPR / CCPA Right to be Forgotten preview"
                                    >
                                        {rtbfLoading === 'preview' ? (
                                            <Loader2 className="w-3 h-3 animate-spin" />
                                        ) : (
                                            <Trash2 className="w-3 h-3" />
                                        )}
                                        GDPR Forget
                                    </button>
                                </div>
                            </div>

                            {uniqueSources.length > 0 && (
                                <div className="px-3 py-1.5 border-b border-line bg-surface flex flex-wrap items-center gap-1.5">
                                    {uniqueSources.map((sourceId) => {
                                        const colorScheme = getEntityColor(sourceId, uniqueSources)
                                        const isHidden = hiddenSources.has(sourceId)
                                        const formatted = formatSourceName(sourceId)
                                        const count = sourceCounts[sourceId] || 0
                                        return (
                                            <button
                                                key={sourceId}
                                                type="button"
                                                onClick={() => toggleSourceFilter(sourceId)}
                                                aria-pressed={!isHidden}
                                                className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-xs border transition-colors ${isHidden
                                                        ? 'bg-subtle text-ink-subtle border-line line-through opacity-65'
                                                        : 'bg-surface text-ink-secondary border-line-strong hover:bg-subtle'
                                                    }`}
                                            >
                                                <span
                                                    className="w-2 h-2 rounded-full shrink-0"
                                                    style={{ backgroundColor: isHidden ? '#94a3b8' : colorScheme.bg }}
                                                />
                                                <span>{formatted}</span>
                                                <span className="font-mono tabular-nums text-[11px] text-ink-muted">
                                                    {count}
                                                </span>
                                            </button>
                                        )
                                    })}
                                    <div className="flex items-center gap-2 ml-auto">
                                        {hasHighVolumeSources && (
                                            <button
                                                type="button"
                                                onClick={() => {
                                                    setGroupHighVolume((v) => !v)
                                                    setExpandedGraphGroups(new Set())
                                                }}
                                                className={`px-2 py-0.5 rounded text-[11px] font-medium border transition-colors ${groupHighVolume
                                                        ? 'bg-accent-soft text-accent border-accent-border'
                                                        : 'bg-surface text-ink-muted border-line hover:text-ink'
                                                    }`}
                                                title="Collapse source tables with >12 entities into a single expandable hub node"
                                            >
                                                {groupHighVolume ? 'Grouped (>12)' : 'Expand all'}
                                            </button>
                                        )}
                                        {hiddenSources.size > 0 && (
                                            <button
                                                type="button"
                                                onClick={() => setHiddenSources(new Set())}
                                                className="text-xs font-medium text-accent hover:underline"
                                            >
                                                Show all
                                            </button>
                                        )}
                                    </div>
                                </div>
                            )}

                            <div className="flex-1 min-h-[500px]">
                                <IdentityGraph
                                    nodes={graphNodes}
                                    edges={graphEdges}
                                    selectedNodeId={selectedNode?.id}
                                    onSelectNode={handleSelectNode}
                                    layoutName={layoutName}
                                    onLayoutChange={setLayoutName}
                                />
                            </div>
                        </section>

                        {/* RIGHT PANE: Tabbed Inspector */}
                        <section className="lg:col-span-5 surface-panel overflow-hidden flex flex-col max-h-[640px]">
                            <div role="tablist" aria-label="Cluster inspector tabs" className="px-2 pt-2 bg-subtle border-b border-line flex items-center gap-1 shrink-0">
                                {(
                                    [
                                        { id: 'golden', label: 'Golden Record' },
                                        { id: 'edges', label: `Edges (${displayedEdges.length})` },
                                        { id: 'entity', label: selectedNode ? 'Selected Entity' : 'Entities' },
                                    ] as const
                                ).map((tab) => {
                                    const isActive = activeTab === tab.id
                                    return (
                                        <button
                                            key={tab.id}
                                            role="tab"
                                            type="button"
                                            aria-selected={isActive}
                                            onClick={() => setActiveTab(tab.id)}
                                            className={`px-3 py-1.5 text-xs font-medium rounded-t-md border-t border-x transition-colors ${isActive
                                                    ? 'bg-surface text-ink border-line -mb-px pb-[7px]'
                                                    : 'bg-transparent text-ink-muted hover:text-ink border-transparent'
                                                }`}
                                        >
                                            {tab.label}
                                        </button>
                                    )
                                })}
                            </div>

                            {/* TAB 1: Golden Record (Dense Key-Value Table) */}
                            {activeTab === 'golden' && (
                                <div className="flex-1 flex flex-col min-h-0">
                                    <div className="p-2.5 border-b border-line flex items-center gap-2 shrink-0">
                                        <div className="relative flex-1">
                                            <Search className="w-3.5 h-3.5 text-ink-subtle absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none" />
                                            <input
                                                type="text"
                                                aria-label="Filter golden record attributes"
                                                placeholder="Filter attributes…"
                                                value={attrFilter}
                                                onChange={(e) => setAttrFilter(e.target.value)}
                                                className="input-field pl-8 py-1 text-xs"
                                            />
                                        </div>
                                        <button
                                            type="button"
                                            onClick={() => setHideEmptyAttrs((v) => !v)}
                                            className="btn-secondary text-xs px-2 py-1 min-h-[28px] shrink-0"
                                        >
                                            {hideEmptyAttrs ? <EyeOff className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
                                            <span className="tabular-nums">
                                                {hideEmptyAttrs ? `${goldenProfileCategories?.totalEmpty || 0} hidden` : 'All'}
                                            </span>
                                        </button>
                                    </div>

                                    <div className="flex-1 overflow-y-auto">
                                        {!goldenProfileCategories ? (
                                            <div className="p-8 text-center text-xs text-ink-muted">
                                                No golden profile attributes available for this cluster.
                                            </div>
                                        ) : (
                                            Object.entries(goldenProfileCategories.categories).map(([catName, attrs]) => {
                                                if (attrs.length === 0) return null
                                                return (
                                                    <div key={catName}>
                                                        <div className="sticky top-0 px-3 py-1.5 bg-subtle border-y border-line text-[11px] font-semibold uppercase tracking-[0.04em] text-ink-muted">
                                                            {catName} ({attrs.length})
                                                        </div>
                                                        <table className="w-full text-left border-collapse">
                                                            <tbody className="divide-y divide-line">
                                                                {attrs.map((attr) => {
                                                                    const isEmpty =
                                                                        attr.value === null ||
                                                                        attr.value === undefined ||
                                                                        attr.value === '' ||
                                                                        attr.value === '-' ||
                                                                        attr.value === '—'
                                                                    const display = isEmpty ? '—' : String(attr.value)
                                                                    return (
                                                                        <tr key={attr.key} className="hover:bg-subtle/60">
                                                                            <td className="px-3 py-1.5 text-xs text-ink-muted w-[44%] align-top">
                                                                                <div className="font-medium text-ink-secondary">{attr.label}</div>
                                                                                <div className="font-mono text-[10px] text-ink-subtle truncate">
                                                                                    {attr.key}
                                                                                </div>
                                                                            </td>
                                                                            <td className="px-3 py-1.5 text-xs align-top break-words">
                                                                                <span
                                                                                    className={
                                                                                        isEmpty
                                                                                            ? 'font-mono text-ink-subtle'
                                                                                            : typeof attr.value === 'boolean'
                                                                                                ? attr.value
                                                                                                    ? 'badge-success font-mono'
                                                                                                    : 'badge-danger font-mono'
                                                                                                : 'font-mono text-ink tabular-nums'
                                                                                    }
                                                                                >
                                                                                    {display}
                                                                                </span>
                                                                            </td>
                                                                        </tr>
                                                                    )
                                                                })}
                                                            </tbody>
                                                        </table>
                                                    </div>
                                                )
                                            })
                                        )}
                                    </div>
                                </div>
                            )}

                            {/* TAB 2: Edges & Rule Provenance */}
                            {activeTab === 'edges' && (
                                <div className="flex-1 flex flex-col min-h-0">
                                    <div className="p-2.5 border-b border-line space-y-2 shrink-0">
                                        <div className="relative">
                                            <Search className="w-3.5 h-3.5 text-ink-subtle absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none" />
                                            <input
                                                type="text"
                                                aria-label="Filter edges by value, rule, or entity key"
                                                placeholder="Filter by value, rule, or entity…"
                                                value={edgeSearch}
                                                onChange={(e) => setEdgeSearch(e.target.value)}
                                                className="input-field pl-8 py-1 text-xs"
                                            />
                                        </div>
                                        {selectedNode && (
                                            <div className="px-2.5 py-1.5 rounded bg-accent-soft border border-accent-border text-xs flex items-center justify-between gap-2">
                                                <span className="text-accent truncate">
                                                    Filtered to <strong>{formatSourceName(selectedNode.source_id)}</strong>
                                                </span>
                                                <button
                                                    type="button"
                                                    onClick={() => setSelectedNode(null)}
                                                    className="text-accent hover:underline font-medium shrink-0"
                                                >
                                                    Show all
                                                </button>
                                            </div>
                                        )}
                                    </div>

                                    <div className="flex-1 overflow-y-auto divide-y divide-line">
                                        {displayedEdges.length === 0 ? (
                                            <div className="p-8 text-center text-xs text-ink-muted">
                                                No matching edges found.
                                            </div>
                                        ) : (
                                            displayedEdges.map((edge, i) => {
                                                const badge = getIdentifierBadge(edge.identifier_type)
                                                const leftSource = edge.left_entity_key.split(':')[0] || 'Entity'
                                                const rightSource = edge.right_entity_key.split(':')[0] || 'Entity'
                                                const leftColor = getEntityColor(leftSource, uniqueSources)
                                                const rightColor = getEntityColor(rightSource, uniqueSources)
                                                const confirmKey = `unlink:${edge.left_entity_key}:${edge.right_entity_key}:${i}`
                                                const isConfirming = confirmOverrideKey === confirmKey

                                                return (
                                                    <div key={i} className="px-3 py-2.5 hover:bg-subtle/60 transition-colors text-xs space-y-1.5">
                                                        <div className="flex items-center justify-between gap-2">
                                                            <div className="flex items-center gap-1.5 min-w-0">
                                                                <span className={`px-1.5 py-0.5 rounded text-[11px] font-medium border ${badge.badgeBg} ${badge.badgeText} ${badge.border}`}>
                                                                    {edge.identifier_type}
                                                                </span>
                                                                <span className="font-mono text-[11px] text-ink-muted truncate">
                                                                    {edge.rule_id || 'EXACT'}
                                                                </span>
                                                            </div>

                                                            <div className="flex items-center gap-1 shrink-0">
                                                                {edge.identifier_value && !isConfirming && (
                                                                    <button
                                                                        type="button"
                                                                        onClick={() => handleExcludeIdentifier(edge.identifier_type, edge.identifier_value)}
                                                                        className="px-1.5 py-0.5 rounded text-[11px] text-ink-muted hover:text-status-warning hover:bg-status-warning-soft border border-line transition-colors"
                                                                        title={`Exclude "${edge.identifier_value}" from future matching`}
                                                                    >
                                                                        Exclude Value
                                                                    </button>
                                                                )}
                                                                {isConfirming ? (
                                                                    <>
                                                                        <button
                                                                            type="button"
                                                                            onClick={() =>
                                                                                handleStewardshipOverride(
                                                                                    'DO_NOT_LINK',
                                                                                    edge.left_entity_key,
                                                                                    edge.right_entity_key
                                                                                )
                                                                            }
                                                                            className="px-2 py-0.5 rounded text-[11px] font-medium bg-status-danger-soft text-status-danger border border-status-danger-border"
                                                                        >
                                                                            Confirm Unlink
                                                                        </button>
                                                                        <button
                                                                            type="button"
                                                                            onClick={() => setConfirmOverrideKey(null)}
                                                                            className="px-1.5 py-0.5 rounded text-[11px] text-ink-muted hover:text-ink"
                                                                        >
                                                                            Cancel
                                                                        </button>
                                                                    </>
                                                                ) : (
                                                                    <button
                                                                        type="button"
                                                                        onClick={() => setConfirmOverrideKey(confirmKey)}
                                                                        className="px-2 py-0.5 rounded text-[11px] text-ink-muted hover:text-status-danger hover:bg-status-danger-soft border border-line hover:border-status-danger-border transition-colors"
                                                                    >
                                                                        Unlink
                                                                    </button>
                                                                )}
                                                            </div>
                                                        </div>

                                                        <div className="font-mono text-xs text-ink bg-subtle px-2 py-1 rounded border border-line truncate" title={edge.identifier_value}>
                                                            {edge.identifier_value}
                                                        </div>

                                                        <div className="flex items-center justify-between text-[11px] text-ink-muted">
                                                            <span className="inline-flex items-center gap-1.5 truncate max-w-[46%]" title={edge.left_entity_key}>
                                                                <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: leftColor.bg }} />
                                                                <span className="truncate">{formatSourceName(leftSource)}</span>
                                                            </span>
                                                            <span className="text-ink-subtle">↔</span>
                                                            <span className="inline-flex items-center gap-1.5 truncate max-w-[46%] justify-end" title={edge.right_entity_key}>
                                                                <span className="truncate">{formatSourceName(rightSource)}</span>
                                                                <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: rightColor.bg }} />
                                                            </span>
                                                        </div>
                                                    </div>
                                                )
                                            })
                                        )}
                                    </div>
                                </div>
                            )}

                            {/* TAB 3: Selected Entity / Cluster Entity List */}
                            {activeTab === 'entity' && (
                                <div className="flex-1 overflow-y-auto p-3 space-y-3">
                                    {selectedNode ? (
                                        <div className="space-y-3">
                                            <div className="flex items-center justify-between gap-2 pb-2 border-b border-line">
                                                <div className="flex items-center gap-2 min-w-0">
                                                    <span className="w-2.5 h-2.5 rounded-full shrink-0" style={{ backgroundColor: selectedNode.color }} />
                                                    <h2 className="text-[13px] font-semibold text-ink truncate">
                                                        {formatSourceName(selectedNode.source_id)}
                                                    </h2>
                                                </div>
                                                <button
                                                    type="button"
                                                    onClick={() => setSelectedNode(null)}
                                                    className="btn-ghost text-xs px-2 py-1 min-h-[28px]"
                                                >
                                                    All entities
                                                </button>
                                            </div>

                                            <dl className="divide-y divide-line border border-line rounded-md overflow-hidden text-xs">
                                                <div className="px-3 py-2 bg-surface flex items-center justify-between gap-2">
                                                    <dt className="text-ink-muted">Source Table</dt>
                                                    <dd className="font-mono text-ink truncate">{selectedNode.source_id}</dd>
                                                </div>
                                                <div className="px-3 py-2 bg-surface flex items-center justify-between gap-2">
                                                    <dt className="text-ink-muted">Source Key</dt>
                                                    <dd className="flex items-center gap-1.5 min-w-0">
                                                        <span className="font-mono text-ink truncate">{selectedNode.source_key}</span>
                                                        <button
                                                            type="button"
                                                            onClick={() => copyToClipboard(selectedNode.source_key, 'srcKey')}
                                                            aria-label="Copy source key"
                                                            className="p-1 rounded text-ink-muted hover:text-ink hover:bg-hover"
                                                        >
                                                            {copiedKey === 'srcKey' ? (
                                                                <Check className="w-3.5 h-3.5 text-status-success" />
                                                            ) : (
                                                                <Copy className="w-3.5 h-3.5" />
                                                            )}
                                                        </button>
                                                    </dd>
                                                </div>
                                                <div className="px-3 py-2 bg-surface flex items-center justify-between gap-2">
                                                    <dt className="text-ink-muted">Entity Key</dt>
                                                    <dd className="font-mono text-ink truncate">{selectedNode.id}</dd>
                                                </div>
                                                <div className="px-3 py-2 bg-surface flex items-center justify-between gap-2">
                                                    <dt className="text-ink-muted">Graph Degree</dt>
                                                    <dd className="font-mono tabular-nums font-medium text-ink">
                                                        {selectedNode.degree || 0} incident links
                                                    </dd>
                                                </div>
                                            </dl>

                                            <div className="p-3 rounded-md bg-subtle border border-line space-y-2">
                                                <div className="text-xs font-semibold text-ink flex items-center gap-1.5">
                                                    <ShieldAlert className="w-3.5 h-3.5 text-status-warning" />
                                                    Stewardship Override
                                                </div>
                                                <p className="text-[11px] text-ink-muted">
                                                    Quarantines this entity into its own singleton cluster on the next pipeline run.
                                                </p>
                                                {confirmOverrideKey === `isolate:${selectedNode.id}` ? (
                                                    <div className="flex items-center gap-2">
                                                        <button
                                                            type="button"
                                                            onClick={() => handleStewardshipOverride('ISOLATE', selectedNode.id)}
                                                            className="btn-danger text-xs"
                                                        >
                                                            Confirm Isolate
                                                        </button>
                                                        <button
                                                            type="button"
                                                            onClick={() => setConfirmOverrideKey(null)}
                                                            className="btn-secondary text-xs"
                                                        >
                                                            Cancel
                                                        </button>
                                                    </div>
                                                ) : (
                                                    <button
                                                        type="button"
                                                        onClick={() => setConfirmOverrideKey(`isolate:${selectedNode.id}`)}
                                                        className="btn-secondary text-xs"
                                                    >
                                                        Isolate Entity
                                                    </button>
                                                )}
                                            </div>
                                        </div>
                                    ) : (
                                        <div className="space-y-2.5">
                                            <p className="text-xs text-ink-muted">
                                                Click a node on the graph or select an entity below to inspect its keys and incident edges.
                                            </p>
                                            {(() => {
                                                const bySource: Record<string, GraphNodeData[]> = {}
                                                for (const n of visibleEntities) {
                                                    bySource[n.source_id] = [...(bySource[n.source_id] || []), n]
                                                }
                                                // Show core/smaller tables first so high-volume event tables don't bury Customers/Orders
                                                const grouped = Object.entries(bySource).sort(
                                                    (a, b) => a[1].length - b[1].length || a[0].localeCompare(b[0])
                                                )
                                                return grouped.map(([srcId, items]) => {
                                                    const isLarge = items.length > 15
                                                    const isExpanded = !isLarge || expandedEntityGroups.has(srcId)
                                                    const color = items[0]?.color || '#64748b'
                                                    return (
                                                        <div key={srcId} className="border border-line rounded-md overflow-hidden">
                                                            <button
                                                                type="button"
                                                                onClick={() =>
                                                                    setExpandedEntityGroups((prev) => {
                                                                        const next = new Set(prev)
                                                                        if (isExpanded) next.delete(srcId)
                                                                        else next.add(srcId)
                                                                        return next
                                                                    })
                                                                }
                                                                className="w-full px-3 py-1.5 bg-subtle hover:bg-hover flex items-center justify-between gap-2 text-xs font-medium text-ink transition-colors"
                                                            >
                                                                <span className="inline-flex items-center gap-2 min-w-0">
                                                                    <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: color }} />
                                                                    <span className="truncate">{formatSourceName(srcId)}</span>
                                                                    <span className="font-mono tabular-nums text-[11px] text-ink-muted">
                                                                        ({items.length})
                                                                    </span>
                                                                </span>
                                                                {isLarge && (
                                                                    <span className="text-[11px] text-accent">
                                                                        {isExpanded ? 'Collapse' : 'Show all'}
                                                                    </span>
                                                                )}
                                                            </button>
                                                            {isExpanded && (
                                                                <div className="divide-y divide-line">
                                                                    {items.map((node) => (
                                                                        <button
                                                                            key={node.id}
                                                                            type="button"
                                                                            onClick={() => handleSelectNode(node)}
                                                                            className="w-full px-3 py-1.5 text-left bg-surface hover:bg-subtle flex items-center justify-between gap-2 text-xs transition-colors"
                                                                        >
                                                                            <span className="font-mono text-[11px] text-ink truncate">
                                                                                {node.source_key}
                                                                            </span>
                                                                            <span className="text-[11px] text-ink-subtle shrink-0">Inspect →</span>
                                                                        </button>
                                                                    ))}
                                                                </div>
                                                            )}
                                                        </div>
                                                    )
                                                })
                                            })()}
                                        </div>
                                    )}
                                </div>
                            )}
                        </section>
                    </div>
                </div>
            )}

            {/* Empty State */}
            {!selectedCluster && !selectedLens && searchQuery.trim().length < 3 && (
                <div className="surface-panel py-16 px-4 text-center">
                    <Search className="w-8 h-8 mx-auto mb-2.5 text-ink-subtle" />
                    <h2 className="text-sm font-semibold text-ink">Search the identity index</h2>
                    <p className="text-xs text-ink-muted mt-1 max-w-md mx-auto">
                        Enter an email address, phone number, customer key, or cluster hash (minimum 3 characters) to inspect its entity graph and Golden Record.
                    </p>
                </div>
            )}

            {/* Lens Diff Modal */}
            {diffModalOpen && (
                <div role="dialog" aria-modal="true" aria-label="Compare Identity Lenses" className="fixed inset-0 z-50 flex items-center justify-center p-4">
                    <div className="fixed inset-0 bg-ink/30" onClick={() => setDiffModalOpen(false)} aria-hidden="true" />
                    <div className="relative z-10 bg-surface border border-line-strong rounded-md w-full max-w-3xl max-h-[85vh] flex flex-col shadow-popover">
                        <div className="flex items-center justify-between px-4 py-3 border-b border-line bg-subtle">
                            <h2 className="text-sm font-semibold text-ink flex items-center gap-2">
                                <GitBranch className="w-4 h-4 text-accent" />
                                Compare Identity Lenses
                            </h2>
                            <button
                                type="button"
                                onClick={() => setDiffModalOpen(false)}
                                aria-label="Close lens diff modal"
                                className="p-1 rounded text-ink-muted hover:text-ink hover:bg-hover"
                            >
                                <X className="w-4 h-4" />
                            </button>
                        </div>
                        <div className="p-4 space-y-4 overflow-y-auto text-xs">
                            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                                <div>
                                    <label className="block text-ink-secondary font-medium mb-1">Lens A</label>
                                    <select
                                        value={diffLensA}
                                        onChange={(e) => setDiffLensA(e.target.value)}
                                        className="input-field text-xs"
                                    >
                                        {availableLenses.map((l) => (
                                            <option key={l.id} value={l.id}>{l.id}</option>
                                        ))}
                                    </select>
                                </div>
                                <div>
                                    <label className="block text-ink-secondary font-medium mb-1">Lens B</label>
                                    <select
                                        value={diffLensB}
                                        onChange={(e) => setDiffLensB(e.target.value)}
                                        className="input-field text-xs"
                                    >
                                        {availableLenses.map((l) => (
                                            <option key={l.id} value={l.id}>{l.id}</option>
                                        ))}
                                    </select>
                                </div>
                            </div>

                            {diffLensA === diffLensB ? (
                                <p className="text-status-warning">Select two different lenses to compare.</p>
                            ) : diffLoading ? (
                                <div className="flex items-center gap-2 text-ink-muted py-6 justify-center">
                                    <Loader2 className="w-4 h-4 animate-spin text-accent" />
                                    Computing cross-lens diff…
                                </div>
                            ) : lensDiffData ? (
                                <div className="space-y-4">
                                    <div className="grid grid-cols-3 gap-2 font-mono tabular-nums">
                                        <div className="surface-subtle p-3">
                                            <div className="text-[11px] font-sans text-ink-muted">{lensDiffData.lens_a} Clusters</div>
                                            <div className="text-lg font-semibold text-ink mt-0.5">{lensDiffData.lens_a_clusters}</div>
                                        </div>
                                        <div className="surface-subtle p-3">
                                            <div className="text-[11px] font-sans text-ink-muted">{lensDiffData.lens_b} Clusters</div>
                                            <div className="text-lg font-semibold text-ink mt-0.5">{lensDiffData.lens_b_clusters}</div>
                                        </div>
                                        <div className="surface-subtle p-3">
                                            <div className="text-[11px] font-sans text-ink-muted">Entities Compared</div>
                                            <div className="text-lg font-semibold text-ink mt-0.5">{lensDiffData.entities_compared}</div>
                                        </div>
                                    </div>

                                    <div>
                                        <h3 className="text-[11px] font-semibold uppercase tracking-[0.04em] text-ink-muted mb-1.5">
                                            Merged in {lensDiffData.lens_b} only ({lensDiffData.merged_in_b_only.length})
                                        </h3>
                                        {lensDiffData.merged_in_b_only.length === 0 ? (
                                            <p className="text-ink-muted">None</p>
                                        ) : (
                                            <div className="border border-line rounded-md divide-y divide-line max-h-40 overflow-y-auto">
                                                {lensDiffData.merged_in_b_only.map((row, i) => (
                                                    <div key={i} className="px-3 py-1.5 flex justify-between gap-2 bg-surface">
                                                        <span className="font-mono text-accent">{row.entity_key}</span>
                                                        <span className="text-ink-muted font-mono truncate">
                                                            {row.resolved_a} → {row.resolved_b}
                                                        </span>
                                                    </div>
                                                ))}
                                            </div>
                                        )}
                                    </div>

                                    <div>
                                        <h3 className="text-[11px] font-semibold uppercase tracking-[0.04em] text-ink-muted mb-1.5">
                                            Merged in {lensDiffData.lens_a} only ({lensDiffData.merged_in_a_only.length})
                                        </h3>
                                        {lensDiffData.merged_in_a_only.length === 0 ? (
                                            <p className="text-ink-muted">None</p>
                                        ) : (
                                            <div className="border border-line rounded-md divide-y divide-line max-h-40 overflow-y-auto">
                                                {lensDiffData.merged_in_a_only.map((row, i) => (
                                                    <div key={i} className="px-3 py-1.5 flex justify-between gap-2 bg-surface">
                                                        <span className="font-mono text-accent">{row.entity_key}</span>
                                                        <span className="text-ink-muted font-mono truncate">
                                                            {row.resolved_a} ← {row.resolved_b}
                                                        </span>
                                                    </div>
                                                ))}
                                            </div>
                                        )}
                                    </div>
                                </div>
                            ) : null}
                        </div>
                    </div>
                </div>
            )}
        </div>
    )
}
