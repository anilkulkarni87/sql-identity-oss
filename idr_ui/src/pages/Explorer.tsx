import { useEffect, useMemo, useRef, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
    Bot,
    Check,
    Copy,
    GitBranch,
    Loader2,
    RefreshCw,
    Search,
    Trash2,
    X,
} from 'lucide-react'
import { api } from '../api/client'
import IdentityGraph, {
    GraphEdgeData,
    GraphLayoutName,
    GraphNodeData,
} from '../components/IdentityGraph'
import { useToast } from '../components/Toast'
import { formatSourceName, getEntityColor } from '../utils/colors'
import { useDebounce } from '../utils/useDebounce'
import AiClusterForensicsBar from './Explorer/AiClusterForensicsBar'
import ClusterInspector from './Explorer/ClusterInspector'
import CrosswalkLineageStrip from './Explorer/CrosswalkLineageStrip'
import ExplorerClusterLists from './Explorer/ExplorerClusterLists'
import LensDiffModal from './Explorer/LensDiffModal'
import { RtbfPreviewBanner, RtbfReceiptBanner } from './Explorer/RtbfPurgeBanners'
import TriageQueuePanel from './Explorer/TriageQueuePanel'
import { InspectorTab, PrivacyForgetResponse } from './Explorer/types'

const GROUP_THRESHOLD = 12

export default function Explorer() {
    const { toast } = useToast()
    const queryClient = useQueryClient()
    const [searchParams, setSearchParams] = useSearchParams()
    const searchInputRef = useRef<HTMLInputElement>(null)

    const [searchQuery, setSearchQuery] = useState('')
    const debouncedSearchQuery = useDebounce(searchQuery, 250)
    const [triageOpen, setTriageOpen] = useState(false)

    const selectedCluster = searchParams.get('cluster')
    const [resultsCollapsed, setResultsCollapsed] = useState<boolean>(Boolean(selectedCluster))
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

    const [selectedNode, setSelectedNode] = useState<GraphNodeData | null>(null)
    const [activeTab, setActiveTab] = useState<InspectorTab>('golden')
    const [layoutName, setLayoutName] = useState<GraphLayoutName>('concentric')
    const [hiddenSources, setHiddenSources] = useState<Set<string>>(new Set())
    const [groupHighVolume, setGroupHighVolume] = useState(true)
    const [expandedGraphGroups, setExpandedGraphGroups] = useState<Set<string>>(new Set())
    const [copiedKey, setCopiedKey] = useState<string | null>(null)
    const [confirmOverrideKey, setConfirmOverrideKey] = useState<string | null>(null)

    const [rtbfPreview, setRtbfPreview] = useState<PrivacyForgetResponse | null>(null)
    const [rtbfReceipt, setRtbfReceipt] = useState<PrivacyForgetResponse | null>(null)
    const [rtbfLoading, setRtbfLoading] = useState<'preview' | 'purge' | null>(null)
    const rtbfInFlightRef = useRef(false)

    useEffect(() => {
        setSelectedNode(null)
        setExpandedGraphGroups(new Set())
    }, [selectedCluster])

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

    const { data: searchResults, isLoading: searching } = useQuery({
        queryKey: ['search', selectedLens, debouncedSearchQuery],
        queryFn: () =>
            selectedLens
                ? api.getLensClusters(selectedLens, debouncedSearchQuery)
                : api.searchEntities(debouncedSearchQuery),
        enabled: isConnected && (selectedLens ? true : debouncedSearchQuery.trim().length >= 3),
    })

    const { data: topClusters } = useQuery({
        queryKey: ['topClusters'],
        queryFn: () => api.getTopClusters(8),
        enabled: isConnected && !selectedCluster && !selectedLens && debouncedSearchQuery.trim().length < 3,
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

    const handleSelectClusterFromList = (resolvedId: string) => {
        setSelectedCluster(resolvedId)
        setResultsCollapsed(true)
        setSelectedNode(null)
        setHiddenSources(new Set())
        setActiveTab('golden')
    }

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
        const degByEntity: Record<string, number> = {}
        for (const ed of clusterDetail.edges || []) {
            degByEntity[ed.left_entity_key] = (degByEntity[ed.left_entity_key] || 0) + 1
            degByEntity[ed.right_entity_key] = (degByEntity[ed.right_entity_key] || 0) + 1
        }
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
                    degree: degByEntity[e.entity_key] || 0,
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
                id: `${srcId}__${tgtId}__${e.identifier_type}__${idx}`,
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
        } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : String(err)
            toast('error', 'Failed to save override', msg)
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
        } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : String(err)
            toast('error', 'Failed to save exclusion', msg)
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
        } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : String(err)
            toast('error', 'GDPR preview failed', msg)
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
        } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : String(err)
            toast('error', 'GDPR purge failed', msg)
        } finally {
            rtbfInFlightRef.current = false
            setRtbfLoading(null)
        }
    }

    return (
        <div className="space-y-4">
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

                            <button
                                type="button"
                                onClick={() => {
                                    if (availableLenses.length >= 2) {
                                        setDiffLensA(availableLenses[0].id)
                                        setDiffLensB(availableLenses[1].id)
                                    } else {
                                        setDiffLensA('default')
                                        setDiffLensB(availableLenses[0].id)
                                    }
                                    setDiffModalOpen(true)
                                }}
                                className="btn-secondary text-xs"
                            >
                                <GitBranch className="w-3.5 h-3.5 text-accent" />
                                Compare Lenses
                            </button>
                        </>
                    )}

                    <button
                        type="button"
                        onClick={() => setTriageOpen((prev) => !prev)}
                        className={`btn-secondary text-xs ${triageOpen ? 'border-accent text-accent bg-accent-soft' : ''}`}
                        title="3-Tier AI Triage Queue (SQL Tier 0 -> LLM Tier 1 -> optional Tier 2 escalation)"
                    >
                        <Bot className="w-3.5 h-3.5 text-accent" />
                        AI Triage
                    </button>

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

            <TriageQueuePanel
                open={triageOpen}
                onClose={() => setTriageOpen(false)}
                onInspectCluster={(resolvedId) => {
                    setSelectedCluster(resolvedId)
                    setResultsCollapsed(true)
                }}
            />

            <ExplorerClusterLists
                isConnected={isConnected}
                hasSetupStatus={Boolean(setupStatus)}
                searching={searching}
                selectedLens={selectedLens}
                searchQuery={searchQuery}
                selectedCluster={selectedCluster}
                resultsCollapsed={resultsCollapsed}
                onToggleResultsCollapsed={() => setResultsCollapsed((v) => !v)}
                searchResults={searchResults}
                topClusters={topClusters}
                onSelectCluster={handleSelectClusterFromList}
            />

            <RtbfReceiptBanner
                receipt={rtbfReceipt}
                onDismiss={() => setRtbfReceipt(null)}
            />

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

            {selectedCluster && clusterDetail && (
                <div className="space-y-3">
                    <AiClusterForensicsBar resolvedId={selectedCluster} />

                    <CrosswalkLineageStrip
                        resolvedId={clusterDetail.resolved_id}
                        history={clusterDetail.crosswalk_history}
                        onSelectCluster={(id) => setSelectedCluster(id)}
                    />

                    <RtbfPreviewBanner
                        selectedCluster={selectedCluster}
                        preview={rtbfPreview}
                        purging={rtbfLoading === 'purge'}
                        disabled={rtbfLoading !== null}
                        onConfirm={() => void handleConfirmRtbf(selectedCluster)}
                        onCancel={() => setRtbfPreview(null)}
                    />

                    <div className="grid grid-cols-1 lg:grid-cols-12 gap-4 items-stretch">
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
                                        <span
                                            className="badge-success font-mono tabular-nums"
                                            title={clusterDetail.confidence_breakdown?.reason || 'Deterministic confidence score'}
                                        >
                                            {(clusterDetail.confidence_score * 100).toFixed(1)}%
                                        </span>
                                    )}
                                    {clusterDetail.confidence_breakdown && clusterDetail.cluster_size > 1 && (
                                        <span
                                            className="badge-neutral font-mono text-[11px]"
                                            title={clusterDetail.confidence_breakdown.reason}
                                        >
                                            {clusterDetail.confidence_breakdown.subject_count} subj
                                            {clusterDetail.confidence_breakdown.child_count > 0
                                                ? ` + ${clusterDetail.confidence_breakdown.child_count} child`
                                                : ''}
                                            {' · '}
                                            {clusterDetail.confidence_breakdown.edge_diversity} rule
                                            {clusterDetail.confidence_breakdown.edge_diversity === 1 ? '' : 's'}
                                            {' · fanout '}
                                            {clusterDetail.confidence_breakdown.subject_fanout}
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
                                        {visibleEntities.length}/{clusterDetail.entities.length} {clusterDetail.entities.length === 1 ? 'entity' : 'entities'} ·{' '}
                                        {graphEdges.length === (clusterDetail.edges?.length ?? 0)
                                            ? `${graphEdges.length} ${graphEdges.length === 1 ? 'edge' : 'edges'}`
                                            : `${graphEdges.length} visible / ${clusterDetail.edges?.length ?? 0} total ${(clusterDetail.edges?.length ?? 0) === 1 ? 'edge' : 'edges'}`}
                                    </span>
                                    <button
                                        type="button"
                                        onClick={() => void handlePreviewRtbf(clusterDetail.resolved_id)}
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
                                                className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-xs border transition-colors ${
                                                    isHidden
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
                                                className={`px-2 py-0.5 rounded text-[11px] font-medium border transition-colors ${
                                                    groupHighVolume
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
                                    key={selectedCluster || 'none'}
                                    nodes={graphNodes}
                                    edges={graphEdges}
                                    selectedNodeId={selectedNode?.id}
                                    onSelectNode={handleSelectNode}
                                    layoutName={layoutName}
                                    onLayoutChange={setLayoutName}
                                />
                            </div>
                        </section>

                        <ClusterInspector
                            clusterDetail={clusterDetail}
                            uniqueSources={uniqueSources}
                            visibleEntities={visibleEntities}
                            selectedNode={selectedNode}
                            activeTab={activeTab}
                            onTabChange={setActiveTab}
                            onSelectNode={handleSelectNode}
                            confirmOverrideKey={confirmOverrideKey}
                            onConfirmOverrideKeyChange={setConfirmOverrideKey}
                            copiedKey={copiedKey}
                            onCopy={copyToClipboard}
                            onStewardshipOverride={(type, keyA, keyB) => void handleStewardshipOverride(type, keyA, keyB)}
                            onExcludeIdentifier={(type, val) => void handleExcludeIdentifier(type, val)}
                        />
                    </div>
                </div>
            )}

            <LensDiffModal
                open={diffModalOpen}
                availableLenses={availableLenses}
                initialLensA={diffLensA}
                initialLensB={diffLensB}
                onClose={() => setDiffModalOpen(false)}
            />
        </div>
    )
}
