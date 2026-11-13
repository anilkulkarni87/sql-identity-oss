import { useState, useMemo, useRef } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import IdentityGraph, { GraphNodeData, GraphEdgeData, GraphLayoutName } from '../components/IdentityGraph'
import {
    Search, X, Filter, Sparkles, Check, Copy, Eye, EyeOff,
    Network, Hash, ShieldAlert, Trash2, GitBranch, Loader2
} from 'lucide-react'
import { api } from '../api/client'
import { useToast } from '../components/Toast'
import { useDebounce } from '../utils/useDebounce'
import {
    getEntityColor,
    formatSourceName,
    getIdentifierBadge,
    formatAttributeKey
} from '../utils/colors'

interface ClusterSummary {
    resolved_id: string
    cluster_size: number
    confidence_score: number | null
}

export default function Explorer() {
    const { toast } = useToast()
    const queryClient = useQueryClient()
    const [searchQuery, setSearchQuery] = useState('')
    const debouncedSearchQuery = useDebounce(searchQuery, 300)

    const [selectedCluster, setSelectedCluster] = useState<string | null>(null)
    const [selectedNode, setSelectedNode] = useState<GraphNodeData | null>(null)
    const [layoutName, setLayoutName] = useState<GraphLayoutName>('concentric')
    const [hiddenSources, setHiddenSources] = useState<Set<string>>(new Set())
    const [hideEmptyAttrs, setHideEmptyAttrs] = useState(true)
    const [attrFilter, setAttrFilter] = useState('')
    const [edgeSearch, setEdgeSearch] = useState('')
    const [copiedKey, setCopiedKey] = useState<string | null>(null)
    const [actionNotice, setActionNotice] = useState<string | null>(null)
    const [rtbfPreview, setRtbfPreview] = useState<any | null>(null)
    const [rtbfReceipt, setRtbfReceipt] = useState<any | null>(null)
    const [rtbfLoading, setRtbfLoading] = useState<'preview' | 'purge' | null>(null)
    const rtbfInFlightRef = useRef(false)

    const { data: searchResults, isLoading: searching } = useQuery({
        queryKey: ['search', debouncedSearchQuery],
        queryFn: () => api.searchEntities(debouncedSearchQuery),
        enabled: debouncedSearchQuery.trim().length >= 3
    })

    const { data: clusterDetail } = useQuery({
        queryKey: ['cluster', selectedCluster],
        queryFn: () => api.getCluster(selectedCluster!),
        enabled: !!selectedCluster
    })

    // Extract dynamic unique sources in this cluster
    const uniqueSources = useMemo(() => {
        if (!clusterDetail?.entities) return []
        const sources = Array.from(new Set(clusterDetail.entities.map(e => e.source_id)))
        return sources.sort()
    }, [clusterDetail])

    // Count entities per source
    const sourceCounts = useMemo(() => {
        const counts: Record<string, number> = {}
        if (!clusterDetail?.entities) return counts
        for (const e of clusterDetail.entities) {
            counts[e.source_id] = (counts[e.source_id] || 0) + 1
        }
        return counts
    }, [clusterDetail])

    // Convert cluster entities to graph nodes with dynamic entity color coding
    const graphNodes: GraphNodeData[] = useMemo(() => {
        if (!clusterDetail?.entities) return []

        return clusterDetail.entities
            .filter(e => !hiddenSources.has(e.source_id))
            .map(e => {
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
                    size: 26
                }
            })
    }, [clusterDetail, uniqueSources, hiddenSources])

    // Convert cluster edges, filtering out hidden sources
    const graphEdges: GraphEdgeData[] = useMemo(() => {
        if (!clusterDetail?.edges) return []

        // Set of active node IDs
        const activeNodeIds = new Set(graphNodes.map(n => n.id))

        return clusterDetail.edges
            .filter(e => activeNodeIds.has(e.left_entity_key) && activeNodeIds.has(e.right_entity_key))
            .map((e, idx) => ({
                id: `edge-${idx}`,
                source: e.left_entity_key,
                target: e.right_entity_key,
                label: e.identifier_type,
                identifier_type: e.identifier_type,
                identifier_value: e.identifier_value
            }))
    }, [clusterDetail, graphNodes])

    // Filtered edges for the right-hand panel
    const displayedEdges = useMemo(() => {
        if (!clusterDetail?.edges) return []

        let list = clusterDetail.edges

        // If a node is selected, show edges connected to that node
        if (selectedNode) {
            list = list.filter(
                e => e.left_entity_key === selectedNode.id || e.right_entity_key === selectedNode.id
            )
        }

        // Apply text search on edge values or types
        if (edgeSearch.trim()) {
            const query = edgeSearch.toLowerCase()
            list = list.filter(
                e =>
                    e.identifier_value.toLowerCase().includes(query) ||
                    e.identifier_type.toLowerCase().includes(query) ||
                    e.left_entity_key.toLowerCase().includes(query) ||
                    e.right_entity_key.toLowerCase().includes(query)
            )
        }

        return list
    }, [clusterDetail, selectedNode, edgeSearch])

    // Categorized Golden Profile Attributes
    const goldenProfileCategories = useMemo(() => {
        if (!clusterDetail?.golden_profile) return null

        const categories: Record<string, Array<{ key: string; label: string; value: unknown }>> = {
            'Identity & Contact': [],
            'Commerce & Transactions': [],
            'Engagement & Activity': [],
            'Location & Address': [],
            'Additional Attributes': []
        }

        const filterLower = attrFilter.trim().toLowerCase()
        let totalEmpty = 0

        for (const [rawKey, rawVal] of Object.entries(clusterDetail.golden_profile)) {
            const isEmpty =
                rawVal === null ||
                rawVal === undefined ||
                rawVal === '' ||
                rawVal === '-' ||
                rawVal === '—'

            if (isEmpty) {
                totalEmpty++
            }

            if (hideEmptyAttrs && isEmpty) {
                continue
            }

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
            if (
                k.includes('name') ||
                k.includes('email') ||
                k.includes('phone') ||
                k.includes('gender') ||
                k.includes('birth') ||
                k.includes('dob') ||
                k.includes('segment') ||
                k.includes('tier') ||
                k.includes('customer') ||
                k.includes('account')
            ) {
                categories['Identity & Contact'].push({ key: rawKey, label, value: rawVal })
            } else if (
                k.includes('amount') ||
                k.includes('spend') ||
                k.includes('value') ||
                k.includes('price') ||
                k.includes('subtotal') ||
                k.includes('discount') ||
                k.includes('cost') ||
                k.includes('points') ||
                k.includes('payment') ||
                k.includes('order') ||
                k.includes('store') ||
                k.includes('balance')
            ) {
                categories['Commerce & Transactions'].push({ key: rawKey, label, value: rawVal })
            } else if (
                k.includes('channel') ||
                k.includes('visit') ||
                k.includes('timestamp') ||
                k.includes('date') ||
                k.includes('event') ||
                k.includes('device') ||
                k.includes('browser') ||
                k.includes('os') ||
                k.includes('ip') ||
                k.includes('session') ||
                k.includes('fraud') ||
                k.includes('score') ||
                k.includes('verified')
            ) {
                categories['Engagement & Activity'].push({ key: rawKey, label, value: rawVal })
            } else if (
                k.includes('address') ||
                k.includes('street') ||
                k.includes('city') ||
                k.includes('state') ||
                k.includes('zip') ||
                k.includes('postal') ||
                k.includes('country')
            ) {
                categories['Location & Address'].push({ key: rawKey, label, value: rawVal })
            } else {
                categories['Additional Attributes'].push({ key: rawKey, label, value: rawVal })
            }
        }

        return { categories, totalEmpty }
    }, [clusterDetail?.golden_profile, hideEmptyAttrs, attrFilter])

    const toggleSourceFilter = (sourceId: string) => {
        setHiddenSources(prev => {
            const next = new Set(prev)
            if (next.has(sourceId)) {
                next.delete(sourceId)
            } else {
                next.add(sourceId)
            }
            return next
        })
    }

    const resetFilters = () => {
        setHiddenSources(new Set())
    }

    const copyToClipboard = (text: string, id: string) => {
        navigator.clipboard.writeText(text)
        setCopiedKey(id)
        setTimeout(() => setCopiedKey(null), 2000)
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
                reason: 'Added from Identity Graph Explorer'
            })
            setActionNotice(
                overrideType === 'ISOLATE'
                    ? `Added ISOLATE override for ${entityKeyA}. It will be quarantined on the next pipeline run.`
                    : `Added DO_NOT_LINK override between ${entityKeyA} and ${entityKeyB}. It will take effect on the next pipeline run.`
            )
            setTimeout(() => setActionNotice(null), 6000)
        } catch (err: any) {
            setActionNotice(`Failed to save override: ${err?.message || err}`)
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
                reason: 'GDPR_CCPA_RTBF'
            })
            setRtbfPreview(res)
            setRtbfReceipt(null)
        } catch (err: any) {
            const msg = `RTBF preview failed: ${err?.message || err}`
            setActionNotice(msg)
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
                reason: 'GDPR_CCPA_RTBF'
            })
            setRtbfPreview(null)
            setRtbfReceipt(res)
            setSelectedCluster(null)
            setSelectedNode(null)
            await queryClient.invalidateQueries({ queryKey: ['search'] })
            await queryClient.invalidateQueries({ queryKey: ['metrics'] })
            toast('success', 'GDPR purge & suppression complete', `${res.entities_purged} entities purged and suppressed from future FULL/INCR runs.`)
        } catch (err: any) {
            const msg = `RTBF purge failed: ${err?.message || err}`
            setActionNotice(msg)
            toast('error', 'GDPR purge failed', String(err?.message || err))
        } finally {
            rtbfInFlightRef.current = false
            setRtbfLoading(null)
        }
    }

    return (
        <div className="space-y-6">
            <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                <div>
                    <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
                        <Network className="w-6 h-6 text-blue-400" />
                        Identity Graph Explorer
                    </h1>
                    <p className="text-sm text-gray-400 mt-1">
                        Search and explore unified entity clusters, inspect incident identifiers, and verify Golden Profiles.
                    </p>
                </div>
            </div>

            {/* Search Bar */}
            <div className="relative">
                <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" />
                <input
                    type="text"
                    placeholder="Search by email, phone, customer ID, or transaction hash (e.g. 'donald.walker', '555-01', 'a28def')..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="w-full pl-12 pr-10 py-3 bg-gray-800/90 rounded-xl border border-gray-700/80 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:outline-none transition-all text-sm placeholder:text-gray-500 shadow-inner"
                />
                {searchQuery && (
                    <button
                        onClick={() => {
                            setSearchQuery('')
                            setSelectedNode(null)
                        }}
                        className="absolute right-4 top-1/2 -translate-y-1/2 text-gray-400 hover:text-white"
                        title="Clear search"
                    >
                        <X className="w-5 h-5" />
                    </button>
                )}
            </div>

            {/* Search Results Dropdown / Card */}
            {searchQuery.trim().length >= 3 && (
                <div className="bg-gray-800/95 backdrop-blur-sm rounded-xl p-4 border border-gray-700/80 shadow-xl space-y-2">
                    <div className="flex items-center justify-between text-xs text-gray-400 pb-2 border-b border-gray-700/60">
                        <span>
                            {searching ? 'Querying identity index...' : `Found ${searchResults?.length || 0} matching clusters`}
                        </span>
                        {searchResults && searchResults.length > 0 && (
                            <span className="text-gray-500">Click a cluster to load the graph</span>
                        )}
                    </div>
                    <div className="space-y-2 max-h-56 overflow-y-auto pr-1">
                        {searchResults?.map((result: ClusterSummary) => {
                            const isSelected = selectedCluster === result.resolved_id
                            return (
                                <button
                                    key={result.resolved_id}
                                    onClick={() => {
                                        setSelectedCluster(result.resolved_id)
                                        setSelectedNode(null)
                                        setHiddenSources(new Set())
                                    }}
                                    className={`w-full text-left p-3 rounded-lg transition-all border ${
                                        isSelected
                                            ? 'bg-blue-600/25 border-blue-500 text-white shadow-md'
                                            : 'bg-gray-900/60 border-gray-700/60 hover:bg-gray-700/60 text-gray-200'
                                    }`}
                                >
                                    <div className="flex items-center justify-between">
                                        <div className="flex items-center gap-2">
                                            <Hash className="w-4 h-4 text-blue-400" />
                                            <span className="font-mono text-xs font-semibold">{result.resolved_id}</span>
                                        </div>
                                        <div className="flex items-center gap-3">
                                            {result.confidence_score !== null && result.confidence_score !== undefined && (
                                                <span className="text-xs px-2 py-0.5 rounded-full bg-emerald-950/60 text-emerald-400 border border-emerald-800/60">
                                                    {(result.confidence_score * 100).toFixed(1)}% match
                                                </span>
                                            )}
                                            <span className="text-xs font-medium text-gray-300 bg-gray-800 px-2.5 py-0.5 rounded-full border border-gray-700">
                                                {result.cluster_size.toLocaleString()} entities
                                            </span>
                                        </div>
                                    </div>
                                </button>
                            )
                        })}
                    </div>
                </div>
            )}

            {/* Selected Cluster Workspace */}
            {actionNotice && (
                <div className="bg-emerald-950/60 border border-emerald-700/80 text-emerald-200 px-4 py-3 rounded-xl text-xs flex items-center justify-between">
                    <span>{actionNotice}</span>
                    <button onClick={() => setActionNotice(null)} className="text-emerald-400 hover:text-white">×</button>
                </div>
            )}

            {rtbfReceipt && (
                <div className="bg-emerald-950/50 border border-emerald-600/70 text-emerald-100 p-4 rounded-xl text-xs space-y-2">
                    <div className="flex items-center justify-between font-semibold text-sm text-emerald-300">
                        <span>GDPR / CCPA Right-to-be-Forgotten Purge Complete ({rtbfReceipt.request_id})</span>
                        <button onClick={() => setRtbfReceipt(null)} className="text-emerald-400 hover:text-white">×</button>
                    </div>
                    <p>
                        Purged <strong>{rtbfReceipt.entities_purged}</strong> entities and <strong>{rtbfReceipt.edges_purged}</strong> edges across <code>idr_out.*</code> and <code>idr_work.*</code>
                        {typeof rtbfReceipt.suppressed_entities === 'number' && rtbfReceipt.suppressed_entities > 0
                            ? ` (${rtbfReceipt.suppressed_entities} entity hashes added to idr_meta.privacy_suppression)`
                            : ''}.
                    </p>
                    <p className="font-mono text-[11px] text-emerald-300/80">
                        Zero-PII Audit SHA-256 Receipt: {rtbfReceipt.target_hash}
                    </p>
                </div>
            )}

            {selectedCluster && clusterDetail && (
                <div className="space-y-6">
                    {/* Persistent resolved_id Crosswalk History Banner */}
                    {clusterDetail.crosswalk_history && clusterDetail.crosswalk_history.length > 0 && (
                        <div className="bg-indigo-950/40 border border-indigo-700/60 rounded-xl p-3.5 text-xs text-indigo-200 space-y-1.5">
                            <div className="flex items-center gap-2 font-semibold text-indigo-300">
                                <GitBranch className="w-4 h-4" />
                                <span>Persistent Cluster ID Crosswalk Lineage (idr_out.resolved_id_crosswalk)</span>
                            </div>
                            <div className="flex flex-wrap gap-2 pt-1">
                                {clusterDetail.crosswalk_history.map((cw, idx) => (
                                    <span key={idx} className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded bg-gray-900/80 border border-indigo-800/60 font-mono text-[11px]">
                                        <span className="text-gray-400">{cw.previous_resolved_id}</span>
                                        <span className="text-indigo-400">→</span>
                                        <span className="text-white font-semibold">{cw.current_resolved_id}</span>
                                        <span className="px-1.5 py-0.2 rounded bg-indigo-900/60 text-indigo-300 text-[10px]">
                                            {cw.change_type} ({cw.entities_moved} entities)
                                        </span>
                                    </span>
                                ))}
                            </div>
                        </div>
                    )}

                    {/* RTBF Preview Confirmation Card */}
                    {rtbfPreview && (
                        <div className="bg-rose-950/60 border border-rose-600/80 rounded-xl p-4 text-xs text-rose-100 space-y-3">
                            <div className="flex items-center justify-between">
                                <span className="font-semibold text-sm text-rose-300 flex items-center gap-2">
                                    <Trash2 className="w-4 h-4" />
                                    Confirm GDPR / CCPA Right-to-be-Forgotten Cascading Purge
                                </span>
                                <button onClick={() => setRtbfPreview(null)} className="text-rose-300 hover:text-white">Cancel</button>
                            </div>
                            <div className="space-y-2">
                                <p>
                                    This will permanently purge cluster <code className="font-mono bg-gray-900 px-1.5 py-0.5 rounded">{selectedCluster}</code> including <strong>{rtbfPreview.entities_purged}</strong> linked entities and <strong>{rtbfPreview.edges_purged}</strong> graph edges across all IDR output &amp; staging tables, add their zero-PII SHA-256 hashes to <code className="font-mono">idr_meta.privacy_suppression</code> (preventing rebuild on future FULL/INCR runs), and record an audit receipt in <code className="font-mono">idr_meta.privacy_audit_log</code>.
                                </p>
                                {rtbfPreview.entity_keys && rtbfPreview.entity_keys.length > 0 && (
                                    <div className="flex flex-wrap gap-1.5 max-h-24 overflow-y-auto p-2 rounded-lg bg-gray-950/60 border border-rose-900/50">
                                        {rtbfPreview.entity_keys.slice(0, 6).map((ek: string) => (
                                            <span key={ek} className="font-mono text-[11px] bg-gray-900 text-rose-200 px-1.5 py-0.5 rounded border border-gray-800">
                                                {ek}
                                            </span>
                                        ))}
                                        {rtbfPreview.entity_keys.length > 6 && (
                                            <span className="font-mono text-[11px] text-rose-300/80 px-1.5 py-0.5">
                                                +{rtbfPreview.entity_keys.length - 6} more entities
                                            </span>
                                        )}
                                    </div>
                                )}
                            </div>
                            <div className="flex gap-2">
                                <button
                                    onClick={() => handleConfirmRtbf(selectedCluster)}
                                    disabled={rtbfLoading !== null}
                                    className="inline-flex items-center gap-2 px-3 py-1.5 bg-rose-600 hover:bg-rose-500 disabled:opacity-60 disabled:cursor-not-allowed text-white font-semibold rounded-lg"
                                >
                                    {rtbfLoading === 'purge' && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
                                    <span>{rtbfLoading === 'purge' ? 'Purging & Suppressing...' : 'Confirm & Purge Identity Graph'}</span>
                                </button>
                                <button
                                    onClick={() => setRtbfPreview(null)}
                                    disabled={rtbfLoading !== null}
                                    className="px-3 py-1.5 bg-gray-800 hover:bg-gray-700 disabled:opacity-50 text-gray-200 rounded-lg border border-gray-600"
                                >
                                    Cancel
                                </button>
                            </div>
                        </div>
                    )}
                    {/* Unified Golden Profile Section */}
                    {clusterDetail.golden_profile && Object.keys(clusterDetail.golden_profile).length > 0 && (
                        <div className="bg-gray-800/90 rounded-xl p-5 border border-blue-500/30 shadow-xl space-y-4">
                            <div className="flex flex-col md:flex-row md:items-center justify-between gap-3 pb-3 border-b border-gray-700/60">
                                <div className="flex items-center gap-3">
                                    <Sparkles className="w-5 h-5 text-blue-400" />
                                    <h2 className="text-lg font-semibold text-white">Unified Golden Profile</h2>
                                    {clusterDetail.super_cluster_id && (
                                        <span className="px-2.5 py-0.5 rounded-full text-xs font-mono bg-purple-900/40 text-purple-300 border border-purple-700/60">
                                            Super-Cluster: {clusterDetail.super_cluster_id.slice(0, 16)}
                                        </span>
                                    )}
                                </div>

                                <div className="flex items-center gap-3">
                                    {/* Attribute Search */}
                                    <div className="relative">
                                        <Search className="w-3.5 h-3.5 absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
                                        <input
                                            type="text"
                                            placeholder="Filter attributes..."
                                            value={attrFilter}
                                            onChange={(e) => setAttrFilter(e.target.value)}
                                            className="pl-8 pr-3 py-1 bg-gray-900/80 rounded-lg text-xs border border-gray-700 focus:border-blue-500 focus:outline-none w-44"
                                        />
                                    </div>

                                    {/* Empty Toggle */}
                                    <button
                                        onClick={() => setHideEmptyAttrs(!hideEmptyAttrs)}
                                        className={`flex items-center gap-1.5 px-3 py-1 rounded-lg text-xs border transition-colors ${
                                            hideEmptyAttrs
                                                ? 'bg-blue-600/20 border-blue-500/40 text-blue-300 hover:bg-blue-600/30'
                                                : 'bg-gray-700 border-gray-600 text-gray-300 hover:bg-gray-600'
                                        }`}
                                        title={hideEmptyAttrs ? 'Show empty attributes' : 'Hide empty attributes'}
                                    >
                                        {hideEmptyAttrs ? <EyeOff className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
                                        {hideEmptyAttrs
                                            ? `Empty Hidden (${goldenProfileCategories?.totalEmpty || 0})`
                                            : 'Showing All'}
                                    </button>
                                </div>
                            </div>

                            {/* Categorized Attributes Display */}
                            <div className="space-y-4">
                                {goldenProfileCategories &&
                                    Object.entries(goldenProfileCategories.categories).map(([catName, attrs]) => {
                                        if (attrs.length === 0) return null
                                        return (
                                            <div key={catName} className="space-y-2">
                                                <h3 className="text-xs font-semibold uppercase tracking-wider text-gray-400">
                                                    {catName} ({attrs.length})
                                                </h3>
                                                <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-2.5">
                                                    {attrs.map(attr => {
                                                        const isValueEmpty =
                                                            attr.value === null ||
                                                            attr.value === undefined ||
                                                            attr.value === '' ||
                                                            attr.value === '-' ||
                                                            attr.value === '—'

                                                        return (
                                                            <div
                                                                key={attr.key}
                                                                className={`rounded-lg p-2.5 border transition-all ${
                                                                    isValueEmpty
                                                                        ? 'bg-gray-900/30 border-gray-800 text-gray-500'
                                                                        : 'bg-gray-900/70 border-gray-700/80 hover:border-gray-600'
                                                                }`}
                                                            >
                                                                <span className="text-[11px] font-medium text-gray-400 block truncate" title={attr.label}>
                                                                    {attr.label}
                                                                </span>
                                                                <span
                                                                    className={`text-xs font-semibold mt-1 block truncate ${
                                                                        isValueEmpty
                                                                            ? 'text-gray-600 font-mono'
                                                                            : typeof attr.value === 'boolean'
                                                                            ? attr.value ? 'text-emerald-400' : 'text-rose-400'
                                                                            : 'text-gray-100'
                                                                    }`}
                                                                    title={String(attr.value ?? '—')}
                                                                >
                                                                    {isValueEmpty ? '—' : String(attr.value)}
                                                                </span>
                                                            </div>
                                                        )
                                                    })}
                                                </div>
                                            </div>
                                        )
                                    })}
                            </div>
                        </div>
                    )}

                    {/* Dynamic Entity Type Filter Bar / Legend */}
                    {uniqueSources.length > 0 && (
                        <div className="bg-gray-800/80 rounded-xl p-3 border border-gray-700/80 flex flex-wrap items-center justify-between gap-3 shadow-md">
                            <div className="flex flex-wrap items-center gap-2">
                                <span className="text-xs font-medium text-gray-400 flex items-center gap-1.5 mr-1">
                                    <Filter className="w-3.5 h-3.5" />
                                    Entity Types ({uniqueSources.length}):
                                </span>
                                {uniqueSources.map(sourceId => {
                                    const colorScheme = getEntityColor(sourceId, uniqueSources)
                                    const isHidden = hiddenSources.has(sourceId)
                                    const formatted = formatSourceName(sourceId)
                                    const count = sourceCounts[sourceId] || 0

                                    return (
                                        <button
                                            key={sourceId}
                                            onClick={() => toggleSourceFilter(sourceId)}
                                            className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border transition-all ${
                                                isHidden
                                                    ? 'bg-gray-900/40 text-gray-500 border-gray-800 line-through opacity-60'
                                                    : `${colorScheme.badgeBg} ${colorScheme.badgeText} ${colorScheme.badgeBorder} shadow-sm hover:brightness-110`
                                            }`}
                                            title={`Toggle ${formatted} in graph`}
                                        >
                                            <span
                                                className={`w-2 h-2 rounded-full ${isHidden ? 'bg-gray-600' : ''}`}
                                                style={!isHidden ? { backgroundColor: colorScheme.bg } : undefined}
                                            />
                                            <span>{formatted}</span>
                                            <span className="opacity-75 font-mono text-[10px]">({count})</span>
                                        </button>
                                    )
                                })}
                            </div>

                            {hiddenSources.size > 0 && (
                                <button
                                    onClick={resetFilters}
                                    className="text-xs text-blue-400 hover:text-blue-300 font-medium underline underline-offset-2 ml-auto"
                                >
                                    Show All ({hiddenSources.size} hidden)
                                </button>
                            )}
                        </div>
                    )}

                    {/* Main Visualizer & Edge Inspector */}
                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                        {/* Graph Canvas */}
                        <div className="lg:col-span-2 space-y-4">
                            <div className="bg-gray-800 rounded-xl p-4 border border-gray-700 shadow-xl space-y-3">
                                <div className="flex flex-wrap items-center justify-between gap-2">
                                    <div className="flex items-center gap-2 min-w-0">
                                        <h3 className="text-base font-semibold text-white whitespace-nowrap">
                                            Cluster Entities ({graphNodes.length} / {clusterDetail.entities.length})
                                        </h3>
                                        <span
                                            className="text-xs font-mono text-gray-400 bg-gray-900 px-2 py-0.5 rounded border border-gray-700 truncate max-w-[220px]"
                                            title={clusterDetail.resolved_id}
                                        >
                                            {clusterDetail.resolved_id}
                                        </span>
                                    </div>
                                    <div className="flex items-center gap-3 shrink-0">
                                        <span className="text-xs text-gray-400 whitespace-nowrap tabular-nums">
                                            {graphEdges.length} connections
                                        </span>
                                        <button
                                            onClick={() => handlePreviewRtbf(clusterDetail.resolved_id)}
                                            disabled={rtbfLoading !== null}
                                            className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs font-medium bg-rose-950/60 hover:bg-rose-900/70 disabled:opacity-60 disabled:cursor-not-allowed text-rose-300 border border-rose-800/70 transition-colors whitespace-nowrap"
                                            title="GDPR / CCPA Right to be Forgotten cascading purge"
                                        >
                                            {rtbfLoading === 'preview' ? (
                                                <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                            ) : (
                                                <Trash2 className="w-3.5 h-3.5" />
                                            )}
                                            <span>{rtbfLoading === 'preview' ? 'Checking...' : 'GDPR Forget'}</span>
                                        </button>
                                    </div>
                                </div>

                                <div className="h-[520px]">
                                    <IdentityGraph
                                        nodes={graphNodes}
                                        edges={graphEdges}
                                        selectedNodeId={selectedNode?.id}
                                        onSelectNode={setSelectedNode}
                                        layoutName={layoutName}
                                        onLayoutChange={setLayoutName}
                                    />
                                </div>
                            </div>

                            {/* Selected Node Details Drawer */}
                            {selectedNode && (
                                <div className="bg-gray-800/90 rounded-xl p-4 border border-blue-500/50 shadow-xl space-y-3 animate-fadeIn">
                                    <div className="flex items-center justify-between pb-2 border-b border-gray-700/60">
                                        <div className="flex items-center gap-2">
                                            <span
                                                className="w-3 h-3 rounded-full"
                                                style={{ backgroundColor: selectedNode.color }}
                                            />
                                            <h4 className="text-sm font-bold text-white">
                                                Selected Entity: {formatSourceName(selectedNode.source_id)}
                                            </h4>
                                            <span className="text-xs font-mono px-2 py-0.5 rounded bg-gray-900 text-gray-400 border border-gray-700">
                                                {selectedNode.source_id}
                                            </span>
                                        </div>
                                        <div className="flex items-center gap-2">
                                            <button
                                                onClick={() => handleStewardshipOverride('ISOLATE', selectedNode.id)}
                                                className="inline-flex items-center gap-1 px-2 py-1 rounded text-xs bg-amber-950/60 hover:bg-amber-900/70 text-amber-300 border border-amber-800/70"
                                                title="Quarantine this entity into a singleton on next run (ISOLATE)"
                                            >
                                                <ShieldAlert className="w-3 h-3" />
                                                <span>Isolate Entity</span>
                                            </button>
                                            <button
                                                onClick={() => setSelectedNode(null)}
                                                className="text-xs text-gray-400 hover:text-white flex items-center gap-1"
                                            >
                                                <X className="w-3.5 h-3.5" />
                                                Deselect
                                            </button>
                                        </div>
                                    </div>

                                    <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-xs">
                                        <div className="bg-gray-900/70 p-3 rounded-lg border border-gray-700/80 flex items-center justify-between">
                                            <div>
                                                <span className="text-gray-400 block text-[11px]">Primary Source Key</span>
                                                <span className="font-mono text-gray-200 font-semibold truncate block mt-0.5">
                                                    {selectedNode.source_key}
                                                </span>
                                            </div>
                                            <button
                                                onClick={() => copyToClipboard(selectedNode.source_key, 'key')}
                                                className="p-1.5 text-gray-400 hover:text-white bg-gray-800 rounded border border-gray-700"
                                                title="Copy source key"
                                            >
                                                {copiedKey === 'key' ? (
                                                    <Check className="w-3.5 h-3.5 text-green-400" />
                                                ) : (
                                                    <Copy className="w-3.5 h-3.5" />
                                                )}
                                            </button>
                                        </div>

                                        <div className="bg-gray-900/70 p-3 rounded-lg border border-gray-700/80 flex items-center justify-between">
                                            <div>
                                                <span className="text-gray-400 block text-[11px]">Graph Degree</span>
                                                <span className="font-mono text-blue-400 font-semibold block mt-0.5">
                                                    {selectedNode.degree || 0} direct incident links
                                                </span>
                                            </div>
                                            <span className="text-xs text-gray-500">
                                                {displayedEdges.length} filtered edges
                                            </span>
                                        </div>
                                    </div>
                                </div>
                            )}
                        </div>

                        {/* Edge Details Panel */}
                        <div className="bg-gray-800 rounded-xl p-4 border border-gray-700 shadow-xl flex flex-col h-[585px]">
                            <div className="flex items-center justify-between pb-3 border-b border-gray-700/60">
                                <div className="flex items-center gap-2">
                                    <h3 className="text-base font-semibold text-white">
                                        Edge Connections ({displayedEdges.length})
                                    </h3>
                                </div>
                                {selectedNode && (
                                    <button
                                        onClick={() => setSelectedNode(null)}
                                        className="text-xs text-blue-400 hover:text-blue-300"
                                    >
                                        Clear Filter
                                    </button>
                                )}
                            </div>

                            {/* Search filter for edges */}
                            <div className="mt-3 mb-2 relative">
                                <Search className="w-3.5 h-3.5 absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
                                <input
                                    type="text"
                                    placeholder="Filter edges by value/type..."
                                    value={edgeSearch}
                                    onChange={(e) => setEdgeSearch(e.target.value)}
                                    className="w-full pl-8 pr-3 py-1.5 bg-gray-900/80 rounded-lg text-xs border border-gray-700 focus:border-blue-500 focus:outline-none"
                                />
                            </div>

                            {selectedNode && (
                                <div className="mb-2 p-2 bg-blue-950/40 rounded-lg border border-blue-800/40 text-xs text-blue-300 flex items-center justify-between">
                                    <span>Filtered by: <strong>{formatSourceName(selectedNode.source_id)}</strong></span>
                                    <span className="font-mono text-[10px] opacity-75">{selectedNode.source_key.slice(0, 10)}…</span>
                                </div>
                            )}

                            {/* Edge list */}
                            <div className="space-y-2.5 overflow-y-auto flex-1 pr-1">
                                {displayedEdges.length === 0 ? (
                                    <div className="text-center py-12 text-gray-500 text-xs">
                                        {selectedNode
                                            ? 'No active edges match the current filters for this node.'
                                            : 'No edges found in this cluster.'}
                                    </div>
                                ) : (
                                    displayedEdges.map((edge, i) => {
                                        const badge = getIdentifierBadge(edge.identifier_type)
                                        const leftSource = edge.left_entity_key.split(':')[0] || 'Entity'
                                        const rightSource = edge.right_entity_key.split(':')[0] || 'Entity'
                                        const leftColor = getEntityColor(leftSource, uniqueSources)
                                        const rightColor = getEntityColor(rightSource, uniqueSources)

                                        return (
                                            <div
                                                key={i}
                                                className="p-3 bg-gray-900/70 hover:bg-gray-700/40 rounded-lg border border-gray-700/60 transition-colors text-xs space-y-2"
                                            >
                                                {/* Identifier Type & Value */}
                                                <div className="flex items-center justify-between">
                                                    <span
                                                        className={`px-2 py-0.5 rounded text-[11px] font-medium border ${badge.badgeBg} ${badge.badgeText} ${badge.border}`}
                                                    >
                                                        {edge.identifier_type}
                                                    </span>
                                                    <span className="font-mono text-[10px] text-gray-400">
                                                        Rule: {edge.rule_id || 'EXACT'}
                                                    </span>
                                                    <button
                                                        onClick={() => handleStewardshipOverride('DO_NOT_LINK', edge.left_entity_key, edge.right_entity_key)}
                                                        className="text-[10px] px-1.5 py-0.5 rounded bg-gray-800 hover:bg-rose-950/60 text-gray-400 hover:text-rose-300 border border-gray-700 hover:border-rose-800/60 transition-colors"
                                                        title="Add DO_NOT_LINK override between these two entities"
                                                    >
                                                        Unlink Pair
                                                    </button>
                                                </div>

                                                <p className="font-mono text-xs text-gray-200 font-medium truncate bg-gray-800/60 px-2 py-1 rounded border border-gray-700/40">
                                                    {edge.identifier_value}
                                                </p>

                                                {/* Connecting Entities */}
                                                <div className="flex items-center justify-between text-[11px] text-gray-400 pt-1 border-t border-gray-800">
                                                    <span className="flex items-center gap-1.5 truncate max-w-[45%]">
                                                        <span
                                                            className="w-2 h-2 rounded-full flex-shrink-0"
                                                            style={{ backgroundColor: leftColor.bg }}
                                                        />
                                                        <span className="truncate" title={edge.left_entity_key}>
                                                            {formatSourceName(leftSource)}
                                                        </span>
                                                    </span>

                                                    <span className="text-gray-500">↔</span>

                                                    <span className="flex items-center gap-1.5 truncate max-w-[45%] justify-end">
                                                        <span className="truncate" title={edge.right_entity_key}>
                                                            {formatSourceName(rightSource)}
                                                        </span>
                                                        <span
                                                            className="w-2 h-2 rounded-full flex-shrink-0"
                                                            style={{ backgroundColor: rightColor.bg }}
                                                        />
                                                    </span>
                                                </div>
                                            </div>
                                        )
                                    })
                                )}
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {/* Empty State */}
            {!selectedCluster && searchQuery.trim().length < 3 && (
                <div className="text-center py-24 text-gray-500 bg-gray-800/30 rounded-2xl border border-gray-800/60">
                    <Search className="w-14 h-14 mx-auto mb-3 opacity-40 text-blue-400" />
                    <h3 className="text-base font-semibold text-gray-300">Search for any identity entity</h3>
                    <p className="text-xs text-gray-400 mt-1 max-w-sm mx-auto">
                        Type an email address, phone number, loyalty ID, or customer key to inspect its cluster and connected graph.
                    </p>
                </div>
            )}
        </div>
    )
}
