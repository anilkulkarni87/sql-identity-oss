import { useEffect, useMemo, useState } from 'react'
import { Check, Copy, Eye, EyeOff, Search, ShieldAlert } from 'lucide-react'
import { GraphNodeData } from '../../components/IdentityGraph'
import {
    formatAttributeKey,
    formatSourceName,
    getEntityColor,
    getIdentifierBadge,
} from '../../utils/colors'
import { ClusterDetailData, InspectorTab } from './types'

interface ClusterInspectorProps {
    clusterDetail: ClusterDetailData
    uniqueSources: string[]
    visibleEntities: GraphNodeData[]
    selectedNode: GraphNodeData | null
    activeTab: InspectorTab
    onTabChange: (tab: InspectorTab) => void
    onSelectNode: (node: GraphNodeData | null) => void
    confirmOverrideKey: string | null
    onConfirmOverrideKeyChange: (key: string | null) => void
    copiedKey: string | null
    onCopy: (text: string, id: string) => void
    onStewardshipOverride: (
        overrideType: 'DO_NOT_LINK' | 'ISOLATE',
        entityKeyA: string,
        entityKeyB?: string
    ) => void
    onExcludeIdentifier: (identifierType: string, identifierValue: string) => void
}

export default function ClusterInspector({
    clusterDetail,
    uniqueSources,
    visibleEntities,
    selectedNode,
    activeTab,
    onTabChange,
    onSelectNode,
    confirmOverrideKey,
    onConfirmOverrideKeyChange,
    copiedKey,
    onCopy,
    onStewardshipOverride,
    onExcludeIdentifier,
}: ClusterInspectorProps) {
    const [hideEmptyAttrs, setHideEmptyAttrs] = useState(true)
    const [attrFilter, setAttrFilter] = useState('')
    const [edgeSearch, setEdgeSearch] = useState('')
    const [expandedEntityGroups, setExpandedEntityGroups] = useState<Set<string>>(new Set())

    useEffect(() => {
        setExpandedEntityGroups(new Set())
    }, [clusterDetail.resolved_id])

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
            } else if (/address|street|city|state|zip|postal|country/.test(k)) {
                categories['Location & Address'].push({ key: rawKey, label, value: rawVal })
            } else if (/amount|spend|value|price|subtotal|discount|cost|points|payment|order|store|balance/.test(k)) {
                categories['Commerce & Transactions'].push({ key: rawKey, label, value: rawVal })
            } else if (/channel|visit|timestamp|date|event|device|browser|\bos\b|\bip\b|ip_address|session|fraud|score|verified/.test(k)) {
                categories['Engagement & Activity'].push({ key: rawKey, label, value: rawVal })
            } else {
                categories['Additional Attributes'].push({ key: rawKey, label, value: rawVal })
            }
        }

        return { categories, totalEmpty }
    }, [clusterDetail?.golden_profile, hideEmptyAttrs, attrFilter])

    return (
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
                            onClick={() => onTabChange(tab.id)}
                            className={`px-3 py-1.5 text-xs font-medium rounded-t-md border-t border-x transition-colors ${
                                isActive
                                    ? 'bg-surface text-ink border-line -mb-px pb-[7px]'
                                    : 'bg-transparent text-ink-muted hover:text-ink border-transparent'
                            }`}
                        >
                            {tab.label}
                        </button>
                    )
                })}
            </div>

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
                                    onClick={() => onSelectNode(null)}
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
                                                        onClick={() => onExcludeIdentifier(edge.identifier_type, edge.identifier_value)}
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
                                                                onStewardshipOverride(
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
                                                            onClick={() => onConfirmOverrideKeyChange(null)}
                                                            className="px-1.5 py-0.5 rounded text-[11px] text-ink-muted hover:text-ink"
                                                        >
                                                            Cancel
                                                        </button>
                                                    </>
                                                ) : (
                                                    <button
                                                        type="button"
                                                        onClick={() => onConfirmOverrideKeyChange(confirmKey)}
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
                                    onClick={() => onSelectNode(null)}
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
                                            onClick={() => onCopy(selectedNode.source_key, 'srcKey')}
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
                                        {(() => {
                                            const deg =
                                                selectedNode.degree ??
                                                (clusterDetail.edges || []).filter(
                                                    (e) =>
                                                        e.left_entity_key === selectedNode.id ||
                                                        e.right_entity_key === selectedNode.id
                                                ).length
                                            return `${deg} incident ${deg === 1 ? 'link' : 'links'}`
                                        })()}
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
                                            onClick={() => onStewardshipOverride('ISOLATE', selectedNode.id)}
                                            className="btn-danger text-xs"
                                        >
                                            Confirm Isolate
                                        </button>
                                        <button
                                            type="button"
                                            onClick={() => onConfirmOverrideKeyChange(null)}
                                            className="btn-secondary text-xs"
                                        >
                                            Cancel
                                        </button>
                                    </div>
                                ) : (
                                    <button
                                        type="button"
                                        onClick={() => onConfirmOverrideKeyChange(`isolate:${selectedNode.id}`)}
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
                                                            onClick={() => onSelectNode(node)}
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
    )
}
