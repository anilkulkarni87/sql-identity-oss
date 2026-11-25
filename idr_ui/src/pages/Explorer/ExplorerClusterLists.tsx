import { Hash, Search } from 'lucide-react'
import { ClusterSummary } from './types'

interface ExplorerClusterListsProps {
    isConnected: boolean
    hasSetupStatus: boolean
    searching: boolean
    selectedLens: string
    searchQuery: string
    selectedCluster: string | null
    resultsCollapsed: boolean
    onToggleResultsCollapsed: () => void
    searchResults?: ClusterSummary[]
    topClusters?: ClusterSummary[]
    onSelectCluster: (resolvedId: string) => void
}

export default function ExplorerClusterLists({
    isConnected,
    hasSetupStatus,
    searching,
    selectedLens,
    searchQuery,
    selectedCluster,
    resultsCollapsed,
    onToggleResultsCollapsed,
    searchResults,
    topClusters,
    onSelectCluster,
}: ExplorerClusterListsProps) {
    const trimmed = searchQuery.trim()

    return (
        <>
            {!selectedLens && trimmed.length > 0 && trimmed.length < 3 && (
                <p className="text-xs text-ink-muted">
                    Type at least 3 characters to query the identity index.
                </p>
            )}

            {(selectedLens || trimmed.length >= 3) && (
                <div className="surface-panel overflow-hidden">
                    <div className="px-3 py-2 bg-subtle border-b border-line flex items-center justify-between text-xs text-ink-muted">
                        <span>
                            {hasSetupStatus && !isConnected
                                ? 'Warehouse disconnected — connect in Identity Studio to search.'
                                : searching
                                    ? 'Querying identity index…'
                                    : `${searchResults?.length || 0} matching clusters`}
                        </span>
                        {searchResults && searchResults.length > 0 && (
                            selectedCluster ? (
                                <button
                                    type="button"
                                    onClick={onToggleResultsCollapsed}
                                    className="text-accent hover:underline font-medium"
                                >
                                    {resultsCollapsed ? `Show all ${searchResults.length} results` : 'Hide list'}
                                </button>
                            ) : (
                                <span>Select a cluster to inspect</span>
                            )
                        )}
                    </div>
                    {(!resultsCollapsed || !selectedCluster) && (
                        <div className="max-h-52 overflow-y-auto divide-y divide-line">
                            {searchResults && searchResults.length === 0 && !searching ? (
                                <div className="px-3 py-5 text-center text-xs text-ink-muted">
                                    No clusters matched &ldquo;{searchQuery}&rdquo;.
                                </div>
                            ) : (
                                searchResults?.map((result) => {
                                    const isSelected = selectedCluster === result.resolved_id
                                    return (
                                        <button
                                            key={result.resolved_id}
                                            type="button"
                                            onClick={() => onSelectCluster(result.resolved_id)}
                                            className={`w-full text-left px-3 py-2 flex items-center justify-between gap-3 transition-colors ${
                                                isSelected
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
                    )}
                </div>
            )}

            {!selectedCluster && !selectedLens && trimmed.length < 3 && (
                topClusters && topClusters.length > 0 ? (
                    <div className="surface-panel overflow-hidden">
                        <div className="px-3.5 py-2.5 bg-subtle border-b border-line flex flex-wrap items-center justify-between gap-2">
                            <div>
                                <h2 className="text-[13px] font-semibold text-ink">Notable Resolved Clusters</h2>
                                <p className="text-[11px] text-ink-muted">
                                    Select a multi-entity cluster below to inspect its graph and Golden Record, or search any identifier above.
                                </p>
                            </div>
                            <span className="badge-neutral font-mono tabular-nums">
                                Top {topClusters.length}
                            </span>
                        </div>
                        <div className="divide-y divide-line">
                            {topClusters.map((cluster) => (
                                <button
                                    key={cluster.resolved_id}
                                    type="button"
                                    onClick={() => onSelectCluster(cluster.resolved_id)}
                                    className="w-full text-left px-3.5 py-2.5 flex items-center justify-between gap-3 bg-surface hover:bg-subtle transition-colors"
                                >
                                    <div className="flex items-center gap-2 min-w-0">
                                        <Hash className="w-3.5 h-3.5 text-accent shrink-0" />
                                        <span className="font-mono text-xs font-medium text-ink truncate">
                                            {cluster.resolved_id}
                                        </span>
                                    </div>
                                    <div className="flex items-center gap-2 shrink-0 font-mono tabular-nums">
                                        {cluster.confidence_score != null && (
                                            <span
                                                className={
                                                    cluster.confidence_score >= 0.8
                                                        ? 'badge-success'
                                                        : cluster.confidence_score >= 0.5
                                                        ? 'badge-warning'
                                                        : 'badge-danger'
                                                }
                                            >
                                                {(cluster.confidence_score * 100).toFixed(1)}%
                                            </span>
                                        )}
                                        <span className="badge-neutral">
                                            {cluster.cluster_size.toLocaleString()} {cluster.cluster_size === 1 ? 'entity' : 'entities'}
                                        </span>
                                    </div>
                                </button>
                            ))}
                        </div>
                    </div>
                ) : (
                    <div className="surface-panel py-16 px-4 text-center">
                        <Search className="w-8 h-8 mx-auto mb-2.5 text-ink-subtle" />
                        <h2 className="text-sm font-semibold text-ink">Search the identity index</h2>
                        <p className="text-xs text-ink-muted mt-1 max-w-md mx-auto">
                            Enter an email address, phone number, customer key, or cluster hash (minimum 3 characters) to inspect its entity graph and Golden Record.
                        </p>
                    </div>
                )
            )}
        </>
    )
}
