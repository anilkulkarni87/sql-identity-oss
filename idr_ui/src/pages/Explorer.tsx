import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import IdentityGraph from '../components/IdentityGraph'
import { Search, X } from 'lucide-react'
import { api } from '../api/client'

interface ClusterSummary {
    resolved_id: string
    cluster_size: number
    confidence_score: number | null
}

export default function Explorer() {
    const [searchQuery, setSearchQuery] = useState('')
    const [selectedCluster, setSelectedCluster] = useState<string | null>(null)

    const { data: searchResults, isLoading: searching } = useQuery({
        queryKey: ['search', searchQuery],
        queryFn: () => api.searchEntities(searchQuery),
        enabled: searchQuery.length >= 3
    })

    const { data: clusterDetail } = useQuery({
        queryKey: ['cluster', selectedCluster],
        queryFn: () => api.getCluster(selectedCluster!),
        enabled: !!selectedCluster
    })

    // Convert cluster data to Cytoscape elements
    const graphElements = clusterDetail ? [
        // Nodes
        ...clusterDetail.entities.map(e => ({
            data: {
                id: e.entity_key,
                label: `${e.source_id}\n${e.source_key.length > 15 ? e.source_key.slice(0, 15) + '...' : e.source_key}`
            }
        })),
        // Edges
        ...clusterDetail.edges.map((e, i) => ({
            data: {
                id: `edge-${i}`,
                source: e.left_entity_key,
                target: e.right_entity_key,
                label: e.identifier_type
            }
        }))
    ] : []

    return (
        <div className="space-y-6">
            <h1 className="text-2xl font-bold">Identity Graph Explorer</h1>

            {/* Search Bar */}
            <div className="relative">
                <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" />
                <input
                    type="text"
                    placeholder="Search by partial email, phone, or ID (e.g. 'john.doe', '555-01')..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="w-full pl-12 pr-4 py-3 bg-gray-800 rounded-xl border border-gray-700 focus:border-blue-500 focus:outline-none"
                />
                {searchQuery && (
                    <button
                        onClick={() => setSearchQuery('')}
                        className="absolute right-4 top-1/2 -translate-y-1/2 text-gray-400 hover:text-white"
                    >
                        <X className="w-5 h-5" />
                    </button>
                )}
            </div>

            {/* Search Results */}
            {searchQuery.length >= 3 && (
                <div className="bg-gray-800 rounded-xl p-4">
                    <h3 className="text-sm text-gray-400 mb-2">
                        {searching ? 'Searching...' : `Found ${searchResults?.length || 0} clusters`}
                    </h3>
                    <div className="space-y-2 max-h-60 overflow-y-auto">
                        {searchResults?.map((result: ClusterSummary) => (
                            <button
                                key={result.resolved_id}
                                onClick={() => setSelectedCluster(result.resolved_id)}
                                className={`w-full text-left p-3 rounded-lg transition-colors ${selectedCluster === result.resolved_id
                                    ? 'bg-blue-600'
                                    : 'bg-gray-700 hover:bg-gray-600'
                                    }`}
                            >
                                <div className="flex justify-between">
                                    <span className="font-mono text-sm">{result.resolved_id}</span>
                                    <span className="text-sm text-gray-300">
                                        {result.cluster_size} entities
                                    </span>
                                </div>
                                {result.confidence_score && (
                                    <div className="text-xs text-gray-400 mt-1">
                                        Confidence: {(result.confidence_score * 100).toFixed(1)}%
                                    </div>
                                )}
                            </button>
                        ))}
                    </div>
                </div>
            )}

            {/* Graph Visualization */}
            {selectedCluster && clusterDetail && (
                <div className="space-y-6">
                    {/* Golden Profile Banner */}
                    {clusterDetail.golden_profile && Object.keys(clusterDetail.golden_profile).length > 0 && (
                        <div className="bg-gray-800 rounded-xl p-4 border border-blue-500/30">
                            <div className="flex items-center justify-between mb-3">
                                <div className="flex items-center gap-3">
                                    <h3 className="text-lg font-semibold text-blue-400">
                                        Unified Golden Profile
                                    </h3>
                                    {clusterDetail.super_cluster_id && (
                                        <span className="px-2.5 py-0.5 rounded-full text-xs font-mono bg-purple-900/40 text-purple-300 border border-purple-700/60">
                                            Super Cluster: {clusterDetail.super_cluster_id.slice(0, 16)}
                                        </span>
                                    )}
                                </div>
                                <span className="text-xs font-mono text-gray-400">
                                    {clusterDetail.resolved_id}
                                </span>
                            </div>
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                {Object.entries(clusterDetail.golden_profile).map(([key, val]) => (
                                    <div key={key} className="bg-gray-900/60 rounded-lg p-3 border border-gray-700">
                                        <span className="text-xs uppercase tracking-wider text-gray-400 block">
                                            {key}
                                        </span>
                                        <span className="text-sm font-medium text-gray-100 mt-1 block truncate" title={String(val ?? '—')}>
                                            {val !== null && val !== undefined && val !== '' ? String(val) : '—'}
                                        </span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}

                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                        {/* Graph */}
                        <div className="lg:col-span-2 bg-gray-800 rounded-xl p-4 h-[500px]">
                            <div className="flex items-center justify-between mb-4">
                                <h3 className="text-lg font-semibold">
                                    Cluster: {selectedCluster.slice(0, 20)}...
                                </h3>
                                {clusterDetail.super_cluster_id && (
                                    <span className="px-2.5 py-0.5 rounded-full text-xs font-mono bg-purple-900/40 text-purple-300 border border-purple-700/60">
                                        Fuzzy Super-Cluster: {clusterDetail.super_cluster_id.slice(0, 16)}
                                    </span>
                                )}
                            </div>
                            <IdentityGraph
                                elements={graphElements}
                            />
                        </div>

                        {/* Edge Details */}
                        <div className="bg-gray-800 rounded-xl p-4">
                            <h3 className="text-lg font-semibold mb-4">Edge Details ({clusterDetail.edges.length})</h3>
                            <div className="space-y-3 max-h-[450px] overflow-y-auto">
                                {clusterDetail.edges.length === 0 ? (
                                    <p className="text-sm text-gray-400">Singleton entity cluster (no connecting edges).</p>
                                ) : (
                                    clusterDetail.edges.map((edge, i) => (
                                        <div key={i} className="p-3 bg-gray-700 rounded-lg text-sm">
                                            <div className="flex justify-between text-gray-400">
                                                <span>{edge.left_entity_key.slice(0, 10)}...</span>
                                                <span>↔</span>
                                                <span>{edge.right_entity_key.slice(0, 10)}...</span>
                                            </div>
                                            <div className="mt-2">
                                                <span className="px-2 py-1 bg-blue-600/30 text-blue-300 rounded text-xs">
                                                    {edge.identifier_type}
                                                </span>
                                                <p className="mt-1 font-mono text-xs text-gray-300 truncate">
                                                    {edge.identifier_value}
                                                </p>
                                            </div>
                                        </div>
                                    ))
                                )}
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {/* Empty State */}
            {!selectedCluster && searchQuery.length < 3 && (
                <div className="text-center py-20 text-gray-500">
                    <Search className="w-16 h-16 mx-auto mb-4 opacity-50" />
                    <p className="text-lg">Search for an email, phone, or identifier</p>
                    <p className="text-sm mt-2">to explore the identity graph</p>
                </div>
            )}
        </div>
    )
}
