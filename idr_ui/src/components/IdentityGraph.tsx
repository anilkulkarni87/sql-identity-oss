import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import CytoscapeComponent from 'react-cytoscapejs'
import type { Core, NodeSingular, EventObject } from 'cytoscape'
import { ZoomIn, ZoomOut, Maximize2, RotateCcw, Tag } from 'lucide-react'
import { useTheme } from '../utils/theme'

export type GraphLayoutName = 'concentric' | 'cose' | 'breadthfirst' | 'circle'

export interface GraphNodeData {
    id: string
    source_id: string
    source_key: string
    label: string
    displayLabel?: string
    color: string
    borderColor: string
    size: number
    degree?: number
    isGroup?: boolean
    groupCount?: number
}

export interface GraphEdgeData {
    id: string
    source: string
    target: string
    label: string
    identifier_type: string
    identifier_value: string
    rule_id?: string
    isFuzzy?: boolean
}

export interface IdentityGraphProps {
    nodes: GraphNodeData[]
    edges: GraphEdgeData[]
    selectedNodeId?: string | null
    onSelectNode?: (node: GraphNodeData | null) => void
    layoutName?: GraphLayoutName
    onLayoutChange?: (layout: GraphLayoutName) => void
}

export default function IdentityGraph({
    nodes,
    edges,
    selectedNodeId,
    onSelectNode,
    layoutName = 'concentric',
    onLayoutChange,
}: IdentityGraphProps) {
    const { theme } = useTheme()
    const cyRef = useRef<Core | null>(null)
    const cyInitializedRef = useRef(false)
    const onSelectNodeRef = useRef(onSelectNode)
    onSelectNodeRef.current = onSelectNode

    const [currentLayout, setCurrentLayout] = useState<GraphLayoutName>(layoutName)
    const [hoveredNode, setHoveredNode] = useState<GraphNodeData | null>(null)
    const [showAllLabels, setShowAllLabels] = useState(false)

    useEffect(() => {
        if (layoutName !== currentLayout) {
            setCurrentLayout(layoutName)
        }
    }, [layoutName, currentLayout])

    const { elements, nodeDegreeMap } = useMemo(() => {
        const degreeMap: Record<string, number> = {}
        for (const e of edges) {
            degreeMap[e.source] = (degreeMap[e.source] || 0) + 1
            degreeMap[e.target] = (degreeMap[e.target] || 0) + 1
        }

        const isLargeCluster = nodes.length > 25

        const cytoscapeNodes = nodes.map((n) => {
            const deg = degreeMap[n.id] || 0
            const size = n.isGroup ? 34 : Math.min(42, Math.max(20, 20 + deg * 2.2))
            const groupWidth = n.isGroup ? Math.max(92, (n.label?.length || 8) * 7 + 24) : size
            const displayLabel = n.isGroup || showAllLabels || !isLargeCluster || deg >= 4 ? n.label : ''

            return {
                data: {
                    ...n,
                    degree: deg,
                    size,
                    groupWidth,
                    displayLabel,
                    fullLabel: n.label,
                },
                classes: n.isGroup ? 'group-hub' : undefined,
            }
        })

        const cytoscapeEdges = edges.map((e) => ({
            data: {
                id: e.id,
                source: e.source,
                target: e.target,
                label: e.label,
                identifier_type: e.identifier_type,
                identifier_value: e.identifier_value,
                rule_id: e.rule_id || 'EXACT',
            },
            classes: e.isFuzzy ? 'fuzzy' : 'exact',
        }))

        return {
            elements: [...cytoscapeNodes, ...cytoscapeEdges],
            nodeDegreeMap: degreeMap,
        }
    }, [nodes, edges, showAllLabels])

    const stylesheet = useMemo(() => {
        const isDark = theme === 'dark'
        const labelFg = isDark ? '#f1f5f9' : '#0f172a'
        const labelBg = isDark ? '#111620' : '#ffffff'
        const edgeColor = isDark ? '#475569' : '#94a3b8'
        const edgeText = isDark ? '#94a3b8' : '#475569'
        const edgeHighlight = isDark ? '#60a5fa' : '#1d4ed8'
        const fuzzyColor = isDark ? '#fbbf24' : '#d97706'
        const selectRing = isDark ? '#f8fafc' : '#0f172a'

        return [
            {
                selector: 'node',
                style: {
                    'background-color': 'data(color)',
                    'border-color': 'data(borderColor)',
                    'border-width': 2,
                    label: 'data(displayLabel)',
                    color: labelFg,
                    'font-size': '10px',
                    'font-family': '"IBM Plex Sans", system-ui, sans-serif',
                    'text-valign': 'bottom',
                    'text-margin-y': 5,
                    'text-background-color': labelBg,
                    'text-background-opacity': 0.85,
                    'text-background-padding': 2,
                    'text-background-shape': 'roundrectangle',
                    width: 'data(size)',
                    height: 'data(size)',
                },
            },
            {
                selector: 'node.group-hub',
                style: {
                    shape: 'round-rectangle',
                    width: 'data(groupWidth)',
                    height: 28,
                    padding: '10px',
                    'border-style': 'dashed',
                    'border-width': 2,
                    'text-valign': 'center',
                    'text-halign': 'center',
                    'text-margin-y': 0,
                    color: '#ffffff',
                    'font-size': '11px',
                    'font-weight': '600',
                    'text-background-opacity': 0,
                },
            },
            {
                selector: 'node.highlighted',
                style: {
                    'border-color': selectRing,
                    'border-width': 2.5,
                    opacity: 1,
                    'z-index': 990,
                },
            },
            {
                selector: 'node.selected',
                style: {
                    'border-color': selectRing,
                    'border-width': 3.5,
                    width: 36,
                    height: 36,
                    'z-index': 999,
                    label: 'data(fullLabel)',
                    'font-size': '11px',
                    'font-weight': '600',
                    color: labelFg,
                    'text-background-color': labelBg,
                    'text-background-opacity': 0.96,
                    'text-background-padding': 4,
                    'text-background-shape': 'roundrectangle',
                },
            },
            {
                selector: 'node.dimmed',
                style: {
                    opacity: 0.18,
                },
            },
            {
                selector: 'edge',
                style: {
                    width: 1.5,
                    'line-color': edgeColor,
                    'curve-style': 'bezier',
                    label: showAllLabels ? 'data(label)' : '',
                    color: edgeText,
                    'font-size': '10px',
                    'font-family': '"JetBrains Mono", ui-monospace, monospace',
                    'text-rotation': 'autorotate',
                    'text-background-color': labelBg,
                    'text-background-opacity': 0.9,
                    'text-background-padding': 2,
                    'text-background-shape': 'roundrectangle',
                },
            },
            {
                selector: 'edge.fuzzy',
                style: {
                    'line-style': 'dashed',
                    'line-dash-pattern': [5, 3],
                    'line-color': fuzzyColor,
                },
            },
            {
                selector: 'edge.highlighted',
                style: {
                    'line-color': edgeHighlight,
                    width: 2.5,
                    opacity: 1,
                    label: 'data(label)',
                    color: edgeHighlight,
                    'z-index': 995,
                },
            },
            {
                selector: 'edge.dimmed',
                style: {
                    opacity: 0.08,
                },
            },
        ]
    }, [theme, showAllLabels])

    const runLayout = useCallback(
        (cy: Core, name: GraphLayoutName) => {
            let config: any
            switch (name) {
                case 'concentric':
                    config = {
                        name: 'concentric',
                        animate: false,
                        concentric: (node: NodeSingular) => nodeDegreeMap[node.id()] || 1,
                        levelWidth: () => 2,
                        minNodeSpacing: nodes.length > 50 ? 35 : 52,
                        spacingFactor: 1.2,
                        avoidOverlap: true,
                        fit: true,
                        padding: 28,
                    }
                    break
                case 'breadthfirst':
                    config = {
                        name: 'breadthfirst',
                        animate: false,
                        directed: false,
                        spacingFactor: 1.35,
                        avoidOverlap: true,
                        fit: true,
                        padding: 28,
                    }
                    break
                case 'cose':
                    config = {
                        name: 'cose',
                        animate: false,
                        nodeRepulsion: () => (nodes.length > 50 ? 55000 : 35000),
                        idealEdgeLength: () => (nodes.length > 50 ? 80 : 100),
                        edgeElasticity: () => 80,
                        gravity: 0.12,
                        numIter: 800,
                        nodeOverlap: 22,
                        fit: true,
                        padding: 28,
                    }
                    break
                case 'circle':
                    config = {
                        name: 'circle',
                        animate: false,
                        spacingFactor: 1.15,
                        avoidOverlap: true,
                        fit: true,
                        padding: 28,
                    }
                    break
            }
            cy.layout(config).run()
        },
        [nodeDegreeMap, nodes.length]
    )

    useEffect(() => {
        const cy = cyRef.current
        if (!cy || elements.length === 0) return
        runLayout(cy, currentLayout)
    }, [currentLayout, elements, runLayout])

    useEffect(() => {
        const cy = cyRef.current
        if (!cy) return

        cy.batch(() => {
            cy.elements().removeClass('selected highlighted dimmed')
            if (selectedNodeId) {
                const selectedNode = cy.getElementById(selectedNodeId)
                if (selectedNode.nonempty()) {
                    selectedNode.addClass('selected')
                    const neighborhood = selectedNode.neighborhood()
                    neighborhood.nodes().addClass('highlighted')
                    neighborhood.edges().addClass('highlighted')
                    cy.elements().not(selectedNode).not(neighborhood).addClass('dimmed')
                }
            }
        })
    }, [selectedNodeId, elements])

    const handleCyInit = useCallback((cy: Core) => {
        cyRef.current = cy
        if (cyInitializedRef.current) return
        cyInitializedRef.current = true

        cy.on('tap', 'node', (evt: EventObject) => {
            const node = evt.target as NodeSingular
            onSelectNodeRef.current?.(node.data() as GraphNodeData)
        })

        cy.on('tap', (evt: EventObject) => {
            if (evt.target === cy) {
                onSelectNodeRef.current?.(null)
            }
        })

        cy.on('mouseover', 'node', (evt: EventObject) => {
            const node = evt.target as NodeSingular
            setHoveredNode(node.data() as GraphNodeData)
        })

        cy.on('mouseout', 'node', () => {
            setHoveredNode(null)
        })
    }, [])

    useEffect(() => {
        return () => {
            const cy = cyRef.current
            if (cy) {
                cy.removeAllListeners()
                cyInitializedRef.current = false
            }
        }
    }, [])

    const switchLayout = (name: GraphLayoutName) => {
        setCurrentLayout(name)
        onLayoutChange?.(name)
    }

    const handleZoomIn = () => {
        const cy = cyRef.current
        if (cy) {
            cy.zoom({
                level: cy.zoom() * 1.25,
                renderedPosition: { x: cy.width() / 2, y: cy.height() / 2 },
            })
        }
    }

    const handleZoomOut = () => {
        const cy = cyRef.current
        if (cy) {
            cy.zoom({
                level: cy.zoom() * 0.8,
                renderedPosition: { x: cy.width() / 2, y: cy.height() / 2 },
            })
        }
    }

    const handleFit = () => cyRef.current?.fit(undefined, 28)

    const handleReset = () => {
        const cy = cyRef.current
        if (cy) {
            cy.reset()
            cy.fit(undefined, 28)
        }
    }

    return (
        <div className="flex flex-col w-full h-full min-h-[480px] bg-surface">
            <div className="px-3 py-2 bg-subtle border-b border-line flex flex-wrap items-center justify-between gap-2 text-xs shrink-0">
                <div className="flex flex-wrap items-center gap-1" role="group" aria-label="Graph layout">
                    {(
                        [
                            { id: 'concentric', label: 'Concentric' },
                            { id: 'breadthfirst', label: 'Hierarchy' },
                            { id: 'cose', label: 'Force' },
                            { id: 'circle', label: 'Circle' },
                        ] as const
                    ).map((l) => (
                        <button
                            key={l.id}
                            type="button"
                            onClick={() => switchLayout(l.id)}
                            aria-pressed={currentLayout === l.id}
                            className={`px-2 py-1 rounded text-xs font-medium transition-colors ${currentLayout === l.id
                                    ? 'bg-surface text-ink border border-line-strong'
                                    : 'text-ink-muted hover:text-ink hover:bg-hover border border-transparent'
                                }`}
                        >
                            {l.label}
                        </button>
                    ))}
                </div>

                <div className="flex items-center gap-1">
                    <button
                        type="button"
                        onClick={() => setShowAllLabels((v) => !v)}
                        aria-pressed={showAllLabels}
                        aria-label="Toggle all node and edge labels"
                        className={`inline-flex items-center gap-1 px-2 py-1 rounded text-xs font-medium border transition-colors ${showAllLabels
                                ? 'bg-accent-soft text-accent border-accent-border'
                                : 'bg-surface text-ink-muted border-line hover:text-ink'
                            }`}
                    >
                        <Tag className="w-3 h-3" />
                        <span>Labels</span>
                    </button>

                    <div className="w-px h-4 bg-line mx-1" aria-hidden="true" />

                    <button
                        type="button"
                        onClick={handleZoomIn}
                        aria-label="Zoom in"
                        className="inline-flex items-center justify-center w-7 h-7 rounded text-ink-muted hover:text-ink hover:bg-hover"
                    >
                        <ZoomIn className="w-3.5 h-3.5" />
                    </button>
                    <button
                        type="button"
                        onClick={handleZoomOut}
                        aria-label="Zoom out"
                        className="inline-flex items-center justify-center w-7 h-7 rounded text-ink-muted hover:text-ink hover:bg-hover"
                    >
                        <ZoomOut className="w-3.5 h-3.5" />
                    </button>
                    <button
                        type="button"
                        onClick={handleFit}
                        aria-label="Fit graph to view"
                        className="inline-flex items-center justify-center w-7 h-7 rounded text-ink-muted hover:text-ink hover:bg-hover"
                    >
                        <Maximize2 className="w-3.5 h-3.5" />
                    </button>
                    <button
                        type="button"
                        onClick={handleReset}
                        aria-label="Reset zoom and position"
                        className="inline-flex items-center justify-center w-7 h-7 rounded text-ink-muted hover:text-ink hover:bg-hover"
                    >
                        <RotateCcw className="w-3.5 h-3.5" />
                    </button>
                </div>
            </div>

            <div className="relative flex-1 min-h-[440px]">
                <div className="absolute bottom-2.5 right-2.5 z-10 flex items-center gap-3 px-2.5 py-1 rounded bg-surface/95 border border-line text-[11px] text-ink-muted pointer-events-none">
                    <span className="inline-flex items-center gap-1.5">
                        <span className="w-4 h-0.5 bg-ink-subtle inline-block" />
                        Exact
                    </span>
                    <span className="inline-flex items-center gap-1.5">
                        <span className="w-4 border-t border-dashed border-status-warning inline-block" />
                        Fuzzy
                    </span>
                </div>

                {hoveredNode && (
                    <div className="absolute bottom-2.5 left-2.5 z-10 max-w-xs bg-surface px-3 py-2 rounded-md border border-line-strong shadow-popover text-xs space-y-0.5 pointer-events-none">
                        <div className="flex items-center gap-1.5">
                            <span
                                className="w-2 h-2 rounded-full shrink-0"
                                style={{ backgroundColor: hoveredNode.color }}
                            />
                            <span className="font-semibold text-ink truncate">{hoveredNode.label}</span>
                        </div>
                        <p className="font-mono text-[11px] text-ink-muted truncate">{hoveredNode.source_key}</p>
                        <p className="text-[11px] text-ink-muted tabular-nums">
                            Degree: <strong className="font-mono text-ink">{nodeDegreeMap[hoveredNode.id] || 0}</strong>
                        </p>
                    </div>
                )}

                <CytoscapeComponent
                    elements={elements}
                    cy={handleCyInit}
                    style={{ width: '100%', height: '100%', minHeight: '440px' }}
                    stylesheet={stylesheet as any}
                />
            </div>
        </div>
    )
}
