import { useEffect, useRef, useState, useMemo } from 'react'
import CytoscapeComponent from 'react-cytoscapejs'
import type { Core, NodeSingular, EventObject } from 'cytoscape'
import { ZoomIn, ZoomOut, Maximize2, RotateCcw, Layers } from 'lucide-react'

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
}

export interface GraphEdgeData {
    id: string
    source: string
    target: string
    label: string
    identifier_type: string
    identifier_value: string
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
    onLayoutChange
}: IdentityGraphProps) {
    const cyRef = useRef<Core | null>(null)
    const [currentLayout, setCurrentLayout] = useState<GraphLayoutName>(layoutName)
    const [hoveredNode, setHoveredNode] = useState<GraphNodeData | null>(null)

    // Sync layout prop if changed from parent
    useEffect(() => {
        if (layoutName !== currentLayout) {
            setCurrentLayout(layoutName)
        }
    }, [layoutName])

    // Compute node degrees for dynamic sizing & central hub placement
    const { elements, nodeDegreeMap } = useMemo(() => {
        const degreeMap: Record<string, number> = {}
        for (const e of edges) {
            degreeMap[e.source] = (degreeMap[e.source] || 0) + 1
            degreeMap[e.target] = (degreeMap[e.target] || 0) + 1
        }

        const isLargeCluster = nodes.length > 25

        const cytoscapeNodes = nodes.map(n => {
            const deg = degreeMap[n.id] || 0
            // Larger nodes for central hubs with high connectivity
            const size = Math.min(46, Math.max(22, 22 + deg * 2.5))
            // Only show labels by default in small clusters or on hub nodes
            const displayLabel = !isLargeCluster || deg >= 4 ? n.label : ''

            return {
                data: {
                    ...n,
                    degree: deg,
                    size,
                    displayLabel,
                    fullLabel: n.label
                }
            }
        })

        const cytoscapeEdges = edges.map(e => ({
            data: {
                id: e.id,
                source: e.source,
                target: e.target,
                label: e.label,
                identifier_type: e.identifier_type,
                identifier_value: e.identifier_value
            }
        }))

        return {
            elements: [...cytoscapeNodes, ...cytoscapeEdges],
            nodeDegreeMap: degreeMap
        }
    }, [nodes, edges])

    // Get layout configuration for Cytoscape
    const getLayoutConfig = (name: GraphLayoutName) => {
        switch (name) {
            case 'concentric':
                return {
                    name: 'concentric',
                    animate: false,
                    concentric: (node: NodeSingular) => {
                        const id = node.id()
                        return nodeDegreeMap[id] || 1
                    },
                    levelWidth: () => 2,
                    minNodeSpacing: nodes.length > 50 ? 35 : 55,
                    spacingFactor: 1.25,
                    avoidOverlap: true,
                    fit: true,
                    padding: 30
                }
            case 'breadthfirst':
                return {
                    name: 'breadthfirst',
                    animate: false,
                    directed: false,
                    spacingFactor: 1.4,
                    avoidOverlap: true,
                    fit: true,
                    padding: 30
                }
            case 'cose':
                return {
                    name: 'cose',
                    animate: false,
                    nodeRepulsion: () => (nodes.length > 50 ? 55000 : 35000),
                    idealEdgeLength: () => (nodes.length > 50 ? 80 : 100),
                    edgeElasticity: () => 80,
                    gravity: 0.12,
                    numIter: 1000,
                    nodeOverlap: 25,
                    fit: true,
                    padding: 30
                }
            case 'circle':
                return {
                    name: 'circle',
                    animate: false,
                    spacingFactor: 1.2,
                    avoidOverlap: true,
                    fit: true,
                    padding: 30
                }
        }
    }

    // Run layout when layout name or elements change
    useEffect(() => {
        const cy = cyRef.current
        if (!cy || elements.length === 0) return

        const layoutConfig = getLayoutConfig(currentLayout)
        const layout = cy.layout(layoutConfig)
        layout.run()
    }, [currentLayout, elements])

    // Apply selection / focus highlighting
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

                    // Dim other elements
                    cy.elements().not(selectedNode).not(neighborhood).addClass('dimmed')
                }
            }
        })
    }, [selectedNodeId, elements])

    // Handle Cy instance initialization
    const handleCyInit = (cy: Core) => {
        cyRef.current = cy

        // Tap on node
        cy.on('tap', 'node', (evt: EventObject) => {
            const node = evt.target as NodeSingular
            const nodeData = node.data() as GraphNodeData
            if (onSelectNode) {
                onSelectNode(nodeData)
            }
        })

        // Tap on background to deselect
        cy.on('tap', (evt: EventObject) => {
            if (evt.target === cy) {
                if (onSelectNode) {
                    onSelectNode(null)
                }
            }
        })

        // Mouse hover interactions for tooltip
        cy.on('mouseover', 'node', (evt: EventObject) => {
            const node = evt.target as NodeSingular
            setHoveredNode(node.data() as GraphNodeData)
        })

        cy.on('mouseout', 'node', () => {
            setHoveredNode(null)
        })
    }

    const switchLayout = (name: GraphLayoutName) => {
        setCurrentLayout(name)
        if (onLayoutChange) {
            onLayoutChange(name)
        }
    }

    const handleZoomIn = () => {
        const cy = cyRef.current
        if (cy) {
            cy.zoom({
                level: cy.zoom() * 1.25,
                renderedPosition: { x: cy.width() / 2, y: cy.height() / 2 }
            })
        }
    }

    const handleZoomOut = () => {
        const cy = cyRef.current
        if (cy) {
            cy.zoom({
                level: cy.zoom() * 0.8,
                renderedPosition: { x: cy.width() / 2, y: cy.height() / 2 }
            })
        }
    }

    const handleFit = () => {
        const cy = cyRef.current
        if (cy) {
            cy.fit(undefined, 30)
        }
    }

    const handleReset = () => {
        const cy = cyRef.current
        if (cy) {
            cy.reset()
            cy.fit(undefined, 30)
        }
    }

    return (
        <div className="relative w-full h-full min-h-[460px] bg-gray-900 rounded-xl overflow-hidden border border-gray-800">
            {/* Top Toolbar: Layout Selector and Controls */}
            <div className="absolute top-3 left-3 z-10 flex items-center gap-1.5 bg-gray-800/90 backdrop-blur-md px-2 py-1.5 rounded-lg border border-gray-700/80 shadow-lg text-xs">
                <span className="text-gray-400 font-medium mr-1 flex items-center gap-1">
                    <Layers className="w-3.5 h-3.5" />
                    Layout:
                </span>
                {(
                    [
                        { id: 'concentric', label: 'Concentric' },
                        { id: 'breadthfirst', label: 'Hierarchy' },
                        { id: 'cose', label: 'Force' },
                        { id: 'circle', label: 'Circle' }
                    ] as const
                ).map(l => (
                    <button
                        key={l.id}
                        onClick={() => switchLayout(l.id)}
                        className={`px-2 py-1 rounded transition-colors ${
                            currentLayout === l.id
                                ? 'bg-blue-600 text-white font-medium shadow-sm'
                                : 'text-gray-300 hover:bg-gray-700'
                        }`}
                        title={`Switch to ${l.label} layout`}
                    >
                        {l.label}
                    </button>
                ))}
            </div>

            {/* Top-Right: Zoom and Reset Controls */}
            <div className="absolute top-3 right-3 z-10 flex items-center gap-1 bg-gray-800/90 backdrop-blur-md p-1 rounded-lg border border-gray-700/80 shadow-lg">
                <button
                    onClick={handleZoomIn}
                    className="p-1.5 text-gray-300 hover:text-white hover:bg-gray-700 rounded transition-colors"
                    title="Zoom in"
                >
                    <ZoomIn className="w-4 h-4" />
                </button>
                <button
                    onClick={handleZoomOut}
                    className="p-1.5 text-gray-300 hover:text-white hover:bg-gray-700 rounded transition-colors"
                    title="Zoom out"
                >
                    <ZoomOut className="w-4 h-4" />
                </button>
                <div className="w-[1px] h-4 bg-gray-700 mx-0.5" />
                <button
                    onClick={handleFit}
                    className="p-1.5 text-gray-300 hover:text-white hover:bg-gray-700 rounded transition-colors"
                    title="Fit to view"
                >
                    <Maximize2 className="w-4 h-4" />
                </button>
                <button
                    onClick={handleReset}
                    className="p-1.5 text-gray-300 hover:text-white hover:bg-gray-700 rounded transition-colors"
                    title="Reset view"
                >
                    <RotateCcw className="w-4 h-4" />
                </button>
            </div>

            {/* Hover Tooltip Overlay */}
            {hoveredNode && (
                <div className="absolute bottom-3 left-3 z-10 max-w-sm bg-gray-800/95 backdrop-blur-md px-3 py-2 rounded-lg border border-gray-700 shadow-xl text-xs space-y-1 pointer-events-none">
                    <div className="flex items-center gap-2">
                        <span
                            className="w-2.5 h-2.5 rounded-full"
                            style={{ backgroundColor: hoveredNode.color }}
                        />
                        <span className="font-semibold text-white">{hoveredNode.label}</span>
                    </div>
                    <p className="font-mono text-gray-400 truncate">Key: {hoveredNode.source_key}</p>
                    <p className="text-gray-400">
                        Connections:{' '}
                        <span className="text-blue-400 font-medium">
                            {nodeDegreeMap[hoveredNode.id] || 0}
                        </span>
                    </p>
                </div>
            )}

            {/* Cytoscape Canvas */}
            <CytoscapeComponent
                elements={elements}
                cy={handleCyInit}
                style={{ width: '100%', height: '100%', minHeight: '460px' }}
                stylesheet={[
                    {
                        selector: 'node',
                        style: {
                            'background-color': 'data(color)',
                            'border-color': 'data(borderColor)',
                            'border-width': 2,
                            'label': 'data(displayLabel)',
                            'color': '#f3f4f6',
                            'font-size': '10px',
                            'font-family': 'Inter, system-ui, sans-serif',
                            'text-valign': 'bottom',
                            'text-margin-y': 6,
                            'width': 'data(size)',
                            'height': 'data(size)',
                            'transition-property': 'background-color, border-width, border-color, opacity, width, height',
                            'transition-duration': 0.15
                        }
                    },
                    {
                        selector: 'node.highlighted',
                        style: {
                            'border-color': '#ffffff',
                            'border-width': 3,
                            'opacity': 1,
                            'z-index': 990
                        }
                    },
                    {
                        selector: 'node.selected',
                        style: {
                            'border-color': '#ffffff',
                            'border-width': 4,
                            'width': 40,
                            'height': 40,
                            'z-index': 999,
                            'label': 'data(fullLabel)',
                            'font-size': '11px',
                            'font-weight': 'bold',
                            'color': '#ffffff',
                            'text-background-color': '#111827',
                            'text-background-opacity': 0.9,
                            'text-background-padding': 4,
                            'text-background-shape': 'roundrectangle'
                        }
                    },
                    {
                        selector: 'node.dimmed',
                        style: {
                            'opacity': 0.15
                        }
                    },
                    {
                        selector: 'edge',
                        style: {
                            'width': 1.5,
                            'line-color': '#4b5563',
                            'curve-style': 'bezier',
                            'label': 'data(label)',
                            'color': '#9ca3af',
                            'font-size': '8px',
                            'font-family': 'Inter, system-ui, sans-serif',
                            'text-rotation': 'autorotate',
                            'text-background-color': '#111827',
                            'text-background-opacity': 0.75,
                            'text-background-padding': 2,
                            'text-background-shape': 'roundrectangle',
                            'transition-property': 'line-color, width, opacity',
                            'transition-duration': 0.15
                        }
                    },
                    {
                        selector: 'edge.highlighted',
                        style: {
                            'line-color': '#60a5fa',
                            'width': 3,
                            'opacity': 1,
                            'color': '#93c5fd',
                            'z-index': 995
                        }
                    },
                    {
                        selector: 'edge.dimmed',
                        style: {
                            'opacity': 0.08
                        }
                    }
                ]}
            />
        </div>
    )
}
