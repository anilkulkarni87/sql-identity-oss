
import CytoscapeComponent from 'react-cytoscapejs'

interface IdentityGraphProps {
    elements: any[]
}

export default function IdentityGraph({ elements }: IdentityGraphProps) {
    return (
        <CytoscapeComponent
            elements={elements}
            style={{ width: '100%', height: '420px' }}
            stylesheet={[
                {
                    selector: 'node',
                    style: {
                        'background-color': '#3b82f6',
                        'label': 'data(label)',
                        'color': '#fff',
                        'font-size': '10px',
                        'text-valign': 'bottom',
                        'text-margin-y': 8,
                        'width': 30,
                        'height': 30
                    }
                },
                {
                    selector: 'edge',
                    style: {
                        'width': 2,
                        'line-color': '#6b7280',
                        'target-arrow-color': '#6b7280',
                        'curve-style': 'bezier',
                        'label': 'data(label)',
                        'color': '#9ca3af',
                        'font-size': '8px',
                        'text-rotation': 'autorotate'
                    }
                }
            ]}
            layout={{ name: 'cose', animate: false }}
        />
    )
}
