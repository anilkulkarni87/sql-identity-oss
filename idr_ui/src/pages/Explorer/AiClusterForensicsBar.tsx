import { useEffect, useState } from 'react'
import { Bot, Loader2, X } from 'lucide-react'
import { api } from '../../api/client'

interface AiClusterForensicsBarProps {
    resolvedId: string
}

export default function AiClusterForensicsBar({ resolvedId }: AiClusterForensicsBarProps) {
    const [aiExplanation, setAiExplanation] = useState<string | null>(null)
    const [aiExplaining, setAiExplaining] = useState(false)
    const [aiCustomPrompt, setAiCustomPrompt] = useState('')

    useEffect(() => {
        setAiExplanation(null)
        setAiCustomPrompt('')
    }, [resolvedId])

    const handleExplain = async () => {
        setAiExplaining(true)
        setAiExplanation(null)
        try {
            const res = await api.explainClusterAi(resolvedId, aiCustomPrompt)
            setAiExplanation(
                `[${res.provider}:${res.model} · PII Masking: ${
                    res.pii_masked ? 'ON' : 'OFF (local)'
                }]\n\n${res.explanation}`
            )
        } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : String(err)
            setAiExplanation(
                `AI Explain unavailable: ${msg} (Configure Ollama or a BYOK key in Settings.)`
            )
        } finally {
            setAiExplaining(false)
        }
    }

    return (
        <>
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
                                void handleExplain()
                            }
                        }}
                        placeholder="Ask a question about this cluster (or click Explain)…"
                        className="input-field py-1 text-xs flex-1"
                    />
                    <button
                        id="btn-explain-cluster-ai"
                        type="button"
                        disabled={aiExplaining}
                        onClick={() => void handleExplain()}
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
                <div className="p-3.5 pr-8 rounded-md bg-subtle border border-line text-xs text-ink space-y-1.5 relative leading-relaxed">
                    <button
                        type="button"
                        onClick={() => setAiExplanation(null)}
                        aria-label="Close AI explanation"
                        className="absolute top-2 right-2 p-1 rounded text-ink-muted hover:text-ink hover:bg-hover"
                    >
                        <X className="w-3.5 h-3.5" />
                    </button>
                    {aiExplanation.split('\n').map((rawLine, lineIdx) => {
                        const line = rawLine.trimEnd()
                        if (!line.trim()) return <div key={lineIdx} className="h-1" />
                        const renderInline = (text: string) =>
                            text.split(/(\*\*[^*]+\*\*|`[^`]+`)/g).map((part, i) => {
                                if (part.startsWith('**') && part.endsWith('**') && part.length > 4) {
                                    return (
                                        <strong key={i} className="font-semibold text-ink">
                                            {part.slice(2, -2)}
                                        </strong>
                                    )
                                }
                                if (part.startsWith('`') && part.endsWith('`') && part.length > 2) {
                                    return (
                                        <code
                                            key={i}
                                            className="px-1 py-0.5 rounded bg-surface border border-line font-mono text-[11px]"
                                        >
                                            {part.slice(1, -1)}
                                        </code>
                                    )
                                }
                                return <span key={i}>{part}</span>
                            })
                        const headingMatch = line.match(/^#{1,4}\s+(.*)$/)
                        if (headingMatch) {
                            return (
                                <div key={lineIdx} className="font-semibold text-ink pt-1">
                                    {renderInline(headingMatch[1])}
                                </div>
                            )
                        }
                        const bulletMatch = line.match(/^[-*]\s+(.*)$/)
                        if (bulletMatch) {
                            return (
                                <div key={lineIdx} className="flex items-start gap-1.5 pl-1">
                                    <span className="text-ink-muted select-none">•</span>
                                    <span>{renderInline(bulletMatch[1])}</span>
                                </div>
                            )
                        }
                        return <p key={lineIdx}>{renderInline(line)}</p>
                    })}
                </div>
            )}
        </>
    )
}
