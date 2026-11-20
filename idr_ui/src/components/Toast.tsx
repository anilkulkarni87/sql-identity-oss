import React, { createContext, useCallback, useContext, useState } from 'react'
import { CheckCircle2, AlertCircle, Info, X } from 'lucide-react'

type ToastKind = 'success' | 'error' | 'info'

interface ToastItem {
    id: number
    kind: ToastKind
    title: string
    detail?: string
}

interface ToastContextValue {
    toast: (kind: ToastKind, title: string, detail?: string) => void
}

const ToastContext = createContext<ToastContextValue>({ toast: () => {} })

export function useToast() {
    return useContext(ToastContext)
}

export function ToastProvider({ children }: { children: React.ReactNode }) {
    const [items, setItems] = useState<ToastItem[]>([])

    const dismiss = useCallback((id: number) => {
        setItems((prev) => prev.filter((item) => item.id !== id))
    }, [])

    const toast = useCallback((kind: ToastKind, title: string, detail?: string) => {
        const id = Date.now() + Math.floor(Math.random() * 1000)
        setItems((prev) => [...prev.slice(-3), { id, kind, title, detail }])
        setTimeout(() => {
            setItems((prev) => prev.filter((item) => item.id !== id))
        }, 4500)
    }, [])

    return (
        <ToastContext.Provider value={{ toast }}>
            {children}
            <div
                aria-live="polite"
                aria-atomic="false"
                className="fixed bottom-4 right-4 z-50 flex flex-col gap-2 max-w-sm w-full pointer-events-none"
            >
                {items.map((item) => {
                    const styles =
                        item.kind === 'success'
                            ? 'bg-surface border-status-success-border text-ink'
                            : item.kind === 'error'
                            ? 'bg-surface border-status-danger-border text-ink'
                            : 'bg-surface border-line-strong text-ink'
                    const Icon =
                        item.kind === 'success'
                            ? CheckCircle2
                            : item.kind === 'error'
                            ? AlertCircle
                            : Info
                    const iconColor =
                        item.kind === 'success'
                            ? 'text-status-success'
                            : item.kind === 'error'
                            ? 'text-status-danger'
                            : 'text-accent'

                    return (
                        <div
                            key={item.id}
                            role={item.kind === 'error' ? 'alert' : 'status'}
                            className={`pointer-events-auto flex items-start gap-2.5 px-3.5 py-3 rounded-md border shadow-popover transition-colors ${styles}`}
                        >
                            <Icon className={`w-4 h-4 shrink-0 mt-0.5 ${iconColor}`} />
                            <div className="flex-1 min-w-0">
                                <p className="text-[13px] font-medium leading-snug text-ink">{item.title}</p>
                                {item.detail && (
                                    <p className="text-xs text-ink-muted mt-0.5 break-words">{item.detail}</p>
                                )}
                            </div>
                            <button
                                type="button"
                                onClick={() => dismiss(item.id)}
                                className="p-1 rounded text-ink-muted hover:text-ink hover:bg-hover transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent"
                                aria-label="Dismiss notification"
                            >
                                <X className="w-3.5 h-3.5" />
                            </button>
                        </div>
                    )
                })}
            </div>
        </ToastContext.Provider>
    )
}
