import React, { createContext, useCallback, useContext, useState } from 'react';
import { CheckCircle2, AlertCircle, Info, X } from 'lucide-react';

type ToastKind = 'success' | 'error' | 'info';

interface ToastItem {
    id: number;
    kind: ToastKind;
    title: string;
    detail?: string;
}

interface ToastContextValue {
    toast: (kind: ToastKind, title: string, detail?: string) => void;
}

const ToastContext = createContext<ToastContextValue>({ toast: () => {} });

export function useToast() {
    return useContext(ToastContext);
}

export function ToastProvider({ children }: { children: React.ReactNode }) {
    const [items, setItems] = useState<ToastItem[]>([]);

    const dismiss = useCallback((id: number) => {
        setItems((prev) => prev.filter((item) => item.id !== id));
    }, []);

    const toast = useCallback((kind: ToastKind, title: string, detail?: string) => {
        const id = Date.now() + Math.floor(Math.random() * 1000);
        setItems((prev) => [...prev.slice(-3), { id, kind, title, detail }]);
        setTimeout(() => {
            setItems((prev) => prev.filter((item) => item.id !== id));
        }, 4200);
    }, []);

    return (
        <ToastContext.Provider value={{ toast }}>
            {children}
            <div className="fixed bottom-5 right-5 z-50 flex flex-col gap-2.5 max-w-sm w-full pointer-events-none">
                {items.map((item) => {
                    const styles =
                        item.kind === 'success'
                            ? 'bg-emerald-950/90 border-emerald-500/40 text-emerald-100'
                            : item.kind === 'error'
                            ? 'bg-rose-950/90 border-rose-500/40 text-rose-100'
                            : 'bg-slate-900/95 border-blue-500/40 text-slate-100';
                    const Icon =
                        item.kind === 'success'
                            ? CheckCircle2
                            : item.kind === 'error'
                            ? AlertCircle
                            : Info;
                    const iconColor =
                        item.kind === 'success'
                            ? 'text-emerald-400'
                            : item.kind === 'error'
                            ? 'text-rose-400'
                            : 'text-blue-400';

                    return (
                        <div
                            key={item.id}
                            className={`pointer-events-auto flex items-start gap-3 p-3.5 rounded-xl border backdrop-blur-md shadow-2xl transition-all ${styles}`}
                        >
                            <Icon className={`w-5 h-5 shrink-0 mt-0.5 ${iconColor}`} />
                            <div className="flex-1 min-w-0">
                                <p className="text-sm font-medium leading-snug">{item.title}</p>
                                {item.detail && (
                                    <p className="text-xs opacity-80 mt-0.5 break-words">{item.detail}</p>
                                )}
                            </div>
                            <button
                                onClick={() => dismiss(item.id)}
                                className="text-slate-400 hover:text-white transition-colors"
                                aria-label="Dismiss notification"
                            >
                                <X className="w-4 h-4" />
                            </button>
                        </div>
                    );
                })}
            </div>
        </ToastContext.Provider>
    );
}

