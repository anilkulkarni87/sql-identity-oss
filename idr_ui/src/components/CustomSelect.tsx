import { useEffect, useRef, useState } from 'react';
import { Check, ChevronDown } from 'lucide-react';

export interface SelectOption {
    value: string;
    label: string;
}

interface CustomSelectProps {
    value: string;
    onChange: (value: string) => void;
    options: SelectOption[];
    className?: string;
    buttonClassName?: string;
    menuClassName?: string;
    placeholder?: string;
    ariaLabel?: string;
}

export function CustomSelect({
    value,
    onChange,
    options,
    className = '',
    buttonClassName = '',
    menuClassName = '',
    placeholder = 'Select...',
    ariaLabel,
}: CustomSelectProps) {
    const [open, setOpen] = useState(false);
    const containerRef = useRef<HTMLDivElement>(null);

    const selected = options.find((o) => o.value === value);

    useEffect(() => {
        if (!open) return;
        const handleOutside = (e: MouseEvent) => {
            if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
                setOpen(false);
            }
        };
        const handleKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') setOpen(false);
        };
        document.addEventListener('mousedown', handleOutside);
        document.addEventListener('keydown', handleKey);
        return () => {
            document.removeEventListener('mousedown', handleOutside);
            document.removeEventListener('keydown', handleKey);
        };
    }, [open]);

    return (
        <div ref={containerRef} className={`relative inline-block ${className}`}>
            <button
                type="button"
                onClick={(e) => {
                    e.stopPropagation();
                    setOpen((prev) => !prev);
                }}
                aria-expanded={open}
                aria-label={ariaLabel}
                className={`w-full flex items-center justify-between gap-2 bg-slate-900/95 hover:bg-slate-800/90 border ${
                    open ? 'border-blue-500 ring-1 ring-blue-500/40' : 'border-slate-700'
                } rounded-lg px-3 py-1.5 text-left text-slate-200 transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 ${buttonClassName}`}
            >
                <span className="truncate">{selected ? selected.label : placeholder}</span>
                <ChevronDown
                    className={`w-3.5 h-3.5 shrink-0 text-slate-400 transition-transform duration-150 ${
                        open ? 'rotate-180 text-blue-400' : ''
                    }`}
                />
            </button>

            {open && (
                <div
                    onClick={(e) => e.stopPropagation()}
                    className={`absolute right-0 mt-1.5 z-50 min-w-[260px] w-max max-w-sm max-h-64 overflow-y-auto rounded-xl bg-slate-900/95 backdrop-blur-md border border-slate-700/90 shadow-2xl shadow-black/60 p-1.5 ${menuClassName}`}
                >
                    {options.map((opt) => {
                        const isSelected = opt.value === value;
                        return (
                            <button
                                key={opt.value}
                                type="button"
                                onClick={() => {
                                    onChange(opt.value);
                                    setOpen(false);
                                }}
                                className={`w-full flex items-center justify-between gap-3 px-2.5 py-2 rounded-lg text-left text-xs transition-colors ${
                                    isSelected
                                        ? 'bg-blue-600/20 text-blue-300 font-medium'
                                        : 'text-slate-300 hover:bg-slate-800/80 hover:text-white'
                                }`}
                            >
                                <span className="truncate">{opt.label}</span>
                                {isSelected && <Check className="w-3.5 h-3.5 shrink-0 text-blue-400" />}
                            </button>
                        );
                    })}
                </div>
            )}
        </div>
    );
}

