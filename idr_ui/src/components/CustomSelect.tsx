import { useEffect, useRef, useState } from 'react'
import { Check, ChevronDown } from 'lucide-react'

export interface SelectOption {
    value: string
    label: string
}

interface CustomSelectProps {
    value: string
    onChange: (value: string) => void
    options: SelectOption[]
    className?: string
    buttonClassName?: string
    menuClassName?: string
    placeholder?: string
    ariaLabel?: string
}

export function CustomSelect({
    value,
    onChange,
    options,
    className = '',
    buttonClassName = '',
    menuClassName = '',
    placeholder = 'Select…',
    ariaLabel,
}: CustomSelectProps) {
    const [open, setOpen] = useState(false)
    const containerRef = useRef<HTMLDivElement>(null)

    const selected = options.find((o) => o.value === value)

    useEffect(() => {
        if (!open) return
        const handleOutside = (e: MouseEvent) => {
            if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
                setOpen(false)
            }
        }
        const handleKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') setOpen(false)
        }
        document.addEventListener('mousedown', handleOutside)
        document.addEventListener('keydown', handleKey)
        return () => {
            document.removeEventListener('mousedown', handleOutside)
            document.removeEventListener('keydown', handleKey)
        }
    }, [open])

    return (
        <div ref={containerRef} className={`relative inline-block ${className}`}>
            <button
                type="button"
                onClick={(e) => {
                    e.stopPropagation()
                    setOpen((prev) => !prev)
                }}
                aria-expanded={open}
                aria-label={ariaLabel}
                className={`w-full flex items-center justify-between gap-2 bg-surface hover:bg-subtle border ${
                    open ? 'border-accent ring-1 ring-accent' : 'border-line-strong'
                } rounded-md px-2.5 py-1.5 text-left text-xs text-ink transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-accent ${buttonClassName}`}
            >
                <span className="truncate">{selected ? selected.label : placeholder}</span>
                <ChevronDown
                    className={`w-3.5 h-3.5 shrink-0 text-ink-muted transition-transform duration-150 ${
                        open ? 'rotate-180 text-accent' : ''
                    }`}
                />
            </button>

            {open && (
                <div
                    onClick={(e) => e.stopPropagation()}
                    className={`absolute right-0 mt-1 z-50 min-w-[240px] w-max max-w-sm max-h-64 overflow-y-auto rounded-md bg-surface border border-line-strong shadow-popover p-1 ${menuClassName}`}
                >
                    {options.map((opt) => {
                        const isSelected = opt.value === value
                        return (
                            <button
                                key={opt.value}
                                type="button"
                                onClick={() => {
                                    onChange(opt.value)
                                    setOpen(false)
                                }}
                                className={`w-full flex items-center justify-between gap-3 px-2.5 py-1.5 rounded text-left text-xs transition-colors ${
                                    isSelected
                                        ? 'bg-selected text-accent font-medium'
                                        : 'text-ink-secondary hover:bg-subtle hover:text-ink'
                                }`}
                            >
                                <span className="truncate">{opt.label}</span>
                                {isSelected && <Check className="w-3.5 h-3.5 shrink-0 text-accent" />}
                            </button>
                        )
                    })}
                </div>
            )}
        </div>
    )
}
