import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Key, Search, ChevronDown, ChevronRight, RefreshCw } from 'lucide-react'
import { api } from '../api/client'

export default function DataModel() {
    const [searchQuery, setSearchQuery] = useState('')
    const [collapsedTables, setCollapsedTables] = useState<Set<string>>(new Set())

    const { data: schema, isLoading, isError, refetch } = useQuery({
        queryKey: ['schema'],
        queryFn: () => api.getSchema(),
    })

    const toggleTable = (fqn: string) => {
        setCollapsedTables((prev) => {
            const next = new Set(prev)
            if (next.has(fqn)) next.delete(fqn)
            else next.add(fqn)
            return next
        })
    }

    const tablesBySchema = useMemo(() => {
        const q = searchQuery.trim().toLowerCase()
        const filtered = (schema || []).filter((table) => {
            if (!q) return true
            return (
                table.table_name.toLowerCase().includes(q) ||
                table.schema_name.toLowerCase().includes(q) ||
                table.fqn.toLowerCase().includes(q) ||
                (table.description || '').toLowerCase().includes(q) ||
                table.columns.some(
                    (c) =>
                        c.name.toLowerCase().includes(q) ||
                        c.type.toLowerCase().includes(q) ||
                        (c.description || '').toLowerCase().includes(q)
                )
            )
        })

        return filtered.reduce((acc, table) => {
            if (!acc[table.schema_name]) acc[table.schema_name] = []
            acc[table.schema_name].push(table)
            return acc
        }, {} as Record<string, typeof filtered>)
    }, [schema, searchQuery])

    return (
        <div className="space-y-5">
            <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-3">
                <div>
                    <h1 className="text-[18px] font-semibold tracking-[-0.015em] text-ink">
                        Data Model
                    </h1>
                    <p className="text-[13px] text-ink-muted mt-0.5">
                        Reference schemas for IDR metadata, staging, and resolved output tables.
                    </p>
                </div>

                <div className="relative w-full sm:w-72">
                    <Search className="w-3.5 h-3.5 text-ink-subtle absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none" />
                    <input
                        type="text"
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        placeholder="Filter tables or columns…"
                        aria-label="Filter data model tables or columns"
                        className="input-field pl-8 text-xs"
                    />
                </div>
            </div>

            {isError && (
                <div role="alert" className="p-3 rounded-md bg-status-danger-soft border border-status-danger-border flex items-center justify-between gap-3">
                    <span className="text-xs text-status-danger">Unable to load schema reference.</span>
                    <button type="button" onClick={() => refetch()} className="btn-secondary text-xs">
                        <RefreshCw className="w-3 h-3" />
                        Retry
                    </button>
                </div>
            )}

            {isLoading ? (
                <div className="space-y-3" aria-busy="true">
                    {[0, 1, 2].map((i) => (
                        <div key={i} className="h-36 surface-panel bg-subtle animate-pulse" />
                    ))}
                </div>
            ) : Object.keys(tablesBySchema).length === 0 ? (
                <div className="surface-panel py-12 text-center text-xs text-ink-muted">
                    No schema tables match &ldquo;{searchQuery}&rdquo;.
                </div>
            ) : (
                <div className="space-y-6">
                    {Object.entries(tablesBySchema).map(([schemaName, tables]) => (
                        <section key={schemaName} className="space-y-3">
                            <div className="flex items-center justify-between border-b border-line pb-1.5">
                                <h2 className="text-[13px] font-semibold text-ink">
                                    Schema{' '}
                                    <span className="font-mono text-accent">{schemaName}</span>
                                </h2>
                                <span className="text-xs font-mono tabular-nums text-ink-muted">
                                    {tables.length} {tables.length === 1 ? 'table' : 'tables'}
                                </span>
                            </div>

                            <div className="space-y-3">
                                {tables.map((table) => {
                                    const isCollapsed = collapsedTables.has(table.fqn)
                                    return (
                                        <div key={table.fqn} className="surface-panel overflow-hidden">
                                            <button
                                                type="button"
                                                onClick={() => toggleTable(table.fqn)}
                                                aria-expanded={!isCollapsed}
                                                className="w-full px-3.5 py-2.5 bg-subtle border-b border-line flex items-start justify-between gap-3 text-left hover:bg-hover transition-colors"
                                            >
                                                <div className="flex items-start gap-2 min-w-0">
                                                    {isCollapsed ? (
                                                        <ChevronRight className="w-4 h-4 text-ink-muted mt-0.5 shrink-0" />
                                                    ) : (
                                                        <ChevronDown className="w-4 h-4 text-ink-muted mt-0.5 shrink-0" />
                                                    )}
                                                    <div className="min-w-0">
                                                        <h3 className="text-[13px] font-semibold font-mono text-ink truncate">
                                                            {table.table_name}
                                                        </h3>
                                                        {table.description && (
                                                            <p className="text-xs text-ink-muted mt-0.5">
                                                                {table.description}
                                                            </p>
                                                        )}
                                                    </div>
                                                </div>
                                                <div className="flex items-center gap-2 shrink-0">
                                                    <span className="text-[11px] font-mono tabular-nums text-ink-muted">
                                                        {table.columns.length} cols
                                                    </span>
                                                    <span className="badge-neutral font-mono hidden sm:inline-flex">
                                                        {table.fqn}
                                                    </span>
                                                </div>
                                            </button>

                                            {!isCollapsed && (
                                                <div className="overflow-x-auto">
                                                    <table className="w-full text-left border-collapse">
                                                        <thead>
                                                            <tr>
                                                                <th scope="col" className="table-th w-56">Column</th>
                                                                <th scope="col" className="table-th w-40">Type</th>
                                                                <th scope="col" className="table-th">Description</th>
                                                            </tr>
                                                        </thead>
                                                        <tbody>
                                                            {table.columns.map((col) => (
                                                                <tr key={col.name} className="bg-surface hover:bg-subtle/60">
                                                                    <td className="table-td font-mono text-xs font-medium text-ink">
                                                                        <span className="inline-flex items-center gap-1.5">
                                                                            <span>{col.name}</span>
                                                                            {col.is_pk && (
                                                                                <span
                                                                                    className="inline-flex items-center gap-0.5 px-1 py-0.2 rounded bg-status-warning-soft text-status-warning border border-status-warning-border text-[10px] font-sans font-semibold"
                                                                                    title="Primary key"
                                                                                >
                                                                                    <Key className="w-2.5 h-2.5" aria-hidden="true" />
                                                                                    PK
                                                                                </span>
                                                                            )}
                                                                        </span>
                                                                    </td>
                                                                    <td className="table-td font-mono text-xs text-accent">
                                                                        {col.type.replace(/_type$/i, '').toUpperCase()}
                                                                    </td>
                                                                    <td className="table-td text-xs text-ink-secondary">
                                                                        {col.description || (
                                                                            <span className="text-ink-muted">—</span>
                                                                        )}
                                                                    </td>
                                                                </tr>
                                                            ))}
                                                        </tbody>
                                                    </table>
                                                </div>
                                            )}
                                        </div>
                                    )
                                })}
                            </div>
                        </section>
                    ))}
                </div>
            )}
        </div>
    )
}
