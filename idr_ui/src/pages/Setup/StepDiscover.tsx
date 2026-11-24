import React, { useState, useEffect } from 'react';
import { Loader2, Search, Table as TableIcon } from "lucide-react";
import { api } from '../../api/client';

interface StepDiscoverProps {
    onNext: (tables: string[]) => void;
    onBack: () => void;
    connectionData: any;
}

export default function StepDiscover({ onNext, onBack, connectionData }: StepDiscoverProps) {
    const [schema, setSchema] = useState('');
    const [tables, setTables] = useState<string[]>([]); // All tables found
    const [filteredTables, setFilteredTables] = useState<string[]>([]);
    const [selectedTables, setSelectedTables] = useState<string[]>([]);
    const [search, setSearch] = useState('');
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => {
        // Optional: Auto-load if platform provides a default schema context
    }, [connectionData]);

    const handleScan = async (e?: React.FormEvent) => {
        e?.preventDefault();
        if (!schema && connectionData.platform !== 'duckdb') return;

        setLoading(true);
        setError(null);
        try {
            const data = await api.discoverTables(schema || undefined);
            setTables(data.tables || []);
            setFilteredTables(data.tables || []);
        } catch (err: any) {
            setError(err.message);
            setTables([]);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        if (!search.trim()) {
            setFilteredTables(tables);
        } else {
            const lower = search.toLowerCase();
            setFilteredTables(tables.filter(t => t.toLowerCase().includes(lower)));
        }
    }, [search, tables]);

    const toggleTable = (fqn: string) => {
        setSelectedTables(prev => {
            if (prev.includes(fqn)) return prev.filter(t => t !== fqn);
            return [...prev, fqn];
        });
    };

    const inputClass = "input-field";
    const btnClass = "btn-primary text-xs";
    const btnOutlineClass = "btn-secondary text-xs";

    return (
        <div className="space-y-4 h-full flex flex-col">
            <div className="flex justify-between items-start gap-3">
                <div>
                    <h2 className="text-[15px] font-semibold text-ink">Discover Sources</h2>
                    <p className="text-xs text-ink-muted">Scan your warehouse for tables to use as identity sources.</p>
                </div>

                <form onSubmit={handleScan} className="flex gap-2 items-end">
                    <div className="space-y-1">
                        <label htmlFor="schema" className="block text-xs font-medium text-ink-secondary">Schema / Dataset</label>
                        <input
                            id="schema"
                            placeholder="e.g. raw_data"
                            value={schema}
                            onChange={e => setSchema(e.target.value)}
                            className={`${inputClass} w-[200px]`}
                        />
                    </div>
                    <button type="submit" className={btnClass} disabled={loading}>
                        {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : "Scan"}
                    </button>
                </form>
            </div>

            {error && (
                <div className="bg-status-danger-soft border border-status-danger-border rounded-md p-3">
                    <h4 className="text-status-danger font-semibold text-xs">Error</h4>
                    <p className="text-ink-secondary text-xs mt-1">{error}</p>
                </div>
            )}

            {/* Table Selection Area */}
            <div className="surface-panel flex-1 flex flex-col min-h-[300px] overflow-hidden">
                <div className="p-2 border-b border-line bg-subtle flex gap-2 items-center">
                    <Search className="w-4 h-4 text-ink-subtle ml-2" />
                    <input
                        placeholder="Filter tables..."
                        className="flex-1 bg-transparent border-none text-xs text-ink focus:outline-none placeholder:text-ink-subtle"
                        value={search}
                        onChange={e => setSearch(e.target.value)}
                    />
                </div>
                <div className="flex-1 overflow-y-auto p-2">
                    {filteredTables.length === 0 && !loading && (
                        <div className="text-center text-ink-muted text-xs p-8">
                            {tables.length > 0 ? "No matches found." : "No tables found. Enter a schema and click Scan."}
                        </div>
                    )}

                    <div className="space-y-1">
                        {filteredTables.map(fqn => (
                            <div
                                key={fqn}
                                className="flex items-center space-x-3 p-2 hover:bg-subtle rounded cursor-pointer transition-colors"
                                onClick={() => toggleTable(fqn)}
                            >
                                <input
                                    type="checkbox"
                                    id={fqn}
                                    checked={selectedTables.includes(fqn)}
                                    onChange={() => toggleTable(fqn)}
                                    className="h-4 w-4 rounded border-line-strong text-accent"
                                />
                                <label htmlFor={fqn} className="cursor-pointer flex-1 flex items-center gap-2 text-xs font-mono text-ink">
                                    <TableIcon className="w-4 h-4 text-accent" />
                                    {fqn}
                                </label>
                            </div>
                        ))}
                    </div>
                </div>
                <div className="p-2 border-t border-line bg-subtle text-xs text-ink-muted flex justify-between tabular-nums">
                    <span>{selectedTables.length} selected</span>
                    <span>{tables.length} total</span>
                </div>
            </div>

            <div className="flex justify-between pt-3 border-t border-line">
                <button className={btnOutlineClass} onClick={onBack}>Back</button>
                <button className={btnClass} onClick={() => onNext(selectedTables)} disabled={selectedTables.length === 0}>
                    Next: Map Columns
                </button>
            </div>
        </div>
    );
}
