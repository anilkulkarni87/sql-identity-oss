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

    const inputClass = "bg-gray-900 border border-gray-700 rounded-md px-3 py-2 text-sm text-gray-100 placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500";
    const btnClass = "px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed flex items-center";
    const btnOutlineClass = "px-4 py-2 border border-gray-600 text-gray-300 rounded-md text-sm font-medium hover:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-gray-500";

    return (
        <div className="space-y-6 h-full flex flex-col">
            <div className="flex justify-between items-start">
                <div>
                    <h2 className="text-xl font-semibold text-white">Discover Sources</h2>
                    <p className="text-sm text-gray-400">Scan your warehouse for tables to use as identity sources.</p>
                </div>

                <form onSubmit={handleScan} className="flex gap-2 items-end">
                    <div className="space-y-1">
                        <label htmlFor="schema" className="block text-sm font-medium text-gray-300">Schema / Dataset</label>
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
                <div className="bg-red-500/10 border border-red-500/50 rounded-md p-3">
                    <h4 className="text-red-400 font-semibold text-sm">Error</h4>
                    <p className="text-red-300 text-sm mt-1">{error}</p>
                </div>
            )}

            {/* Table Selection Area */}
            <div className="border border-gray-700 rounded-md flex-1 flex flex-col min-h-[300px] bg-gray-900/50">
                <div className="p-2 border-b border-gray-700 bg-gray-800/50 flex gap-2 items-center">
                    <Search className="w-4 h-4 text-gray-400 ml-2" />
                    <input
                        placeholder="Filter tables..."
                        className="flex-1 bg-transparent border-none text-sm text-gray-200 focus:ring-0 placeholder-gray-500"
                        value={search}
                        onChange={e => setSearch(e.target.value)}
                    />
                </div>
                <div className="flex-1 overflow-y-auto p-2">
                    {filteredTables.length === 0 && !loading && (
                        <div className="text-center text-gray-500 p-8">
                            {tables.length > 0 ? "No matches found." : "No tables found. Enter a schema and click Scan."}
                        </div>
                    )}

                    <div className="space-y-1">
                        {filteredTables.map(fqn => (
                            <div
                                key={fqn}
                                className="flex items-center space-x-3 p-2 hover:bg-gray-800 rounded cursor-pointer transition-colors"
                                onClick={() => toggleTable(fqn)}
                            >
                                <input
                                    type="checkbox"
                                    id={fqn}
                                    checked={selectedTables.includes(fqn)}
                                    onChange={() => toggleTable(fqn)}
                                    className="h-4 w-4 rounded border-gray-600 text-blue-600 focus:ring-blue-500 bg-gray-700"
                                />
                                <label htmlFor={fqn} className="cursor-pointer flex-1 flex items-center gap-2 text-sm text-gray-200">
                                    <TableIcon className="w-4 h-4 text-blue-400" />
                                    {fqn}
                                </label>
                            </div>
                        ))}
                    </div>
                </div>
                <div className="p-2 border-t border-gray-700 bg-gray-800/50 text-xs text-gray-400 flex justify-between">
                    <span>{selectedTables.length} selected</span>
                    <span>{tables.length} total</span>
                </div>
            </div>

            <div className="flex justify-between pt-4">
                <button className={btnOutlineClass} onClick={onBack}>Back</button>
                <button className={btnClass} onClick={() => onNext(selectedTables)} disabled={selectedTables.length === 0}>
                    Next: Map Columns
                </button>
            </div>
        </div>
    );
}
