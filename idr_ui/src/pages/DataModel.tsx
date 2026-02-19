import { useQuery } from '@tanstack/react-query';
import { Database, Table, Key } from 'lucide-react';
import { api } from '../api/client';

export default function DataModel() {
    const { data: schema, isLoading } = useQuery({
        queryKey: ['schema'],
        queryFn: () => api.getSchema()
    });

    if (isLoading) {
        return <div className="p-8 text-center text-gray-500">Loading data model...</div>;
    }

    // Group tables by schema
    const tablesBySchema = schema?.reduce((acc, table) => {
        if (!acc[table.schema_name]) acc[table.schema_name] = [];
        acc[table.schema_name].push(table);
        return acc;
    }, {} as Record<string, typeof schema>) || {};

    return (
        <div className="space-y-8">
            <header>
                <h1 className="text-2xl font-bold flex items-center gap-2">
                    <Database className="w-6 h-6 text-blue-400" />
                    Data Model
                </h1>
                <p className="text-gray-400 mt-2">
                    Reference documentation for the IDR system tables, metadata, and output schemas.
                </p>
            </header>

            {Object.entries(tablesBySchema).map(([schemaName, tables]) => (
                <section key={schemaName} className="space-y-4">
                    <h2 className="text-xl font-semibold text-gray-200 border-b border-gray-700 pb-2">
                        Schema: <span className="text-blue-400 font-mono">{schemaName}</span>
                    </h2>

                    <div className="grid grid-cols-1 gap-6">
                        {tables.map(table => (
                            <div key={table.fqn} className="bg-gray-800 rounded-lg border border-gray-700 overflow-hidden">
                                <div className="p-4 bg-gray-800/50 border-b border-gray-700 flex items-start justify-between">
                                    <div>
                                        <h3 className="text-lg font-bold font-mono flex items-center gap-2">
                                            <Table className="w-4 h-4 text-gray-500" />
                                            {table.table_name}
                                        </h3>
                                        {table.description && (
                                            <p className="text-sm text-gray-400 mt-1">{table.description}</p>
                                        )}
                                    </div>
                                    <span className="text-xs font-mono px-2 py-1 bg-gray-900 rounded text-gray-500">
                                        {table.fqn}
                                    </span>
                                </div>

                                <div className="overflow-x-auto">
                                    <table className="w-full text-left text-sm">
                                        <thead className="bg-gray-900/50 text-gray-400">
                                            <tr>
                                                <th className="px-4 py-2 font-medium">Column</th>
                                                <th className="px-4 py-2 font-medium">Type</th>
                                                <th className="px-4 py-2 font-medium">Description</th>
                                            </tr>
                                        </thead>
                                        <tbody className="divide-y divide-gray-700">
                                            {table.columns.map(col => (
                                                <tr key={col.name} className="hover:bg-gray-700/30">
                                                    <td className="px-4 py-2 font-mono text-gray-200 flex items-center gap-2">
                                                        {col.name}
                                                        {col.is_pk && (
                                                            <Key className="w-3 h-3 text-yellow-500" />
                                                        )}
                                                    </td>
                                                    <td className="px-4 py-2 text-blue-300/80 font-mono text-xs">
                                                        {col.type}
                                                    </td>
                                                    <td className="px-4 py-2 text-gray-400">
                                                        {col.description || <span className="text-gray-600 italic">No description</span>}
                                                    </td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        ))}
                    </div>
                </section>
            ))}
        </div>
    );
}
