import { useState } from 'react';
import { Loader2, CheckCircle2, Settings, Play, Save, ClipboardList } from "lucide-react";
import { IDRConfig } from '../../types';
import { api } from '../../api/client';


export default function StepReview({ config, onBack, onComplete, initialSaved = false, readOnly = false, warningMessage = null }: { config: IDRConfig, onBack: () => void, onComplete: () => void, initialSaved?: boolean, readOnly?: boolean, warningMessage?: string | null }) {
    const [loading, setLoading] = useState(false);
    const [saving, setSaving] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [success, setSuccess] = useState(false);
    const [configSaved, setConfigSaved] = useState(initialSaved);

    // Run Configuration State
    const [runMode, setRunMode] = useState<'FULL' | 'INCREMENTAL'>('INCREMENTAL');
    const [strictMode, setStrictMode] = useState(false);
    const [maxIterations, setMaxIterations] = useState(10);
    const [dryRun, setDryRun] = useState(false);
    const [runResult, setRunResult] = useState<any>(null);

    const handleSave = async () => {
        if (readOnly) return;
        setSaving(true);
        setError(null);
        try {
            await api.saveSetupConfig(config);
            setConfigSaved(true);
        } catch (err: any) {
            setError(err.message || String(err));
        } finally {
            setSaving(false);
        }
    };

    const handleRun = async () => {
        setLoading(true);
        setError(null);
        try {
            const result = await api.runSetup({
                mode: runMode === 'INCREMENTAL' ? 'INCR' : 'FULL',
                strict: strictMode,
                max_iterations: maxIterations,
                dry_run: dryRun,
            });
            setRunResult(result);
            setSuccess(true);
        } catch (err: any) {
            setError(err.message || String(err));
        } finally {
            setLoading(false);
        }
    };

    const btnClass = "px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed flex items-center";
    const btnOutlineClass = "px-4 py-2 border border-gray-600 text-gray-300 rounded-md text-sm font-medium hover:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-gray-500";

    if (success) {
        if (dryRun && runResult?.dry_run_summary) {
            return (
                <div className="flex flex-col items-center justify-center h-[500px] space-y-6 text-center max-w-2xl mx-auto">
                    <div className="rounded-full bg-purple-500/20 p-6 border border-purple-500/30">
                        <ClipboardList className="w-16 h-16 text-purple-400" />
                    </div>
                    <div className="space-y-2">
                        <h2 className="text-2xl font-bold text-white">Dry Run Complete</h2>
                        <p className="text-gray-400">Analysis finished. No changes were committed to the ID graph.</p>
                    </div>

                    <div className="w-full bg-gray-800 rounded-lg p-6 border border-gray-700 grid grid-cols-1 gap-4 text-left">
                        <h3 className="font-semibold text-gray-300 border-b border-gray-700 pb-2">Proposed Changes</h3>

                        <div className="flex justify-between items-center px-2">
                            <span className="text-gray-400">Entities Impacted</span>
                            <span className="text-xl font-bold text-white">{runResult.dry_run_summary.proposed_changes || 0}</span>
                        </div>
                        {runResult.dry_run_summary.error && (
                            <div className="bg-red-900/20 text-red-200 p-2 rounded text-sm">
                                Error fetching details: {runResult.dry_run_summary.error}
                            </div>
                        )}
                        <p className="text-xs text-gray-500 mt-2">
                            To apply these changes, disable Dry Run and run the pipeline again.
                        </p>
                    </div>

                    <div className="flex gap-4">
                        <button className={btnOutlineClass} onClick={() => { setSuccess(false); setRunResult(null); }}>Back to Config</button>
                        <button className={btnClass} onClick={() => { setSuccess(false); setRunResult(null); setDryRun(false); }}>Run for Real</button>
                    </div>
                </div>
            );
        }

        return (
            <div className="flex flex-col items-center justify-center h-[400px] space-y-6 text-center">
                <div className="rounded-full bg-green-500/20 p-6 border border-green-500/30">
                    <CheckCircle2 className="w-16 h-16 text-green-400" />
                </div>
                <div className="space-y-2">
                    <h2 className="text-2xl font-bold text-white">Setup & Run Complete!</h2>
                    <p className="text-gray-400">Your Identity Resolution pipeline has finished successfully.</p>
                </div>
                <button className={btnClass} onClick={onComplete}>Go to Dashboard</button>
            </div>
        );
    }

    return (
        <div className="space-y-6 h-full flex flex-col">
            <div>
                <h2 className="text-xl font-semibold text-white">Review Configuration</h2>
                <p className="text-sm text-gray-400">Verify your setup before saving.</p>
            </div>

            <div className="flex-1 overflow-y-auto space-y-6">

                {/* Read Only / Warning Alert */}
                {readOnly && (
                    <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-4 flex gap-3">
                        <div className="text-yellow-400">
                            <Settings className="w-5 h-5" />
                        </div>
                        <div className="space-y-1">
                            <h4 className="text-sm font-semibold text-yellow-400">Limited Permissions Mode</h4>
                            <p className="text-xs text-yellow-200/80">
                                {warningMessage || "Unable to save configuration due to missing write permissions. You can review the config but cannot save changes."}
                            </p>
                        </div>
                    </div>
                )}

                <div className="space-y-2">
                    <h3 className="text-sm font-semibold uppercase tracking-wider text-gray-500">Source Tables</h3>
                    <div className="space-y-2">
                        {config.sources?.map((src, i) => (
                            <div key={i} className="p-3 border border-gray-700 rounded-lg bg-gray-800 text-sm">
                                <div className="flex justify-between items-center mb-2">
                                    <span className="font-semibold text-gray-200">{src.table}</span>
                                    <span className="text-xs bg-gray-700 px-2 py-1 rounded text-gray-300">Key: {src.entity_key}</span>
                                </div>
                                <div className="text-gray-400 text-xs">
                                    Attributes: {(src.attributes || []).map(a => a.name).join(', ') || 'None'}
                                </div>
                            </div>
                        ))}
                    </div>
                </div>

                <div className="space-y-2">
                    <h3 className="text-sm font-semibold uppercase tracking-wider text-gray-500">Matching Rules ({(config.rules?.length || 0) + (config.fuzzy_rules?.length || 0)})</h3>
                    <div className="grid grid-cols-1 gap-2">
                        {config.rules?.map((rule, i) => (
                            <div key={`exact-${i}`} className="p-3 border border-gray-700 rounded-lg bg-gray-800 text-sm flex justify-between items-center">
                                <span className="font-medium text-gray-300">Exact Match: <span className="text-white">{(rule.match_keys || []).join(', ')}</span></span>
                                <span className="text-xs bg-gray-700 px-2 py-1 rounded text-gray-400">Priority: {rule.priority}</span>
                            </div>
                        ))}
                        {config.fuzzy_rules?.map((rule, i) => (
                            <div key={`fuzzy-${i}`} className="p-3 border border-purple-900/50 rounded-lg bg-gray-800 text-sm flex justify-between items-center">
                                <div className="flex flex-col">
                                    <span className="font-medium text-purple-300">Fuzzy Match ({(rule.threshold || 0) * 100}%)</span>
                                    <span className="text-xs text-gray-500">Block: {rule.blocking_key}</span>
                                </div>
                                <span className="text-xs bg-gray-700 px-2 py-1 rounded text-gray-400">Priority: {rule.priority}</span>
                            </div>
                        ))}
                    </div>
                </div>

                {config.survivorship && config.survivorship.length > 0 && (
                    <div className="space-y-2">
                        <h3 className="text-sm font-semibold uppercase tracking-wider text-gray-500">Survivorship Rules</h3>
                        <div className="grid grid-cols-1 gap-2">
                            {config.survivorship.map((rule, i) => (
                                <div key={i} className="p-3 border border-gray-700 rounded-lg bg-gray-800 text-sm flex justify-between items-center">
                                    <div className="flex gap-2 items-center">
                                        <span className="font-medium text-gray-300">{rule.attribute}</span>
                                        <span className="text-xs bg-blue-900/50 text-blue-300 px-2 py-0.5 rounded border border-blue-900">{rule.strategy}</span>
                                    </div>
                                    <div className="text-xs text-gray-500">
                                        {rule.strategy === 'PRIORITY' ?
                                            `Priority: ${(rule.source_priority || []).join(' > ')}` :
                                            'Field: Source Watermark'
                                        }
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>
                )}

                <div className="bg-gray-900 p-4 rounded-lg overflow-x-auto border border-gray-700">
                    <h3 className="text-xs font-semibold mb-2 text-gray-500">JSON Preview</h3>
                    <pre className="text-xs font-mono text-gray-300">{JSON.stringify(config, null, 2)}</pre>
                </div>

                {/* Run Configuration */}
                <div className="space-y-4 pt-4 border-t border-gray-700">
                    <div className="flex items-center gap-2">
                        <Settings className="w-5 h-5 text-gray-400" />
                        <h2 className="text-lg font-semibold text-white">Run Configuration</h2>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div className="space-y-2">
                            <label className="text-sm font-medium text-gray-300">Run Mode</label>
                            <select
                                value={runMode}
                                onChange={(e) => setRunMode(e.target.value as any)}
                                className="w-full bg-gray-900 border border-gray-700 rounded-md px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
                            >
                                <option value="INCREMENTAL">Incremental (Faster)</option>
                                <option value="FULL">Full Refresh (Reset All)</option>
                            </select>
                        </div>

                        <div className="space-y-2">
                            <label className="text-sm font-medium text-gray-300">Max Iterations</label>
                            <input
                                type="number"
                                value={maxIterations}
                                onChange={(e) => setMaxIterations(parseInt(e.target.value) || 1)}
                                className="w-full bg-gray-900 border border-gray-700 rounded-md px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
                            />
                        </div>

                        <div className="md:col-span-2 flex flex-col sm:flex-row sm:items-center gap-6 pt-2">
                            <label className="flex items-center gap-2 cursor-pointer bg-gray-800 p-3 rounded-lg border border-gray-700 hover:border-gray-600 transition-colors">
                                <input
                                    type="checkbox"
                                    checked={strictMode}
                                    onChange={(e) => setStrictMode(e.target.checked)}
                                    className="w-4 h-4 rounded border-gray-700 bg-gray-900 text-blue-600 focus:ring-0"
                                />
                                <div className="text-sm">
                                    <span className="font-medium text-gray-300 block">Strict Mode</span>
                                    <span className="text-xs text-gray-500">Validation only (slower)</span>
                                </div>
                            </label>

                            <label className="flex items-center gap-2 cursor-pointer bg-purple-900/20 p-3 rounded-lg border border-purple-900/50 hover:border-purple-500/50 transition-colors">
                                <input
                                    type="checkbox"
                                    checked={dryRun}
                                    onChange={(e) => setDryRun(e.target.checked)}
                                    className="w-4 h-4 rounded border-gray-700 bg-gray-900 text-purple-500 focus:ring-0"
                                />
                                <div className="text-sm">
                                    <span className="font-medium text-purple-300 block">Dry Run</span>
                                    <span className="text-xs text-purple-200/50">Preview changes without saving</span>
                                </div>
                            </label>
                        </div>
                    </div>
                </div>
            </div>

            {error && (
                <div className="bg-red-500/10 border border-red-500/50 rounded-md p-3">
                    <h4 className="text-red-400 font-semibold text-sm">Error</h4>
                    <p className="text-red-300 text-sm mt-1">{error}</p>
                </div>
            )}

            <div className="flex justify-between pt-4">
                <button className={btnOutlineClass} onClick={onBack} disabled={loading || saving}>Back</button>
                <div className="flex gap-3">
                    <button
                        className={`${btnOutlineClass} ${configSaved ? 'border-green-500 text-green-400' : ''} ${readOnly ? 'opacity-50 cursor-not-allowed' : ''}`}
                        onClick={handleSave}
                        disabled={loading || saving || readOnly}
                        title={readOnly ? "Read-only mode" : "Save configuration"}
                    >
                        {saving ? <Loader2 className="mr-2 w-4 h-4 animate-spin" /> : <Save className="mr-2 w-4 h-4" />}
                        {configSaved ? 'Config Saved' : 'Save Config'}
                    </button>

                    <button
                        className={`${btnClass} ${dryRun ? 'bg-purple-600 hover:bg-purple-700' : ''}`}
                        onClick={handleRun}
                        disabled={loading || saving || !configSaved}
                        title={!configSaved ? "Save configuration first" : ""}
                    >
                        {loading ? <Loader2 className="mr-2 w-4 h-4 animate-spin" /> : <Play className="mr-2 w-4 h-4" />}
                        {dryRun ? 'Start Dry Run' : 'Run Pipeline'}
                    </button>
                </div>
            </div>
        </div>
    );
}
