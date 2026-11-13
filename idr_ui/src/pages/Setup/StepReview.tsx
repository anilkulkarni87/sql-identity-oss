import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
    Loader2, CheckCircle2, Settings, Play, Save, ClipboardList,
    ChevronDown, ChevronRight, Database, Shield, Layers, Code,
    Activity, Clock, Cpu, FileSpreadsheet, GitMerge, Network, Sparkles,
    ArrowRight, Copy, Check, Edit3, AlertCircle
} from "lucide-react";
import { IDRConfig } from '../../types';
import { api } from '../../api/client';
import { useToast } from '../../components/Toast';


export default function StepReview({
    config,
    onBack,
    onComplete,
    initialSaved = false,
    readOnly = false,
    warningMessage = null,
    onUpdateConfig
}: {
    config: IDRConfig,
    onBack: () => void,
    onComplete: () => void,
    initialSaved?: boolean,
    readOnly?: boolean,
    warningMessage?: string | null,
    onUpdateConfig?: (config: IDRConfig) => void
}) {
    const { toast } = useToast();
    const navigate = useNavigate();
    type ReviewPhase = 'idle' | 'saving' | 'running' | 'success';
    const [phase, setPhase] = useState<ReviewPhase>('idle');
    const [error, setError] = useState<string | null>(null);
    const [configSaved, setConfigSaved] = useState(initialSaved);

    // JSON Edit State
    const [isEditingJson, setIsEditingJson] = useState(false);
    const [jsonInput, setJsonInput] = useState('');
    const [jsonError, setJsonError] = useState<string | null>(null);
    const [jsonCopied, setJsonCopied] = useState(false);
    const [jsonSuccess, setJsonSuccess] = useState<string | null>(null);

    // Run Configuration State
    const [runMode, setRunMode] = useState<'FULL' | 'INCREMENTAL'>('INCREMENTAL');
    const [strictMode, setStrictMode] = useState(false);
    const [maxIterations, setMaxIterations] = useState(30);
    const [maxGroupSize, setMaxGroupSize] = useState(10000);
    const [dryRun, setDryRun] = useState(false);
    const [runResult, setRunResult] = useState<any>(null);

    // Collapsible sections (all collapsed by default so Run Controls are immediately visible!)
    const [openSection, setOpenSection] = useState<'sources' | 'rules' | 'survivorship' | 'json' | null>(null);

    // Live Run Progress Polling State
    const [liveProgress, setLiveProgress] = useState<{
        run_id?: string | null;
        status: string;
        elapsed_seconds?: number;
        current_stage?: string | null;
        current_detail?: string | null;
        stages: Array<{
            name: string;
            label: string;
            order: number;
            status: 'pending' | 'running' | 'completed' | 'skipped';
            duration_seconds?: number | null;
            rows_affected?: number | null;
            detail?: string | null;
        }>;
    } | null>(null);

    const [saveProgress, setSaveProgress] = useState<{
        status: string;
        step?: string;
        label?: string;
        percent: number;
        detail?: string;
        elapsed_seconds?: number;
    } | null>(null);

    const saving = phase === 'saving';
    const loading = phase === 'running';
    const success = phase === 'success';

    useEffect(() => {
        if (phase !== 'saving') return;
        let cancelled = false;

        const poll = async () => {
            try {
                const prog = await api.getSaveConfigProgress();
                if (cancelled) return;
                setSaveProgress(prog);
                if (prog.status === 'SUCCESS') {
                    setConfigSaved(true);
                    setPhase('idle');
                    toast('success', 'Configuration saved', prog.message || 'Metadata persisted to warehouse.');
                } else if (prog.status === 'ERROR') {
                    const errMsg = prog.error || prog.detail || 'Failed to save configuration';
                    setError(errMsg);
                    setPhase('idle');
                    toast('error', 'Failed to save configuration', errMsg);
                }
            } catch {
                // Keep polling transiently
            }
        };

        poll();
        const interval = setInterval(poll, 350);
        return () => {
            cancelled = true;
            clearInterval(interval);
        };
    }, [phase]);

    useEffect(() => {
        if (phase !== 'running') return;

        let cancelled = false;

        // Try Server-Sent Events (SSE) for instant real-time stage updates
        let eventSource: EventSource | null = null;
        try {
            const rawBaseUrl = import.meta.env.VITE_API_BASE_URL?.trim();
            const baseUrl = (rawBaseUrl && rawBaseUrl.length > 0 ? rawBaseUrl : '/api').replace(/\/$/, '');
            eventSource = new EventSource(`${baseUrl}/setup/run/events`);
            eventSource.onmessage = (event) => {
                if (cancelled || !event.data) return;
                try {
                    const parsed = JSON.parse(event.data);
                    if (parsed.progress) {
                        setLiveProgress(parsed.progress);
                    } else if (parsed.stages) {
                        setLiveProgress(parsed);
                    }
                } catch {
                    // Ignore JSON parse errors from keepalives
                }
            };
            eventSource.onerror = () => {
                eventSource?.close();
                eventSource = null;
            };
        } catch {
            // Fall back to HTTP polling if EventSource is not supported
        }

        const poll = async () => {
            try {
                const prog = await api.getRunProgress();
                if (!cancelled && prog && prog.status !== 'IDLE') {
                    setLiveProgress(prog);
                }
            } catch (err) {
                console.warn("Live progress poll error:", err);
            }
        };

        poll();
        const interval = setInterval(poll, 300); // 300ms polling for responsive, low-latency stage transitions
        return () => {
            cancelled = true;
            clearInterval(interval);
            if (eventSource) {
                eventSource.close();
            }
        };
    }, [phase]);

    const handleSave = async () => {
        if (readOnly) return;
        setPhase('saving');
        setError(null);
        setSaveProgress({
            status: 'SAVING',
            step: 'validating',
            label: 'Validating configuration',
            percent: 5,
            detail: 'Starting configuration save...',
            elapsed_seconds: 0,
        });
        try {
            const cleanConfig = {
                ...config,
                sources: config.sources || [],
                rules: config.rules || [],
                fuzzy_rules: config.fuzzy_rules || [],
                survivorship: config.survivorship || [],
                exclusions: config.exclusions || [],
                overrides: config.overrides || [],
            };
            await api.saveSetupConfigAsync(cleanConfig);
        } catch (err: any) {
            setError(err.message || String(err));
            setPhase('idle');
        }
    };

    const handleRun = async () => {
        setPhase('running');
        setError(null);
        setLiveProgress({
            status: 'RUNNING',
            elapsed_seconds: 0,
            current_stage: 'preflight',
            current_detail: 'Starting pipeline...',
            stages: [
                { name: 'preflight', label: 'Preflight & Schema Check', order: 1, status: 'running' },
                { name: 'entity_extraction', label: 'Entity Extraction', order: 2, status: 'pending' },
                { name: 'identifier_extraction', label: 'Identifier Extraction', order: 3, status: 'pending' },
                { name: 'attribute_extraction', label: 'Attribute Extraction', order: 4, status: 'pending' },
                { name: 'edge_building', label: 'Edge Building', order: 5, status: 'pending' },
                { name: 'label_propagation', label: 'Label Propagation (Graph)', order: 6, status: 'pending' },
                ...(strictMode ? [] : [{ name: 'fuzzy_matching', label: 'Fuzzy Matching', order: 7, status: 'pending' as const }]),
                {
                    name: dryRun ? 'dry_run_analysis' : 'output_generation',
                    label: dryRun ? 'Dry Run Analysis' : 'Output & Golden Profiles',
                    order: strictMode ? 7 : 8,
                    status: 'pending'
                }
            ]
        });
        try {
            const result = await api.runSetup({
                mode: runMode === 'INCREMENTAL' ? 'INCR' : 'FULL',
                strict: strictMode,
                max_iterations: maxIterations,
                max_group_size: maxGroupSize,
                dry_run: dryRun,
            });
            // Fetch final progress snapshot so all completed stages reflect accurate row counts & durations
            try {
                const finalProg = await api.getRunProgress();
                if (finalProg && finalProg.status !== 'IDLE') {
                    setLiveProgress(finalProg);
                }
            } catch {
                // Ignore transient errors on final snapshot
            }
            setRunResult(result);
            setPhase('success');
        } catch (err: any) {
            setError(err.message || String(err));
            setPhase('idle');
        }
    };

    const toggleSection = (sec: 'sources' | 'rules' | 'survivorship' | 'json') => {
        setOpenSection(prev => (prev === sec ? null : sec));
    };

    const getStageIcon = (stageName: string, isRunning: boolean) => {
        if (isRunning) return <Loader2 className="w-4 h-4 text-blue-400 animate-spin" />;
        const s = stageName.toLowerCase();
        if (s.includes('preflight')) return <Database className="w-4 h-4 text-gray-400" />;
        if (s.includes('entity')) return <Layers className="w-4 h-4 text-cyan-400" />;
        if (s.includes('identifier')) return <Cpu className="w-4 h-4 text-purple-400" />;
        if (s.includes('attribute')) return <FileSpreadsheet className="w-4 h-4 text-amber-400" />;
        if (s.includes('edge')) return <GitMerge className="w-4 h-4 text-blue-400" />;
        if (s.includes('label') || s.includes('propagation')) return <Network className="w-4 h-4 text-emerald-400" />;
        if (s.includes('fuzzy')) return <Sparkles className="w-4 h-4 text-pink-400" />;
        return <Activity className="w-4 h-4 text-blue-400" />;
    };

    const btnClass = "px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed flex items-center";
    const btnOutlineClass = "px-4 py-2 border border-gray-600 text-gray-300 rounded-md text-sm font-medium hover:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-gray-500 flex items-center";

    const totalSources = config.sources?.length || 0;
    const totalExactRules = config.rules?.length || 0;
    const totalFuzzyRules = config.fuzzy_rules?.length || 0;
    const totalSurvivorship = config.survivorship?.length || 0;
    const totalExclusions = (config.exclusions?.length || 0) + (config.use_default_exclusions ? 33 : 0);
    const totalOverrides = config.overrides?.length || 0;

    // Progress calculation for live & completed panel
    const stagesList = liveProgress?.stages || [];
    const completedStages = stagesList.filter(s => s.status === 'completed' || s.status === 'skipped').length;
    const totalStages = stagesList.length || 8;
    const activeStageObj = stagesList.find(s => s.status === 'running');
    const isCompleted = success || liveProgress?.status === 'SUCCESS' || liveProgress?.status === 'DRY_RUN_COMPLETE';
    const progressPercent = isCompleted
        ? 100
        : Math.min(
            98,
            Math.round(((completedStages + (activeStageObj ? 0.4 : 0)) / Math.max(1, totalStages)) * 100)
        );

    return (
        <div className="space-y-6 flex flex-col">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 border-b border-gray-700 pb-4">
                <div>
                    <h2 className="text-xl font-semibold text-white">Review & Run Pipeline</h2>
                    <p className="text-sm text-gray-400">Save your configuration and trigger identity resolution.</p>
                </div>

                {/* Top Action Bar — No Scrolling Needed! */}
                <div className="flex items-center gap-3">
                    <button className={btnOutlineClass} onClick={onBack} disabled={loading || saving}>
                        Back
                    </button>
                    <button
                        className={`${btnOutlineClass} ${configSaved ? 'border-green-500 text-green-400' : ''} ${readOnly ? 'opacity-50 cursor-not-allowed' : ''}`}
                        onClick={handleSave}
                        disabled={loading || saving || readOnly}
                        title={readOnly ? "Read-only mode" : "Save configuration"}
                    >
                        {saving ? <Loader2 className="mr-2 w-4 h-4 animate-spin" /> : <Save className="mr-2 w-4 h-4" />}
                        {saving ? `Saving (${saveProgress?.percent ?? 0}%)` : configSaved ? 'Config Saved' : 'Save Config'}
                    </button>
                    <button
                        className={`${btnClass} ${dryRun ? 'bg-purple-600 hover:bg-purple-700' : ''}`}
                        onClick={handleRun}
                        disabled={loading || saving || !configSaved}
                        title={!configSaved ? "Save configuration first" : ""}
                    >
                        {loading ? <Loader2 className="mr-2 w-4 h-4 animate-spin" /> : <Play className="mr-2 w-4 h-4" />}
                        {loading ? 'Running Pipeline...' : dryRun ? 'Start Dry Run' : 'Run Pipeline'}
                    </button>
                </div>
            </div>

            {/* Smooth Batched Save Progress Banner */}
            {saving && saveProgress && (
                <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-4 space-y-2.5">
                    <div className="flex items-center justify-between text-sm">
                        <div className="flex items-center gap-2 text-blue-300 font-medium">
                            <Loader2 className="w-4 h-4 animate-spin" />
                            <span>{saveProgress.label || 'Saving configuration...'}</span>
                        </div>
                        <div className="flex items-center gap-3 text-xs text-blue-200/80 font-mono">
                            <span>{saveProgress.elapsed_seconds?.toFixed(1) ?? '0.0'}s</span>
                            <span>{saveProgress.percent}%</span>
                        </div>
                    </div>
                    <div className="w-full h-2 bg-gray-800 rounded-full overflow-hidden">
                        <div
                            className="h-full bg-gradient-to-r from-blue-500 to-indigo-400 transition-all duration-300 ease-out"
                            style={{ width: `${Math.min(100, Math.max(5, saveProgress.percent || 5))}%` }}
                        />
                    </div>
                    {saveProgress.detail && (
                        <p className="text-xs text-blue-200/70">{saveProgress.detail}</p>
                    )}
                </div>
            )}

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

            {error && (
                <div className="bg-red-500/10 border border-red-500/50 rounded-md p-3">
                    <h4 className="text-red-400 font-semibold text-sm">Error</h4>
                    <p className="text-red-300 text-sm mt-1">{error}</p>
                </div>
            )}

            {/* LIVE & COMPLETED PIPELINE EXECUTION PROGRESS PANEL */}
            {(loading || success || (liveProgress && liveProgress.status !== 'IDLE')) && liveProgress && (
                <div className={`border rounded-xl p-5 space-y-4 shadow-lg transition-all ${
                    success
                        ? 'bg-emerald-950/20 border-emerald-500/40'
                        : 'bg-gray-900/90 border-blue-500/40'
                }`}>
                    <div className="flex flex-wrap items-center justify-between gap-3">
                        <div className="flex items-center gap-3">
                            <div className={`p-2 rounded-lg border ${
                                success
                                    ? 'bg-emerald-500/20 border-emerald-500/30'
                                    : 'bg-blue-500/20 border-blue-500/30'
                            }`}>
                                {loading ? (
                                    <Loader2 className="w-5 h-5 text-blue-400 animate-spin" />
                                ) : (
                                    <CheckCircle2 className="w-5 h-5 text-emerald-400" />
                                )}
                            </div>
                            <div>
                                <div className="flex items-center gap-2">
                                    <h3 className="text-sm font-bold text-white">
                                        {success
                                            ? `Identity Resolution Complete (${runResult?.status || liveProgress.status})`
                                            : `Executing Pipeline (${runMode === 'INCREMENTAL' ? 'Incremental' : 'Full Refresh'})`}
                                    </h3>
                                    {liveProgress.run_id && (
                                        <span className="text-xs font-mono px-2 py-0.5 rounded bg-gray-800 text-gray-300 border border-gray-700">
                                            {liveProgress.run_id}
                                        </span>
                                    )}
                                </div>
                                <p className={`text-xs mt-0.5 ${success ? 'text-emerald-300' : 'text-blue-300'}`}>
                                    {success
                                        ? `Finished in ${Math.round(runResult?.duration_seconds ?? liveProgress.elapsed_seconds ?? 0)}s — All stages completed`
                                        : (liveProgress.current_detail || 'Processing stage...')}
                                </p>
                            </div>
                        </div>

                        <div className="flex items-center gap-4 text-xs font-mono">
                            <span className="px-2.5 py-1 rounded bg-gray-800 border border-gray-700 text-gray-300 flex items-center gap-1.5">
                                <Clock className={`w-3.5 h-3.5 ${success ? 'text-emerald-400' : 'text-blue-400'}`} />
                                {Math.round(liveProgress.elapsed_seconds || runResult?.duration_seconds || 0)}s elapsed
                            </span>
                            <span className={`font-bold ${success ? 'text-emerald-400' : 'text-blue-400'}`}>
                                Stage {Math.min(completedStages, totalStages)} of {totalStages} ({progressPercent}%)
                            </span>
                        </div>
                    </div>

                    {/* Overall Progress Bar */}
                    <div className="w-full bg-gray-800 h-2 rounded-full overflow-hidden">
                        <div
                            className={`h-full rounded-full transition-all duration-500 ${
                                success
                                    ? 'bg-gradient-to-r from-emerald-600 to-teal-400'
                                    : 'bg-gradient-to-r from-blue-600 to-cyan-400'
                            }`}
                            style={{ width: `${progressPercent}%` }}
                        />
                    </div>

                    {/* 8-Stage Status Grid */}
                    <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-4 gap-2.5 pt-1">
                        {stagesList.map((st) => {
                            const isRunning = !success && st.status === 'running';
                            const isDone = success || st.status === 'completed';
                            return (
                                <div
                                    key={st.name}
                                    className={`p-3 rounded-lg border text-xs transition-all ${
                                        isRunning
                                            ? 'bg-blue-950/40 border-blue-500/60 shadow-md'
                                            : isDone
                                            ? 'bg-emerald-950/20 border-emerald-700/40'
                                            : 'bg-gray-800/40 border-gray-700/50 opacity-60'
                                    }`}
                                >
                                    <div className="flex items-center justify-between gap-1.5">
                                        <div className="flex items-center gap-1.5 truncate">
                                            {isDone ? (
                                                <CheckCircle2 className="w-4 h-4 text-emerald-400 shrink-0" />
                                            ) : (
                                                getStageIcon(st.name, isRunning)
                                            )}
                                            <span className={`font-semibold truncate ${isRunning ? 'text-white' : isDone ? 'text-gray-200' : 'text-gray-400'}`}>
                                                {st.label}
                                            </span>
                                        </div>
                                        {isDone && st.duration_seconds !== null && st.duration_seconds !== undefined && (
                                            <span className="font-mono text-[11px] text-emerald-400 shrink-0">
                                                {st.duration_seconds}s
                                            </span>
                                        )}
                                    </div>
                                    <div className="flex justify-between items-center mt-2 text-[11px] font-mono text-gray-400">
                                        <span>#{st.order}</span>
                                        {isRunning ? (
                                            <span className="text-blue-300 truncate max-w-[140px]">
                                                {st.detail || 'Running...'}
                                            </span>
                                        ) : isDone ? (
                                            <span className="text-gray-300">
                                                {(st.rows_affected ?? 0).toLocaleString()} rows
                                            </span>
                                        ) : (
                                            <span>Queued</span>
                                        )}
                                    </div>
                                </div>
                            );
                        })}
                    </div>

                    {/* RUN RESULTS & ACTIONS PANEL (Rendered immediately upon success) */}
                    {success && runResult && (
                        <div className="pt-4 border-t border-gray-700/70 space-y-4">
                            {dryRun && runResult.dry_run_summary ? (
                                <div className="space-y-3">
                                    <div className="flex items-center gap-2 text-purple-300 font-semibold text-sm">
                                        <ClipboardList className="w-4 h-4" />
                                        Dry Run Analysis — Proposed Graph Changes (No Writes Committed)
                                    </div>
                                    <div className="grid grid-cols-1 sm:grid-cols-4 gap-3 text-xs">
                                        <div className="bg-gray-800/80 p-3 rounded-lg border border-gray-700">
                                            <span className="text-gray-400 block">Proposed Changes</span>
                                            <span className="text-lg font-mono font-bold text-white">
                                                {(runResult.dry_run_summary.proposed_changes ?? 0).toLocaleString()}
                                            </span>
                                        </div>
                                        <div className="bg-gray-800/80 p-3 rounded-lg border border-gray-700">
                                            <span className="text-gray-400 block">New Entities</span>
                                            <span className="text-lg font-mono font-bold text-green-400">
                                                {(runResult.dry_run_summary.new_entities ?? 0).toLocaleString()}
                                            </span>
                                        </div>
                                        <div className="bg-gray-800/80 p-3 rounded-lg border border-gray-700">
                                            <span className="text-gray-400 block">Moved Entities</span>
                                            <span className="text-lg font-mono font-bold text-amber-400">
                                                {(runResult.dry_run_summary.moved_entities ?? 0).toLocaleString()}
                                            </span>
                                        </div>
                                        <div className="bg-gray-800/80 p-3 rounded-lg border border-gray-700">
                                            <span className="text-gray-400 block">Unchanged Entities</span>
                                            <span className="text-lg font-mono font-bold text-gray-300">
                                                {(runResult.dry_run_summary.unchanged_entities ?? 0).toLocaleString()}
                                            </span>
                                        </div>
                                    </div>
                                    <div className="flex gap-3 pt-1">
                                        <button
                                            className={btnClass}
                                            onClick={() => { setDryRun(false); handleRun(); }}
                                        >
                                            <Play className="w-4 h-4 mr-1.5" />
                                            Execute Real Pipeline (Commit Changes)
                                        </button>
                                        <button
                                            className={btnOutlineClass}
                                            onClick={() => { setPhase('idle'); setRunResult(null); }}
                                        >
                                            Edit Configuration
                                        </button>
                                    </div>
                                </div>
                            ) : (
                                <div className="space-y-3">
                                    <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 text-xs">
                                        <div className="bg-gray-800/80 p-3 rounded-lg border border-gray-700">
                                            <span className="text-gray-400 block">Entities Processed</span>
                                            <span className="text-lg font-mono font-bold text-white">
                                                {(runResult.entities_processed ?? 0).toLocaleString()}
                                            </span>
                                        </div>
                                        <div className="bg-gray-800/80 p-3 rounded-lg border border-gray-700">
                                            <span className="text-gray-400 block">Edges Created</span>
                                            <span className="text-lg font-mono font-bold text-blue-400">
                                                {(runResult.edges_created ?? 0).toLocaleString()}
                                            </span>
                                        </div>
                                        <div className="bg-gray-800/80 p-3 rounded-lg border border-gray-700">
                                            <span className="text-gray-400 block">Clusters Impacted</span>
                                            <span className="text-lg font-mono font-bold text-emerald-400">
                                                {(runResult.clusters_impacted ?? 0).toLocaleString()}
                                            </span>
                                        </div>
                                    </div>
                                    <div className="flex flex-wrap items-center gap-3 pt-1">
                                        <button
                                            className={btnClass}
                                            onClick={() => navigate('/explorer')}
                                        >
                                            <Network className="w-4 h-4 mr-1.5" />
                                            Open Graph Explorer
                                            <ArrowRight className="w-4 h-4 ml-1.5" />
                                        </button>
                                        <button
                                            className={btnOutlineClass}
                                            onClick={() => navigate('/runs')}
                                        >
                                            <Activity className="w-4 h-4 mr-1.5" />
                                            View Run History
                                        </button>
                                        <button
                                            className={btnOutlineClass}
                                            onClick={() => { setPhase('idle'); setRunResult(null); }}
                                        >
                                            Run Pipeline Again
                                        </button>
                                        <button
                                            className={btnOutlineClass}
                                            onClick={onComplete}
                                        >
                                            Go to Dashboard
                                        </button>
                                    </div>
                                </div>
                            )}
                        </div>
                    )}
                </div>
            )}

            {/* Run Configuration Card — Above the Fold! */}
            <div className="bg-gray-900/70 border border-gray-700 rounded-xl p-5 space-y-4">
                <div className="flex items-center gap-2">
                    <Settings className="w-5 h-5 text-blue-400" />
                    <h3 className="text-base font-semibold text-white">Run Configuration</h3>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div className="space-y-1.5">
                        <label className="text-xs font-medium text-gray-300">Run Mode</label>
                        <select
                            value={runMode}
                            onChange={(e) => setRunMode(e.target.value as any)}
                            disabled={loading}
                            className="w-full bg-gray-900 border border-gray-700 rounded-md px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
                        >
                            <option value="INCREMENTAL">Incremental (Delta Watermark — Faster)</option>
                            <option value="FULL">Full Refresh (Recompute All)</option>
                        </select>
                    </div>

                    <div className="space-y-1.5">
                        <label className="text-xs font-medium text-gray-300">Max Graph Iterations</label>
                        <input
                            type="number"
                            min="1"
                            max="100"
                            value={maxIterations}
                            disabled={loading}
                            onChange={(e) => setMaxIterations(parseInt(e.target.value) || 1)}
                            className="w-full bg-gray-900 border border-gray-700 rounded-md px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
                        />
                    </div>

                    <div className="space-y-1.5">
                        <label className="text-xs font-medium text-gray-300 flex items-center justify-between">
                            <span>Max Group Size Guardrail</span>
                            <span className="text-[10px] text-amber-400 font-mono">Anti-Megacluster</span>
                        </label>
                        <input
                            type="number"
                            min="2"
                            value={maxGroupSize}
                            disabled={loading}
                            onChange={(e) => setMaxGroupSize(parseInt(e.target.value) || 10000)}
                            className="w-full bg-gray-900 border border-gray-700 rounded-md px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
                            placeholder="10000"
                        />
                    </div>

                    <div className="md:col-span-3 grid grid-cols-1 sm:grid-cols-2 gap-3 pt-1">
                        <label className="flex items-center gap-2.5 cursor-pointer bg-gray-800/80 p-3 rounded-lg border border-gray-700 hover:border-gray-600 transition-colors">
                            <input
                                type="checkbox"
                                checked={strictMode}
                                disabled={loading}
                                onChange={(e) => setStrictMode(e.target.checked)}
                                className="w-4 h-4 rounded border-gray-700 bg-gray-900 text-blue-600 focus:ring-0"
                            />
                            <div className="text-sm">
                                <span className="font-medium text-gray-200 block">Strict Deterministic Mode</span>
                                <span className="text-xs text-gray-400">Skip probabilistic fuzzy matching pass</span>
                            </div>
                        </label>

                        <label className="flex items-center gap-2.5 cursor-pointer bg-purple-900/20 p-3 rounded-lg border border-purple-900/50 hover:border-purple-500/50 transition-colors">
                            <input
                                type="checkbox"
                                checked={dryRun}
                                disabled={loading}
                                onChange={(e) => setDryRun(e.target.checked)}
                                className="w-4 h-4 rounded border-gray-700 bg-gray-900 text-purple-500 focus:ring-0"
                            />
                            <div className="text-sm">
                                <span className="font-medium text-purple-300 block">Dry Run Preview</span>
                                <span className="text-xs text-purple-200/60">Preview cluster impact without writing to production tables</span>
                            </div>
                        </label>
                    </div>
                </div>
            </div>

            {/* Compact Configuration Summary KPI Strip */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <button
                    type="button"
                    onClick={() => toggleSection('sources')}
                    className={`p-3.5 rounded-xl border text-left transition-all ${
                        openSection === 'sources'
                            ? 'bg-blue-950/30 border-blue-500/60'
                            : 'bg-gray-900/60 border-gray-700 hover:border-gray-600'
                    }`}
                >
                    <div className="flex items-center justify-between text-xs text-gray-400">
                        <span className="flex items-center gap-1.5 font-medium">
                            <Database className="w-3.5 h-3.5 text-blue-400" /> Source Tables
                        </span>
                        {openSection === 'sources' ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
                    </div>
                    <div className="text-xl font-bold text-white mt-1">{totalSources}</div>
                    <div className="text-[11px] text-gray-500 mt-0.5">Click to inspect mappings</div>
                </button>

                <button
                    type="button"
                    onClick={() => toggleSection('rules')}
                    className={`p-3.5 rounded-xl border text-left transition-all ${
                        openSection === 'rules'
                            ? 'bg-blue-950/30 border-blue-500/60'
                            : 'bg-gray-900/60 border-gray-700 hover:border-gray-600'
                    }`}
                >
                    <div className="flex items-center justify-between text-xs text-gray-400">
                        <span className="flex items-center gap-1.5 font-medium">
                            <Shield className="w-3.5 h-3.5 text-emerald-400" /> Rules & Guardrails
                        </span>
                        {openSection === 'rules' ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
                    </div>
                    <div className="text-xl font-bold text-white mt-1">{totalExactRules + totalFuzzyRules}</div>
                    <div className="text-[11px] text-gray-500 mt-0.5">
                        {totalExclusions} exclusions, {totalOverrides} overrides
                    </div>
                </button>

                <button
                    type="button"
                    onClick={() => toggleSection('survivorship')}
                    className={`p-3.5 rounded-xl border text-left transition-all ${
                        openSection === 'survivorship'
                            ? 'bg-blue-950/30 border-blue-500/60'
                            : 'bg-gray-900/60 border-gray-700 hover:border-gray-600'
                    }`}
                >
                    <div className="flex items-center justify-between text-xs text-gray-400">
                        <span className="flex items-center gap-1.5 font-medium">
                            <Layers className="w-3.5 h-3.5 text-purple-400" /> Survivorship
                        </span>
                        {openSection === 'survivorship' ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
                    </div>
                    <div className="text-xl font-bold text-white mt-1">{totalSurvivorship}</div>
                    <div className="text-[11px] text-gray-500 mt-0.5">Golden record attributes</div>
                </button>

                <button
                    type="button"
                    onClick={() => toggleSection('json')}
                    className={`p-3.5 rounded-xl border text-left transition-all ${
                        openSection === 'json'
                            ? 'bg-blue-950/30 border-blue-500/60'
                            : 'bg-gray-900/60 border-gray-700 hover:border-gray-600'
                    }`}
                >
                    <div className="flex items-center justify-between text-xs text-gray-400">
                        <span className="flex items-center gap-1.5 font-medium">
                            <Code className="w-3.5 h-3.5 text-amber-400" /> Raw JSON
                        </span>
                        {openSection === 'json' ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
                    </div>
                    <div className="text-sm font-semibold text-gray-200 mt-1.5">View Manifest</div>
                    <div className="text-[11px] text-gray-500 mt-0.5">Toggle full config JSON</div>
                </button>
            </div>

            {/* Collapsible Detail Drawer */}
            {openSection === 'sources' && (
                <div className="bg-gray-900/80 border border-gray-700 rounded-xl p-4 space-y-2 max-h-72 overflow-y-auto">
                    <h4 className="text-xs font-semibold uppercase tracking-wider text-gray-400 mb-2">
                        Configured Source Tables ({totalSources})
                    </h4>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                        {config.sources?.map((src, i) => (
                            <div key={i} className="p-3 border border-gray-700/80 rounded-lg bg-gray-800/70 text-xs space-y-1">
                                <div className="flex justify-between items-center gap-2">
                                    <span className="font-semibold text-gray-200 truncate">{src.table}</span>
                                    <span className="bg-gray-700 px-2 py-0.5 rounded text-gray-300 shrink-0">
                                        PK: {src.entity_key}
                                    </span>
                                </div>
                                <div className="text-gray-400 flex flex-wrap gap-x-3 gap-y-0.5">
                                    <span>Watermark: <strong className="text-gray-300">{src.watermark_column || 'None (Delta by PK)'}</strong></span>
                                    <span>IDs: <strong className="text-blue-300">{(src.identifiers || []).map(id => id.type).join(', ') || 'Inferred'}</strong></span>
                                </div>
                                <div className="text-gray-500 truncate">
                                    Attrs: {(src.attributes || []).map(a => a.name).join(', ') || 'None'}
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {openSection === 'rules' && (
                <div className="bg-gray-900/80 border border-gray-700 rounded-xl p-4 space-y-2 max-h-72 overflow-y-auto">
                    <h4 className="text-xs font-semibold uppercase tracking-wider text-gray-400 mb-2">
                        Matching Rules ({totalExactRules + totalFuzzyRules})
                    </h4>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                        {config.rules?.map((rule, i) => (
                            <div key={`exact-${i}`} className="p-2.5 border border-gray-700 rounded-lg bg-gray-800/70 text-xs flex justify-between items-center">
                                <div className="flex items-center flex-wrap gap-1">
                                    <span className="font-medium text-gray-200">
                                        Exact: {(rule.match_keys || []).join(', ')}
                                    </span>
                                    <span className="text-gray-400">({rule.canonicalize || 'LOWERCASE'})</span>
                                    <span className="text-[10px] text-amber-300 font-mono px-1.5 py-0.5 rounded bg-amber-950/40 border border-amber-800/40">max {rule.max_group_size || 10000}</span>
                                </div>
                                <span className="bg-gray-700 px-2 py-0.5 rounded text-gray-300">Priority {rule.priority}</span>
                            </div>
                        ))}
                        {config.fuzzy_rules?.map((rule, i) => (
                            <div key={`fuzzy-${i}`} className="p-2.5 border border-purple-900/50 rounded-lg bg-gray-800/70 text-xs flex justify-between items-center">
                                <div>
                                    <span className="font-medium text-purple-300">Fuzzy ({(rule.threshold || 0) * 100}%)</span>
                                    <span className="ml-2 text-gray-400">Block: {rule.blocking_key}</span>
                                </div>
                                <span className="bg-gray-700 px-2 py-0.5 rounded text-gray-300">Priority {rule.priority}</span>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {openSection === 'survivorship' && (
                <div className="bg-gray-900/80 border border-gray-700 rounded-xl p-4 space-y-2 max-h-72 overflow-y-auto">
                    <h4 className="text-xs font-semibold uppercase tracking-wider text-gray-400 mb-2">
                        Survivorship Rules ({totalSurvivorship})
                    </h4>
                    <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-2">
                        {(config.survivorship || []).map((rule, i) => (
                            <div key={i} className="p-2.5 border border-gray-700 rounded-lg bg-gray-800/70 text-xs flex justify-between items-center">
                                <span className="font-medium text-gray-200 truncate">{rule.attribute}</span>
                                <span className="bg-blue-900/40 text-blue-300 px-2 py-0.5 rounded border border-blue-800/60 shrink-0">
                                    {rule.strategy}
                                </span>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {openSection === 'json' && (
                <div className="bg-gray-900 p-4 rounded-xl border border-gray-700 space-y-3">
                    <div className="flex items-center justify-between">
                        <h4 className="text-xs font-semibold uppercase tracking-wider text-gray-400">
                            JSON Configuration Manifest
                        </h4>
                        <div className="flex items-center gap-2">
                            <button
                                onClick={() => {
                                    navigator.clipboard.writeText(JSON.stringify(config, null, 2));
                                    setJsonCopied(true);
                                    setTimeout(() => setJsonCopied(false), 2000);
                                }}
                                className="inline-flex items-center gap-1.5 px-2.5 py-1 text-xs bg-gray-800 hover:bg-gray-700 text-gray-300 rounded border border-gray-700 transition-colors"
                            >
                                {jsonCopied ? <Check className="w-3.5 h-3.5 text-green-400" /> : <Copy className="w-3.5 h-3.5 text-gray-400" />}
                                {jsonCopied ? 'Copied!' : 'Copy JSON'}
                            </button>
                            {!isEditingJson ? (
                                <button
                                    onClick={() => {
                                        setJsonInput(JSON.stringify(config, null, 2));
                                        setJsonError(null);
                                        setJsonSuccess(null);
                                        setIsEditingJson(true);
                                    }}
                                    className="inline-flex items-center gap-1.5 px-2.5 py-1 text-xs bg-blue-900/40 hover:bg-blue-800/60 text-blue-300 rounded border border-blue-700/60 transition-colors"
                                >
                                    <Edit3 className="w-3.5 h-3.5" />
                                    Edit / Paste JSON
                                </button>
                            ) : (
                                <button
                                    onClick={() => {
                                        setIsEditingJson(false);
                                        setJsonError(null);
                                    }}
                                    className="px-2.5 py-1 text-xs bg-gray-800 hover:bg-gray-700 text-gray-400 rounded border border-gray-700"
                                >
                                    Cancel
                                </button>
                            )}
                        </div>
                    </div>

                    {jsonError && (
                        <div className="p-3 bg-red-950/50 border border-red-800 text-red-300 rounded-lg text-xs flex items-center gap-2">
                            <AlertCircle className="w-4 h-4 shrink-0 text-red-400" />
                            <span>{jsonError}</span>
                        </div>
                    )}

                    {jsonSuccess && (
                        <div className="p-3 bg-green-950/50 border border-green-800 text-green-300 rounded-lg text-xs flex items-center gap-2">
                            <CheckCircle2 className="w-4 h-4 shrink-0 text-green-400" />
                            <span>{jsonSuccess}</span>
                        </div>
                    )}

                    {isEditingJson ? (
                        <div className="space-y-3">
                            <textarea
                                value={jsonInput}
                                onChange={(e) => setJsonInput(e.target.value)}
                                rows={14}
                                className="w-full bg-gray-950 border border-blue-700/80 rounded-lg p-3 font-mono text-xs text-blue-200 focus:outline-none focus:ring-1 focus:ring-blue-500"
                                placeholder="Paste your full IDR JSON configuration manifest here..."
                            />
                            <div className="flex justify-end gap-2">
                                <button
                                    onClick={() => {
                                        try {
                                            const parsed = JSON.parse(jsonInput);
                                            if (!parsed.sources || !Array.isArray(parsed.sources)) {
                                                setJsonError("Configuration must include a 'sources' array.");
                                                return;
                                            }
                                            setJsonError(null);
                                            onUpdateConfig?.(parsed);
                                            setIsEditingJson(false);
                                            setJsonSuccess(`Successfully applied config with ${parsed.sources.length} sources and ${(parsed.rules || []).length} rules!`);
                                        } catch (err: any) {
                                            setJsonError(`Invalid JSON syntax: ${err.message}`);
                                        }
                                    }}
                                    className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md text-xs font-semibold shadow-sm transition-colors"
                                >
                                    Apply JSON Manifest
                                </button>
                            </div>
                        </div>
                    ) : (
                        <pre className="text-xs font-mono text-gray-300 overflow-x-auto max-h-72 p-2 bg-gray-950 rounded border border-gray-800">
                            {JSON.stringify(config, null, 2)}
                        </pre>
                    )}
                </div>
            )}
        </div>
    );
}
