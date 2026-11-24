import ProfilePreviewPanel from '../../components/ProfilePreviewPanel';
import AIReviewPanel from '../../components/AIReviewPanel';
import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
    Loader2, CheckCircle2, Settings, Play, Save, ClipboardList,
    ChevronDown, ChevronRight, Database, Shield, Layers, Code,
    Activity, Clock, Cpu, FileSpreadsheet, GitMerge, Network,
    ArrowRight, Copy, Check, Edit3, AlertCircle, RotateCcw,  Sparkles
} from "lucide-react";
import { IDRConfig } from '../../types';
import { api } from '../../api/client';
import { useToast } from '../../components/Toast';
import { CustomSelect } from '../../components/CustomSelect';


export default function StepReview({
    config,
    onBack,
    onComplete,
    initialSaved = false,
    hasUnsavedChanges = false,
    readOnly = false,
    warningMessage = null,
    onUpdateConfig,
    onSaved,
    externalSaving,
    externalSaveProgress,
    onTriggerSave,
    onDiscardDraft,
}: {
    config: IDRConfig,
    onBack: () => void,
    onComplete: () => void,
    initialSaved?: boolean,
    hasUnsavedChanges?: boolean,
    readOnly?: boolean,
    warningMessage?: string | null,
    onUpdateConfig?: (config: IDRConfig) => void,
    onSaved?: () => void,
    externalSaving?: boolean,
    externalSaveProgress?: {
        status: string;
        step?: string;
        label?: string;
        percent: number;
        detail?: string;
        elapsed_seconds?: number;
    } | null,
    onTriggerSave?: () => void,
    onDiscardDraft?: () => void,
}) {
    const { toast } = useToast();
    const navigate = useNavigate();
    type ReviewPhase = 'idle' | 'saving' | 'running' | 'success';
    const [phase, setPhase] = useState<ReviewPhase>('idle');
    const [error, setError] = useState<string | null>(null);
    const [configSaved, setConfigSaved] = useState(initialSaved && !hasUnsavedChanges);

    useEffect(() => {
        setConfigSaved(initialSaved && !hasUnsavedChanges);
    }, [initialSaved, hasUnsavedChanges]);

    // JSON Edit State
    const [isEditingJson, setIsEditingJson] = useState(false);
    const [jsonInput, setJsonInput] = useState('');
    const [jsonError, setJsonError] = useState<string | null>(null);
    const [jsonCopied, setJsonCopied] = useState(false);
    const [jsonSuccess, setJsonSuccess] = useState<string | null>(null);

    // Run Configuration State
    const [runMode, setRunMode] = useState<'FULL' | 'INCREMENTAL'>('FULL');
    const [strictMode, setStrictMode] = useState(false);
    const [maxIterations, setMaxIterations] = useState(30);
    const [maxGroupSize, setMaxGroupSize] = useState(10000);
    const [maxMovedEntitiesPct, setMaxMovedEntitiesPct] = useState('');
    const [maxClusterDeltaPct, setMaxClusterDeltaPct] = useState('');
    const [selectedLens, setSelectedLens] = useState('');
    const [dryRun, setDryRun] = useState(true);
    const [emitActivationDelta, setEmitActivationDelta] = useState<boolean>(
        Boolean(config.activation_delta?.enabled)
    );
    const [runResult, setRunResult] = useState<any>(null);

    useEffect(() => {
        setEmitActivationDelta(Boolean(config.activation_delta?.enabled));
    }, [config.activation_delta?.enabled]);
    const [critiqueLoading, setCritiqueLoading] = useState(false);
    const [critiqueReport, setCritiqueReport] = useState<Awaited<ReturnType<typeof api.critiqueDryRunAi>> | null>(null);
    const [previewBase, setPreviewBase] = useState('');
    const [previewOptions, setPreviewOptions] = useState('');
    const currentPreviewOptions = JSON.stringify([runMode, strictMode, maxIterations, maxGroupSize, selectedLens && selectedLens !== '__none__' ? selectedLens : 'default']);

    const handleCritiqueDryRun = async () => {
        if (!runResult) return;
        setCritiqueLoading(true);
        try {
            const res = await api.critiqueDryRunAi(
                config as unknown as Record<string, unknown>,
                runResult.run_id,
                runResult.draft_hash,
                selectedLens && selectedLens !== '__none__' ? selectedLens : 'default'
            );
            setCritiqueReport(res);

        } catch (err: any) {
            toast('error', 'Dry Run Critique failed', String(err?.message || err));
        } finally {
            setCritiqueLoading(false);
        }
    };

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

    const [localSaveProgress, setLocalSaveProgress] = useState<{
        status: string;
        step?: string;
        label?: string;
        percent: number;
        detail?: string;
        elapsed_seconds?: number;
    } | null>(null);

    const saving = externalSaving ?? (phase === 'saving');
    const saveProgress = externalSaveProgress ?? localSaveProgress;
    const loading = phase === 'running';
    const success = phase === 'success';

    useEffect(() => {
        if (phase !== 'saving' || onTriggerSave) return;
        let cancelled = false;

        const poll = async () => {
            try {
                const prog = await api.getSaveConfigProgress();
                if (cancelled) return;
                setLocalSaveProgress(prog);
                if (prog.status === 'SUCCESS') {
                    setConfigSaved(true);
                    setPhase('idle');
                    onSaved?.();
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
    }, [phase, onTriggerSave, onSaved]);

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
        if (onTriggerSave) {
            onTriggerSave();
            return;
        }
        setPhase('saving');
        setError(null);
        setLocalSaveProgress({
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
                lenses: config.lenses || [],
            };
            await api.saveSetupConfigAsync(cleanConfig);
        } catch (err: any) {
            setError(err.message || String(err));
            setPhase('idle');
        }
    };

    const handleRun = async (overrideDryRun?: boolean) => {
        const effectiveDryRun = overrideDryRun ?? dryRun;
        const hasLenses = !effectiveDryRun && selectedLens !== '__none__' && ((config.lenses || []).length > 0 || Boolean(selectedLens));
        if (overrideDryRun !== undefined) {
            setDryRun(overrideDryRun);
        }
        setPhase('running');
        setError(null);
        setRunResult(null);
        setCritiqueReport(null);
        setLiveProgress({
            status: 'RUNNING',
            elapsed_seconds: 0,
            current_stage: 'preflight',
            current_detail: (!effectiveDryRun && (!configSaved || hasUnsavedChanges))
                ? 'Saving updated configuration to warehouse...'
                : effectiveDryRun
                ? 'Running ephemeral dry run (saved config untouched)...'
                : 'Starting pipeline...',
            stages: [

            { name: 'preflight', label: 'Preflight & Schema Check', order: 1, status: 'running' },
                { name: 'entity_extraction', label: 'Entity Extraction', order: 2, status: 'pending' },
                { name: 'identifier_extraction', label: 'Identifier Extraction', order: 3, status: 'pending' },
                { name: 'attribute_extraction', label: 'Attribute Extraction', order: 4, status: 'pending' },
                { name: 'edge_building', label: 'Edge Building', order: 5, status: 'pending' },
                { name: 'label_propagation', label: 'Label Propagation (Graph)', order: 6, status: 'pending' },
                ...(strictMode ? [] : [{ name: 'fuzzy_matching', label: 'Fuzzy Matching', order: 7, status: 'pending' as const }]),
                ...(hasLenses
                    ? [{name: 'lens_resolution', label: 'Validate Multi-Lens Views', order: strictMode ? 7 : 8, status: 'pending' as const}]
                    : []),
                {
                    name: effectiveDryRun ? 'dry_run_analysis' : 'output_generation',
                    label: effectiveDryRun ? 'Dry Run Analysis' : 'Output & Golden Profiles',
                    order: (strictMode ? 7 : 8) + (hasLenses ? 1 : 0),
                    status: 'pending'
                },
            ]
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
                lenses: config.lenses || [],
            };

            // Real commits persist config first; Dry Runs evaluate draft config ephemerally without saving.
            if (!effectiveDryRun && !readOnly && (!configSaved || hasUnsavedChanges)) {
                await api.saveSetupConfig(cleanConfig);
                setConfigSaved(true);
                onSaved?.();
            }

            const parsedMovedPct = maxMovedEntitiesPct.trim() ? parseFloat(maxMovedEntitiesPct) : undefined;
            const parsedClusterPct = maxClusterDeltaPct.trim() ? parseFloat(maxClusterDeltaPct) : undefined;
            const result = await api.runSetup({
                mode: runMode === 'INCREMENTAL' ? 'INCR' : 'FULL',
                strict: strictMode,
                max_iterations: maxIterations,
                max_group_size: maxGroupSize,
                ...(parsedMovedPct && parsedMovedPct > 0 ? { max_moved_entities_pct: parsedMovedPct } : {}),
                ...(parsedClusterPct && parsedClusterPct > 0 ? { max_cluster_delta_pct: parsedClusterPct } : {}),
                ...(selectedLens === '__none__'
                    ? { skip_lenses: true }
                    : selectedLens
                        ? { lens: selectedLens }
                        : {}),
                dry_run: effectiveDryRun,
                ...(!effectiveDryRun ? { emit_activation_delta: emitActivationDelta } : {}),
                ...(effectiveDryRun ? { config: cleanConfig, ...(config.evaluation ? {ground_truth: config.evaluation} : {}) } : {}),
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
            if (result.status === 'FAILED') {
                setLiveProgress(null);
                setError(String(result.error || 'Pipeline execution aborted or failed.'));
                setPhase('idle');
                return;
            }
            setRunResult(result);
            setPreviewBase(JSON.stringify(config));
            setPreviewOptions(currentPreviewOptions);
            // Review is a separate user action; preview evidence stays on the server.
            setCritiqueReport(null);
            setPhase('success');
        } catch (err: any) {
            setLiveProgress(null);
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
        if (s.includes('preflight')) return <Database className="w-4 h-4 text-ink-muted" />;
        if (s.includes('entity')) return <Layers className="w-4 h-4 text-accent" />;
        if (s.includes('identifier')) return <Cpu className="w-4 h-4 text-accent" />;
        if (s.includes('attribute')) return <FileSpreadsheet className="w-4 h-4 text-status-warning" />;
        if (s.includes('edge')) return <GitMerge className="w-4 h-4 text-accent" />;
        if (s.includes('label') || s.includes('propagation')) return <Network className="w-4 h-4 text-status-success" />;
        if (s.includes('fuzzy')) return <GitMerge className="w-4 h-4 text-status-warning" />;
        return <Activity className="w-4 h-4 text-accent" />;
    };

    const formatStageMetric = (stageName: string, rowsAffected: number | null | undefined): string => {
        const s = stageName.toLowerCase();
        const cnt = rowsAffected ?? 0;
        if (s.includes('preflight')) {
            return cnt > 0 ? `${cnt.toLocaleString()} sources verified` : 'Schema verified';
        }
        if (s.includes('entity')) return `${cnt.toLocaleString()} entities`;
        if (s.includes('identifier')) return `${cnt.toLocaleString()} identifiers`;
        if (s.includes('attribute')) {
            return cnt > 0 ? `${cnt.toLocaleString()} attributes` : 'Skipped (exact only)';
        }
        if (s.includes('edge')) return `${cnt.toLocaleString()} edges`;
        if (s.includes('label') || s.includes('propagation')) return `${cnt.toLocaleString()} clusters`;
        if (s.includes('fuzzy')) {
            return cnt > 0 ? `${cnt.toLocaleString()} super-clusters` : 'Skipped';
        }
        if (s.includes('dry_run')) {
            const total = cnt > 0 ? cnt : (runResult?.dry_run_summary?.total_entities ?? runResult?.entities_processed ?? 0);
            return total > 0 ? `${Number(total).toLocaleString()} entities analyzed` : 'Diff analyzed';
        }
        if (s.includes('lens')) return `${cnt.toLocaleString()} lenses`;
        if (s.includes('output')) return `${cnt.toLocaleString()} profiles`;
        return `${cnt.toLocaleString()} rows`;
    };

    const btnClass = "btn-primary text-xs";
    const btnOutlineClass = "btn-secondary text-xs";

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
        <div className="space-y-4 flex flex-col">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-line pb-3.5">
                <div>
                    <h2 className="text-[15px] font-semibold text-ink">Review &amp; Run Pipeline</h2>
                    <p className="text-xs text-ink-muted mt-0.5">Preview changes with a Dry Run or execute the live pipeline.</p>
                </div>

                {/* Top Action Bar */}
                <div className="flex items-center gap-2">
                    <button className={btnOutlineClass} onClick={onBack} disabled={loading || saving}>
                        ← Back
                    </button>
                    {!onTriggerSave && (
                        <button
                            className={`${btnOutlineClass} ${configSaved ? 'border-status-success-border text-status-success' : ''} ${readOnly ? 'opacity-50 cursor-not-allowed' : ''}`}
                            onClick={handleSave}
                            disabled={loading || saving || readOnly}
                            title={readOnly ? "Read-only mode" : "Save configuration"}
                        >
                            {saving ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Save className="w-3.5 h-3.5" />}
                            {saving ? `Saving (${saveProgress?.percent ?? 0}%)` : configSaved ? 'Config Saved' : 'Save Config'}
                        </button>
                    )}
                    <button
                        className={btnClass}
                        onClick={() => handleRun()}
                        disabled={loading || saving || readOnly || totalSources === 0}
                        title={hasUnsavedChanges ? "Saves pending edits and executes pipeline" : ""}
                    >
                        {loading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Play className="w-3.5 h-3.5" />}
                        {loading ? 'Running Pipeline...' : dryRun ? 'Start Dry Run' : 'Run Pipeline'}
                    </button>
                </div>
            </div>

            {/* Smooth Batched Save Progress Banner */}
            {saving && saveProgress && (
                <div className="surface-panel bg-accent-soft/40 border-accent-border p-3.5 space-y-2">
                    <div className="flex items-center justify-between text-xs">
                        <div className="flex items-center gap-2 text-accent font-medium">
                            <Loader2 className="w-3.5 h-3.5 animate-spin" />
                            <span>{saveProgress.label || 'Saving configuration...'}</span>
                        </div>
                        <div className="flex items-center gap-3 text-xs text-ink-secondary font-mono tabular-nums">
                            <span>{saveProgress.elapsed_seconds?.toFixed(1) ?? '0.0'}s</span>
                            <span>{saveProgress.percent}%</span>
                        </div>
                    </div>
                    <div className="w-full h-1.5 bg-subtle rounded-full overflow-hidden">
                        <div
                            className="h-full bg-accent transition-all duration-300 ease-out"
                            style={{ width: `${Math.min(100, Math.max(5, saveProgress.percent || 5))}%` }}
                        />
                    </div>
                    {saveProgress.detail && (
                        <p className="text-xs text-ink-muted">{saveProgress.detail}</p>
                    )}
                </div>
            )}

            {/* Read Only / Warning Alert */}
            {readOnly && (
                <div className="bg-status-warning-soft border border-status-warning-border rounded-md p-3.5 flex gap-3">
                    <div className="text-status-warning">
                        <Settings className="w-4 h-4 mt-0.5" />
                    </div>
                    <div className="space-y-0.5">
                        <h4 className="text-xs font-semibold text-status-warning">Limited Permissions Mode</h4>
                        <p className="text-xs text-ink-secondary">
                            {warningMessage || "Unable to save configuration due to missing write permissions. You can review the config but cannot save changes."}
                        </p>
                    </div>
                </div>
            )}

            {error && (
                <div className="bg-status-danger-soft border border-status-danger-border rounded-md p-3">
                    <h4 className="text-status-danger font-semibold text-xs">Error</h4>
                    <p className="text-ink-secondary text-xs mt-1">{error}</p>
                </div>
            )}

            {/* LIVE & COMPLETED PIPELINE EXECUTION PROGRESS PANEL */}
            {(loading || success) && liveProgress && (
                <div className={`surface-panel p-4 space-y-4 transition-colors ${
                    success
                        ? 'border-status-success-border'
                        : 'border-accent-border'
                }`}>
                    <div className="flex flex-wrap items-center justify-between gap-3">
                        <div className="flex items-center gap-2.5">
                            <div className={`p-1.5 rounded-md border ${
                                success
                                    ? 'bg-status-success-soft border-status-success-border'
                                    : 'bg-accent-soft border-accent-border'
                            }`}>
                                {loading ? (
                                    <Loader2 className="w-4 h-4 text-accent animate-spin" />
                                ) : (
                                    <CheckCircle2 className="w-4 h-4 text-status-success" />
                                )}
                            </div>
                            <div>
                                <div className="flex items-center gap-2">
                                    <h3 className="text-[13px] font-semibold text-ink">
                                        {success
                                            ? `Identity Resolution Complete (${runResult?.status || liveProgress.status})`
                                            : `Executing Pipeline (${runMode === 'INCREMENTAL' ? 'Incremental' : 'Full Refresh'})`}
                                    </h3>
                                    {liveProgress.run_id && (
                                        <span className="badge-neutral font-mono">
                                            {liveProgress.run_id}
                                        </span>
                                    )}
                                </div>
                                <p className={`text-xs mt-0.5 ${success ? 'text-status-success' : 'text-accent'}`}>
                                    {success
                                        ? `Finished in ${Number((runResult?.duration_seconds ?? liveProgress.elapsed_seconds ?? 0).toFixed(2))}s — All stages completed`
                                        : (liveProgress.current_detail || 'Processing stage...')}
                                </p>
                            </div>
                        </div>

                        <div className="flex items-center gap-3 text-xs font-mono tabular-nums">
                            <span className="badge-neutral py-1 px-2">
                                <Clock className={`w-3 h-3 ${success ? 'text-status-success' : 'text-accent'}`} />
                                {Number((liveProgress.elapsed_seconds || runResult?.duration_seconds || 0).toFixed(2))}s elapsed
                            </span>
                            <span className={`font-semibold ${success ? 'text-status-success' : 'text-accent'}`}>
                                Stage {Math.min(completedStages, totalStages)} of {totalStages} ({progressPercent}%)
                            </span>
                        </div>
                    </div>

                    {/* Overall Progress Bar */}
                    <div className="w-full bg-subtle h-1.5 rounded-full overflow-hidden">
                        <div
                            className={`h-full rounded-full transition-all duration-500 ${
                                success ? 'bg-status-success' : 'bg-accent'
                            }`}
                            style={{ width: `${progressPercent}%` }}
                        />
                    </div>

                    {/* 8-Stage Status Grid */}
                    <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-4 gap-2 pt-0.5">
                        {stagesList.map((st) => {
                            const isRunning = !success && st.status === 'running';
                            const isDone = success || st.status === 'completed';
                            return (
                                <div
                                    key={st.name}
                                    className={`p-2.5 rounded-md border text-xs transition-colors ${
                                        isRunning
                                            ? 'bg-selected border-accent-border'
                                            : isDone
                                            ? 'bg-status-success-soft/40 border-status-success-border'
                                            : 'bg-subtle border-line opacity-70'
                                    }`}
                                >
                                    <div className="flex items-center justify-between gap-1.5">
                                        <div className="flex items-center gap-1.5 truncate">
                                            {isDone ? (
                                                <CheckCircle2 className="w-3.5 h-3.5 text-status-success shrink-0" />
                                            ) : (
                                                getStageIcon(st.name, isRunning)
                                            )}
                                            <span className={`font-semibold truncate ${isRunning || isDone ? 'text-ink' : 'text-ink-muted'}`}>
                                                {st.label}
                                            </span>
                                        </div>
                                        {isDone && st.duration_seconds !== null && st.duration_seconds !== undefined && (
                                            <span className="font-mono tabular-nums text-[11px] text-status-success shrink-0">
                                                {Number(st.duration_seconds.toFixed(3))}s
                                            </span>
                                        )}
                                    </div>
                                    <div className="flex justify-between items-center mt-1.5 text-[11px] font-mono tabular-nums text-ink-muted">
                                        <span>#{st.order}</span>
                                        {isRunning ? (
                                            <span className="text-accent truncate max-w-[140px]">
                                                {st.detail || 'Running...'}
                                            </span>
                                        ) : isDone ? (
                                            <span className="text-ink-secondary">
                                                {formatStageMetric(st.name, st.rows_affected)}
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
                        <div className="pt-4 border-t border-line space-y-4">
                            {Boolean(runResult.dry_run_summary) ? (
                                <div className="space-y-3">
                                    <div className="flex flex-wrap items-center justify-between gap-2">
                                        <div className="flex items-center gap-2 text-ink font-semibold text-[13px]">
                                            <ClipboardList className="w-4 h-4 text-accent" />
                                            Dry Run Analysis — Proposed Graph Changes (No Writes Committed)
                                        </div>
                                        <span className="badge-accent">
                                            {hasUnsavedChanges
                                                ? 'Draft Previewed — Saved Config Untouched'
                                                : 'Saved Config Previewed — Output Tables Untouched'}
                                        </span>
                                    </div>
                                    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-2.5 text-xs tabular-nums">
                                        <div className="surface-subtle p-3">
                                            <span className="text-ink-muted block">New Entities</span>
                                            <span className="text-lg font-mono font-semibold text-status-success">
                                                {(runResult.dry_run_summary.new_entities ?? 0).toLocaleString()}
                                            </span>
                                        </div>
                                        <div className="surface-subtle p-3">
                                            <span className="text-ink-muted block">Moved Entities</span>
                                            <span className="text-lg font-mono font-semibold text-status-warning">
                                                {(runResult.dry_run_summary.moved_entities ?? 0).toLocaleString()}
                                            </span>
                                        </div>
                                        <div className="surface-subtle p-3">
                                            <span className="text-ink-muted block">Edges Delta</span>
                                            <span className={`text-lg font-mono font-semibold ${
                                                (runResult.dry_run_summary.edges_delta ?? 0) === 0
                                                    ? 'text-ink'
                                                    : (runResult.dry_run_summary.edges_delta ?? 0) < 0
                                                    ? 'text-status-danger'
                                                    : 'text-accent'
                                            }`}>
                                                {(runResult.dry_run_summary.edges_delta ?? 0) > 0 ? '+' : ''}
                                                {(runResult.dry_run_summary.edges_delta ?? 0).toLocaleString()}
                                            </span>
                                            {runResult.dry_run_summary.current_edges !== null && runResult.dry_run_summary.current_edges !== undefined && (
                                                <span className="text-[11px] text-ink-muted block font-mono">
                                                    {(runResult.dry_run_summary.proposed_edges ?? 0).toLocaleString()} vs {runResult.dry_run_summary.current_edges.toLocaleString()}
                                                </span>
                                            )}
                                        </div>
                                        <div className="surface-subtle p-3">
                                            <span className="text-ink-muted block">Clusters Delta</span>
                                            <span className={`text-lg font-mono font-semibold ${
                                                (runResult.dry_run_summary.clusters_delta ?? 0) === 0
                                                    ? 'text-ink'
                                                    : 'text-accent'
                                            }`}>
                                                {(runResult.dry_run_summary.clusters_delta ?? 0) > 0 ? '+' : ''}
                                                {(runResult.dry_run_summary.clusters_delta ?? 0).toLocaleString()}
                                            </span>
                                            {runResult.dry_run_summary.current_clusters !== null && runResult.dry_run_summary.current_clusters !== undefined && (
                                                <span className="text-[11px] text-ink-muted block font-mono">
                                                    {(runResult.dry_run_summary.proposed_clusters ?? 0).toLocaleString()} vs {runResult.dry_run_summary.current_clusters.toLocaleString()}
                                                </span>
                                            )}
                                        </div>
                                        <div className="surface-subtle p-3">
                                            <span className="text-ink-muted block">Unchanged Entities</span>
                                            <span className="text-lg font-mono font-semibold text-ink">
                                                {(runResult.dry_run_summary.unchanged_entities ?? 0).toLocaleString()}
                                            </span>
                                        </div>
                                    </div>
                                    {(((runResult.dry_run_summary.shadow_preserved_entities ?? 0) > 0) ||
                                      ((runResult.dry_run_summary.edges_delta ?? 0) !== 0 &&
                                       (runResult.dry_run_summary.moved_entities ?? 0) === 0 &&
                                       (runResult.dry_run_summary.new_entities ?? 0) === 0)) && (
                                        <div className="p-2.5 rounded-md bg-selected border border-accent-border text-xs text-ink-secondary space-y-1">
                                            <div>
                                                {(runResult.dry_run_summary.shadow_preserved_entities ?? 0) > 0 ? (
                                                    <>
                                                        <strong className="text-ink">{(runResult.dry_run_summary.shadow_preserved_entities ?? 0).toLocaleString()}</strong> unchanged{' '}
                                                        {(runResult.dry_run_summary.shadow_preserved_entities ?? 0) === 1 ? 'entity' : 'entities'} lost edges under this configuration but stayed in the same cluster via corroborating identifier(s):{' '}
                                                        <code className="font-mono text-ink">{(runResult.dry_run_summary.corroborating_identifier_types || []).join(', ') || 'SECONDARY_IDENTIFIER'}</code>.
                                                    </>
                                                ) : (
                                                    <>
                                                        Graph edges changed by <strong className="text-ink">{(runResult.dry_run_summary.edges_delta ?? 0).toLocaleString()}</strong>, while cluster memberships remained intact because those entities remain linked via corroborating identifiers.
                                                    </>
                                                )}
                                            </div>
                                            {runResult.dry_run_summary.edges_delta_by_type &&
                                             Object.keys(runResult.dry_run_summary.edges_delta_by_type).length > 0 && (
                                                <div className="text-[11px] font-mono text-accent">
                                                    Edge deltas by type:{' '}
                                                    {Object.entries(runResult.dry_run_summary.edges_delta_by_type as Record<string, number>)
                                                        .map(([k, v]) => `${k}: ${v >= 0 ? '+' : ''}${v.toLocaleString()}`)
                                                        .join(' | ')}
                                                </div>
                                            )}
                                        </div>
                                    )}
                                    <div className="flex flex-wrap gap-2 pt-1">
                                        <button
                                            type="button"
                                            disabled={critiqueLoading || previewBase !== JSON.stringify(config) || previewOptions !== currentPreviewOptions}
                                            onClick={handleCritiqueDryRun}
                                            title="Inspects zero-PII Dry Run graph metrics (cluster sizes, singletons, zero-edge rules, supernodes) and suggests one-click draft fixes"
                                            className="btn-secondary text-xs border-accent-border text-accent"
                                        >
                                            {critiqueLoading ? (
                                                <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                            ) : (
                                                <Sparkles className="w-3.5 h-3.5" />
                                            )}
                                            {critiqueLoading ? 'Analyzing Graph…' : 'Analyze & Refine with AI'}
                                            {!critiqueLoading && (
                                                <span className="px-1.5 py-0.2 rounded bg-accent/15 border border-accent-border text-[10px] font-semibold uppercase tracking-wide">
                                                    Beta
                                                </span>
                                            )}
                                        </button>
                                        <button
                                            className={btnClass}
                                            onClick={() => handleRun(false)}
                                        >
                                            <Play className="w-3.5 h-3.5" />
                                            {hasUnsavedChanges ? 'Save & Execute Real Pipeline' : 'Execute Real Pipeline (Commit Changes)'}
                                        </button>
                                        {hasUnsavedChanges && (
                                            <button
                                                type="button"
                                                className={btnOutlineClass}
                                                onClick={handleSave}
                                                disabled={saving || readOnly}
                                            >
                                                <Save className="w-3.5 h-3.5" />
                                                Save Config Only
                                            </button>
                                        )}
                                        {hasUnsavedChanges && onDiscardDraft && (
                                            <button
                                                type="button"
                                                className="btn-ghost text-xs text-status-warning"
                                                onClick={() => {
                                                    onDiscardDraft();
                                                    setPhase('idle');
                                                    setRunResult(null);
                                                    setCritiqueReport(null);
                                                }}
                                            >
                                                <RotateCcw className="w-3.5 h-3.5" />
                                                Discard Draft Changes
                                            </button>
                                        )}
                                        <button
                                            className={btnOutlineClass}
                                            onClick={() => { setPhase('idle'); setRunResult(null); }}
                                        >
                                            Edit Configuration
                                        </button>
                                    </div>

                                    {/* AI Dry-Run Graph Critique Scorecard & One-Click Patches */}
                                    {runResult.review?.evaluation_acceptance && <div className="surface-subtle p-3 text-xs space-y-2 text-ink-secondary">
                                        <p className="font-semibold text-ink">Labeled evaluation: {runResult.review.evaluation_acceptance.status.replace(/_/g,' ')}</p>
                                        <p className="tabular-nums">{runResult.review.evaluation?.evaluated_entities ?? 0} labeled records evaluated · precision {runResult.review.evaluation?.precision ?? 'N/A'} · recall {runResult.review.evaluation?.recall ?? 'N/A'}</p>
                                        {runResult.review.evaluation_acceptance.reasons.map((reason: string) => <p key={reason}>{reason}</p>)}
                                        <p className="text-ink-muted">Accuracy measurements cover the labeled population only. Graph completion does not imply labeled acceptance passed.</p>
                                    </div>}
                                    <ProfilePreviewPanel key={`profiles-${runResult.run_id}`} config={config} runId={runResult.run_id} fingerprint={runResult.draft_hash}
                                        lens={selectedLens && selectedLens !== '__none__' ? selectedLens : 'default'}
                                        initialValidation={runResult.profile_validation || runResult.review?.profile_validation}
                                        stale={previewBase !== JSON.stringify(config) || previewOptions !== currentPreviewOptions || (runResult.review?.lens || runResult.lens || 'default') !== (selectedLens && selectedLens !== '__none__' ? selectedLens : 'default')} />
                                    {!critiqueReport && !critiqueLoading && (
                                        <p className="text-[11px] text-ink-muted">
                                            <strong className="text-ink">Analyze &amp; Refine with AI (Beta)</strong> runs only when you click it. It inspects aggregate Dry Run graph metrics (cluster sizes, singletons, zero-edge rules &amp; supernodes — zero raw PII) and recommends one-click draft fixes before you commit.
                                        </p>
                                    )}
                                    {critiqueReport && <AIReviewPanel key={critiqueReport.run_id}
                                        report={critiqueReport} config={config}
                                        stale={previewBase !== JSON.stringify(config) || previewOptions !== currentPreviewOptions || critiqueReport.lens !== (selectedLens && selectedLens !== '__none__' ? selectedLens : 'default')}
                                        onUpdate={onUpdateConfig} onPreview={() => handleRun(true)} />}

                                </div>
                            ) : (
                                <div className="space-y-3">
                                    <div className="grid grid-cols-1 sm:grid-cols-3 gap-2.5 text-xs tabular-nums">
                                        <div className="surface-subtle p-3">
                                            <span className="text-ink-muted block">Entities Processed</span>
                                            <span className="text-lg font-mono font-semibold text-ink">
                                                {(runResult.entities_processed ?? 0).toLocaleString()}
                                            </span>
                                        </div>
                                        <div className="surface-subtle p-3">
                                            <span className="text-ink-muted block">Edges Created</span>
                                            <span className="text-lg font-mono font-semibold text-accent">
                                                {(runResult.edges_created ?? 0).toLocaleString()}
                                            </span>
                                        </div>
                                        <div className="surface-subtle p-3">
                                            <span className="text-ink-muted block">Clusters Resolved</span>
                                            <span className="text-lg font-mono font-semibold text-status-success">
                                                {(runResult.clusters_resolved ?? runResult.clusters_impacted ?? 0).toLocaleString()}
                                            </span>
                                            {runResult.clusters_changed !== undefined && <p className="text-xs text-ink-muted mt-1" title="Distinct primary cluster IDs affected by record additions, removals or changed memberships; includes retired IDs.">{runResult.clusters_changed.toLocaleString()} cluster IDs changed</p>}
                                        </div>
                                    </div>
                                    {runResult.grouped_output_counts && Object.keys(runResult.grouped_output_counts).length > 0 && <p className="text-xs text-ink-secondary font-mono tabular-nums">Grouped state outputs: {Object.entries(runResult.grouped_output_counts).map(([id, count]) => `${id}: ${count} rows`).join(' · ')}</p>}
                                    {runResult.activation_delta_summary && Object.keys(runResult.activation_delta_summary).length > 0 && (
                                        <div className="flex flex-wrap items-center gap-2 p-2.5 rounded-md bg-subtle border border-line text-xs tabular-nums">
                                            <span className="font-semibold text-ink">Activation Delta:</span>
                                            {Object.entries(runResult.activation_delta_summary as Record<string, number>).map(([k, v]) => (
                                                <span
                                                    key={k}
                                                    className={`font-mono ${k.startsWith('RETIRED') ? 'badge-danger' : 'badge-neutral'}`}
                                                >
                                                    {k}: {Number(v).toLocaleString()}
                                                </span>
                                            ))}
                                        </div>
                                    )}
                                    <div className="flex flex-wrap items-center gap-2 pt-1">
                                        <button
                                            className={btnClass}
                                            onClick={() => navigate('/explorer')}
                                        >
                                            <Network className="w-3.5 h-3.5" />
                                            Open Graph Explorer
                                            <ArrowRight className="w-3.5 h-3.5" />
                                        </button>
                                        <button
                                            className={btnOutlineClass}
                                            onClick={() => navigate('/runs')}
                                        >
                                            <Activity className="w-3.5 h-3.5" />
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

            {/* Run Configuration Card */}
            <div className="surface-subtle p-4 space-y-4">
                <div className="flex items-center gap-2">
                    <Settings className="w-4 h-4 text-accent" />
                    <h3 className="text-[13px] font-semibold text-ink">Run Configuration</h3>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-3.5">
                    <div className="space-y-1.5">
                        <label className="text-xs font-medium text-ink-secondary block">Run Mode</label>
                        <CustomSelect
                            value={runMode}
                            onChange={(val) => {
                                if (!loading) setRunMode(val as 'FULL' | 'INCREMENTAL');
                            }}
                            options={[
                                { value: 'INCREMENTAL', label: 'Incremental (Delta Watermark — Faster)' },
                                { value: 'FULL', label: 'Full Refresh (Recompute All)' },
                            ]}
                            className="w-full"
                            buttonClassName="text-xs py-1.5"
                            menuClassName="left-0 right-0"
                        />
                    </div>

                    {(config.lenses || []).length > 0 && (
                        <div className="space-y-1.5">
                            <label className="text-xs font-medium text-ink-secondary block">Target Lens (Optional)</label>
                            <CustomSelect
                                value={selectedLens}
                                onChange={(val) => {
                                    if (!loading) setSelectedLens(val);
                                }}
                                options={[
                                    { value: '', label: `All Lenses (${(config.lenses || []).length}) + Primary` },
                                    { value: '__none__', label: 'Primary Only (Skip Lenses)' },
                                    ...(config.lenses || []).map((l) => ({ value: l.id, label: l.id })),
                                ]}
                                ariaLabel="Target Lens"
                                className="w-full"
                                buttonClassName="text-xs py-1.5"
                                menuClassName="left-0 right-0"
                            />
                        </div>
                    )}
                </div>

                <label className="flex items-center gap-2.5 cursor-pointer bg-surface p-3 rounded-md border border-line hover:border-line-strong transition-colors">
                    <input
                        type="checkbox"
                        checked={dryRun}
                        disabled={loading}
                        onChange={(e) => setDryRun(e.target.checked)}
                        className="w-4 h-4 rounded border-line-strong text-accent"
                    />
                    <div className="text-xs">
                        <span className="font-semibold text-ink block">Dry Run Mode (Preview Only)</span>
                        <span className="text-ink-muted">Run in isolated scratch schemas without modifying production tables</span>
                    </div>
                </label>

                <details className="rounded-md border border-line bg-surface p-3 text-xs">
                    <summary className="cursor-pointer font-medium text-ink-secondary hover:text-ink select-none">
                        Advanced runtime guardrails &amp; circuit breakers
                    </summary>
                    <div className="mt-3 space-y-3.5">
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">
                            <div className="space-y-1">
                                <label className="text-xs font-medium text-ink-secondary block">Max Graph Iterations</label>
                                <input
                                    type="number"
                                    min="1"
                                    max="100"
                                    value={maxIterations}
                                    disabled={loading}
                                    onChange={(e) => setMaxIterations(parseInt(e.target.value) || 1)}
                                    className="input-field font-mono tabular-nums text-xs py-1.5"
                                />
                            </div>
                            <div className="space-y-1">
                                <label className="text-xs font-medium text-ink-secondary flex items-center justify-between">
                                    <span>Max Group Size</span>
                                    <span className="badge-warning font-mono">Guardrail</span>
                                </label>
                                <input
                                    type="number"
                                    min="2"
                                    value={maxGroupSize}
                                    disabled={loading}
                                    onChange={(e) => setMaxGroupSize(parseInt(e.target.value) || 10000)}
                                    className="input-field font-mono tabular-nums text-xs py-1.5"
                                    placeholder="10000"
                                />
                            </div>
                            <div className="space-y-1">
                                <label className="text-xs font-medium text-ink-secondary flex items-center justify-between">
                                    <span>Max Moved Entities %</span>
                                    <span className="badge-danger font-mono">Breaker</span>
                                </label>
                                <input
                                    type="number"
                                    min="0.1"
                                    max="100"
                                    step="0.5"
                                    value={maxMovedEntitiesPct}
                                    disabled={loading}
                                    onChange={(e) => setMaxMovedEntitiesPct(e.target.value)}
                                    className="input-field font-mono tabular-nums text-xs py-1.5"
                                    placeholder="Unbounded"
                                />
                            </div>
                            <div className="space-y-1">
                                <label className="text-xs font-medium text-ink-secondary flex items-center justify-between">
                                    <span>Max Cluster Delta %</span>
                                    <span className="badge-danger font-mono">Breaker</span>
                                </label>
                                <input
                                    type="number"
                                    min="0.1"
                                    max="100"
                                    step="0.5"
                                    value={maxClusterDeltaPct}
                                    disabled={loading}
                                    onChange={(e) => setMaxClusterDeltaPct(e.target.value)}
                                    className="input-field font-mono tabular-nums text-xs py-1.5"
                                    placeholder="Unbounded"
                                />
                            </div>
                        </div>

                        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                            <label className="flex items-center gap-2.5 cursor-pointer bg-subtle p-2.5 rounded-md border border-line hover:border-line-strong transition-colors">
                                <input
                                    type="checkbox"
                                    checked={strictMode}
                                    disabled={loading}
                                    onChange={(e) => setStrictMode(e.target.checked)}
                                    className="w-4 h-4 rounded border-line-strong text-accent"
                                />
                                <div className="text-xs">
                                    <span className="font-medium text-ink block">Strict Deterministic Mode</span>
                                    <span className="text-ink-muted">Skip probabilistic fuzzy matching pass</span>
                                </div>
                            </label>

                            <label
                                title={dryRun ? 'Activation delta is only emitted during real pipeline commits' : undefined}
                                className={`flex items-center gap-2.5 bg-subtle p-2.5 rounded-md border border-line transition-colors ${
                                    dryRun ? 'opacity-60 cursor-not-allowed' : 'cursor-pointer hover:border-line-strong'
                                }`}
                            >
                                <input
                                    type="checkbox"
                                    checked={emitActivationDelta}
                                    disabled={loading || dryRun}
                                    onChange={(e) => setEmitActivationDelta(e.target.checked)}
                                    className="w-4 h-4 rounded border-line-strong text-accent"
                                />
                                <div className="text-xs">
                                    <span className="font-medium text-ink block">
                                        Emit Activation Delta (idr_out.identity_activation_delta)
                                    </span>
                                    <span className="text-ink-muted">
                                        {dryRun
                                            ? 'Disabled during Dry Run (no output tables written)'
                                            : 'Write NEW, MERGED, SPLIT, PROFILE_CHANGED & RETIRED changelog rows'}
                                    </span>
                                </div>
                            </label>
                        </div>
                    </div>
                </details>
            </div>

            {/* Compact Configuration Summary KPI Strip */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 tabular-nums">
                <button
                    type="button"
                    onClick={() => toggleSection('sources')}
                    className={`p-3.5 rounded-md border text-left transition-colors ${
                        openSection === 'sources'
                            ? 'bg-selected border-accent-border'
                            : 'bg-surface border-line hover:border-line-strong'
                    }`}
                >
                    <div className="flex items-center justify-between text-xs text-ink-muted">
                        <span className="flex items-center gap-1.5 font-medium">
                            <Database className="w-3.5 h-3.5 text-accent" /> Source Tables
                        </span>
                        {openSection === 'sources' ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
                    </div>
                    <div className="text-xl font-semibold font-mono text-ink mt-1">{totalSources}</div>
                    <div className="text-[11px] text-ink-muted mt-0.5">Click to inspect mappings</div>
                </button>

                <button
                    type="button"
                    onClick={() => toggleSection('rules')}
                    className={`p-3.5 rounded-md border text-left transition-colors ${
                        openSection === 'rules'
                            ? 'bg-selected border-accent-border'
                            : 'bg-surface border-line hover:border-line-strong'
                    }`}
                >
                    <div className="flex items-center justify-between text-xs text-ink-muted">
                        <span className="flex items-center gap-1.5 font-medium">
                            <Shield className="w-3.5 h-3.5 text-status-success" /> Rules &amp; Guardrails
                        </span>
                        {openSection === 'rules' ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
                    </div>
                    <div className="text-xl font-semibold font-mono text-ink mt-1">{totalExactRules + totalFuzzyRules}</div>
                    <div className="text-[11px] text-ink-muted mt-0.5">
                        {totalExclusions} exclusions, {totalOverrides} overrides
                    </div>
                </button>

                <button
                    type="button"
                    onClick={() => toggleSection('survivorship')}
                    className={`p-3.5 rounded-md border text-left transition-colors ${
                        openSection === 'survivorship'
                            ? 'bg-selected border-accent-border'
                            : 'bg-surface border-line hover:border-line-strong'
                    }`}
                >
                    <div className="flex items-center justify-between text-xs text-ink-muted">
                        <span className="flex items-center gap-1.5 font-medium">
                            <Layers className="w-3.5 h-3.5 text-accent" /> Survivorship
                        </span>
                        {openSection === 'survivorship' ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
                    </div>
                    <div className="text-xl font-semibold font-mono text-ink mt-1">{totalSurvivorship}</div>
                    <div className="text-[11px] text-ink-muted mt-0.5">Golden record attributes</div>
                </button>

                <button
                    type="button"
                    onClick={() => toggleSection('json')}
                    className={`p-3.5 rounded-md border text-left transition-colors ${
                        openSection === 'json'
                            ? 'bg-selected border-accent-border'
                            : 'bg-surface border-line hover:border-line-strong'
                    }`}
                >
                    <div className="flex items-center justify-between text-xs text-ink-muted">
                        <span className="flex items-center gap-1.5 font-medium">
                            <Code className="w-3.5 h-3.5 text-status-warning" /> Raw JSON
                        </span>
                        {openSection === 'json' ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
                    </div>
                    <div className="text-sm font-semibold text-ink mt-1.5">View Manifest</div>
                    <div className="text-[11px] text-ink-muted mt-0.5">Toggle full config JSON</div>
                </button>
            </div>

            {/* Collapsible Detail Drawer */}
            {openSection === 'sources' && (
                <div className="surface-subtle p-4 space-y-2 max-h-72 overflow-y-auto">
                    <h4 className="text-[11px] font-semibold uppercase tracking-[0.04em] text-ink-muted mb-2 tabular-nums">
                        Configured Source Tables ({totalSources})
                    </h4>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                        {config.sources?.map((src, i) => (
                            <div key={i} className="p-3 surface-panel text-xs space-y-1">
                                <div className="flex justify-between items-center gap-2">
                                    <span className="font-mono font-semibold text-ink truncate">{src.table}</span>
                                    <span className="badge-neutral font-mono shrink-0">
                                        PK: {src.entity_key}
                                    </span>
                                </div>
                                <div className="text-ink-muted flex flex-wrap gap-x-3 gap-y-0.5">
                                    <span>Watermark: <strong className="text-ink-secondary">{src.watermark_column || 'None (Delta by PK)'}</strong></span>
                                    <span>IDs: <strong className="text-accent">{(src.identifiers || []).map(id => id.type).join(', ') || 'Inferred'}</strong></span>
                                </div>
                                <div className="text-ink-muted truncate">
                                    Attrs: {(src.attributes || []).map(a => a.name).join(', ') || 'None'}
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {openSection === 'rules' && (
                <div className="surface-subtle p-4 space-y-2 max-h-72 overflow-y-auto">
                    <h4 className="text-[11px] font-semibold uppercase tracking-[0.04em] text-ink-muted mb-2 tabular-nums">
                        Matching Rules ({totalExactRules + totalFuzzyRules})
                    </h4>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                        {config.rules?.map((rule, i) => (
                            <div key={`exact-${i}`} className="p-2.5 surface-panel text-xs flex justify-between items-center">
                                <div className="flex items-center flex-wrap gap-1.5">
                                    <span className="font-medium text-ink">
                                        Exact: {(rule.match_keys || []).join(', ')}
                                    </span>
                                    <span className="text-ink-muted font-mono">({rule.canonicalize || 'LOWERCASE'})</span>
                                    <span className="badge-warning font-mono tabular-nums">max {rule.max_group_size || 10000}</span>
                                </div>
                                <span className="badge-neutral tabular-nums">Priority {rule.priority}</span>
                            </div>
                        ))}
                        {config.fuzzy_rules?.map((rule, i) => (
                            <div key={`fuzzy-${i}`} className="p-2.5 surface-panel text-xs flex justify-between items-center">
                                <div>
                                    <span className="font-medium text-status-warning">Fuzzy ({(rule.threshold || 0) * 100}%)</span>
                                    <span className="ml-2 text-ink-muted font-mono">Block: {rule.blocking_key}</span>
                                </div>
                                <span className="badge-neutral tabular-nums">Priority {rule.priority}</span>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {openSection === 'survivorship' && (
                <div className="surface-subtle p-4 space-y-2 max-h-72 overflow-y-auto">
                    <h4 className="text-[11px] font-semibold uppercase tracking-[0.04em] text-ink-muted mb-2 tabular-nums">
                        Survivorship Rules ({totalSurvivorship})
                    </h4>
                    <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-2">
                        {(config.survivorship || []).map((rule, i) => (
                            <div key={i} className="p-2.5 surface-panel text-xs flex justify-between items-center gap-2">
                                <span className="font-mono font-medium text-ink truncate">{rule.attribute}</span>
                                <span className="badge-accent font-mono shrink-0">
                                    {rule.strategy}
                                </span>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {openSection === 'json' && (
                <div className="surface-subtle p-4 space-y-3">
                    <div className="flex items-center justify-between">
                        <h4 className="text-[11px] font-semibold uppercase tracking-[0.04em] text-ink-muted">
                            JSON Configuration Manifest
                        </h4>
                        <div className="flex items-center gap-2">
                            <button
                                onClick={() => {
                                    navigator.clipboard.writeText(JSON.stringify(config, null, 2));
                                    setJsonCopied(true);
                                    setTimeout(() => setJsonCopied(false), 2000);
                                }}
                                className="btn-secondary text-xs py-1 px-2.5 min-h-[28px]"
                            >
                                {jsonCopied ? <Check className="w-3.5 h-3.5 text-status-success" /> : <Copy className="w-3.5 h-3.5 text-ink-muted" />}
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
                                    className="btn-secondary text-xs py-1 px-2.5 min-h-[28px]"
                                >
                                    <Edit3 className="w-3.5 h-3.5 text-accent" />
                                    Edit / Paste JSON
                                </button>
                            ) : (
                                <button
                                    onClick={() => {
                                        setIsEditingJson(false);
                                        setJsonError(null);
                                    }}
                                    className="btn-ghost text-xs py-1 px-2.5 min-h-[28px]"
                                >
                                    Cancel
                                </button>
                            )}
                        </div>
                    </div>

                    {jsonError && (
                        <div className="p-3 bg-status-danger-soft border border-status-danger-border text-status-danger rounded-md text-xs flex items-center gap-2">
                            <AlertCircle className="w-4 h-4 shrink-0" />
                            <span>{jsonError}</span>
                        </div>
                    )}

                    {jsonSuccess && (
                        <div className="p-3 bg-status-success-soft border border-status-success-border text-status-success rounded-md text-xs flex items-center gap-2">
                            <CheckCircle2 className="w-4 h-4 shrink-0" />
                            <span>{jsonSuccess}</span>
                        </div>
                    )}

                    {isEditingJson ? (
                        <div className="space-y-3">
                            <textarea
                                value={jsonInput}
                                onChange={(e) => setJsonInput(e.target.value)}
                                rows={14}
                                className="input-field font-mono text-xs p-3"
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
                                    className="btn-primary text-xs"
                                >
                                    Apply JSON Manifest
                                </button>
                            </div>
                        </div>
                    ) : (
                        <pre className="text-xs font-mono text-ink-secondary overflow-x-auto max-h-72 p-3 bg-surface rounded-md border border-line">
                            {JSON.stringify(config, null, 2)}
                        </pre>
                    )}
                </div>
            )}
        </div>
    );
}
