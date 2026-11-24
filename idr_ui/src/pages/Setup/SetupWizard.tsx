import { useEffect, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import {
    Database, GitGraph, Play, Layers, Shield, FileCode2, X,
    AlertCircle, Save, Loader2, CheckCircle2, ArrowLeftRight,
    History, RotateCcw
} from "lucide-react";
import { IDRConfig } from '../../types';
import { api } from '../../api/client';
import { useToast } from '../../components/Toast';
import { useWizardMachine, WizardStage, STUDIO_TABS } from './useWizardMachine';

import StepConnect from './StepConnect';
import StepMap from './StepMap';
import StepRules from './StepRules';
import StepSurvivorship from './StepSurvivorship';
import StepReview from './StepReview';

export default function SetupWizard() {
    const machine = useWizardMachine();
    const { toast } = useToast();
    const [searchParams] = useSearchParams();
    const queryClient = useQueryClient();

    const [isImportModalOpen, setIsImportModalOpen] = useState(false);
    const [importText, setImportText] = useState('');
    const [importError, setImportError] = useState<string | null>(null);

    // Config Version History & Rollback Modal State
    const [isHistoryModalOpen, setIsHistoryModalOpen] = useState(false);
    const [historyLoading, setHistoryLoading] = useState(false);
    const [historySnapshots, setHistorySnapshots] = useState<Array<{
        config_hash: string;
        created_at: string;
        sources_count: number;
        rules_count: number;
        exclusions_count: number;
        config: Record<string, unknown>;
    }>>([]);
    const [restoringHash, setRestoringHash] = useState<string | null>(null);

    // Unified Studio Header Save State
    const [loadingConfig, setLoadingConfig] = useState(true);
    const [saving, setSaving] = useState(false);
    const [saveError, setSaveError] = useState<string | null>(null);
    const [saveProgress, setSaveProgress] = useState<{
        status: string;
        step?: string;
        label?: string;
        percent: number;
        detail?: string;
        elapsed_seconds?: number;
    } | null>(null);

    const checkConfigAndNavigate = async (preferredStage: WizardStage = 'map', connOverride?: any) => {
        setLoadingConfig(true);
        try {
            const status = await api.getSetupStatus();
            queryClient.setQueryData(['setupStatus'], status);
            if (!status.connected) {
                machine.setIsConnected(false);
                machine.goToStage('connect');
                return false;
            }
            machine.setIsConnected(true);
            if (!connOverride && !machine.connectionData && status.platform) {
                machine.setConnectionData({ platform: status.platform });
            }

            if (status.configured) {
                const config = await api.getSetupConfig();
                if (config && config.sources && config.sources.length > 0) {
                    machine.hydrateFromSavedConfig(config as unknown as IDRConfig, preferredStage);
                    return true;
                }
            }

            machine.resetDraftForUnconfiguredWarehouse(preferredStage === 'connect' ? 'connect' : 'map');
        } catch (error) {
            console.error("Failed to load setup config:", error);
        } finally {
            setLoadingConfig(false);
        }
        return false;
    };

    useEffect(() => {
        const requestedStep = searchParams.get('step') as WizardStage | null;
        checkConfigAndNavigate(requestedStep === 'connect' ? 'connect' : 'map');
    }, []);

    // Poll save progress whenever Studio header save is active
    useEffect(() => {
        if (!saving) return;
        let cancelled = false;

        const poll = async () => {
            try {
                const prog = await api.getSaveConfigProgress();
                if (cancelled) return;
                setSaveProgress(prog);
                if (prog.status === 'SUCCESS') {
                    setSaving(false);
                    machine.markConfigSaved();
                    queryClient.invalidateQueries({ queryKey: ['setupStatus'] });
                    queryClient.invalidateQueries({ queryKey: ['setupConfig'] });
                    toast('success', 'Configuration saved', prog.message || 'Metadata persisted to warehouse.');
                } else if (prog.status === 'ERROR') {
                    const errMsg = prog.error || prog.detail || 'Failed to save configuration';
                    setSaveError(errMsg);
                    setSaving(false);
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
    }, [saving]);

    const handleSaveFromHeader = async () => {
        if (machine.initWarning || saving) return;
        const sources = machine.mappingConfig.sources || [];
        if (sources.length === 0) {
            toast('error', 'Cannot save yet', 'Select and map at least one source table first.');
            return;
        }
        setSaving(true);
        setSaveError(null);
        setSaveProgress({
            status: 'SAVING',
            step: 'validating',
            label: 'Validating configuration',
            percent: 5,
            detail: 'Preparing metadata manifest...',
            elapsed_seconds: 0,
        });
        try {
            const cleanConfig: IDRConfig = {
                ...machine.mappingConfig,
                sources,
                rules: machine.mappingConfig.rules || [],
                fuzzy_rules: machine.mappingConfig.fuzzy_rules || [],
                survivorship: machine.mappingConfig.survivorship || [],
                exclusions: machine.mappingConfig.exclusions || [],
                overrides: machine.mappingConfig.overrides || [],
                lenses: machine.mappingConfig.lenses || [],
            };
            await api.saveSetupConfigAsync(cleanConfig);
        } catch (err: any) {
            const msg = err.message || String(err);
            setSaveError(msg);
            setSaving(false);
            toast('error', 'Save failed', msg);
        }
    };

    const handleDiscardDraft = async () => {
        try {
            const saved = await api.getSetupConfig();
            if (saved && saved.sources && saved.sources.length > 0) {
                const targetTab = machine.stage === 'review' ? 'review' : machine.stage;
                machine.hydrateFromSavedConfig(saved as unknown as IDRConfig, targetTab);
                toast('success', 'Draft discarded', 'Reverted to currently saved warehouse configuration.');
            } else {
                machine.setHasUnsavedChanges(false);
            }
        } catch (err: any) {
            toast('error', 'Failed to reload saved config', err.message || String(err));
        }
    };

    const openHistoryModal = async () => {
        setIsHistoryModalOpen(true);
        setHistoryLoading(true);
        try {
            const res = await api.getConfigHistory(15);
            setHistorySnapshots(res.snapshots || []);
        } catch (err: any) {
            toast('error', 'Failed to load version history', err.message || String(err));
            setHistorySnapshots([]);
        } finally {
            setHistoryLoading(false);
        }
    };

    const handleRestoreSnapshot = async (
        snapshot: { config_hash: string; config: Record<string, unknown> },
        commitImmediately: boolean
    ) => {
        const cfg = snapshot.config as unknown as IDRConfig;
        if (commitImmediately) {
            setRestoringHash(snapshot.config_hash);
            try {
                await api.saveSetupConfig(cfg);
                machine.hydrateFromSavedConfig(cfg, machine.stage === 'review' ? 'review' : 'map');
                queryClient.invalidateQueries({ queryKey: ['setupStatus'] });
                queryClient.invalidateQueries({ queryKey: ['setupConfig'] });
                setIsHistoryModalOpen(false);
                toast('success', 'Configuration rolled back', `Restored and saved version ${snapshot.config_hash.slice(0, 8)}.`);
            } catch (err: any) {
                toast('error', 'Rollback failed', err.message || String(err));
            } finally {
                setRestoringHash(null);
            }
        } else {
            machine.setMappingConfig(cfg);
            if (cfg.sources) {
                machine.setSelectedTables(cfg.sources.map(s => s.table).filter(Boolean));
            }
            setIsHistoryModalOpen(false);
            toast('info', 'Loaded version as draft', 'Inspect or run a Dry Run before saving.');
        }
    };

    const getStageIcon = (sId: typeof STUDIO_TABS[number]['id']) => {
        switch (sId) {
            case 'map': return <GitGraph className="w-4 h-4" />;
            case 'rules': return <Shield className="w-4 h-4" />;
            case 'survivorship': return <Layers className="w-4 h-4" />;
            case 'review': return <Play className="w-4 h-4" />;
        }
    };

    const getTabCount = (sId: typeof STUDIO_TABS[number]['id']): string | null => {
        if (sId === 'map') {
            const cnt = machine.mappingConfig.sources?.length || machine.selectedTables.length;
            return cnt > 0 ? String(cnt) : null;
        }
        if (sId === 'rules') {
            const cnt = (machine.mappingConfig.rules?.length || 0) + (machine.mappingConfig.fuzzy_rules?.length || 0);
            return cnt > 0 ? String(cnt) : null;
        }
        if (sId === 'survivorship') {
            const cnt = machine.mappingConfig.survivorship?.length || 0;
            return cnt > 0 ? String(cnt) : null;
        }
        return null;
    };

    const sourceCount = machine.mappingConfig.sources?.length || 0;
    const platformLabel = (machine.connectionData?.platform || '').toString().toUpperCase();

    return (
        <div className="space-y-4">
            {/* Sticky Identity Studio Header (offset below AppShell h-12 sticky bar) */}
            <header className="sticky top-14 z-20 surface-panel px-4 py-3 flex flex-col lg:flex-row lg:items-center justify-between gap-3">
                <div className="space-y-0.5">
                    <div className="flex flex-wrap items-center gap-2">
                        <h1 className="text-[18px] font-semibold tracking-[-0.015em] text-ink">
                            Identity Studio
                        </h1>

                        {machine.isConnected ? (
                            <button
                                type="button"
                                onClick={() => machine.goToStage(machine.stage === 'connect' ? 'map' : 'connect')}
                                className="badge-success hover:opacity-90 transition-opacity cursor-pointer font-mono"
                                title="Switch or inspect warehouse connection"
                            >
                                <span className="w-1.5 h-1.5 rounded-full bg-status-success" />
                                <Database className="w-3 h-3" />
                                <span>{platformLabel || 'WAREHOUSE'}</span>
                                <ArrowLeftRight className="w-3 h-3 opacity-75" />
                            </button>
                        ) : (
                            <span className="badge-neutral">
                                <span className="w-1.5 h-1.5 rounded-full bg-ink-subtle" />
                                Not Connected
                            </span>
                        )}

                        {machine.isConnected && machine.stage !== 'connect' && (
                            machine.hasUnsavedChanges ? (
                                <span className="badge-warning">
                                    Unsaved Changes
                                </span>
                            ) : machine.isConfigured ? (
                                <span className="badge-neutral">
                                    <CheckCircle2 className="w-3 h-3 text-status-success" />
                                    Saved
                                </span>
                            ) : null
                        )}
                    </div>
                    <p className="text-ink-muted text-xs">
                        Discover sources, model matching rules &amp; golden record survivorship, and execute pipeline runs.
                    </p>
                </div>

                {/* Studio Action Bar */}
                <div className="flex flex-wrap items-center gap-2">
                    <button
                        type="button"
                        onClick={() => {
                            setImportText(JSON.stringify(machine.mappingConfig, null, 2));
                            setImportError(null);
                            setIsImportModalOpen(true);
                        }}
                        className="btn-secondary text-xs"
                    >
                        <FileCode2 className="w-3.5 h-3.5 text-accent" />
                        JSON
                    </button>

                    {machine.isConnected && machine.stage !== 'connect' && (
                        <>
                            {machine.isConfigured && (
                                <button
                                    type="button"
                                    onClick={openHistoryModal}
                                    className="btn-secondary text-xs"
                                    title="View saved configuration versions & rollback"
                                >
                                    <History className="w-3.5 h-3.5 text-ink-muted" />
                                    History
                                </button>
                            )}

                            {machine.hasUnsavedChanges && machine.isConfigured && (
                                <button
                                    type="button"
                                    onClick={handleDiscardDraft}
                                    disabled={saving}
                                    className="btn-ghost text-xs text-status-warning"
                                    title="Discard unsaved draft changes and reload saved config"
                                >
                                    <RotateCcw className="w-3.5 h-3.5" />
                                    Discard
                                </button>
                            )}

                            <button
                                type="button"
                                onClick={handleSaveFromHeader}
                                disabled={saving || !!machine.initWarning || sourceCount === 0}
                                title={machine.initWarning ? 'Read-only mode' : 'Persist configuration to idr_meta'}
                                className={
                                    machine.hasUnsavedChanges && machine.stage === 'review'
                                        ? 'btn-primary text-xs'
                                        : machine.hasUnsavedChanges
                                        ? 'btn-secondary text-xs border-accent text-accent'
                                        : 'btn-secondary text-xs'
                                }
                            >
                                {saving ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Save className="w-3.5 h-3.5" />}
                                {saving ? `Saving (${saveProgress?.percent ?? 0}%)` : 'Save Config'}
                            </button>

                            {machine.stage !== 'review' && (
                                <button
                                    type="button"
                                    onClick={() => machine.goToStage('review')}
                                    disabled={sourceCount === 0}
                                    className="btn-primary text-xs"
                                >
                                    <Play className="w-3.5 h-3.5" />
                                    Run Pipeline
                                </button>
                            )}
                        </>
                    )}
                </div>
            </header>

            {/* Studio Header Save Progress Banner (when saving from Tabs 1–3) */}
            {saving && saveProgress && machine.stage !== 'review' && (
                <div className="surface-panel bg-accent-soft/40 border-accent-border p-3.5 space-y-2">
                    <div className="flex items-center justify-between text-xs">
                        <span className="text-accent font-medium flex items-center gap-2">
                            <Loader2 className="w-3.5 h-3.5 animate-spin" />
                            {saveProgress.label || 'Saving configuration...'}
                        </span>
                        <span className="font-mono tabular-nums text-ink">{saveProgress.percent}%</span>
                    </div>
                    <div className="w-full h-1.5 bg-subtle rounded-full overflow-hidden">
                        <div
                            className="h-full bg-accent transition-all duration-300"
                            style={{ width: `${Math.min(100, Math.max(5, saveProgress.percent || 5))}%` }}
                        />
                    </div>
                </div>
            )}

            {saveError && machine.stage !== 'review' && (
                <div className="p-3 bg-status-danger-soft border border-status-danger-border rounded-md text-xs text-status-danger flex items-center justify-between gap-2">
                    <span>{saveError}</span>
                    <button type="button" onClick={() => setSaveError(null)} className="text-status-danger hover:opacity-80">
                        ×
                    </button>
                </div>
            )}

            {/* Configuration Version History & Rollback Modal */}
            {isHistoryModalOpen && (
                <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-ink/40">
                    <div className="bg-surface border border-line-strong rounded-md shadow-popover max-w-2xl w-full p-5 space-y-4">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center gap-2">
                                <History className="w-4 h-4 text-accent" />
                                <h3 className="text-[15px] font-semibold text-ink">Configuration History &amp; Rollback</h3>
                            </div>
                            <button
                                onClick={() => setIsHistoryModalOpen(false)}
                                className="p-1 hover:bg-hover rounded text-ink-muted hover:text-ink"
                            >
                                <X className="w-4 h-4" />
                            </button>
                        </div>
                        <p className="text-xs text-ink-muted">
                            Restore any previously saved configuration snapshot from <code className="font-mono">idr_out.config_snapshot</code>. You can load it as a draft (for Dry Run preview) or restore &amp; save immediately.
                        </p>

                        <div className="max-h-96 overflow-y-auto space-y-2 pr-1">
                            {historyLoading ? (
                                <div className="flex justify-center py-10">
                                    <Loader2 className="w-6 h-6 animate-spin text-accent" />
                                </div>
                            ) : historySnapshots.length === 0 ? (
                                <div className="text-center py-8 text-xs text-ink-muted border border-dashed border-line-strong rounded-md">
                                    No saved configuration snapshots found yet.
                                </div>
                            ) : (
                                historySnapshots.map((snap, idx) => {
                                    const isRestoring = restoringHash === snap.config_hash;
                                    return (
                                        <div
                                            key={`${snap.config_hash}-${snap.created_at}-${idx}`}
                                            className="p-3 surface-subtle flex flex-col sm:flex-row sm:items-center justify-between gap-3"
                                        >
                                            <div className="space-y-1 min-w-0">
                                                <div className="flex items-center gap-2">
                                                    <span className="font-mono text-xs font-semibold text-accent">
                                                        {snap.config_hash.slice(0, 10)}
                                                    </span>
                                                    {idx === 0 && (
                                                        <span className="badge-success">
                                                            Latest Saved
                                                        </span>
                                                    )}
                                                    <span className="text-[11px] text-ink-muted font-mono tabular-nums truncate">
                                                        {snap.created_at}
                                                    </span>
                                                </div>
                                                <div className="flex flex-wrap gap-3 text-xs text-ink-secondary tabular-nums">
                                                    <span><strong className="text-ink">{snap.sources_count}</strong> sources</span>
                                                    <span><strong className="text-ink">{snap.rules_count}</strong> rules</span>
                                                    <span><strong className="text-ink">{snap.exclusions_count}</strong> exclusions</span>
                                                </div>
                                            </div>

                                            <div className="flex items-center gap-2 shrink-0">
                                                <button
                                                    type="button"
                                                    onClick={() => handleRestoreSnapshot(snap, false)}
                                                    className="btn-secondary text-xs"
                                                >
                                                    Load Draft
                                                </button>
                                                <button
                                                    type="button"
                                                    disabled={isRestoring || !!machine.initWarning}
                                                    onClick={() => handleRestoreSnapshot(snap, true)}
                                                    className="btn-primary text-xs"
                                                >
                                                    {isRestoring ? <Loader2 className="w-3 h-3 animate-spin" /> : <RotateCcw className="w-3 h-3" />}
                                                    Restore &amp; Save
                                                </button>
                                            </div>
                                        </div>
                                    );
                                })
                            )}
                        </div>
                    </div>
                </div>
            )}

            {/* Import JSON Modal */}
            {isImportModalOpen && (
                <div
                    role="dialog"
                    aria-modal="true"
                    aria-labelledby="import-json-modal-title"
                    className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-ink/40"
                >
                    <div className="bg-surface border border-line-strong rounded-md shadow-popover max-w-2xl w-full p-5 space-y-4">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center gap-2">
                                <FileCode2 className="w-4 h-4 text-accent" />
                                <h3 id="import-json-modal-title" className="text-[15px] font-semibold text-ink">Import / Export Configuration JSON</h3>
                            </div>
                            <button
                                onClick={() => setIsImportModalOpen(false)}
                                aria-label="Close Import/Export Configuration JSON modal"
                                className="p-1 hover:bg-hover rounded text-ink-muted hover:text-ink"
                            >
                                <X className="w-4 h-4" />
                            </button>
                        </div>
                        <p className="text-xs text-ink-muted">
                            Paste an IDR JSON configuration manifest (sources, matching rules, exclusions, survivorship) or copy your current manifest.
                        </p>

                        {importError && (
                            <div className="p-3 bg-status-danger-soft border border-status-danger-border text-status-danger rounded-md text-xs flex items-center gap-2">
                                <AlertCircle className="w-4 h-4 shrink-0" />
                                <span>{importError}</span>
                            </div>
                        )}

                        <textarea
                            value={importText}
                            onChange={(e) => setImportText(e.target.value)}
                            rows={15}
                            className="input-field font-mono text-xs p-3"
                            placeholder='{"sources": [{"id": "...", "table": "...", "entity_key": "...", "identifiers": [...]}], "rules": [...]}'
                        />

                        <div className="flex justify-end gap-2 pt-1">
                            <button
                                onClick={() => setIsImportModalOpen(false)}
                                className="btn-secondary text-xs"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={() => {
                                    try {
                                        const parsed = JSON.parse(importText);
                                        if (!parsed.sources || !Array.isArray(parsed.sources)) {
                                            setImportError("JSON must contain a 'sources' array.");
                                            return;
                                        }
                                        setImportError(null);
                                        machine.setMappingConfig(parsed);
                                        if (parsed.sources) {
                                            machine.setSelectedTables(
                                                parsed.sources.map((s: any) => s.table).filter(Boolean)
                                            );
                                        }
                                        if (machine.isConnected) {
                                            machine.goToStage('map');
                                        }
                                        setIsImportModalOpen(false);
                                        toast('success', 'Manifest applied', 'Review tabs and click Save Config to persist.');
                                    } catch (err: any) {
                                        setImportError(`Invalid JSON: ${err.message}`);
                                    }
                                }}
                                className="btn-primary text-xs"
                            >
                                Apply Configuration
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {/* Persistent 4-Tab Studio Bar */}
            {machine.isConnected && machine.stage !== 'connect' && (
                <nav className="grid grid-cols-2 sm:grid-cols-4 gap-1.5 surface-subtle p-1">
                    {STUDIO_TABS.map(tab => {
                        const isActive = machine.stage === tab.id;
                        const isClickable = machine.canNavigateTo(tab.id);
                        const count = getTabCount(tab.id);
                        return (
                            <button
                                key={tab.id}
                                type="button"
                                disabled={!isClickable}
                                onClick={() => isClickable && machine.goToStage(tab.id)}
                                className={`flex items-center justify-between gap-2 px-3 py-2 rounded-md text-xs font-medium transition-colors ${
                                    isActive
                                        ? 'bg-surface text-ink border border-line-strong font-semibold'
                                        : isClickable
                                        ? 'text-ink-secondary hover:text-ink hover:bg-hover border border-transparent'
                                        : 'text-ink-subtle opacity-50 cursor-not-allowed border border-transparent'
                                }`}
                            >
                                <span className="flex items-center gap-2 truncate">
                                    <span className={isActive ? 'text-accent shrink-0' : 'text-ink-muted shrink-0'}>
                                        {getStageIcon(tab.id)}
                                    </span>
                                    <span className="truncate">
                                        {tab.stepNumber}. {tab.label}
                                    </span>
                                </span>
                                {count !== null && (
                                    <span className={`font-mono tabular-nums ${isActive ? 'badge-accent' : 'badge-neutral'}`}>
                                        {count}
                                    </span>
                                )}
                            </button>
                        );
                    })}
                </nav>
            )}

            {/* Main Studio Workspace */}
            <div className="surface-panel p-4 sm:p-5 min-h-[520px]">
                {loadingConfig ? (
                    <div className="flex flex-col items-center justify-center py-28 text-center space-y-2.5">
                        <Loader2 className="w-7 h-7 text-accent animate-spin" />
                        <div className="text-sm font-medium text-ink">
                            Loading warehouse configuration &amp; metadata...
                        </div>
                        <p className="text-xs text-ink-muted max-w-sm">
                            Fetching sources, matching rules, survivorship, and lenses in parallel.
                        </p>
                    </div>
                ) : (
                <>
                {machine.stage === 'connect' && (
                    <StepConnect
                        isConnected={machine.isConnected}
                        onCancel={machine.isConnected ? () => machine.goToStage('map') : undefined}
                        onNext={async (data: any) => {
                            machine.handleConnectionSuccess(data);
                            queryClient.invalidateQueries({ queryKey: ['setupStatus'] });
                            queryClient.invalidateQueries({ queryKey: ['health'] });
                            await checkConfigAndNavigate('map', data);
                        }}
                    />
                )}

                {machine.stage === 'map' && (
                    <StepMap
                        tables={machine.selectedTables}
                        initialConfig={machine.mappingConfig}
                        connectionData={machine.connectionData}
                        onTablesChange={(tables) => {
                            machine.setSelectedTables(tables);
                            machine.setHasUnsavedChanges(true);
                        }}
                        onChange={(config: IDRConfig) => machine.setMappingConfig(config)}
                        onNext={(config: IDRConfig) => {
                            machine.setMappingConfig(config);
                            machine.goToStage('rules');
                        }}
                        onBack={() => machine.goToStage('connect')}
                    />
                )}

                {machine.stage === 'rules' && (
                    <StepRules
                        config={machine.mappingConfig}
                        onChange={(config: IDRConfig) => machine.setMappingConfig(config)}
                        onNext={(config: IDRConfig) => {
                            machine.setMappingConfig(config);
                            machine.goToStage('survivorship');
                        }}
                        onBack={() => machine.goToStage('map')}
                    />
                )}

                {machine.stage === 'survivorship' && (
                    <StepSurvivorship
                        config={machine.mappingConfig}
                        onChange={(config: IDRConfig) => machine.setMappingConfig(config)}
                        onNext={(config: IDRConfig) => {
                            machine.setMappingConfig(config);
                            machine.goToStage('review');
                        }}
                        onBack={() => machine.goToStage('rules')}
                    />
                )}

                {machine.stage === 'review' && (
                    <StepReview
                        config={machine.mappingConfig}
                        onBack={() => machine.goToStage('survivorship')}
                        onComplete={() => {
                            window.location.href = "/";
                        }}
                        initialSaved={machine.isConfigured}
                        hasUnsavedChanges={machine.hasUnsavedChanges}
                        readOnly={!!machine.initWarning}
                        warningMessage={machine.initWarning}
                        onSaved={machine.markConfigSaved}
                        externalSaving={saving}
                        externalSaveProgress={saveProgress}
                        onTriggerSave={handleSaveFromHeader}
                        onDiscardDraft={handleDiscardDraft}
                        onUpdateConfig={(newConfig: IDRConfig) => {
                            machine.setMappingConfig(newConfig);
                            if (newConfig.sources) {
                                machine.setSelectedTables(
                                    newConfig.sources.map(s => s.table).filter(Boolean)
                                );
                            }
                        }}
                    />
                )}
                </>
                )}
            </div>
        </div>
    );
}
