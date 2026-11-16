import { useEffect, useState } from 'react';
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
        try {
            const status = await api.getSetupStatus();
            if (status.connected) {
                machine.setIsConnected(true);
                if (!connOverride && !machine.connectionData && status.platform) {
                    machine.setConnectionData({ platform: status.platform });
                }
            }

            if (status.configured) {
                const config = await api.getSetupConfig();
                if (config && config.sources && config.sources.length > 0) {
                    machine.hydrateFromSavedConfig(config as unknown as IDRConfig, preferredStage);
                    return true;
                }
            }

            if (status.connected) {
                machine.goToStage('map');
            }
        } catch (error) {
            console.error("Failed to load setup config:", error);
        }
        return false;
    };

    useEffect(() => {
        checkConfigAndNavigate('map');
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
        <div className="container mx-auto max-w-6xl py-6 space-y-6 text-slate-100">
            {/* Sticky Identity Studio Header */}
            <header className="sticky top-2 z-30 bg-slate-900/90 backdrop-blur-md border border-slate-800 rounded-2xl px-5 py-4 shadow-xl flex flex-col lg:flex-row lg:items-center justify-between gap-4">
                <div className="space-y-1">
                    <div className="flex flex-wrap items-center gap-2.5">
                        <h1 className="text-xl sm:text-2xl font-extrabold tracking-tight text-white">
                            Identity Studio
                        </h1>

                        {machine.isConnected ? (
                            <button
                                type="button"
                                onClick={() => machine.goToStage(machine.stage === 'connect' ? 'map' : 'connect')}
                                className="inline-flex items-center gap-2 px-2.5 py-1 rounded-full bg-emerald-950/60 hover:bg-emerald-900/60 border border-emerald-500/30 text-xs font-medium text-emerald-300 transition-colors"
                                title="Switch or inspect warehouse connection"
                            >
                                <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
                                <Database className="w-3 h-3" />
                                <span>{platformLabel || 'WAREHOUSE'}</span>
                                <ArrowLeftRight className="w-3 h-3 text-emerald-400/70" />
                            </button>
                        ) : (
                            <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-slate-800 border border-slate-700 text-xs text-slate-400">
                                <span className="w-2 h-2 rounded-full bg-slate-500" />
                                Not Connected
                            </span>
                        )}

                        {machine.isConnected && machine.stage !== 'connect' && (
                            machine.hasUnsavedChanges ? (
                                <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full bg-amber-950/60 border border-amber-500/40 text-[11px] font-medium text-amber-300">
                                    Unsaved Changes
                                </span>
                            ) : machine.isConfigured ? (
                                <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full bg-slate-800/80 border border-slate-700 text-[11px] font-medium text-slate-300">
                                    <CheckCircle2 className="w-3 h-3 text-emerald-400" />
                                    Saved
                                </span>
                            ) : null
                        )}
                    </div>
                    <p className="text-slate-400 text-xs">
                        Discover sources, model matching rules &amp; golden record survivorship, and execute pipeline runs.
                    </p>
                </div>

                {/* Studio Action Bar */}
                <div className="flex flex-wrap items-center gap-2.5">
                    <button
                        type="button"
                        onClick={() => {
                            setImportText(JSON.stringify(machine.mappingConfig, null, 2));
                            setImportError(null);
                            setIsImportModalOpen(true);
                        }}
                        className="inline-flex items-center gap-1.5 px-3 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 rounded-xl text-xs font-semibold transition-colors"
                    >
                        <FileCode2 className="w-3.5 h-3.5 text-blue-400" />
                        JSON
                    </button>

                    {machine.isConnected && machine.stage !== 'connect' && (
                        <>
                            {machine.isConfigured && (
                                <button
                                    type="button"
                                    onClick={openHistoryModal}
                                    className="inline-flex items-center gap-1.5 px-3 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 rounded-xl text-xs font-semibold transition-colors"
                                    title="View saved configuration versions & rollback"
                                >
                                    <History className="w-3.5 h-3.5 text-purple-400" />
                                    History
                                </button>
                            )}

                            {machine.hasUnsavedChanges && machine.isConfigured && (
                                <button
                                    type="button"
                                    onClick={handleDiscardDraft}
                                    disabled={saving}
                                    className="inline-flex items-center gap-1.5 px-3 py-2 bg-amber-950/50 hover:bg-amber-900/50 text-amber-200 border border-amber-700/60 rounded-xl text-xs font-semibold transition-colors"
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
                                className={`inline-flex items-center gap-1.5 px-3.5 py-2 rounded-xl text-xs font-semibold border transition-all disabled:opacity-45 disabled:cursor-not-allowed ${
                                    machine.hasUnsavedChanges
                                        ? 'bg-emerald-600 hover:bg-emerald-500 text-white border-emerald-500 shadow-lg shadow-emerald-950/30'
                                        : 'bg-slate-800 hover:bg-slate-700 text-slate-200 border-slate-700'
                                }`}
                            >
                                {saving ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Save className="w-3.5 h-3.5" />}
                                {saving ? `Saving (${saveProgress?.percent ?? 0}%)` : 'Save Config'}
                            </button>

                            <button
                                type="button"
                                onClick={() => machine.goToStage('review')}
                                disabled={sourceCount === 0}
                                className={`inline-flex items-center gap-1.5 px-4 py-2 rounded-xl text-xs font-semibold transition-all disabled:opacity-45 disabled:cursor-not-allowed ${
                                    machine.stage === 'review'
                                        ? 'bg-blue-500 text-white shadow-lg shadow-blue-950/40'
                                        : 'bg-blue-600 hover:bg-blue-500 text-white shadow-md'
                                }`}
                            >
                                <Play className="w-3.5 h-3.5" />
                                Run Pipeline
                            </button>
                        </>
                    )}
                </div>
            </header>

            {/* Studio Header Save Progress Banner (when saving from Tabs 1–3) */}
            {saving && saveProgress && machine.stage !== 'review' && (
                <div className="bg-blue-950/35 border border-blue-500/40 rounded-xl p-3.5 space-y-2">
                    <div className="flex items-center justify-between text-xs">
                        <span className="text-blue-300 font-medium flex items-center gap-2">
                            <Loader2 className="w-3.5 h-3.5 animate-spin" />
                            {saveProgress.label || 'Saving configuration...'}
                        </span>
                        <span className="font-mono text-blue-200">{saveProgress.percent}%</span>
                    </div>
                    <div className="w-full h-1.5 bg-slate-800 rounded-full overflow-hidden">
                        <div
                            className="h-full bg-gradient-to-r from-blue-500 to-indigo-400 transition-all duration-300"
                            style={{ width: `${Math.min(100, Math.max(5, saveProgress.percent || 5))}%` }}
                        />
                    </div>
                </div>
            )}

            {saveError && machine.stage !== 'review' && (
                <div className="p-3 bg-rose-950/50 border border-rose-500/40 rounded-xl text-xs text-rose-300 flex items-center justify-between gap-2">
                    <span>{saveError}</span>
                    <button type="button" onClick={() => setSaveError(null)} className="text-rose-400 hover:text-white">
                        ×
                    </button>
                </div>
            )}

            {/* Configuration Version History & Rollback Modal */}
            {isHistoryModalOpen && (
                <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/75 backdrop-blur-sm">
                    <div className="bg-slate-900 border border-slate-700 rounded-2xl shadow-2xl max-w-2xl w-full p-6 space-y-4">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center gap-2">
                                <History className="w-5 h-5 text-purple-400" />
                                <h3 className="text-lg font-bold text-white">Configuration History &amp; Rollback</h3>
                            </div>
                            <button
                                onClick={() => setIsHistoryModalOpen(false)}
                                className="p-1 hover:bg-slate-800 rounded text-slate-400 hover:text-white"
                            >
                                <X className="w-5 h-5" />
                            </button>
                        </div>
                        <p className="text-xs text-slate-400">
                            Restore any previously saved configuration snapshot from <code>idr_out.config_snapshot</code>. You can load it as a draft (for Dry Run preview) or restore &amp; save immediately.
                        </p>

                        <div className="max-h-96 overflow-y-auto space-y-2 pr-1">
                            {historyLoading ? (
                                <div className="flex justify-center py-10">
                                    <Loader2 className="w-6 h-6 animate-spin text-blue-400" />
                                </div>
                            ) : historySnapshots.length === 0 ? (
                                <div className="text-center py-8 text-xs text-slate-500 border border-dashed border-slate-800 rounded-xl">
                                    No saved configuration snapshots found yet.
                                </div>
                            ) : (
                                historySnapshots.map((snap, idx) => {
                                    const isRestoring = restoringHash === snap.config_hash;
                                    return (
                                        <div
                                            key={`${snap.config_hash}-${snap.created_at}-${idx}`}
                                            className="p-3.5 rounded-xl bg-slate-950/70 border border-slate-800 flex flex-col sm:flex-row sm:items-center justify-between gap-3"
                                        >
                                            <div className="space-y-1 min-w-0">
                                                <div className="flex items-center gap-2">
                                                    <span className="font-mono text-xs font-semibold text-blue-300">
                                                        {snap.config_hash.slice(0, 10)}
                                                    </span>
                                                    {idx === 0 && (
                                                        <span className="px-1.5 py-0.5 rounded bg-emerald-950/60 border border-emerald-700/50 text-[10px] font-semibold text-emerald-300">
                                                            Latest Saved
                                                        </span>
                                                    )}
                                                    <span className="text-[11px] text-slate-500 font-mono truncate">
                                                        {snap.created_at}
                                                    </span>
                                                </div>
                                                <div className="flex flex-wrap gap-3 text-xs text-slate-400">
                                                    <span><strong className="text-slate-200">{snap.sources_count}</strong> sources</span>
                                                    <span><strong className="text-slate-200">{snap.rules_count}</strong> rules</span>
                                                    <span><strong className="text-slate-200">{snap.exclusions_count}</strong> exclusions</span>
                                                </div>
                                            </div>

                                            <div className="flex items-center gap-2 shrink-0">
                                                <button
                                                    type="button"
                                                    onClick={() => handleRestoreSnapshot(snap, false)}
                                                    className="px-2.5 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 rounded-lg text-xs font-medium"
                                                >
                                                    Load Draft
                                                </button>
                                                <button
                                                    type="button"
                                                    disabled={isRestoring || !!machine.initWarning}
                                                    onClick={() => handleRestoreSnapshot(snap, true)}
                                                    className="px-3 py-1.5 bg-purple-600 hover:bg-purple-500 disabled:opacity-50 text-white rounded-lg text-xs font-semibold inline-flex items-center gap-1.5"
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
                <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/75 backdrop-blur-sm">
                    <div className="bg-slate-900 border border-slate-700 rounded-2xl shadow-2xl max-w-2xl w-full p-6 space-y-4">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center gap-2">
                                <FileCode2 className="w-5 h-5 text-blue-400" />
                                <h3 className="text-lg font-bold text-white">Import / Export Configuration JSON</h3>
                            </div>
                            <button
                                onClick={() => setIsImportModalOpen(false)}
                                className="p-1 hover:bg-slate-800 rounded text-slate-400 hover:text-white"
                            >
                                <X className="w-5 h-5" />
                            </button>
                        </div>
                        <p className="text-xs text-slate-400">
                            Paste an IDR JSON configuration manifest (sources, matching rules, exclusions, survivorship) or copy your current manifest.
                        </p>

                        {importError && (
                            <div className="p-3 bg-rose-950/60 border border-rose-800 text-rose-300 rounded-lg text-xs flex items-center gap-2">
                                <AlertCircle className="w-4 h-4 shrink-0 text-rose-400" />
                                <span>{importError}</span>
                            </div>
                        )}

                        <textarea
                            value={importText}
                            onChange={(e) => setImportText(e.target.value)}
                            rows={15}
                            className="w-full bg-slate-950 border border-slate-700 rounded-xl p-3 font-mono text-xs text-slate-200 focus:outline-none focus:border-blue-500"
                            placeholder='{"sources": [{"id": "...", "table": "...", "entity_key": "...", "identifiers": [...]}], "rules": [...]}'
                        />

                        <div className="flex justify-end gap-3 pt-2">
                            <button
                                onClick={() => setIsImportModalOpen(false)}
                                className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg text-xs font-medium"
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
                                className="px-5 py-2 bg-blue-600 hover:bg-blue-500 text-white rounded-lg text-xs font-semibold shadow-md transition-colors"
                            >
                                Apply Configuration
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {/* Persistent 4-Tab Studio Bar */}
            {machine.isConnected && machine.stage !== 'connect' && (
                <nav className="grid grid-cols-2 sm:grid-cols-4 gap-2 bg-slate-900/70 p-1.5 rounded-2xl border border-slate-800">
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
                                className={`flex items-center justify-between gap-2 px-3.5 py-2.5 rounded-xl text-xs font-semibold transition-all ${
                                    isActive
                                        ? 'bg-blue-600/20 text-blue-200 border border-blue-500/50 shadow-sm'
                                        : isClickable
                                        ? 'text-slate-300 hover:bg-slate-800/80 border border-transparent'
                                        : 'text-slate-600 opacity-50 cursor-not-allowed border border-transparent'
                                }`}
                            >
                                <span className="flex items-center gap-2 truncate">
                                    <span className={`p-1 rounded-lg ${isActive ? 'bg-blue-500/20 text-blue-300' : 'bg-slate-800 text-slate-400'}`}>
                                        {getStageIcon(tab.id)}
                                    </span>
                                    <span className="truncate">
                                        {tab.stepNumber}. {tab.label}
                                    </span>
                                </span>
                                {count !== null && (
                                    <span className={`px-2 py-0.5 rounded-full text-[11px] font-mono ${
                                        isActive ? 'bg-blue-500/30 text-blue-200' : 'bg-slate-800 text-slate-400'
                                    }`}>
                                        {count}
                                    </span>
                                )}
                            </button>
                        );
                    })}
                </nav>
            )}

            {/* Main Studio Workspace */}
            <div className="bg-slate-900/55 rounded-2xl border border-slate-800/90 shadow-xl p-5 sm:p-6 min-h-[520px]">
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
            </div>
        </div>
    );
}
