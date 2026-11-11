import { useEffect, useState } from 'react';
import { Database, Search, GitGraph, Play, Layers, Shield, FileCode2, X, AlertCircle } from "lucide-react";
import { IDRConfig } from '../../types';
import { api } from '../../api/client';
import { useWizardMachine, WizardStage, WIZARD_STAGES } from './useWizardMachine';

import StepConnect from './StepConnect';
import StepDiscover from './StepDiscover';
import StepMap from './StepMap';
import StepRules from './StepRules';
import StepSurvivorship from './StepSurvivorship';
import StepReview from './StepReview';

export default function SetupWizard() {
    const machine = useWizardMachine();
    const [isImportModalOpen, setIsImportModalOpen] = useState(false);
    const [importText, setImportText] = useState('');
    const [importError, setImportError] = useState<string | null>(null);

    const checkConfigAndNavigate = async () => {
        try {
            const status = await api.getSetupStatus();
            if (status.connected) {
                machine.setIsConnected(true);
            }

            if (status.configured) {
                const config = await api.getSetupConfig();
                if (config && config.sources && config.sources.length > 0) {
                    machine.hydrateFromSavedConfig(config as unknown as IDRConfig);
                    return true;
                }
            }
        } catch (error) {
            console.error("Failed to load setup config:", error);
        }
        return false;
    };

    useEffect(() => {
        checkConfigAndNavigate();
    }, []);

    const getStageIcon = (sId: WizardStage) => {
        switch (sId) {
            case 'connect': return <Database className="w-6 h-6" />;
            case 'discover': return <Search className="w-6 h-6" />;
            case 'map': return <GitGraph className="w-6 h-6" />;
            case 'rules': return <Shield className="w-6 h-6" />;
            case 'survivorship': return <Layers className="w-6 h-6" />;
            case 'review': return <Play className="w-6 h-6" />;
        }
    };

    const renderStepIcon = (s: typeof WIZARD_STAGES[number]) => {
        const isActive = machine.stage === s.id;
        const isCompleted = machine.currentStepNumber > s.stepNumber;
        const isClickable = machine.canNavigateTo(s.id);

        let className = "flex flex-col items-center gap-2 px-6 py-2 border-b-2 transition-colors ";
        if (isActive) className += "border-blue-500 text-blue-400 font-bold";
        else if (isCompleted) className += "border-green-500 text-green-400";
        else className += "border-transparent text-gray-500";

        if (isClickable) className += " cursor-pointer hover:bg-gray-800 rounded-t";
        else className += " opacity-50 cursor-not-allowed";

        return (
            <div
                key={s.id}
                className={className}
                onClick={() => isClickable && machine.goToStage(s.id)}
                title={isClickable ? `Go to ${s.label}` : `Complete previous steps to unlock ${s.label}`}
            >
                {getStageIcon(s.id)}
                <span className="text-xs font-semibold uppercase">{s.label}</span>
            </div>
        );
    };

    return (
        <div className="container mx-auto max-w-5xl py-10 space-y-8 text-gray-100">
            <header className="flex flex-col sm:flex-row items-center justify-between gap-4">
                <div className="space-y-1 text-center sm:text-left">
                    <h1 className="text-3xl font-extrabold tracking-tight">Setup IDR</h1>
                    <p className="text-gray-400 text-sm">Connect your data warehouse and configure identity resolution in minutes.</p>
                </div>
                <button
                    onClick={() => {
                        setImportText(JSON.stringify(machine.mappingConfig, null, 2));
                        setImportError(null);
                        setIsImportModalOpen(true);
                    }}
                    className="inline-flex items-center gap-2 px-3.5 py-2 bg-gray-800 hover:bg-gray-700 text-blue-400 hover:text-blue-300 border border-gray-700 hover:border-blue-500 rounded-lg text-xs font-semibold shadow-sm transition-all"
                >
                    <FileCode2 className="w-4 h-4" />
                    Import / Paste JSON
                </button>
            </header>

            {/* Import JSON Modal */}
            {isImportModalOpen && (
                <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/75 backdrop-blur-sm animate-in fade-in duration-150">
                    <div className="bg-gray-900 border border-gray-700 rounded-xl shadow-2xl max-w-2xl w-full p-6 space-y-4">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center gap-2">
                                <FileCode2 className="w-5 h-5 text-blue-400" />
                                <h3 className="text-lg font-bold text-white">Import Configuration JSON</h3>
                            </div>
                            <button
                                onClick={() => setIsImportModalOpen(false)}
                                className="p-1 hover:bg-gray-800 rounded text-gray-400 hover:text-white"
                            >
                                <X className="w-5 h-5" />
                            </button>
                        </div>
                        <p className="text-xs text-gray-400">
                            Paste an existing IDR JSON configuration manifest (e.g. from Databricks, BigQuery, or an exported YAML) to instantly load sources, identifiers, rules, and mappings.
                        </p>

                        {importError && (
                            <div className="p-3 bg-red-950/60 border border-red-800 text-red-300 rounded-lg text-xs flex items-center gap-2">
                                <AlertCircle className="w-4 h-4 shrink-0 text-red-400" />
                                <span>{importError}</span>
                            </div>
                        )}

                        <textarea
                            value={importText}
                            onChange={(e) => setImportText(e.target.value)}
                            rows={15}
                            className="w-full bg-gray-950 border border-gray-700 rounded-lg p-3 font-mono text-xs text-gray-200 focus:outline-none focus:border-blue-500"
                            placeholder='{"sources": [{"id": "...", "table": "...", "entity_key": "...", "identifiers": [...]}], "rules": [...]}'
                        />

                        <div className="flex justify-end gap-3 pt-2">
                            <button
                                onClick={() => setIsImportModalOpen(false)}
                                className="px-4 py-2 bg-gray-800 hover:bg-gray-700 text-gray-300 rounded-md text-xs font-medium"
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
                                        machine.hydrateFromSavedConfig(parsed);
                                        machine.goToStage('review');
                                        setIsImportModalOpen(false);
                                    } catch (err: any) {
                                        setImportError(`Invalid JSON: ${err.message}`);
                                    }
                                }}
                                className="px-5 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md text-xs font-semibold shadow-md transition-colors"
                            >
                                Apply & Go to Review
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {/* Stepper with explicit State Machine transitions */}
            <div className="flex justify-center w-full mb-8 overflow-x-auto">
                {WIZARD_STAGES.map(renderStepIcon)}
            </div>

            <div className="bg-gray-800 rounded-xl border border-gray-700 min-h-[500px] flex flex-col">
                <div className="p-6 flex-1">
                    {machine.stage === 'connect' && (
                        <StepConnect
                            isConnected={machine.isConnected}
                            onNext={async (data: any) => {
                                machine.handleConnectionSuccess(data);
                                const skipped = await checkConfigAndNavigate();
                                if (!skipped) {
                                    machine.nextStage();
                                }
                            }}
                        />
                    )}
                    {machine.stage === 'discover' && (
                        <StepDiscover
                            connectionData={machine.connectionData}
                            onNext={(tables: string[]) => {
                                machine.setSelectedTables(tables);
                                machine.nextStage();
                            }}
                            onBack={machine.prevStage}
                        />
                    )}
                    {machine.stage === 'map' && (
                        <StepMap
                            tables={machine.selectedTables}
                            initialConfig={machine.mappingConfig}
                            onNext={(config: IDRConfig) => {
                                machine.setMappingConfig(config);
                                machine.nextStage();
                            }}
                            onBack={machine.prevStage}
                        />
                    )}
                    {machine.stage === 'rules' && (
                        <StepRules
                            config={machine.mappingConfig}
                            onNext={(config: IDRConfig) => {
                                machine.setMappingConfig(config);
                                machine.nextStage();
                            }}
                            onBack={machine.prevStage}
                        />
                    )}
                    {machine.stage === 'survivorship' && (
                        <StepSurvivorship
                            config={machine.mappingConfig}
                            onNext={(config: IDRConfig) => {
                                machine.setMappingConfig(config);
                                machine.nextStage();
                            }}
                            onBack={machine.prevStage}
                        />
                    )}
                    {machine.stage === 'review' && (
                        <StepReview
                            config={machine.mappingConfig}
                            onBack={machine.prevStage}
                            onComplete={() => {
                                window.location.href = "/";
                            }}
                            initialSaved={machine.isConfigured}
                            readOnly={!!machine.initWarning}
                            warningMessage={machine.initWarning}
                            onUpdateConfig={(newConfig: IDRConfig) => {
                                machine.hydrateFromSavedConfig(newConfig);
                            }}
                        />
                    )}
                </div>
            </div>
        </div>
    );
}
