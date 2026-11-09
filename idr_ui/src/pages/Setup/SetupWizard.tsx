import { useEffect } from 'react';
import { Database, Search, GitGraph, Play, Layers, Shield } from "lucide-react";
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
            <header className="text-center space-y-2">
                <h1 className="text-3xl font-extrabold tracking-tight">Setup IDR</h1>
                <p className="text-gray-400">Connect your data warehouse and configure identity resolution in minutes.</p>
            </header>

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
                        />
                    )}
                </div>
            </div>
        </div>
    );
}
