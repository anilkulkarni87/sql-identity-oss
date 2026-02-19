import React, { useState, useEffect } from 'react';
import { Database, Search, GitGraph, Play, Layers, Shield } from "lucide-react";
import { IDRConfig } from '../../types';
import { api } from '../../api/client';

import StepConnect from './StepConnect';
import StepDiscover from './StepDiscover';
import StepMap from './StepMap';
import StepRules from './StepRules';
import StepSurvivorship from './StepSurvivorship';
import StepReview from './StepReview';

export default function SetupWizard() {
    const [step, setStep] = useState(1);
    const [connectionData, setConnectionData] = useState<any>(null);
    const [initWarning, setInitWarning] = useState<string | null>(null);
    const [selectedTables, setSelectedTables] = useState<string[]>([]);
    const [mappingConfig, setMappingConfig] = useState<IDRConfig>({ sources: [] }); // Initialize with empty sources
    const [isConfigured, setIsConfigured] = useState(false);

    const [isConnected, setIsConnected] = useState(false);

    const checkConfigAndNavigate = async () => {
        try {
            // Check status first to see if we are connected/have meta
            const status = await api.getSetupStatus();

            if (status.connected) {
                setIsConnected(true);
            }

            if (status.configured) {
                setIsConfigured(true);

                // Fetch existing config
                const config = await api.getSetupConfig();
                if (config && config.sources && config.sources.length > 0) {
                    setMappingConfig(config as unknown as IDRConfig);
                    setStep(6); // Jump to Run
                    return true;
                }
            }
        } catch (error) {
            console.error("Failed to load setup config:", error);
        }
        return false;
    };

    useEffect(() => {
        // Check on mount (in case already connected/configured from previous session)
        checkConfigAndNavigate();
    }, []);

    const nextStep = () => setStep(s => s + 1);
    const prevStep = () => setStep(s => s - 1);

    const renderStepIcon = (s: number, icon: React.ReactNode, label: string) => {
        const isActive = step === s;
        const isCompleted = step > s;
        // Allow clicking 'Run' (Step 6) if already configured
        const isClickable = s === 6 && isConfigured;

        let className = "flex flex-col items-center gap-2 px-6 py-2 border-b-2 transition-colors ";
        if (isActive) className += "border-blue-500 text-blue-400";
        else if (isCompleted) className += "border-green-500 text-green-400";
        else className += "border-transparent text-gray-500";

        if (isClickable) className += " cursor-pointer hover:bg-gray-800 rounded-t";

        return (
            <div key={s} className={className} onClick={() => isClickable && setStep(s)}>
                {icon}
                <span className="text-xs font-semibold uppercase">{label}</span>
            </div>
        );
    };

    return (
        <div className="container mx-auto max-w-5xl py-10 space-y-8 text-gray-100">

            <header className="text-center space-y-2">
                <h1 className="text-3xl font-extrabold tracking-tight">Setup IDR</h1>
                <p className="text-gray-400">Connect your data warehouse and configure identity resolution in minutes.</p>
            </header>

            {/* Stepper */}
            <div className="flex justify-center w-full mb-8 overflow-x-auto">
                {renderStepIcon(1, <Database className="w-6 h-6" />, "Connect")}
                {renderStepIcon(2, <Search className="w-6 h-6" />, "Discover")}
                {renderStepIcon(3, <GitGraph className="w-6 h-6" />, "Map")}
                {renderStepIcon(4, <Shield className="w-6 h-6" />, "Rules")}
                {renderStepIcon(5, <Layers className="w-6 h-6" />, "Survivorship")}
                {renderStepIcon(6, <Play className="w-6 h-6" />, "Run")}
            </div>

            <div className="bg-gray-800 rounded-xl border border-gray-700 min-h-[500px] flex flex-col">
                <div className="p-6 flex-1">
                    {step === 1 && (
                        <StepConnect
                            isConnected={isConnected}
                            onNext={async (data: any) => {
                                setConnectionData(data);
                                if (data.warning) {
                                    setInitWarning(data.warning);
                                } else {
                                    setInitWarning(null);
                                }
                                // Check if we have an existing config to jump to
                                const skipped = await checkConfigAndNavigate();
                                if (!skipped) {
                                    nextStep();
                                }
                            }}
                        />
                    )}
                    {step === 2 && (
                        <StepDiscover
                            connectionData={connectionData}
                            onNext={(tables: string[]) => {
                                setSelectedTables(tables);
                                nextStep();
                            }}
                            onBack={prevStep}
                        />
                    )}
                    {step === 3 && (
                        <StepMap
                            tables={selectedTables}
                            onNext={(config: IDRConfig) => {
                                setMappingConfig(config);
                                nextStep();
                            }}
                            onBack={prevStep}
                        />
                    )}
                    {step === 4 && (
                        <StepRules
                            config={mappingConfig}
                            onNext={(config: IDRConfig) => {
                                setMappingConfig(config);
                                nextStep();
                            }}
                            onBack={prevStep}
                        />
                    )}
                    {step === 5 && (
                        <StepSurvivorship
                            config={mappingConfig}
                            onNext={(config: IDRConfig) => {
                                setMappingConfig(config);
                                nextStep();
                            }}
                            onBack={prevStep}
                        />
                    )}
                    {step === 6 && (
                        <StepReview
                            config={mappingConfig}
                            onBack={prevStep}
                            onComplete={() => {
                                window.location.href = "/"; // Navigate to dashboard
                            }}
                            initialSaved={isConfigured}
                            readOnly={!!initWarning}
                            warningMessage={initWarning}
                        />
                    )}
                </div>
            </div>
        </div>
    );
}
