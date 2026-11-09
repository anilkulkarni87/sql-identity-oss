import { useState, useCallback } from 'react';
import { IDRConfig } from '../../types';

export type WizardStage = 'connect' | 'discover' | 'map' | 'rules' | 'survivorship' | 'review';

export interface WizardStageMeta {
    id: WizardStage;
    stepNumber: number;
    label: string;
}

export const WIZARD_STAGES: WizardStageMeta[] = [
    { id: 'connect', stepNumber: 1, label: 'Connect' },
    { id: 'discover', stepNumber: 2, label: 'Discover' },
    { id: 'map', stepNumber: 3, label: 'Map' },
    { id: 'rules', stepNumber: 4, label: 'Rules' },
    { id: 'survivorship', stepNumber: 5, label: 'Survivorship' },
    { id: 'review', stepNumber: 6, label: 'Run' },
];

export function useWizardMachine() {
    const [stage, setStage] = useState<WizardStage>('connect');
    const [isConnected, setIsConnected] = useState(false);
    const [isConfigured, setIsConfigured] = useState(false);
    const [connectionData, setConnectionData] = useState<any>(null);
    const [initWarning, setInitWarning] = useState<string | null>(null);
    const [selectedTables, setSelectedTables] = useState<string[]>([]);
    const [mappingConfig, setMappingConfig] = useState<IDRConfig>({ sources: [] });

    const currentStepNumber = WIZARD_STAGES.find(s => s.id === stage)?.stepNumber || 1;

    // Guard rules defining if a user/agent is permitted to transition to target stage
    const canNavigateTo = useCallback((target: WizardStage): boolean => {
        // If already configured, user can freely review or modify any step
        if (isConfigured) return true;

        const targetMeta = WIZARD_STAGES.find(s => s.id === target);
        if (!targetMeta) return false;
        const targetNum = targetMeta.stepNumber;

        // Can always view current or previous steps
        if (targetNum <= currentStepNumber) return true;

        // Forward transition guards
        if (targetNum >= 2 && !isConnected) return false;
        if (targetNum >= 3 && selectedTables.length === 0) return false;
        if (targetNum >= 4 && (!mappingConfig.sources || mappingConfig.sources.length === 0)) return false;
        
        return true;
    }, [isConfigured, currentStepNumber, isConnected, selectedTables, mappingConfig]);

    const goToStage = useCallback((target: WizardStage) => {
        if (canNavigateTo(target)) {
            setStage(target);
        }
    }, [canNavigateTo]);

    const nextStage = useCallback(() => {
        const nextMeta = WIZARD_STAGES.find(s => s.stepNumber === currentStepNumber + 1);
        if (nextMeta && canNavigateTo(nextMeta.id)) {
            setStage(nextMeta.id);
        }
    }, [currentStepNumber, canNavigateTo]);

    const prevStage = useCallback(() => {
        const prevMeta = WIZARD_STAGES.find(s => s.stepNumber === currentStepNumber - 1);
        if (prevMeta) {
            setStage(prevMeta.id);
        }
    }, [currentStepNumber]);

    const handleConnectionSuccess = useCallback((data: any) => {
        setConnectionData(data);
        setIsConnected(true);
        if (data?.warning) {
            setInitWarning(data.warning);
        } else {
            setInitWarning(null);
        }
    }, []);

    const hydrateFromSavedConfig = useCallback((config: IDRConfig) => {
        setIsConfigured(true);
        setIsConnected(true);
        setMappingConfig(config);
        if (config.sources) {
            setSelectedTables(
                config.sources.map(s => s.table).filter((t): t is string => Boolean(t))
            );
        }
        setStage('review');
    }, []);

    return {
        stage,
        currentStepNumber,
        isConnected,
        isConfigured,
        connectionData,
        initWarning,
        selectedTables,
        mappingConfig,
        canNavigateTo,
        goToStage,
        nextStage,
        prevStage,
        setIsConnected,
        setIsConfigured,
        setSelectedTables,
        setMappingConfig,
        handleConnectionSuccess,
        hydrateFromSavedConfig,
    };
}
