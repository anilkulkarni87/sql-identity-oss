import { useState, useCallback } from 'react';
import { IDRConfig } from '../../types';

export type WizardStage = 'connect' | 'discover' | 'map' | 'rules' | 'survivorship' | 'review';

export interface WizardStageMeta {
    id: Exclude<WizardStage, 'connect' | 'discover'>;
    stepNumber: number;
    label: string;
}

export const STUDIO_TABS: WizardStageMeta[] = [
    { id: 'map', stepNumber: 1, label: 'Sources & Mappings' },
    { id: 'rules', stepNumber: 2, label: 'Matching Rules' },
    { id: 'survivorship', stepNumber: 3, label: 'Survivorship' },
    { id: 'review', stepNumber: 4, label: 'Run & Verify' },
];

// Preserved alias for backwards compatibility
export const WIZARD_STAGES = STUDIO_TABS;

export function useWizardMachine() {
    const [stage, setStage] = useState<WizardStage>('connect');
    const [isConnected, setIsConnected] = useState(false);
    const [isConfigured, setIsConfigured] = useState(false);
    const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);
    const [connectionData, setConnectionData] = useState<any>(null);
    const [initWarning, setInitWarning] = useState<string | null>(null);
    const [selectedTables, setSelectedTables] = useState<string[]>([]);
    const [mappingConfig, setMappingConfig] = useState<IDRConfig>({ sources: [] });

    const normalizedStage: WizardStage = stage === 'discover' ? 'map' : stage;
    const currentStepNumber = STUDIO_TABS.find(s => s.id === normalizedStage)?.stepNumber || 0;

    const canNavigateTo = useCallback((target: WizardStage): boolean => {
        const dest = target === 'discover' ? 'map' : target;
        if (dest === 'connect') return true;
        if (!isConnected) return false;
        if (dest === 'map') return true;
        if (isConfigured) return true;

        const hasSources = (mappingConfig.sources && mappingConfig.sources.length > 0) || selectedTables.length > 0;
        return hasSources;
    }, [isConnected, isConfigured, mappingConfig, selectedTables]);

    const goToStage = useCallback((target: WizardStage) => {
        const dest = target === 'discover' ? 'map' : target;
        if (canNavigateTo(dest)) {
            setStage(dest);
        }
    }, [canNavigateTo]);

    const nextStage = useCallback(() => {
        if (normalizedStage === 'connect') {
            if (isConnected) setStage('map');
            return;
        }
        const nextMeta = STUDIO_TABS.find(s => s.stepNumber === currentStepNumber + 1);
        if (nextMeta && canNavigateTo(nextMeta.id)) {
            setStage(nextMeta.id);
        }
    }, [normalizedStage, currentStepNumber, canNavigateTo, isConnected]);

    const prevStage = useCallback(() => {
        const prevMeta = STUDIO_TABS.find(s => s.stepNumber === currentStepNumber - 1);
        if (prevMeta) {
            setStage(prevMeta.id);
        } else if (normalizedStage === 'map') {
            setStage('connect');
        }
    }, [normalizedStage, currentStepNumber]);

    const handleConnectionSuccess = useCallback((data: any) => {
        setConnectionData(data);
        setIsConnected(true);
        setHasUnsavedChanges(false);
        if (data?.warning) {
            setInitWarning(data.warning);
        } else {
            setInitWarning(null);
        }
        setStage('map');
    }, []);

    const resetDraftForUnconfiguredWarehouse = useCallback((targetStage: WizardStage = 'map') => {
        setIsConnected(true);
        setIsConfigured(false);
        setHasUnsavedChanges(false);
        setSelectedTables([]);
        setMappingConfig({ sources: [] });
        setStage(targetStage === 'discover' ? 'map' : targetStage);
    }, []);

    const updateConfigDraft = useCallback((config: IDRConfig) => {
        setMappingConfig(config);
        setHasUnsavedChanges(true);
    }, []);

    const markConfigSaved = useCallback(() => {
        setIsConfigured(true);
        setHasUnsavedChanges(false);
    }, []);

    const hydrateFromSavedConfig = useCallback((config: IDRConfig, targetStage: WizardStage = 'map') => {
        setIsConfigured(true);
        setIsConnected(true);
        setHasUnsavedChanges(false);
        setMappingConfig(config);
        if (config.sources) {
            setSelectedTables(
                config.sources.map(s => s.table).filter((t): t is string => Boolean(t))
            );
        }
        setStage(targetStage === 'discover' ? 'map' : targetStage);
    }, []);

    return {
        stage: normalizedStage,
        currentStepNumber,
        isConnected,
        isConfigured,
        hasUnsavedChanges,
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
        setHasUnsavedChanges,
        setConnectionData,
        setSelectedTables,
        setMappingConfig: updateConfigDraft,
        markConfigSaved,
        handleConnectionSuccess,
        resetDraftForUnconfiguredWarehouse,
        hydrateFromSavedConfig,
    };
}
