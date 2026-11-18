import { useState, useEffect, useRef } from 'react';
import {
    GitGraph, ArrowUp, ArrowDown, Trash2, Plus, Wand2,
    ShieldAlert, Link2, AlertCircle
} from "lucide-react";
import { ExclusionRule, IDRConfig, LensRule, MatchingRule, OverrideRule } from '../../types';
import { api } from '../../api/client';
import { CustomSelect } from '../../components/CustomSelect';

const BUILTIN_CANONICALIZERS = [
    'EMAIL_STANDARD',
    'EMAIL_STRIP_PLUS',
    'PHONE_US_10',
    'PHONE_DIGITS',
    'ALPHANUMERIC_UPPER',
    'LOWERCASE',
    'UPPERCASE',
    'EXACT',
    'NONE',
];

interface StepRulesProps {
    config: IDRConfig;
    onChange?: (config: IDRConfig) => void;
    onNext: (config: IDRConfig) => void;
    onBack?: () => void;
}

interface FuzzyTemplate {
    id: string;
    label: string;
    sql_template: string;
    default_threshold: number;
    description: string;
}

export default function StepRules({ config, onChange, onNext, onBack }: StepRulesProps) {
    const combineFromConfig = (cfg: IDRConfig): MatchingRule[] =>
        [
            ...(cfg.rules || []),
            ...(cfg.fuzzy_rules || []).map(f => ({ ...f, type: 'FUZZY' })),
        ].sort((a, b) => (a.priority || 0) - (b.priority || 0));

    const [rules, setRules] = useState<MatchingRule[]>(() => combineFromConfig(config));
    const [exclusions, setExclusions] = useState<ExclusionRule[]>(config.exclusions || []);
    const [useDefaultExclusions, setUseDefaultExclusions] = useState<boolean>(!!config.use_default_exclusions);
    const [newExclType, setNewExclType] = useState<string>('ANY');
    const [newExclValue, setNewExclValue] = useState<string>('');
    const [newExclMatch, setNewExclMatch] = useState<'EXACT' | 'LIKE'>('EXACT');
    const [overrides, setOverrides] = useState<OverrideRule[]>(config.overrides || []);
    const [newOvType, setNewOvType] = useState<'FORCE_LINK' | 'DO_NOT_LINK' | 'ISOLATE'>('DO_NOT_LINK');
    const [newOvKeyA, setNewOvKeyA] = useState<string>('');
    const [newOvKeyB, setNewOvKeyB] = useState<string>('');
    const [newOvReason, setNewOvReason] = useState<string>('');
    const [lenses, setLenses] = useState<LensRule[]>(config.lenses || []);
    const [newLensId, setNewLensId] = useState<string>('');
    const [newLensInclude, setNewLensInclude] = useState<string[]>([]);
    const [newLensExclude, setNewLensExclude] = useState<string[]>([]);
    const [newLensSources, setNewLensSources] = useState<string[]>([]);
    const [newLensMaxGroup, setNewLensMaxGroup] = useState<string>('');
    const [openLensDropdown, setOpenLensDropdown] = useState<'include' | 'exclude' | 'sources' | null>(null);
    const [customSourceGlob, setCustomSourceGlob] = useState<string>('');
    const [availableTypes, setAvailableTypes] = useState<string[]>([]);
    const [availableAttributes, setAvailableAttributes] = useState<string[]>([]);
    const [fuzzyTemplates, setFuzzyTemplates] = useState<FuzzyTemplate[]>([]);
    const [validationError, setValidationError] = useState<string | null>(null);

    const latestConfigRef = useRef(config);
    latestConfigRef.current = config;

    const lastEmittedSigRef = useRef<string>('');

    const serializeRuleSlice = (
        r: MatchingRule[],
        f: MatchingRule[],
        e: ExclusionRule[],
        o: OverrideRule[],
        d: boolean,
        l: LensRule[]
    ) => JSON.stringify({ r, f, e, o, d, l });

    useEffect(() => {
        const incomingSig = serializeRuleSlice(
            config.rules || [],
            config.fuzzy_rules || [],
            config.exclusions || [],
            config.overrides || [],
            !!config.use_default_exclusions,
            config.lenses || []
        );
        if (incomingSig !== lastEmittedSigRef.current) {
            setRules(combineFromConfig(config));
            setExclusions(config.exclusions || []);
            setUseDefaultExclusions(!!config.use_default_exclusions);
            setOverrides(config.overrides || []);
            setLenses(config.lenses || []);
            lastEmittedSigRef.current = incomingSig;
        }

        const typeSet = new Set<string>();
        const attrSet = new Set<string>();
        (config.sources || []).forEach(src => {
            (src.identifiers || []).forEach(id => {
                if (id.is_match_key !== false && id.type) typeSet.add(id.type);
            });
            (src.attributes || []).forEach(attr => {
                if (attr.name) attrSet.add(attr.name);
            });
        });
        const typesArr = Array.from(typeSet).sort();
        const attrsArr = Array.from(attrSet).sort();
        setAvailableTypes(typesArr);
        setAvailableAttributes(attrsArr.length > 0 ? attrsArr : typesArr);
        if (typesArr.length > 0 && newExclType === 'ANY') {
            setNewExclType(typesArr[0]);
        }
    }, [config]);

    useEffect(() => {
        api.getFuzzyTemplates()
            .then(data => setFuzzyTemplates(data.templates || []))
            .catch(err => console.error("Failed to fetch templates", err));
    }, []);

    const resolveTemplateSql = (templateSql: string, colName: string) => {
        const safeCol = colName || availableAttributes[0] || availableTypes[0] || 'first_name';
        return templateSql.replace(/\{col\}/g, safeCol);
    };

    const emitChange = (
        nextRules: MatchingRule[],
        nextExcl: ExclusionRule[] = exclusions,
        nextOv: OverrideRule[] = overrides,
        nextDefExcl: boolean = useDefaultExclusions,
        nextLenses: LensRule[] = lenses
    ) => {
        const exact = nextRules.filter(r => r.type === 'EXACT');
        const fuzzy = nextRules.filter(r => r.type === 'FUZZY').map(r => ({
            ...r,
            score_expr: resolveTemplateSql(r.score_expr || '', r.fuzzy_attribute || availableAttributes[0] || 'first_name'),
        }));
        lastEmittedSigRef.current = serializeRuleSlice(exact, fuzzy, nextExcl, nextOv, nextDefExcl, nextLenses);
        setValidationError(null);
        onChange?.({
            ...latestConfigRef.current,
            rules: exact,
            fuzzy_rules: fuzzy,
            exclusions: nextExcl,
            overrides: nextOv,
            lenses: nextLenses,
            use_default_exclusions: nextDefExcl,
        });
    };

    const updatePriorities = (newRules: MatchingRule[]) => {
        const numbered = newRules.map((r, i) => ({ ...r, priority: i + 1 }));
        setRules(numbered);
        emitChange(numbered);
    };

    const clearAllFuzzyRules = () => {
        updatePriorities(rules.filter(r => r.type !== 'FUZZY'));
    };

    const moveRule = (idx: number, direction: 'up' | 'down') => {
        const newRules = [...rules];
        if (direction === 'up' && idx > 0) {
            [newRules[idx], newRules[idx - 1]] = [newRules[idx - 1], newRules[idx]];
        } else if (direction === 'down' && idx < newRules.length - 1) {
            [newRules[idx], newRules[idx + 1]] = [newRules[idx + 1], newRules[idx]];
        }
        updatePriorities(newRules);
    };

    const deleteRule = (idx: number) => {
        updatePriorities(rules.filter((_, i) => i !== idx));
    };

    const generateId = () => Math.floor(Math.random() * 1000000);

    const addExactRule = (type: string) => {
        const lowerType = type.toLowerCase();
        const defaultCanon = lowerType.includes('email')
            ? 'EMAIL_STANDARD'
            : lowerType.includes('phone')
            ? 'PHONE_US_10'
            : 'LOWERCASE';
        const newRule: MatchingRule = {
            id: generateId(),
            type: 'EXACT',
            match_keys: [type],
            priority: rules.length + 1,
            canonicalize: defaultCanon,
            max_group_size: 10000,
        };
        updatePriorities([...rules, newRule]);
    };

    const addFuzzyRule = () => {
        const defaultAttr = availableAttributes[0] || availableTypes[0] || 'first_name';
        const tmpl = fuzzyTemplates[0] || {
            sql_template: 'jaro_winkler_similarity(<a>.{col}, <b>.{col})',
            default_threshold: 0.85,
        };
        const newRule: MatchingRule = {
            id: generateId(),
            type: 'FUZZY',
            match_keys: [],
            priority: rules.length + 1,
            blocking_key: availableTypes[0] || '',
            fuzzy_attribute: defaultAttr,
            score_expr: resolveTemplateSql(tmpl.sql_template, defaultAttr),
            threshold: tmpl.default_threshold,
        };
        updatePriorities([...rules, newRule]);
    };

    const updateRuleField = (idx: number, field: keyof MatchingRule, value: any) => {
        const newRules = [...rules];
        const current = { ...newRules[idx], [field]: value };
        if (field === 'fuzzy_attribute' && current.score_expr) {
            current.score_expr = current.score_expr
                .replace(/\{col\}/g, value)
                .replace(/<a>\.[a-zA-Z0-9_]+/g, `<a>.${value}`)
                .replace(/<b>\.[a-zA-Z0-9_]+/g, `<b>.${value}`);
        }
        newRules[idx] = current;
        setRules(newRules);
        emitChange(newRules);
    };

    const applyTemplate = (idx: number, tmplId: string) => {
        const tmpl = fuzzyTemplates.find(t => t.id === tmplId);
        if (tmpl) {
            const newRules = [...rules];
            const col = newRules[idx].fuzzy_attribute || availableAttributes[0] || availableTypes[0] || 'first_name';
            newRules[idx] = {
                ...newRules[idx],
                fuzzy_attribute: col,
                score_expr: resolveTemplateSql(tmpl.sql_template, col),
                threshold: tmpl.default_threshold,
            };
            setRules(newRules);
            emitChange(newRules);
        }
    };

    const addExclusion = () => {
        const trimmed = newExclValue.trim();
        if (!trimmed) return;
        const type = newExclType || availableTypes[0] || 'ANY';
        let normalizedVal = trimmed.replace(/\*/g, '%');
        let resolvedMatch: 'EXACT' | 'LIKE' = newExclMatch;
        if (resolvedMatch === 'LIKE') {
            if (!normalizedVal.includes('%') && !normalizedVal.includes('_')) {
                normalizedVal = `%${normalizedVal}%`;
            }
        } else if (normalizedVal.includes('%')) {
            resolvedMatch = 'LIKE';
        }
        const nextExcl = [
            ...exclusions,
            { type, value: normalizedVal, match: resolvedMatch, reason: 'UI exclusion' },
        ];
        setExclusions(nextExcl);
        setNewExclValue('');
        emitChange(rules, nextExcl, overrides, useDefaultExclusions);
    };

    const removeExclusion = (idx: number) => {
        const nextExcl = exclusions.filter((_, i) => i !== idx);
        setExclusions(nextExcl);
        emitChange(rules, nextExcl, overrides, useDefaultExclusions);
    };

    const toggleDefaultExclusions = (checked: boolean) => {
        setUseDefaultExclusions(checked);
        emitChange(rules, exclusions, overrides, checked);
    };

    const addOverride = () => {
        const ka = newOvKeyA.trim();
        const kb = newOvKeyB.trim();
        if (!ka) return;
        if (newOvType !== 'ISOLATE' && !kb) return;
        const nextOv = [
            ...overrides,
            {
                override_type: newOvType,
                entity_key_a: ka,
                entity_key_b: newOvType === 'ISOLATE' ? '__ISOLATE__' : kb,
                reason: newOvReason.trim() || 'UI stewardship override',
            },
        ];
        setOverrides(nextOv);
        setNewOvKeyA('');
        setNewOvKeyB('');
        setNewOvReason('');
        emitChange(rules, exclusions, nextOv, useDefaultExclusions);
    };

    const removeOverride = (idx: number) => {
        const nextOv = overrides.filter((_, i) => i !== idx);
        setOverrides(nextOv);
        emitChange(rules, exclusions, nextOv, useDefaultExclusions);
    };

    const handleNext = () => {
        if (rules.length === 0) {
            setValidationError("Define at least one deterministic or fuzzy matching rule to continue.");
            return;
        }
        const exact = rules.filter(r => r.type === 'EXACT');
        const fuzzy = rules.filter(r => r.type === 'FUZZY').map(r => ({
            ...r,
            score_expr: resolveTemplateSql(r.score_expr || '', r.fuzzy_attribute || availableAttributes[0] || 'first_name'),
        }));

        onNext({
            ...config,
            rules: exact,
            fuzzy_rules: fuzzy,
            exclusions,
            overrides,
            lenses,
            use_default_exclusions: useDefaultExclusions,
        });
    };

    const addLens = () => {
        const id = newLensId.trim().replace(/\s+/g, '_');
        if (!id) return;
        const parsedMax = newLensMaxGroup.trim() ? parseInt(newLensMaxGroup, 10) : undefined;
        const nextLenses: LensRule[] = [
            ...lenses.filter(l => l.id !== id),
            {
                id,
                include_types: newLensInclude.map(t => t.toUpperCase()),
                exclude_types: newLensExclude.map(t => t.toUpperCase()),
                source_filter: newLensSources,
                ...(parsedMax && parsedMax > 0 ? { max_group_size: parsedMax } : {}),
            },
        ];
        setLenses(nextLenses);
        setNewLensId('');
        setNewLensInclude([]);
        setNewLensExclude([]);
        setNewLensSources([]);
        setNewLensMaxGroup('');
        setOpenLensDropdown(null);
        emitChange(rules, exclusions, overrides, useDefaultExclusions, nextLenses);
    };

    const toggleLensSelection = (
        selected: string[],
        setSelected: (next: string[]) => void,
        value: string
    ) => {
        setSelected(
            selected.includes(value)
                ? selected.filter(v => v !== value)
                : [...selected, value]
        );
    };

    const availableSourceIds = Array.from(
        new Set((config.sources || []).map(s => s.id).filter(Boolean))
    ).sort();

    const lensTypeOptions = Array.from(
        new Set([
            ...availableTypes.map(t => t.toUpperCase()),
            ...rules.flatMap(r => [
                ...(r.match_keys || []),
                ...((r as unknown as { match_key?: string }).match_key
                    ? [(r as unknown as { match_key?: string }).match_key!]
                    : []),
            ]).map(t => String(t).trim().toUpperCase()).filter(Boolean),
            ...(config.rules || []).flatMap(r => [
                ...(r.match_keys || []),
                ...((r as unknown as { match_key?: string }).match_key
                    ? [(r as unknown as { match_key?: string }).match_key!]
                    : []),
            ]).map(t => String(t).trim().toUpperCase()).filter(Boolean),
            ...(config.sources || []).flatMap(s =>
                (s.identifiers || []).map(i => String(i.type || '').trim().toUpperCase())
            ).filter(Boolean),
        ])
    ).sort();

    const removeLens = (idx: number) => {
        const nextLenses = lenses.filter((_, i) => i !== idx);
        setLenses(nextLenses);
        emitChange(rules, exclusions, overrides, useDefaultExclusions, nextLenses);
    };

    const blockingKeyOptions = Array.from(new Set([...availableTypes, ...availableAttributes])).map(t => ({
        value: t,
        label: t,
    }));

    return (
        <div className="space-y-6 flex flex-col">
            <div>
                <h2 className="text-xl font-bold tracking-tight text-white">Matching Rules &amp; Guardrails</h2>
                <p className="text-sm text-slate-400">
                    Define deterministic Exact rules, probabilistic Fuzzy matching rules, toxic value blocklists, and steward overrides.
                </p>
            </div>

            {validationError && (
                <div className="bg-amber-950/50 border border-amber-500/40 rounded-xl p-3 text-xs text-amber-200 flex items-center gap-2">
                    <AlertCircle className="w-4 h-4 text-amber-400 shrink-0" />
                    <span>{validationError}</span>
                </div>
            )}

            <div className="space-y-3">
                {rules.map((rule, idx) => {
                    const matchedTemplate = fuzzyTemplates.find(
                        t => resolveTemplateSql(t.sql_template, rule.fuzzy_attribute || availableAttributes[0] || 'first_name') === rule.score_expr
                    );
                    return (
                        <div key={rule.id} className="flex gap-4 p-4 bg-slate-900/75 border border-slate-800 rounded-xl shadow-sm">
                            <div className="flex flex-col items-center gap-1 pt-1">
                                <button
                                    type="button"
                                    onClick={() => moveRule(idx, 'up')}
                                    disabled={idx === 0}
                                    aria-label={`Move rule ${rule.priority} up`}
                                    className="p-1 hover:bg-slate-800 rounded text-slate-400 disabled:opacity-30"
                                >
                                    <ArrowUp className="w-4 h-4" />
                                </button>
                                <span className="text-xs font-mono font-bold text-slate-500">{rule.priority}</span>
                                <button
                                    type="button"
                                    onClick={() => moveRule(idx, 'down')}
                                    disabled={idx === rules.length - 1}
                                    aria-label={`Move rule ${rule.priority} down`}
                                    className="p-1 hover:bg-slate-800 rounded text-slate-400 disabled:opacity-30"
                                >
                                    <ArrowDown className="w-4 h-4" />
                                </button>
                            </div>

                            <div className="flex-1 space-y-3 min-w-0">
                                <div className="flex items-center gap-2.5">
                                    <span className={`text-[11px] font-semibold px-2 py-0.5 rounded-full border ${
                                        rule.type === 'EXACT'
                                            ? 'bg-blue-950/60 text-blue-300 border-blue-800/70'
                                            : 'bg-purple-950/60 text-purple-300 border-purple-800/70'
                                    }`}>
                                        {rule.type}
                                    </span>
                                    {rule.type === 'EXACT' ? (
                                        <div className="flex items-center gap-2">
                                            <GitGraph className="w-4 h-4 text-blue-400" />
                                            <span className="font-semibold text-sm text-slate-100">
                                                Match by {rule.match_keys.join(' + ')}
                                            </span>
                                        </div>
                                    ) : (
                                        <div className="flex items-center gap-2">
                                            <Wand2 className="w-4 h-4 text-purple-400" />
                                            <span className="font-semibold text-sm text-slate-100">Fuzzy Similarity Rule</span>
                                        </div>
                                    )}
                                </div>

                                {rule.type === 'EXACT' ? (
                                    <div className="space-y-2.5">
                                        <div className="flex flex-wrap items-center gap-2.5">
                                            <label className="text-xs text-slate-400">Normalization:</label>
                                            <CustomSelect
                                                value={
                                                    BUILTIN_CANONICALIZERS.includes((rule.canonicalize || 'LOWERCASE').toUpperCase())
                                                        ? (rule.canonicalize || 'LOWERCASE').toUpperCase()
                                                        : 'CUSTOM_SQL'
                                                }
                                                onChange={(val) => {
                                                    if (val === 'CUSTOM_SQL') {
                                                        updateRuleField(idx, 'canonicalize', 'LOWER(TRIM({val}))');
                                                    } else {
                                                        updateRuleField(idx, 'canonicalize', val);
                                                    }
                                                }}
                                                options={[
                                                    { value: 'EMAIL_STANDARD', label: 'Email Standard (Trim + Lowercase, keeps +tag)' },
                                                    { value: 'EMAIL_STRIP_PLUS', label: 'Email Strip +Tag (user+tag@domain -> user@domain)' },
                                                    { value: 'PHONE_US_10', label: 'Phone US 10-Digit (Strips +1 & punctuation)' },
                                                    { value: 'PHONE_DIGITS', label: 'Phone Digits Only (Strips non-digits)' },
                                                    { value: 'ALPHANUMERIC_UPPER', label: 'Alphanumeric Upper (Strips spaces/hyphens)' },
                                                    { value: 'LOWERCASE', label: 'Lowercase' },
                                                    { value: 'UPPERCASE', label: 'Uppercase' },
                                                    { value: 'EXACT', label: 'Exact / None' },
                                                    { value: 'CUSTOM_SQL', label: 'Custom Warehouse SQL ({val})...' },
                                                ]}
                                                buttonClassName="text-xs py-1.5 px-2.5"
                                                menuClassName="left-0 right-auto min-w-[330px]"
                                            />
                                        </div>
                                        {!BUILTIN_CANONICALIZERS.includes((rule.canonicalize || 'LOWERCASE').toUpperCase()) && (
                                            <div className="flex items-center gap-2">
                                                <span className="text-[11px] font-mono text-blue-400">SQL Expression:</span>
                                                <input
                                                    type="text"
                                                    value={rule.canonicalize || ''}
                                                    onChange={(e) => updateRuleField(idx, 'canonicalize', e.target.value)}
                                                    placeholder="e.g. LPAD(LTRIM(TRIM({val}), '0'), 8, '0')"
                                                    className="flex-1 bg-slate-950 border border-blue-700/60 text-xs font-mono text-slate-200 rounded-lg px-2.5 py-1.5 focus:outline-none focus:border-blue-400"
                                                />
                                            </div>
                                        )}
                                        <div className="flex flex-wrap items-center gap-2 pt-2 border-t border-slate-800/80">
                                            <label className="text-xs text-slate-400">Max Group Size:</label>
                                            <input
                                                type="number"
                                                min="2"
                                                value={rule.max_group_size ?? 10000}
                                                onChange={(e) => updateRuleField(idx, 'max_group_size', parseInt(e.target.value, 10) || 10000)}
                                                className="w-24 bg-slate-950 border border-slate-700 text-xs font-mono text-slate-200 rounded-md px-2 py-1 focus:outline-none focus:border-blue-500"
                                            />
                                            <span className="text-[11px] text-slate-500">
                                                Skips identifier values linked to more records than this threshold to avoid mega-clusters.
                                            </span>
                                        </div>
                                    </div>
                                ) : (
                                    <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 bg-slate-950/60 p-3.5 rounded-xl border border-slate-800">
                                        <div>
                                            <label className="text-[11px] text-slate-400 block mb-1">Blocking Key</label>
                                            <CustomSelect
                                                value={rule.blocking_key || ''}
                                                onChange={(val) => updateRuleField(idx, 'blocking_key', val)}
                                                options={blockingKeyOptions}
                                                className="w-full"
                                                buttonClassName="text-xs py-1.5 bg-slate-900"
                                                menuClassName="left-0 right-auto min-w-[200px]"
                                            />
                                        </div>
                                        <div>
                                            <label className="text-[11px] text-slate-400 block mb-1">Compare Attribute</label>
                                            <CustomSelect
                                                value={rule.fuzzy_attribute || availableAttributes[0] || ''}
                                                onChange={(val) => updateRuleField(idx, 'fuzzy_attribute', val)}
                                                options={availableAttributes.map(a => ({ value: a, label: a }))}
                                                className="w-full"
                                                buttonClassName="text-xs py-1.5 bg-slate-900"
                                                menuClassName="left-0 right-auto min-w-[200px]"
                                            />
                                        </div>
                                        <div>
                                            <label className="text-[11px] text-slate-400 block mb-1">Similarity Method</label>
                                            <CustomSelect
                                                value={matchedTemplate?.id || ''}
                                                onChange={(val) => applyTemplate(idx, val)}
                                                placeholder="Select template..."
                                                options={fuzzyTemplates.map(t => ({ value: t.id, label: t.label }))}
                                                className="w-full"
                                                buttonClassName="text-xs py-1.5 bg-slate-900"
                                                menuClassName="right-0 left-auto min-w-[260px]"
                                            />
                                        </div>
                                        <div className="sm:col-span-3 pt-1">
                                            <div className="flex justify-between text-xs text-slate-400 mb-1">
                                                <span>Similarity Threshold</span>
                                                <span className="text-purple-300 font-mono font-semibold">{rule.threshold}</span>
                                            </div>
                                            <input
                                                type="range"
                                                min="0.5"
                                                max="1.0"
                                                step="0.01"
                                                value={rule.threshold || 0.85}
                                                onChange={(e) => updateRuleField(idx, 'threshold', parseFloat(e.target.value))}
                                                className="w-full accent-purple-500"
                                            />
                                        </div>
                                        {rule.score_expr && (
                                            <div className="sm:col-span-3">
                                                <span className="text-[11px] font-mono text-slate-500 block truncate" title={rule.score_expr}>
                                                    SQL: {rule.score_expr}
                                                </span>
                                            </div>
                                        )}
                                    </div>
                                )}
                            </div>
                            <button
                                type="button"
                                onClick={() => deleteRule(idx)}
                                aria-label={`Delete rule ${rule.priority}`}
                                className="p-2 h-fit hover:bg-rose-950/40 text-slate-500 hover:text-rose-400 rounded-lg transition-colors"
                            >
                                <Trash2 className="w-4 h-4" />
                            </button>
                        </div>
                    );
                })}

                {rules.length === 0 && (
                    <div className="text-center p-8 border border-dashed border-slate-700 rounded-xl text-xs text-slate-500">
                        No matching rules defined yet. Add an Exact or Fuzzy rule below to get started.
                    </div>
                )}
            </div>

            {/* Add Rule Toolbar */}
            <div className="pt-4 border-t border-slate-800 space-y-2.5">
                <label className="text-xs font-semibold uppercase tracking-wider text-slate-400 block">Add Matching Rule</label>
                <div className="flex flex-wrap items-center gap-2">
                    {availableTypes.map(type => (
                        <button
                            key={type}
                            type="button"
                            onClick={() => addExactRule(type)}
                            className="flex items-center gap-1.5 px-3 py-1.5 bg-slate-900 border border-slate-700 hover:border-blue-500 hover:text-blue-300 rounded-lg text-xs font-medium text-slate-300 transition-all"
                        >
                            <Plus className="w-3.5 h-3.5" /> Exact: {type}
                        </button>
                    ))}
                    <div className="w-px bg-slate-800 mx-1 h-6" />
                    <button
                        type="button"
                        onClick={addFuzzyRule}
                        className="flex items-center gap-1.5 px-3 py-1.5 bg-purple-950/40 border border-purple-800/70 hover:border-purple-500 hover:text-purple-200 rounded-lg text-xs font-medium text-purple-300 transition-all"
                    >
                        <Wand2 className="w-3.5 h-3.5" /> Add Fuzzy Rule
                    </button>
                    {rules.some(r => r.type === 'FUZZY') && (
                        <button
                            type="button"
                            onClick={clearAllFuzzyRules}
                            className="flex items-center gap-1.5 px-3 py-1.5 bg-rose-950/30 border border-rose-800/60 hover:border-rose-500 hover:text-rose-200 rounded-lg text-xs text-rose-400 transition-all"
                        >
                            <Trash2 className="w-3.5 h-3.5" /> Remove All Fuzzy
                        </button>
                    )}
                </div>
            </div>

            {/* Governance Section: Exclusions & Manual Overrides */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 pt-2">
                {/* Exclusions */}
                <div className="bg-slate-900/75 border border-slate-800 rounded-xl p-4 space-y-3">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                        <div className="flex items-center gap-2">
                            <ShieldAlert className="w-4 h-4 text-amber-400" />
                            <h4 className="font-semibold text-sm text-slate-200">Identifier Exclusions</h4>
                        </div>
                        <label className="inline-flex items-center gap-2 text-[11px] text-amber-300 cursor-pointer bg-amber-950/40 border border-amber-800/60 px-2.5 py-1 rounded-md">
                            <input
                                type="checkbox"
                                checked={useDefaultExclusions}
                                onChange={(e) => toggleDefaultExclusions(e.target.checked)}
                                className="accent-amber-500 rounded"
                            />
                            <span>Default Toxic Pack (33 rules)</span>
                        </label>
                    </div>
                    <p className="text-xs text-slate-400">
                        Prevent placeholder values (<code>test@test.com</code>, <code>0000000000</code>) from chaining unrelated entities.
                    </p>
                    <div className="flex flex-wrap gap-2">
                        <CustomSelect
                            value={newExclType || availableTypes[0] || 'ANY'}
                            onChange={setNewExclType}
                            options={[
                                { value: 'ANY', label: 'ANY (All Types)' },
                                ...availableTypes.map(t => ({ value: t, label: t })),
                            ]}
                            buttonClassName="text-xs py-1.5 bg-slate-950"
                            menuClassName="left-0 right-auto min-w-[160px]"
                        />
                        <CustomSelect
                            value={newExclMatch}
                            onChange={(val) => setNewExclMatch(val as 'EXACT' | 'LIKE')}
                            options={[
                                { value: 'EXACT', label: 'EXACT' },
                                { value: 'LIKE', label: 'LIKE (%)' },
                            ]}
                            buttonClassName="text-xs py-1.5 bg-slate-950"
                            menuClassName="left-0 right-auto min-w-[140px]"
                        />
                        <input
                            type="text"
                            value={newExclValue}
                            onChange={(e) => setNewExclValue(e.target.value)}
                            placeholder="Value or pattern"
                            className="flex-1 min-w-[140px] bg-slate-950 border border-slate-700 text-xs text-slate-200 rounded-lg px-3 py-1.5 focus:outline-none focus:border-amber-500"
                        />
                        <button
                            type="button"
                            onClick={addExclusion}
                            className="px-3 py-1.5 bg-amber-600/25 border border-amber-600/60 hover:bg-amber-600/40 text-amber-200 rounded-lg text-xs font-medium"
                        >
                            + Add
                        </button>
                    </div>
                    {exclusions.length > 0 && (
                        <div className="flex flex-wrap gap-1.5 pt-1">
                            {exclusions.map((ex, i) => (
                                <span
                                    key={`${ex.type}-${ex.value}-${i}`}
                                    className="inline-flex items-center gap-1.5 px-2.5 py-1 bg-slate-950 border border-slate-700 rounded-full text-xs text-slate-300"
                                >
                                    <span className="text-amber-400 font-semibold">{ex.type}</span>
                                    <span className="text-slate-500">({ex.match || ex.match_type || 'EXACT'})</span>
                                    <span className="font-mono">{ex.value}</span>
                                    <button
                                        type="button"
                                        onClick={() => removeExclusion(i)}
                                        aria-label={`Remove exclusion ${ex.value}`}
                                        className="text-slate-500 hover:text-rose-400 ml-0.5"
                                    >
                                        ×
                                    </button>
                                </span>
                            ))}
                        </div>
                    )}
                </div>

                {/* Manual Stewardship Overrides */}
                <div className="bg-slate-900/75 border border-slate-800 rounded-xl p-4 space-y-3">
                    <div className="flex items-center gap-2">
                        <Link2 className="w-4 h-4 text-emerald-400" />
                        <h4 className="font-semibold text-sm text-slate-200">Stewardship Overrides</h4>
                    </div>
                    <p className="text-xs text-slate-400">
                        Force-merge (<code>FORCE_LINK</code>), block false merges (<code>DO_NOT_LINK</code>), or quarantine (<code>ISOLATE</code>).
                    </p>
                    <div className="flex flex-wrap gap-2">
                        <CustomSelect
                            value={newOvType}
                            onChange={(val) => setNewOvType(val as 'FORCE_LINK' | 'DO_NOT_LINK' | 'ISOLATE')}
                            options={[
                                { value: 'DO_NOT_LINK', label: 'DO_NOT_LINK (Block Pair)' },
                                { value: 'FORCE_LINK', label: 'FORCE_LINK (Merge Pair)' },
                                { value: 'ISOLATE', label: 'ISOLATE (Quarantine Entity)' },
                            ]}
                            buttonClassName="text-xs py-1.5 bg-slate-950"
                            menuClassName="left-0 right-auto min-w-[210px]"
                        />
                        <input
                            type="text"
                            value={newOvKeyA}
                            onChange={(e) => setNewOvKeyA(e.target.value)}
                            placeholder="Entity Key A (crm:101)"
                            className="min-w-[130px] flex-1 bg-slate-950 border border-slate-700 text-xs text-slate-200 rounded-lg px-2.5 py-1.5 font-mono"
                        />
                        {newOvType !== 'ISOLATE' && (
                            <input
                                type="text"
                                value={newOvKeyB}
                                onChange={(e) => setNewOvKeyB(e.target.value)}
                                placeholder="Entity Key B (web:502)"
                                className="min-w-[130px] flex-1 bg-slate-950 border border-slate-700 text-xs text-slate-200 rounded-lg px-2.5 py-1.5 font-mono"
                            />
                        )}
                        <input
                            type="text"
                            value={newOvReason}
                            onChange={(e) => setNewOvReason(e.target.value)}
                            placeholder="Reason (optional)"
                            className="min-w-[120px] flex-1 bg-slate-950 border border-slate-700 text-xs text-slate-200 rounded-lg px-2.5 py-1.5"
                        />
                        <button
                            type="button"
                            onClick={addOverride}
                            className="px-3 py-1.5 bg-emerald-600/25 border border-emerald-600/60 hover:bg-emerald-600/40 text-emerald-200 rounded-lg text-xs font-medium"
                        >
                            + Add
                        </button>
                    </div>
                    {overrides.length > 0 && (
                        <div className="flex flex-wrap gap-1.5 pt-1">
                            {overrides.map((ov, i) => (
                                <span
                                    key={`${ov.override_type}-${ov.entity_key_a}-${ov.entity_key_b}-${i}`}
                                    className="inline-flex items-center gap-1.5 px-2.5 py-1 bg-slate-950 border border-slate-700 rounded-full text-xs text-slate-300"
                                >
                                    <span className="text-emerald-400 font-semibold">{ov.override_type}</span>
                                    <span className="font-mono">{ov.entity_key_a}</span>
                                    {ov.override_type !== 'ISOLATE' && ov.entity_key_b && (
                                        <>
                                            <span className="text-slate-500">↔</span>
                                            <span className="font-mono">{ov.entity_key_b}</span>
                                        </>
                                    )}
                                    <button
                                        type="button"
                                        onClick={() => removeOverride(i)}
                                        aria-label={`Remove override ${ov.entity_key_a || i + 1}`}
                                        className="text-slate-500 hover:text-rose-400 ml-0.5"
                                    >
                                        ×
                                    </button>
                                </span>
                            ))}
                        </div>
                    )}
                </div>
            </div>

            {/* Multi-Lens Identity Views */}
            <div className="bg-slate-900/50 border border-slate-800 rounded-xl p-4 space-y-3">
                <div className="flex items-center justify-between">
                    <div>
                        <h3 className="text-sm font-semibold text-white">Multi-Lens Identity Views (Optional)</h3>
                        <p className="text-xs text-slate-400">
                            Materialize multiple policy-bounded identity graphs (e.g., strict_person vs household) from one scan.
                        </p>
                    </div>
                </div>
                <div className="grid grid-cols-1 sm:grid-cols-5 gap-2">
                    <input
                        type="text"
                        value={newLensId}
                        onChange={(e) => setNewLensId(e.target.value)}
                        placeholder="Lens ID (e.g. strict_person)"
                        className="bg-slate-950 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs text-white"
                    />

                    {/* Include Types Dropdown */}
                    <div className="relative">
                        <button
                            type="button"
                            onClick={() => setOpenLensDropdown(openLensDropdown === 'include' ? null : 'include')}
                            className="w-full flex items-center justify-between bg-slate-950 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs text-left text-white hover:border-slate-500"
                        >
                            <span className="truncate">
                                {newLensInclude.length > 0
                                    ? `Include: ${newLensInclude.join(', ')}`
                                    : 'Include types (all)'}
                            </span>
                            <span className="text-slate-400 ml-1">▾</span>
                        </button>
                        {openLensDropdown === 'include' && (
                            <div className="absolute z-30 mt-1 w-56 bg-slate-900 border border-slate-700 rounded-lg shadow-xl p-2 space-y-1 max-h-52 overflow-y-auto">
                                {lensTypeOptions.length === 0 ? (
                                    <div className="text-[11px] text-slate-400 px-1 py-1">No identifier types found</div>
                                ) : (
                                    lensTypeOptions.map(t => (
                                        <label
                                            key={t}
                                            className="flex items-center gap-2 px-2 py-1 rounded hover:bg-slate-800 cursor-pointer text-xs text-slate-200"
                                        >
                                            <input
                                                type="checkbox"
                                                checked={newLensInclude.includes(t)}
                                                onChange={() => toggleLensSelection(newLensInclude, setNewLensInclude, t)}
                                                className="rounded border-slate-600 bg-slate-950 text-blue-500"
                                            />
                                            <span>{t}</span>
                                        </label>
                                    ))
                                )}
                                {newLensInclude.length > 0 && (
                                    <button
                                        type="button"
                                        onClick={() => setNewLensInclude([])}
                                        className="w-full text-left px-2 pt-1 text-[11px] text-slate-400 hover:text-white border-t border-slate-800"
                                    >
                                        Clear selection
                                    </button>
                                )}
                            </div>
                        )}
                    </div>

                    {/* Exclude Types Dropdown */}
                    <div className="relative">
                        <button
                            type="button"
                            onClick={() => setOpenLensDropdown(openLensDropdown === 'exclude' ? null : 'exclude')}
                            className="w-full flex items-center justify-between bg-slate-950 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs text-left text-white hover:border-slate-500"
                        >
                            <span className="truncate">
                                {newLensExclude.length > 0
                                    ? `Exclude: ${newLensExclude.join(', ')}`
                                    : 'Exclude types (none)'}
                            </span>
                            <span className="text-slate-400 ml-1">▾</span>
                        </button>
                        {openLensDropdown === 'exclude' && (
                            <div className="absolute z-30 mt-1 w-56 bg-slate-900 border border-slate-700 rounded-lg shadow-xl p-2 space-y-1 max-h-52 overflow-y-auto">
                                {lensTypeOptions.length === 0 ? (
                                    <div className="text-[11px] text-slate-400 px-1 py-1">No identifier types found</div>
                                ) : (
                                    lensTypeOptions.map(t => (
                                        <label
                                            key={t}
                                            className="flex items-center gap-2 px-2 py-1 rounded hover:bg-slate-800 cursor-pointer text-xs text-slate-200"
                                        >
                                            <input
                                                type="checkbox"
                                                checked={newLensExclude.includes(t)}
                                                onChange={() => toggleLensSelection(newLensExclude, setNewLensExclude, t)}
                                                className="rounded border-slate-600 bg-slate-950 text-blue-500"
                                            />
                                            <span>{t}</span>
                                        </label>
                                    ))
                                )}
                                {newLensExclude.length > 0 && (
                                    <button
                                        type="button"
                                        onClick={() => setNewLensExclude([])}
                                        className="w-full text-left px-2 pt-1 text-[11px] text-slate-400 hover:text-white border-t border-slate-800"
                                    >
                                        Clear selection
                                    </button>
                                )}
                            </div>
                        )}
                    </div>

                    {/* Source Filter Dropdown */}
                    <div className="relative">
                        <button
                            type="button"
                            onClick={() => setOpenLensDropdown(openLensDropdown === 'sources' ? null : 'sources')}
                            className="w-full flex items-center justify-between bg-slate-950 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs text-left text-white hover:border-slate-500"
                        >
                            <span className="truncate">
                                {newLensSources.length > 0
                                    ? `Sources: ${newLensSources.join(', ')}`
                                    : 'Source filter (all)'}
                            </span>
                            <span className="text-slate-400 ml-1">▾</span>
                        </button>
                        {openLensDropdown === 'sources' && (
                            <div className="absolute z-30 mt-1 w-64 bg-slate-900 border border-slate-700 rounded-lg shadow-xl p-2 space-y-1 max-h-60 overflow-y-auto">
                                {availableSourceIds.map(sid => (
                                    <label
                                        key={sid}
                                        className="flex items-center gap-2 px-2 py-1 rounded hover:bg-slate-800 cursor-pointer text-xs text-slate-200"
                                    >
                                        <input
                                            type="checkbox"
                                            checked={newLensSources.includes(sid)}
                                            onChange={() => toggleLensSelection(newLensSources, setNewLensSources, sid)}
                                            className="rounded border-slate-600 bg-slate-950 text-blue-500"
                                        />
                                        <span className="truncate">{sid}</span>
                                    </label>
                                ))}
                                <div className="pt-1.5 mt-1 border-t border-slate-800 flex gap-1">
                                    <input
                                        type="text"
                                        value={customSourceGlob}
                                        onChange={(e) => setCustomSourceGlob(e.target.value)}
                                        placeholder="Custom glob (e.g. crm*)"
                                        className="flex-1 bg-slate-950 border border-slate-700 rounded px-2 py-1 text-[11px] text-white"
                                    />
                                    <button
                                        type="button"
                                        onClick={() => {
                                            const g = customSourceGlob.trim();
                                            if (g && !newLensSources.includes(g)) {
                                                setNewLensSources([...newLensSources, g]);
                                            }
                                            setCustomSourceGlob('');
                                        }}
                                        className="px-2 py-1 bg-slate-800 hover:bg-slate-700 text-[11px] text-blue-300 rounded"
                                    >
                                        Add
                                    </button>
                                </div>
                                {newLensSources.length > 0 && (
                                    <button
                                        type="button"
                                        onClick={() => setNewLensSources([])}
                                        className="w-full text-left px-2 pt-1 text-[11px] text-slate-400 hover:text-white border-t border-slate-800"
                                    >
                                        Clear selection
                                    </button>
                                )}
                            </div>
                        )}
                    </div>

                    <div className="flex gap-2">
                        <input
                            type="number"
                            min="2"
                            value={newLensMaxGroup}
                            onChange={(e) => setNewLensMaxGroup(e.target.value)}
                            placeholder="Max size"
                            className="w-24 bg-slate-950 border border-slate-700 rounded-lg px-2 py-1.5 text-xs text-white"
                        />
                        <button
                            type="button"
                            onClick={addLens}
                            className="px-3 py-1.5 bg-blue-600 hover:bg-blue-700 text-white rounded-lg text-xs font-medium whitespace-nowrap"
                        >
                            + Add Lens
                        </button>
                    </div>
                </div>
                {lenses.length > 0 && (
                    <div className="space-y-1.5">
                        {lenses.map((l, idx) => (
                            <div
                                key={`${l.id}-${idx}`}
                                className="flex items-center justify-between bg-slate-950/70 border border-slate-800 rounded-lg px-3 py-2 text-xs"
                            >
                                <div className="flex flex-wrap items-center gap-2 text-slate-300">
                                    <span className="font-mono font-semibold text-blue-300">{l.id}</span>
                                    {l.include_types && l.include_types.length > 0 && (
                                        <span className="text-slate-400">include: {l.include_types.join(', ')}</span>
                                    )}
                                    {l.exclude_types && l.exclude_types.length > 0 && (
                                        <span className="text-slate-400">exclude: {l.exclude_types.join(', ')}</span>
                                    )}
                                    {l.source_filter && l.source_filter.length > 0 && (
                                        <span className="text-slate-400">sources: {l.source_filter.join(', ')}</span>
                                    )}
                                    {l.max_group_size && (
                                        <span className="text-amber-400">max_size: {l.max_group_size}</span>
                                    )}
                                </div>
                                <button
                                    type="button"
                                    onClick={() => removeLens(idx)}
                                    className="text-rose-400 hover:text-rose-300 text-xs"
                                >
                                    Remove
                                </button>
                            </div>
                        ))}
                    </div>
                )}
            </div>

            <div className="flex justify-between pt-3 border-t border-slate-800/80">
                {onBack ? (
                    <button
                        type="button"
                        onClick={onBack}
                        className="px-4 py-2 border border-slate-700 text-slate-300 rounded-lg text-xs font-medium hover:bg-slate-800"
                    >
                        ← Back to Sources
                    </button>
                ) : <div />}
                <button
                    type="button"
                    onClick={handleNext}
                    className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg text-xs font-semibold transition-colors"
                >
                    Continue to Survivorship →
                </button>
            </div>
        </div>
    );
}
