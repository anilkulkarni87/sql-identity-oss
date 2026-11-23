import AIConfigPolicy from '../../components/AIConfigPolicy'
import AIProposalReview from '../../components/AIProposalReview'
import { defaultAIContext, localGoalObjectives } from '../../types'
import { useEffect, useRef, useState } from 'react'
import {
    GitGraph, ArrowUp, ArrowDown, Trash2, Wand2,
    ShieldAlert, Link2, AlertCircle, Bot, Loader2
} from 'lucide-react'
import { ExclusionRule, IDRConfig, LensRule, MatchingRule, OverrideRule } from '../../types'
import { api } from '../../api/client'
import { AiProgressStepper, AiProgressEvent } from '../../components/AiProgressStepper'
import { CustomSelect } from '../../components/CustomSelect'
import { useToast } from '../../components/Toast'

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
]

interface StepRulesProps {
    config: IDRConfig
    onChange?: (config: IDRConfig) => void
    onNext: (config: IDRConfig) => void
    onBack?: () => void
}

interface FuzzyTemplate {
    id: string
    label: string
    sql_template: string
    default_threshold: number
    description: string
}

type RulesSubTab = 'match' | 'lenses' | 'exclusions' | 'overrides'

export default function StepRules({ config, onChange, onNext, onBack }: StepRulesProps) {
    const { toast } = useToast()
    const [activeSubTab, setActiveSubTab] = useState<RulesSubTab>('match')

    const combineFromConfig = (cfg: IDRConfig): MatchingRule[] =>
        [
            ...(cfg.rules || []),
            ...(cfg.fuzzy_rules || []).map((f) => ({ ...f, type: 'FUZZY' })),
        ].sort((a, b) => (a.priority || 0) - (b.priority || 0))

    const [rules, setRules] = useState<MatchingRule[]>(() => combineFromConfig(config))
    const [confirmClearFuzzy, setConfirmClearFuzzy] = useState(false)
    const [exclusions, setExclusions] = useState<ExclusionRule[]>(config.exclusions || [])
    const [useDefaultExclusions, setUseDefaultExclusions] = useState<boolean>(!!config.use_default_exclusions)
    const [newExclType, setNewExclType] = useState<string>('ANY')
    const [newExclValue, setNewExclValue] = useState<string>('')
    const [newExclMatch, setNewExclMatch] = useState<'EXACT' | 'LIKE'>('EXACT')
    const [overrides, setOverrides] = useState<OverrideRule[]>(config.overrides || [])
    const [newOvType, setNewOvType] = useState<'FORCE_LINK' | 'DO_NOT_LINK' | 'ISOLATE'>('DO_NOT_LINK')
    const [newOvKeyA, setNewOvKeyA] = useState<string>('')
    const [newOvKeyB, setNewOvKeyB] = useState<string>('')
    const [newOvReason, setNewOvReason] = useState<string>('')
    const [lenses, setLenses] = useState<LensRule[]>(config.lenses || [])
    const [newLensId, setNewLensId] = useState<string>('')
    const [newLensInclude, setNewLensInclude] = useState<string[]>([])
    const [newLensExclude, setNewLensExclude] = useState<string[]>([])
    const [newLensSources, setNewLensSources] = useState<string[]>([])
    const [newLensMaxGroup, setNewLensMaxGroup] = useState<string>('')
    const [openLensDropdown, setOpenLensDropdown] = useState<'include' | 'exclude' | 'sources' | null>(null)
    const [customSourceGlob, setCustomSourceGlob] = useState<string>('')
    const [availableTypes, setAvailableTypes] = useState<string[]>([])
    const [availableAttributes, setAvailableAttributes] = useState<string[]>([])
    const [fuzzyTemplates, setFuzzyTemplates] = useState<FuzzyTemplate[]>([])
    const [validationError, setValidationError] = useState<string | null>(null)

    // AI Rule Generation state directly inside Rules tab
    const [aiRuleGoal, setAiRuleGoal] = useState('')
    const [proposalBase, setProposalBase] = useState('')
    const [aiContext, setAiContext] = useState(config.ai_assistance || {...defaultAIContext})
    const generationContext = {...aiContext, objectives: Array.from(new Set([...(aiContext.objectives || []), ...localGoalObjectives(aiRuleGoal)]))}
    useEffect(() => { if (config.ai_assistance) setAiContext(config.ai_assistance) }, [config.ai_assistance])
    const [aiGenerating, setAiGenerating] = useState(false)
    const [aiProgress, setAiProgress] = useState<AiProgressEvent | null>(null)
    const [aiReport, setAiReport] = useState<Awaited<ReturnType<typeof api.suggestConfigAi>> | null>(null)

    const latestConfigRef = useRef(config)
    latestConfigRef.current = config

    const lastEmittedSigRef = useRef<string>('')

    const serializeRuleSlice = (
        r: MatchingRule[],
        f: MatchingRule[],
        e: ExclusionRule[],
        o: OverrideRule[],
        d: boolean,
        l: LensRule[]
    ) => JSON.stringify({ r, f, e, o, d, l })

    useEffect(() => {
        const incomingSig = serializeRuleSlice(
            config.rules || [],
            config.fuzzy_rules || [],
            config.exclusions || [],
            config.overrides || [],
            !!config.use_default_exclusions,
            config.lenses || []
        )
        if (incomingSig !== lastEmittedSigRef.current) {
            setRules(combineFromConfig(config))
            setExclusions(config.exclusions || [])
            setUseDefaultExclusions(!!config.use_default_exclusions)
            setOverrides(config.overrides || [])
            setLenses(config.lenses || [])
            lastEmittedSigRef.current = incomingSig
        }

        const typeSet = new Set<string>()
        const attrSet = new Set<string>()
        ;(config.sources || []).forEach((src) => {
            ;(src.identifiers || []).forEach((id) => {
                if (id.is_match_key !== false && id.type) typeSet.add(id.type)
            })
            ;(src.attributes || []).forEach((attr) => {
                if (attr.name) attrSet.add(attr.name)
            })
        })
        const typesArr = Array.from(typeSet).sort()
        const attrsArr = Array.from(attrSet).sort()
        setAvailableTypes(typesArr)
        setAvailableAttributes(attrsArr.length > 0 ? attrsArr : typesArr)
        if (typesArr.length > 0 && newExclType === 'ANY') {
            setNewExclType(typesArr[0])
        }
    }, [config])

    useEffect(() => {
        api.getFuzzyTemplates()
            .then((data) => setFuzzyTemplates(data.templates || []))
            .catch((err) => console.error('Failed to fetch templates', err))
    }, [])

    const resolveTemplateSql = (templateSql: string, colName: string) => {
        const safeCol = colName || availableAttributes[0] || availableTypes[0] || 'first_name'
        return templateSql.replace(/\{col\}/g, safeCol)
    }

    const emitChange = (
        nextRules: MatchingRule[],
        nextExcl: ExclusionRule[] = exclusions,
        nextOv: OverrideRule[] = overrides,
        nextDefExcl: boolean = useDefaultExclusions,
        nextLenses: LensRule[] = lenses
    ) => {
        const exact = nextRules.filter((r) => r.type === 'EXACT')
        const fuzzy = nextRules
            .filter((r) => r.type === 'FUZZY')
            .map((r) => ({
                ...r,
                score_expr: resolveTemplateSql(
                    r.score_expr || '',
                    r.fuzzy_attribute || availableAttributes[0] || 'first_name'
                ),
            }))
        lastEmittedSigRef.current = serializeRuleSlice(exact, fuzzy, nextExcl, nextOv, nextDefExcl, nextLenses)
        setValidationError(null)
        onChange?.({
            ...latestConfigRef.current,
            rules: exact,
            fuzzy_rules: fuzzy,
            exclusions: nextExcl,
            overrides: nextOv,
            lenses: nextLenses,
            use_default_exclusions: nextDefExcl,
        })
    }

    const handleAiGenerateRules = async () => {
        const tables = (config.sources || []).map((s) => s.table).filter(Boolean)
        if (tables.length === 0) {
            toast('error', 'No source tables', 'Add at least one source table in Sources & Mappings first.')
            return
        }
        setAiGenerating(true)
        setAiProgress({
            stage: 'profiling',
            step: 1,
            total_steps: 5,
            message: `Connecting to warehouse to profile ${tables.length} table(s)…`,
        })
        setValidationError(null)
        try {
            const base = latestConfigRef.current
            setProposalBase(JSON.stringify({config:base, context:generationContext}))
            const res = await api.suggestConfigAiStream(tables, '', setAiProgress, {
                mode: 'rules', current_config: base,
                context: generationContext,
            })
            setAiReport(res)
        } catch (err: any) {
            setValidationError(`AI Rule Generator failed: ${err?.message || err}`)
        } finally {
            setAiGenerating(false)
            setAiProgress(null)
        }
    }

    const updatePriorities = (newRules: MatchingRule[]) => {
        const numbered = newRules.map((r, i) => ({ ...r, priority: i + 1 }))
        setRules(numbered)
        emitChange(numbered)
    }

    const clearAllFuzzyRules = () => {
        updatePriorities(rules.filter((r) => r.type !== 'FUZZY'))
        setConfirmClearFuzzy(false)
    }

    const moveRule = (idx: number, direction: 'up' | 'down') => {
        const newRules = [...rules]
        if (direction === 'up' && idx > 0) {
            ;[newRules[idx], newRules[idx - 1]] = [newRules[idx - 1], newRules[idx]]
        } else if (direction === 'down' && idx < newRules.length - 1) {
            ;[newRules[idx], newRules[idx + 1]] = [newRules[idx + 1], newRules[idx]]
        }
        updatePriorities(newRules)
    }

    const deleteRule = (idx: number) => {
        updatePriorities(rules.filter((_, i) => i !== idx))
    }

    const generateId = () => Math.floor(Math.random() * 1000000)

    const addExactRule = (type: string) => {
        const lowerType = type.toLowerCase()
        const defaultCanon = lowerType.includes('email')
            ? 'EMAIL_STANDARD'
            : lowerType.includes('phone')
            ? 'PHONE_US_10'
            : 'LOWERCASE'
        const newRule: MatchingRule = {
            id: generateId(),
            type: 'EXACT',
            match_keys: [type],
            priority: rules.length + 1,
            canonicalize: defaultCanon,
            max_group_size: 10000,
        }
        updatePriorities([...rules, newRule])
    }

    const addFuzzyRule = () => {
        const defaultAttr = availableAttributes[0] || availableTypes[0] || 'first_name'
        const tmpl = fuzzyTemplates[0] || {
            sql_template: 'jaro_winkler_similarity(<a>.{col}, <b>.{col})',
            default_threshold: 0.85,
        }
        const newRule: MatchingRule = {
            id: generateId(),
            type: 'FUZZY',
            match_keys: [],
            priority: rules.length + 1,
            blocking_key: availableTypes[0] || '',
            fuzzy_attribute: defaultAttr,
            score_expr: resolveTemplateSql(tmpl.sql_template, defaultAttr),
            threshold: tmpl.default_threshold,
        }
        updatePriorities([...rules, newRule])
    }

    const updateRuleField = (idx: number, field: keyof MatchingRule, value: any) => {
        const newRules = [...rules]
        const current = { ...newRules[idx], [field]: value }
        if (field === 'fuzzy_attribute' && current.score_expr) {
            current.score_expr = current.score_expr
                .replace(/\{col\}/g, value)
                .replace(/<a>\.[a-zA-Z0-9_]+/g, `<a>.${value}`)
                .replace(/<b>\.[a-zA-Z0-9_]+/g, `<b>.${value}`)
        }
        newRules[idx] = current
        setRules(newRules)
        emitChange(newRules)
    }

    const applyTemplate = (idx: number, tmplId: string) => {
        const tmpl = fuzzyTemplates.find((t) => t.id === tmplId)
        if (tmpl) {
            const newRules = [...rules]
            const col = newRules[idx].fuzzy_attribute || availableAttributes[0] || availableTypes[0] || 'first_name'
            newRules[idx] = {
                ...newRules[idx],
                fuzzy_attribute: col,
                score_expr: resolveTemplateSql(tmpl.sql_template, col),
                threshold: tmpl.default_threshold,
            }
            setRules(newRules)
            emitChange(newRules)
        }
    }

    const addExclusion = () => {
        const trimmed = newExclValue.trim()
        if (!trimmed) return
        const type = newExclType || availableTypes[0] || 'ANY'
        let normalizedVal = trimmed.replace(/\*/g, '%')
        let resolvedMatch: 'EXACT' | 'LIKE' = newExclMatch
        if (resolvedMatch === 'LIKE') {
            if (!normalizedVal.includes('%') && !normalizedVal.includes('_')) {
                normalizedVal = `%${normalizedVal}%`
            }
        } else if (normalizedVal.includes('%')) {
            resolvedMatch = 'LIKE'
        }
        const nextExcl = [...exclusions, { type, value: normalizedVal, match: resolvedMatch, reason: 'UI exclusion' }]
        setExclusions(nextExcl)
        setNewExclValue('')
        emitChange(rules, nextExcl, overrides, useDefaultExclusions)
    }

    const removeExclusion = (idx: number) => {
        const nextExcl = exclusions.filter((_, i) => i !== idx)
        setExclusions(nextExcl)
        emitChange(rules, nextExcl, overrides, useDefaultExclusions)
    }

    const toggleDefaultExclusions = (checked: boolean) => {
        setUseDefaultExclusions(checked)
        emitChange(rules, exclusions, overrides, checked)
    }

    const addOverride = () => {
        const ka = newOvKeyA.trim()
        const kb = newOvKeyB.trim()
        if (!ka) return
        if (newOvType !== 'ISOLATE' && !kb) return
        const nextOv = [
            ...overrides,
            {
                override_type: newOvType,
                entity_key_a: ka,
                entity_key_b: newOvType === 'ISOLATE' ? '__ISOLATE__' : kb,
                reason: newOvReason.trim() || 'UI stewardship override',
            },
        ]
        setOverrides(nextOv)
        setNewOvKeyA('')
        setNewOvKeyB('')
        setNewOvReason('')
        emitChange(rules, exclusions, nextOv, useDefaultExclusions)
    }

    const removeOverride = (idx: number) => {
        const nextOv = overrides.filter((_, i) => i !== idx)
        setOverrides(nextOv)
        emitChange(rules, exclusions, nextOv, useDefaultExclusions)
    }

    const handleNext = () => {
        if (rules.length === 0) {
            setValidationError('Define at least one deterministic or fuzzy matching rule to continue.')
            return
        }
        const exact = rules.filter((r) => r.type === 'EXACT')
        const fuzzy = rules
            .filter((r) => r.type === 'FUZZY')
            .map((r) => ({
                ...r,
                score_expr: resolveTemplateSql(
                    r.score_expr || '',
                    r.fuzzy_attribute || availableAttributes[0] || 'first_name'
                ),
            }))

        onNext({
            ...config,
            rules: exact,
            fuzzy_rules: fuzzy,
            exclusions,
            overrides,
            lenses,
            use_default_exclusions: useDefaultExclusions,
        })
    }

    const addLens = () => {
        const id = newLensId.trim().replace(/\s+/g, '_')
        if (!id) return
        const parsedMax = newLensMaxGroup.trim() ? parseInt(newLensMaxGroup, 10) : undefined
        const nextLenses: LensRule[] = [
            ...lenses.filter((l) => l.id !== id),
            {
                id,
                include_types: newLensInclude.map((t) => t.toUpperCase()),
                exclude_types: newLensExclude.map((t) => t.toUpperCase()),
                source_filter: newLensSources,
                ...(parsedMax && parsedMax > 0 ? { max_group_size: parsedMax } : {}),
            },
        ]
        setLenses(nextLenses)
        setNewLensId('')
        setNewLensInclude([])
        setNewLensExclude([])
        setNewLensSources([])
        setNewLensMaxGroup('')
        setOpenLensDropdown(null)
        emitChange(rules, exclusions, overrides, useDefaultExclusions, nextLenses)
    }

    const toggleLensSelection = (
        selected: string[],
        setSelected: (next: string[]) => void,
        value: string
    ) => {
        setSelected(
            selected.includes(value)
                ? selected.filter((v) => v !== value)
                : [...selected, value]
        )
    }

    const availableSourceIds = Array.from(
        new Set((config.sources || []).map((s) => s.id).filter(Boolean))
    ).sort()

    const lensTypeOptions = Array.from(
        new Set([
            ...availableTypes.map((t) => t.toUpperCase()),
            ...rules
                .flatMap((r) => [
                    ...(r.match_keys || []),
                    ...((r as unknown as { match_key?: string }).match_key
                        ? [(r as unknown as { match_key?: string }).match_key!]
                        : []),
                ])
                .map((t) => String(t).trim().toUpperCase())
                .filter(Boolean),
            ...(config.rules || [])
                .flatMap((r) => [
                    ...(r.match_keys || []),
                    ...((r as unknown as { match_key?: string }).match_key
                        ? [(r as unknown as { match_key?: string }).match_key!]
                        : []),
                ])
                .map((t) => String(t).trim().toUpperCase())
                .filter(Boolean),
            ...(config.sources || [])
                .flatMap((s) => (s.identifiers || []).map((i) => String(i.type || '').trim().toUpperCase()))
                .filter(Boolean),
        ])
    ).sort()

    const removeLens = (idx: number) => {
        const nextLenses = lenses.filter((_, i) => i !== idx)
        setLenses(nextLenses)
        emitChange(rules, exclusions, overrides, useDefaultExclusions, nextLenses)
    }

    const blockingKeyOptions = Array.from(new Set([...availableTypes, ...availableAttributes])).map((t) => ({
        value: t,
        label: t,
    }))

    const totalExclusionsCount = exclusions.length + (useDefaultExclusions ? 33 : 0)

    return (
        <div className="space-y-4 flex flex-col">
            <div>
                <h2 className="text-[15px] font-semibold text-ink">Matching Rules &amp; Guardrails</h2>
                <p className="text-xs text-ink-muted mt-0.5">
                    Define deterministic Exact rules, probabilistic Fuzzy rules, Multi-Lens views, blocklists, and stewardship overrides.
                </p>
            </div>

            <AIConfigPolicy context={aiContext} tables={config.sources.map(s => s.table)} onChange={setAiContext} showBasics />
            {aiReport && <AIProposalReview proposal={aiReport}
                stale={proposalBase !== JSON.stringify({config, context:generationContext})}
                onApply={(candidate) => {
                    const nextRules = combineFromConfig(candidate)
                    setRules(nextRules)
                    lastEmittedSigRef.current = serializeRuleSlice(candidate.rules || [], candidate.fuzzy_rules || [],
                        exclusions, overrides, useDefaultExclusions, lenses)
                    onChange?.(candidate)
                    setAiReport(null)
                    toast('success', 'Rules applied to draft', 'Mappings are preserved. Run a preview to evaluate changes.')
                }} />}
            {validationError && (
                <div role="alert" className="p-3 rounded-md bg-status-warning-soft border border-status-warning-border text-xs text-status-warning flex items-center gap-2">
                    <AlertCircle className="w-4 h-4 shrink-0" />
                    <span>{validationError}</span>
                </div>
            )}

            {/* Sub-Tab Bar: Cleanly surfaces Match Rules, Lenses, Blocklist, and Stewardship Overrides */}
            <div role="tablist" aria-label="Rule configuration tabs" className="border-b border-line flex flex-wrap items-center gap-1">
                {(
                    [
                        { id: 'match', label: `Match Rules (${rules.length})` },
                        { id: 'lenses', label: `Multi-Lens Views (${lenses.length})` },
                        { id: 'exclusions', label: `Blocklist Exclusions (${totalExclusionsCount})` },
                        { id: 'overrides', label: `Stewardship Overrides (${overrides.length})` },
                    ] as const
                ).map((tab) => {
                    const isActive = activeSubTab === tab.id
                    return (
                        <button
                            key={tab.id}
                            role="tab"
                            type="button"
                            aria-selected={isActive}
                            onClick={() => setActiveSubTab(tab.id)}
                            className={`px-3 py-2 text-xs font-medium border-b-2 transition-colors ${
                                isActive
                                    ? 'border-accent text-accent font-semibold'
                                    : 'border-transparent text-ink-muted hover:text-ink'
                            }`}
                        >
                            {tab.label}
                        </button>
                    )
                })}
            </div>

            {/* SUB-TAB 1: Match Rules + AI Rule Generator */}
            {activeSubTab === 'match' && (
                <div className="space-y-4">
                    {/* Inline Zero-PII AI Rule Generator Bar */}
                    <div className="surface-subtle p-3 flex flex-col sm:flex-row sm:items-center gap-2">
                        <div className="flex items-center gap-1.5 text-xs font-semibold text-ink shrink-0">
                            <Bot className="w-4 h-4 text-accent" />
                            <span>AI Rule Generator</span>
                        </div>
                        <input
                            type="text"
                            aria-label="Local objective hints: precision, coverage, cross-source, auditability"
                            value={aiRuleGoal}
                            onChange={(e) => setAiRuleGoal(e.target.value)}
                            placeholder="Precision, coverage, cross-source matching, auditability; other text stays local."
                            className="input-field flex-1 text-xs py-1"
                        />
                        <button
                            type="button"
                            disabled={aiGenerating}
                            onClick={handleAiGenerateRules}
                            title="Profiles your mapped tables (zero PII) and drafts deterministic rules, fuzzy rules, and multi-lens views"
                            className="btn-secondary text-xs shrink-0"
                        >
                            {aiGenerating ? (
                                <>
                                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                    Generating…
                                </>
                            ) : (
                                <>
                                    <Bot className="w-3.5 h-3.5 text-accent" />
                                    AI Suggest Rules
                                    <span className="px-1.5 py-0.2 rounded bg-accent/15 text-accent border border-accent/30 text-[10px] font-semibold uppercase">
                                        Beta
                                    </span>
                                </>
                            )}
                        </button>
                    </div>

                    {aiGenerating && (
                        <AiProgressStepper
                            progress={aiProgress}
                            tableCount={(config.sources || []).length}
                        />
                    )}

                    {aiReport?.explanations?.rules && aiReport.explanations.rules.length > 0 && (
                        <div className="surface-panel p-3 space-y-2 text-xs">
                            <div className="flex items-center justify-between">
                                <span className="font-semibold text-ink">
                                    AI Rule Reasoning ({aiReport.provider}:{aiReport.model})
                                </span>
                                <button
                                    type="button"
                                    onClick={() => setAiReport(null)}
                                    className="text-ink-muted hover:text-ink"
                                >
                                    Dismiss
                                </button>
                            </div>
                            <div className="divide-y divide-line">
                                {aiReport.explanations.rules.map((item, i) => (
                                    <div key={i} className="py-1.5 space-y-0.5">
                                        <div className="flex items-center justify-between">
                                            <span className="font-mono font-semibold text-ink">{item.target}</span>
                                            {item.confidence && <span className="badge-neutral">{item.confidence}</span>}
                                        </div>
                                        <p className="text-ink-secondary">{item.summary}</p>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}

                    <div className="space-y-2.5">
                        {rules.map((rule, idx) => {
                            const matchedTemplate = fuzzyTemplates.find(
                                (t) =>
                                    resolveTemplateSql(
                                        t.sql_template,
                                        rule.fuzzy_attribute || availableAttributes[0] || 'first_name'
                                    ) === rule.score_expr
                            )
                            return (
                                <div key={rule.id} className="flex gap-3 p-3.5 bg-surface border border-line rounded-md">
                                    <div className="flex flex-col items-center gap-0.5 pt-0.5 shrink-0">
                                        <button
                                            type="button"
                                            onClick={() => moveRule(idx, 'up')}
                                            disabled={idx === 0}
                                            aria-label={`Move rule ${rule.priority} up`}
                                            className="p-1 hover:bg-hover rounded text-ink-muted hover:text-ink disabled:opacity-30"
                                        >
                                            <ArrowUp className="w-3.5 h-3.5" />
                                        </button>
                                        <span className="text-[11px] font-mono tabular-nums font-semibold text-ink-muted">
                                            #{idx + 1}
                                        </span>
                                        <button
                                            type="button"
                                            onClick={() => moveRule(idx, 'down')}
                                            disabled={idx === rules.length - 1}
                                            aria-label={`Move rule ${idx + 1} down`}
                                            className="p-1 hover:bg-hover rounded text-ink-muted hover:text-ink disabled:opacity-30"
                                        >
                                            <ArrowDown className="w-3.5 h-3.5" />
                                        </button>
                                    </div>

                                    <div className="flex-1 space-y-2.5 min-w-0">
                                        <div className="flex items-center justify-between gap-2">
                                            <div className="flex items-center gap-2">
                                                <span
                                                    className={
                                                        rule.type === 'EXACT'
                                                            ? 'badge-accent font-mono'
                                                            : 'badge-warning border-dashed font-mono'
                                                    }
                                                >
                                                    {rule.type}
                                                </span>
                                                {rule.type === 'EXACT' ? (
                                                    <div className="flex items-center gap-1.5">
                                                        <GitGraph className="w-3.5 h-3.5 text-accent" />
                                                        <span className="font-semibold text-[13px] text-ink">
                                                            Match by {rule.match_keys.join(' + ')}
                                                        </span>
                                                    </div>
                                                ) : (
                                                    <div className="flex items-center gap-1.5">
                                                        <Wand2 className="w-3.5 h-3.5 text-status-warning" />
                                                        <span className="font-semibold text-[13px] text-ink">
                                                            Fuzzy Similarity Rule
                                                        </span>
                                                    </div>
                                                )}
                                            </div>
                                            <button
                                                type="button"
                                                onClick={() => deleteRule(idx)}
                                                aria-label={`Delete rule ${idx + 1}`}
                                                className="p-1.5 hover:bg-status-danger-soft text-ink-muted hover:text-status-danger rounded transition-colors"
                                            >
                                                <Trash2 className="w-3.5 h-3.5" />
                                            </button>
                                        </div>

                                        {rule.type === 'EXACT' ? (
                                            <div className="space-y-2">
                                                <div className="flex flex-wrap items-center gap-2.5">
                                                    <label className="text-xs text-ink-muted">Normalization:</label>
                                                    <CustomSelect
                                                        value={
                                                            BUILTIN_CANONICALIZERS.includes(
                                                                (rule.canonicalize || 'LOWERCASE').toUpperCase()
                                                            )
                                                                ? (rule.canonicalize || 'LOWERCASE').toUpperCase()
                                                                : 'CUSTOM_SQL'
                                                        }
                                                        onChange={(val) => {
                                                            if (val === 'CUSTOM_SQL') {
                                                                updateRuleField(idx, 'canonicalize', 'LOWER(TRIM({val}))')
                                                            } else {
                                                                updateRuleField(idx, 'canonicalize', val)
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
                                                            { value: 'CUSTOM_SQL', label: 'Custom Warehouse SQL ({val})…' },
                                                        ]}
                                                        buttonClassName="text-xs py-1 px-2.5"
                                                        menuClassName="left-0 right-auto min-w-[330px]"
                                                    />
                                                </div>
                                                {!BUILTIN_CANONICALIZERS.includes((rule.canonicalize || 'LOWERCASE').toUpperCase()) && (
                                                    <div className="flex items-center gap-2">
                                                        <span className="text-[11px] font-mono text-accent">SQL Expression:</span>
                                                        <input
                                                            type="text"
                                                            value={rule.canonicalize || ''}
                                                            onChange={(e) => updateRuleField(idx, 'canonicalize', e.target.value)}
                                                            placeholder="e.g. LPAD(LTRIM(TRIM({val}), '0'), 8, '0')"
                                                            className="input-field flex-1 font-mono text-xs py-1"
                                                        />
                                                    </div>
                                                )}
                                                <div className="flex flex-wrap items-center gap-2 pt-1.5 border-t border-line">
                                                    <label className="text-xs text-ink-muted">Max Group Size:</label>
                                                    <input
                                                        type="number"
                                                        min="2"
                                                        value={rule.max_group_size ?? 10000}
                                                        onChange={(e) =>
                                                            updateRuleField(idx, 'max_group_size', parseInt(e.target.value, 10) || 10000)
                                                        }
                                                        className="input-field w-24 font-mono tabular-nums text-xs py-1"
                                                    />
                                                    <span className="text-[11px] text-ink-muted">
                                                        Skips identifier values linked to more records than this threshold.
                                                    </span>
                                                </div>
                                            </div>
                                        ) : (
                                            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 bg-subtle p-3 rounded-md border border-line">
                                                <div>
                                                    <label className="text-[11px] text-ink-muted block mb-1">Blocking Key</label>
                                                    <CustomSelect
                                                        value={rule.blocking_key || ''}
                                                        onChange={(val) => updateRuleField(idx, 'blocking_key', val)}
                                                        options={blockingKeyOptions}
                                                        className="w-full"
                                                        buttonClassName="text-xs py-1"
                                                        menuClassName="left-0 right-auto min-w-[200px]"
                                                    />
                                                </div>
                                                <div>
                                                    <label className="text-[11px] text-ink-muted block mb-1">Compare Attribute</label>
                                                    <CustomSelect
                                                        value={rule.fuzzy_attribute || availableAttributes[0] || ''}
                                                        onChange={(val) => updateRuleField(idx, 'fuzzy_attribute', val)}
                                                        options={availableAttributes.map((a) => ({ value: a, label: a }))}
                                                        className="w-full"
                                                        buttonClassName="text-xs py-1"
                                                        menuClassName="left-0 right-auto min-w-[200px]"
                                                    />
                                                </div>
                                                <div>
                                                    <label className="text-[11px] text-ink-muted block mb-1">Similarity Method</label>
                                                    <CustomSelect
                                                        value={matchedTemplate?.id || ''}
                                                        onChange={(val) => applyTemplate(idx, val)}
                                                        placeholder="Select template…"
                                                        options={fuzzyTemplates.map((t) => ({ value: t.id, label: t.label }))}
                                                        className="w-full"
                                                        buttonClassName="text-xs py-1"
                                                        menuClassName="right-0 left-auto min-w-[260px]"
                                                    />
                                                </div>
                                                <div className="sm:col-span-3 pt-1">
                                                    <div className="flex justify-between text-xs text-ink-muted mb-1">
                                                        <span>Similarity Threshold</span>
                                                        <span className="text-status-warning font-mono tabular-nums font-semibold">
                                                            {rule.threshold}
                                                        </span>
                                                    </div>
                                                    <input
                                                        type="range"
                                                        min="0.5"
                                                        max="1.0"
                                                        step="0.01"
                                                        value={rule.threshold || 0.85}
                                                        onChange={(e) => updateRuleField(idx, 'threshold', parseFloat(e.target.value))}
                                                        className="w-full accent-[var(--accent)]"
                                                    />
                                                </div>
                                                {rule.score_expr && (
                                                    <div className="sm:col-span-3">
                                                        <span className="text-[11px] font-mono text-ink-muted block truncate" title={rule.score_expr}>
                                                            SQL: {rule.score_expr}
                                                        </span>
                                                    </div>
                                                )}
                                            </div>
                                        )}
                                    </div>
                                </div>
                            )
                        })}

                        {rules.length === 0 && (
                            <div className="text-center p-8 border border-dashed border-line-strong rounded-md text-xs text-ink-muted">
                                No matching rules defined yet. Add an Exact or Fuzzy rule below or click AI Suggest Rules above.
                            </div>
                        )}
                    </div>

                    <div className="pt-3 border-t border-line flex flex-wrap items-center gap-2">
                        {(() => {
                            const existingKeys = new Set(
                                rules
                                    .filter((r) => r.type !== 'FUZZY')
                                    .flatMap((r) => (r.match_keys || []).map((k) => k.toLowerCase()))
                            )
                            const unconfigured = availableTypes.filter((t) => !existingKeys.has(t.toLowerCase()))
                            if (unconfigured.length === 0) return null
                            return (
                                <select
                                    aria-label="Add exact matching rule"
                                    value=""
                                    onChange={(e) => {
                                        if (e.target.value) addExactRule(e.target.value)
                                    }}
                                    className="btn-secondary text-xs pr-7 cursor-pointer"
                                >
                                    <option value="">+ Add Exact Rule ({unconfigured.length} available)…</option>
                                    {unconfigured.map((type) => (
                                        <option key={type} value={type}>
                                            Exact: {type}
                                        </option>
                                    ))}
                                </select>
                            )
                        })()}
                        <button type="button" onClick={addFuzzyRule} className="btn-secondary text-xs">
                            <Wand2 className="w-3.5 h-3.5 text-status-warning" /> Add Fuzzy Rule
                        </button>
                        {rules.some((r) => r.type === 'FUZZY') && (
                            confirmClearFuzzy ? (
                                <div className="inline-flex items-center gap-1.5 ml-auto">
                                    <button type="button" onClick={clearAllFuzzyRules} className="btn-danger text-xs">
                                        Confirm Remove Fuzzy
                                    </button>
                                    <button type="button" onClick={() => setConfirmClearFuzzy(false)} className="btn-ghost text-xs">
                                        Cancel
                                    </button>
                                </div>
                            ) : (
                                <button
                                    type="button"
                                    onClick={() => setConfirmClearFuzzy(true)}
                                    className="btn-ghost text-xs text-status-danger ml-auto"
                                >
                                    <Trash2 className="w-3.5 h-3.5" /> Remove All Fuzzy
                                </button>
                            )
                        )}
                    </div>
                </div>
            )}

            {/* SUB-TAB 2: Multi-Lens Identity Views */}
            {activeSubTab === 'lenses' && (
                <div className="surface-panel p-4 space-y-3">
                    <div>
                        <h3 className="text-[13px] font-semibold text-ink">Multi-Lens Identity Views</h3>
                        <p className="text-xs text-ink-muted">
                            Materialize multiple policy-bounded identity graphs (e.g., <code className="font-mono">strict_person</code> vs <code className="font-mono">household</code>) from one scan.
                        </p>
                    </div>
                    <div className="grid grid-cols-1 sm:grid-cols-5 gap-2">
                        <input
                            type="text"
                            value={newLensId}
                            onChange={(e) => setNewLensId(e.target.value)}
                            placeholder="Lens ID (e.g. strict_person)"
                            className="input-field font-mono text-xs"
                        />

                        <div className="relative">
                            <button
                                type="button"
                                onClick={() => setOpenLensDropdown(openLensDropdown === 'include' ? null : 'include')}
                                className="input-field flex items-center justify-between text-xs text-left"
                            >
                                <span className="truncate">
                                    {newLensInclude.length > 0 ? `Include: ${newLensInclude.join(', ')}` : 'Include types (all)'}
                                </span>
                                <span className="text-ink-muted ml-1">▾</span>
                            </button>
                            {openLensDropdown === 'include' && (
                                <div className="absolute z-30 mt-1 w-56 bg-surface border border-line-strong rounded-md shadow-popover p-2 space-y-1 max-h-52 overflow-y-auto">
                                    {lensTypeOptions.length === 0 ? (
                                        <div className="text-[11px] text-ink-muted px-1 py-1">No identifier types found</div>
                                    ) : (
                                        lensTypeOptions.map((t) => (
                                            <label
                                                key={t}
                                                className="flex items-center gap-2 px-2 py-1 rounded hover:bg-subtle cursor-pointer text-xs text-ink"
                                            >
                                                <input
                                                    type="checkbox"
                                                    checked={newLensInclude.includes(t)}
                                                    onChange={() => toggleLensSelection(newLensInclude, setNewLensInclude, t)}
                                                    className="rounded border-line-strong text-accent"
                                                />
                                                <span className="font-mono">{t}</span>
                                            </label>
                                        ))
                                    )}
                                </div>
                            )}
                        </div>

                        <div className="relative">
                            <button
                                type="button"
                                onClick={() => setOpenLensDropdown(openLensDropdown === 'exclude' ? null : 'exclude')}
                                className="input-field flex items-center justify-between text-xs text-left"
                            >
                                <span className="truncate">
                                    {newLensExclude.length > 0 ? `Exclude: ${newLensExclude.join(', ')}` : 'Exclude types (none)'}
                                </span>
                                <span className="text-ink-muted ml-1">▾</span>
                            </button>
                            {openLensDropdown === 'exclude' && (
                                <div className="absolute z-30 mt-1 w-56 bg-surface border border-line-strong rounded-md shadow-popover p-2 space-y-1 max-h-52 overflow-y-auto">
                                    {lensTypeOptions.length === 0 ? (
                                        <div className="text-[11px] text-ink-muted px-1 py-1">No identifier types found</div>
                                    ) : (
                                        lensTypeOptions.map((t) => (
                                            <label
                                                key={t}
                                                className="flex items-center gap-2 px-2 py-1 rounded hover:bg-subtle cursor-pointer text-xs text-ink"
                                            >
                                                <input
                                                    type="checkbox"
                                                    checked={newLensExclude.includes(t)}
                                                    onChange={() => toggleLensSelection(newLensExclude, setNewLensExclude, t)}
                                                    className="rounded border-line-strong text-accent"
                                                />
                                                <span className="font-mono">{t}</span>
                                            </label>
                                        ))
                                    )}
                                </div>
                            )}
                        </div>

                        <div className="relative">
                            <button
                                type="button"
                                onClick={() => setOpenLensDropdown(openLensDropdown === 'sources' ? null : 'sources')}
                                className="input-field flex items-center justify-between text-xs text-left"
                            >
                                <span className="truncate">
                                    {newLensSources.length > 0 ? `Sources: ${newLensSources.join(', ')}` : 'Source filter (all)'}
                                </span>
                                <span className="text-ink-muted ml-1">▾</span>
                            </button>
                            {openLensDropdown === 'sources' && (
                                <div className="absolute z-30 mt-1 w-64 bg-surface border border-line-strong rounded-md shadow-popover p-2 space-y-1 max-h-60 overflow-y-auto">
                                    {availableSourceIds.map((sid) => (
                                        <label
                                            key={sid}
                                            className="flex items-center gap-2 px-2 py-1 rounded hover:bg-subtle cursor-pointer text-xs text-ink"
                                        >
                                            <input
                                                type="checkbox"
                                                checked={newLensSources.includes(sid)}
                                                onChange={() => toggleLensSelection(newLensSources, setNewLensSources, sid)}
                                                className="rounded border-line-strong text-accent"
                                            />
                                            <span className="truncate font-mono">{sid}</span>
                                        </label>
                                    ))}
                                    <div className="pt-1.5 mt-1 border-t border-line flex gap-1">
                                        <input
                                            type="text"
                                            value={customSourceGlob}
                                            onChange={(e) => setCustomSourceGlob(e.target.value)}
                                            placeholder="Custom glob (e.g. crm*)"
                                            className="input-field flex-1 text-[11px] py-1"
                                        />
                                        <button
                                            type="button"
                                            onClick={() => {
                                                const g = customSourceGlob.trim()
                                                if (g && !newLensSources.includes(g)) {
                                                    setNewLensSources([...newLensSources, g])
                                                }
                                                setCustomSourceGlob('')
                                            }}
                                            className="btn-secondary text-[11px] px-2 py-1 min-h-[26px]"
                                        >
                                            Add
                                        </button>
                                    </div>
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
                                className="input-field w-24 font-mono text-xs"
                            />
                            <button type="button" onClick={addLens} className="btn-primary text-xs whitespace-nowrap">
                                + Add Lens
                            </button>
                        </div>
                    </div>

                    {lenses.length > 0 && (
                        <div className="divide-y divide-line border border-line rounded-md overflow-hidden">
                            {lenses.map((l, idx) => (
                                <div key={`${l.id}-${idx}`} className="flex items-center justify-between bg-surface px-3 py-2 text-xs">
                                    <div className="flex flex-wrap items-center gap-2 text-ink-secondary">
                                        <span className="font-mono font-semibold text-accent">{l.id}</span>
                                        {l.include_types && l.include_types.length > 0 && (
                                            <span className="badge-neutral font-mono">include: {l.include_types.join(', ')}</span>
                                        )}
                                        {l.exclude_types && l.exclude_types.length > 0 && (
                                            <span className="badge-neutral font-mono">exclude: {l.exclude_types.join(', ')}</span>
                                        )}
                                        {l.source_filter && l.source_filter.length > 0 && (
                                            <span className="badge-neutral font-mono">sources: {l.source_filter.join(', ')}</span>
                                        )}
                                        {l.max_group_size && (
                                            <span className="badge-warning font-mono">max_size: {l.max_group_size}</span>
                                        )}
                                    </div>
                                    <button
                                        type="button"
                                        onClick={() => removeLens(idx)}
                                        className="text-status-danger hover:underline text-xs"
                                    >
                                        Remove
                                    </button>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            )}

            {/* SUB-TAB 3: Blocklist Exclusions */}
            {activeSubTab === 'exclusions' && (
                <div className="surface-panel p-4 space-y-3">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                        <div className="flex items-center gap-2">
                            <ShieldAlert className="w-4 h-4 text-status-warning" />
                            <h3 className="font-semibold text-[13px] text-ink">Identifier Exclusions (Blocklist)</h3>
                        </div>
                        <label className="inline-flex items-center gap-2 text-xs text-ink-secondary cursor-pointer bg-subtle border border-line px-2.5 py-1 rounded-md">
                            <input
                                type="checkbox"
                                checked={useDefaultExclusions}
                                onChange={(e) => toggleDefaultExclusions(e.target.checked)}
                                className="rounded border-line-strong text-accent"
                            />
                            <span>Include Default Toxic Pack (33 rules)</span>
                        </label>
                    </div>
                    <p className="text-xs text-ink-muted">
                        Prevent placeholder values (<code className="font-mono">test@test.com</code>, <code className="font-mono">0000000000</code>) from chaining unrelated entities.
                    </p>
                    <div className="flex flex-wrap gap-2">
                        <CustomSelect
                            value={newExclType || availableTypes[0] || 'ANY'}
                            onChange={setNewExclType}
                            options={[
                                { value: 'ANY', label: 'ANY (All Types)' },
                                ...availableTypes.map((t) => ({ value: t, label: t })),
                            ]}
                            buttonClassName="text-xs py-1.5"
                            menuClassName="left-0 right-auto min-w-[160px]"
                        />
                        <CustomSelect
                            value={newExclMatch}
                            onChange={(val) => setNewExclMatch(val as 'EXACT' | 'LIKE')}
                            options={[
                                { value: 'EXACT', label: 'EXACT' },
                                { value: 'LIKE', label: 'LIKE (%)' },
                            ]}
                            buttonClassName="text-xs py-1.5"
                            menuClassName="left-0 right-auto min-w-[140px]"
                        />
                        <input
                            type="text"
                            value={newExclValue}
                            onChange={(e) => setNewExclValue(e.target.value)}
                            placeholder="Value or pattern"
                            className="input-field flex-1 min-w-[140px] font-mono text-xs"
                        />
                        <button type="button" onClick={addExclusion} className="btn-primary text-xs">
                            + Add Exclusion
                        </button>
                    </div>
                    {exclusions.length > 0 && (
                        <div className="flex flex-wrap gap-1.5 pt-1">
                            {exclusions.map((ex, i) => (
                                <span
                                    key={`${ex.type}-${ex.value}-${i}`}
                                    className="inline-flex items-center gap-1.5 px-2.5 py-1 bg-subtle border border-line rounded text-xs text-ink-secondary"
                                >
                                    <span className="text-status-warning font-mono font-semibold">{ex.type}</span>
                                    <span className="text-ink-muted">({ex.match || ex.match_type || 'EXACT'})</span>
                                    <span className="font-mono text-ink">{ex.value}</span>
                                    <button
                                        type="button"
                                        onClick={() => removeExclusion(i)}
                                        aria-label={`Remove exclusion ${ex.value}`}
                                        className="text-ink-muted hover:text-status-danger ml-1"
                                    >
                                        ×
                                    </button>
                                </span>
                            ))}
                        </div>
                    )}
                </div>
            )}

            {/* SUB-TAB 4: Manual Stewardship Overrides */}
            {activeSubTab === 'overrides' && (
                <div className="surface-panel p-4 space-y-3">
                    <div className="flex items-center gap-2">
                        <Link2 className="w-4 h-4 text-status-success" />
                        <h3 className="font-semibold text-[13px] text-ink">Stewardship Overrides</h3>
                    </div>
                    <p className="text-xs text-ink-muted">
                        Force-merge (<code className="font-mono">FORCE_LINK</code>), block false merges (<code className="font-mono">DO_NOT_LINK</code>), or quarantine (<code className="font-mono">ISOLATE</code>).
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
                            buttonClassName="text-xs py-1.5"
                            menuClassName="left-0 right-auto min-w-[210px]"
                        />
                        <input
                            type="text"
                            value={newOvKeyA}
                            onChange={(e) => setNewOvKeyA(e.target.value)}
                            placeholder="Entity Key A (crm:101)"
                            className="input-field min-w-[130px] flex-1 font-mono text-xs"
                        />
                        {newOvType !== 'ISOLATE' && (
                            <input
                                type="text"
                                value={newOvKeyB}
                                onChange={(e) => setNewOvKeyB(e.target.value)}
                                placeholder="Entity Key B (web:502)"
                                className="input-field min-w-[130px] flex-1 font-mono text-xs"
                            />
                        )}
                        <input
                            type="text"
                            value={newOvReason}
                            onChange={(e) => setNewOvReason(e.target.value)}
                            placeholder="Reason (optional)"
                            className="input-field min-w-[120px] flex-1 text-xs"
                        />
                        <button type="button" onClick={addOverride} className="btn-primary text-xs">
                            + Add Override
                        </button>
                    </div>
                    {overrides.length > 0 && (
                        <div className="flex flex-wrap gap-1.5 pt-1">
                            {overrides.map((ov, i) => (
                                <span
                                    key={`${ov.override_type}-${ov.entity_key_a}-${ov.entity_key_b}-${i}`}
                                    className="inline-flex items-center gap-1.5 px-2.5 py-1 bg-subtle border border-line rounded text-xs text-ink-secondary"
                                >
                                    <span className="text-status-success font-mono font-semibold">{ov.override_type}</span>
                                    <span className="font-mono text-ink">{ov.entity_key_a}</span>
                                    {ov.override_type !== 'ISOLATE' && ov.entity_key_b && (
                                        <>
                                            <span className="text-ink-muted">↔</span>
                                            <span className="font-mono text-ink">{ov.entity_key_b}</span>
                                        </>
                                    )}
                                    <button
                                        type="button"
                                        onClick={() => removeOverride(i)}
                                        aria-label={`Remove override ${ov.entity_key_a || i + 1}`}
                                        className="text-ink-muted hover:text-status-danger ml-1"
                                    >
                                        ×
                                    </button>
                                </span>
                            ))}
                        </div>
                    )}
                </div>
            )}

            <div className="flex justify-between pt-3 border-t border-line">
                {onBack ? (
                    <button type="button" onClick={onBack} className="btn-secondary text-xs">
                        ← Back to Sources
                    </button>
                ) : (
                    <div />
                )}
                <button type="button" onClick={handleNext} className="btn-primary text-xs">
                    Continue to Survivorship →
                </button>
            </div>
        </div>
    )
}
