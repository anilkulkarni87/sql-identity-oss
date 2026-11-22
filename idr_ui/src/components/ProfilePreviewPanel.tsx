import { useState } from 'react'
import { api } from '../api/client'
import type { IDRConfig, ProfileValidation, ProfilePreviewResult } from '../types'

export default function ProfilePreviewPanel({config, runId, fingerprint, lens, stale, initialValidation}: {
    config: IDRConfig; runId: string; fingerprint: string; lens: string; stale: boolean; initialValidation?: ProfileValidation;
}) {
    const [validation, setValidation] = useState(initialValidation)
    const [selected, setSelected] = useState<string[]>([])
    const [grouped, setGrouped] = useState<string[]>([])
    const [mode, setMode] = useState<'targeted' | 'full'>('targeted')
    const [maxClusters, setMaxClusters] = useState(25)
    const [maxMembers, setMaxMembers] = useState(10000)
    const [seconds, setSeconds] = useState(15)
    const [allowScan, setAllowScan] = useState(false)
    const [allowFull, setAllowFull] = useState(false)
    const [busy, setBusy] = useState(false)
    const [error, setError] = useState('')
    const [result, setResult] = useState<ProfilePreviewResult | null>(null)
    async function request(data: boolean) {
        setBusy(true); setError(''); setResult(null)
        try {
            const response = await api.previewProfiles({config, run_id: runId, draft_hash: fingerprint, lens,
                options: data ? {mode, attributes: selected, grouped_outputs: grouped, max_clusters: maxClusters, max_members: maxMembers,
                    timeout_seconds: seconds, allow_unknown_scan_cost: allowScan, allow_full_population: allowFull} : {mode:'validate'}})
            if (response.mode === 'validate') setValidation(response as ProfileValidation)
            else setResult(response as ProfilePreviewResult)
        } catch (e) {setError(e instanceof Error ? e.message : String(e))}
        finally {setBusy(false)}
    }
    return <details className="rounded border border-slate-600 p-4 space-y-3 text-xs bg-slate-900" aria-label="Golden profile preview">
        <summary className="cursor-pointer font-semibold">Golden profile validation and optional preview</summary>
        <p>Validation uses configuration metadata only. Profile values are never rebuilt automatically during Dry Run.</p>
        <button type="button" onClick={() => request(false)} disabled={busy} className="rounded border p-2">Validate profile configuration</button>
        {validation && <p role="status">{validation.attributes.length} configured profile fields validated. Business-key uniqueness, overlapping sources and currency compatibility require separate review.</p>}
        {stale && <p className="text-amber-300">Run a fresh graph preview before previewing profile values for the changed draft or view.</p>}
        <fieldset disabled={busy} className="space-y-2"><legend>Select profile fields</legend>
            {(config.survivorship || []).map(rule => <label key={rule.attribute} className="inline-flex items-center gap-1 mr-3"><input type="checkbox" checked={selected.includes(rule.attribute)} onChange={e => {setResult(null); setSelected(e.target.checked ? [...selected, rule.attribute] : selected.filter(v => v !== rule.attribute))}} />{rule.attribute}</label>)}
        </fieldset>
        {(config.grouped_outputs || []).map(output => <label key={output.id} className="flex items-center gap-2"><input type="checkbox" checked={grouped.includes(output.id)} onChange={e=>{setResult(null);setGrouped(e.target.checked?[...grouped,output.id]:grouped.filter(v=>v!==output.id))}} />Grouped state: {output.id}</label>)}
        <div className="flex flex-wrap gap-3">
            <label>Scope <select disabled={busy} value={mode} onChange={e => {setMode(e.target.value as typeof mode); setResult(null); setAllowFull(false)}} className="bg-slate-800 p-2"><option value="targeted">Complete cluster subset</option><option value="full">Full evaluated population</option></select></label>
            <label>Maximum clusters <input aria-label="Maximum preview clusters" disabled={busy || mode === 'full'} type="number" min="1" max="1000" value={maxClusters} onChange={e => {setMaxClusters(Number(e.target.value)); setResult(null)}} className="bg-slate-800 p-2 w-20" /></label>
            <label>Maximum member records <input aria-label="Maximum preview members" disabled={busy} type="number" min="1" value={maxMembers} onChange={e => {setMaxMembers(Number(e.target.value)); setResult(null)}} className="bg-slate-800 p-2 w-28" /></label>
            <label>Timeout in seconds <input aria-label="Profile preview timeout" disabled={busy} type="number" min="1" max="120" value={seconds} onChange={e => setSeconds(Number(e.target.value))} className="bg-slate-800 p-2 w-20" /></label>
        </div>
        <p className="text-amber-300">Cluster and record limits bound the result, not source scan size. Query timeouts are enforced across DuckDB, BigQuery (with optional max_bytes_billed), Snowflake, and Databricks.</p>
        <label className="flex items-center gap-2"><input type="checkbox" checked={allowScan} onChange={e => setAllowScan(e.target.checked)} />Allow source scans with unknown cost, subject to the timeout</label>
        {mode === 'full' && <label className="flex items-center gap-2"><input type="checkbox" checked={allowFull} onChange={e => setAllowFull(e.target.checked)} />Explicitly allow a full-population profile preview</label>}
        <button type="button" onClick={() => request(true)} disabled={busy || stale || (!selected.length && !grouped.length) || !allowScan || (mode === 'full' && !allowFull)} className="rounded bg-purple-700 p-2 disabled:opacity-40">{busy ? 'Working…' : 'Run profile preview'}</button>
        {error && <p role="alert" className="text-red-300">{error}</p>}
        {result && <div className="space-y-2"><p>{result.selected_clusters} complete clusters · {result.selected_members} member records · {result.profile_count} proposed profiles. Current source values are evaluated against frozen graph membership.</p>
            {Object.entries(result.grouped_outputs || {}).map(([id, stats])=><p key={id}>{id}: {stats.rows} grouped states · {stats.conflicts} unresolved conflicts</p>)}
            <table className="w-full text-left"><caption className="text-left">Selected profiles only; totals are not population estimates</caption><thead><tr><th>Field</th><th>Paired profiles changed</th><th>Non-null after</th><th>Selected sum before → after</th></tr></thead><tbody>
                {result.attributes.map(field => {
                    const fmtNum = (v: unknown) => v == null ? 'NULL' : typeof v === 'number' ? v.toLocaleString(undefined, { maximumFractionDigits: 2 }) : String(v)
                    return <tr key={field.attribute}><td>{field.attribute}</td><td>{field.changed_profiles ?? 'No comparable baseline'}</td><td>{field.after.non_null} / {field.after.profiles}</td><td>{field.after.sample_sum !== undefined ? `${field.before?.sample_sum !== undefined ? fmtNum(field.before.sample_sum) : 'Unavailable'} → ${fmtNum(field.after.sample_sum)}` : 'Not numeric'}</td></tr>
                })}
            </tbody></table>{result.limitations.map(text => <p key={text}>{text}</p>)}
        </div>}
    </details>
}
