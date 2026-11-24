import { useState } from 'react'
import { Copy, Check, GitCompareArrows } from 'lucide-react'
import type { ActivationDeltaConfig, IDRConfig } from '../types'

const SYNC_SQL_SNIPPET = `SELECT
  d.run_id,
  d.activation_status,
  d.change_type,
  d.resolved_id,
  d.superseded_by_resolved_id,
  d.previous_resolved_ids_json,
  d.emitted_at,
  g.*
FROM idr_out.identity_activation_delta d
LEFT JOIN idr_out.golden_profiles g
  ON g.resolved_id = d.resolved_id
WHERE d.lens_id = 'default'
  AND d.emitted_at > :last_synced_at
ORDER BY d.emitted_at ASC;`

export default function ActivationDeltaEditor({
    config,
    onChange,
}: {
    config: IDRConfig
    onChange: (next: IDRConfig) => void
}) {
    const [copied, setCopied] = useState(false)
    const current: ActivationDeltaConfig = {
        enabled: Boolean(config.activation_delta?.enabled),
        include_lenses: Boolean(config.activation_delta?.include_lenses),
        seed_initial_full: Boolean(config.activation_delta?.seed_initial_full),
        retention_days: config.activation_delta?.retention_days ?? 30,
    }
    const hasLenses = (config.lenses || []).length > 0

    const update = (patch: Partial<ActivationDeltaConfig>) => {
        const nextBlock: ActivationDeltaConfig = { ...current, ...patch }
        onChange({ ...config, activation_delta: nextBlock })
    }

    const handleCopySql = async () => {
        try {
            await navigator.clipboard.writeText(SYNC_SQL_SNIPPET)
            setCopied(true)
            setTimeout(() => setCopied(false), 1800)
        } catch {
            // ignore clipboard permission errors
        }
    }

    return (
        <details
            open={current.enabled}
            className="surface-subtle p-3.5 text-xs space-y-3"
        >
            <summary className="cursor-pointer font-semibold text-ink flex flex-wrap items-center justify-between gap-2">
                <span className="inline-flex items-center gap-1.5">
                    <GitCompareArrows className="w-3.5 h-3.5 text-accent" />
                    Activation delta changelog (Reverse-ETL &amp; CRM sync)
                </span>
                <span className={`font-mono ${current.enabled ? 'badge-success' : 'badge-neutral'}`}>
                    {current.enabled ? 'Enabled' : 'Opt-in · 0.0s overhead when off'}
                </span>
            </summary>

            <p className="text-ink-secondary">
                Materializes <code className="font-mono text-accent">idr_out.identity_activation_delta</code> inside Stage 8 with cluster-level transition events (<code className="font-mono">NEW</code>, <code className="font-mono">MERGED</code>, <code className="font-mono">SPLIT</code>, <code className="font-mono">PROFILE_CHANGED</code>) and explicit retirement tombstones (<code className="font-mono">RETIRED_MERGED</code> with <code className="font-mono">superseded_by_resolved_id</code>, <code className="font-mono">RETIRED_DELETED</code>).
            </p>

            <label className="flex items-start gap-2.5 p-2.5 rounded-md bg-surface border border-line cursor-pointer hover:border-line-strong transition-colors">
                <input
                    type="checkbox"
                    checked={current.enabled}
                    onChange={(e) => update({ enabled: e.target.checked })}
                    className="mt-0.5 rounded border-line-strong text-accent"
                />
                <div>
                    <span className="font-medium text-ink block">
                        Enable idr_out.identity_activation_delta
                    </span>
                    <span className="text-ink-muted">
                        Disabled by default so standard runs pay zero extra SQL statements. No-op incremental re-extractions are suppressed automatically.
                    </span>
                </div>
            </label>

            {current.enabled && (
                <div className="space-y-3 pt-1">
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                        <div className="p-2.5 rounded-md bg-surface border border-line space-y-1">
                            <label className="font-medium text-ink block">
                                Retention Window (Days)
                            </label>
                            <input
                                type="number"
                                min={0}
                                max={3650}
                                value={current.retention_days ?? 30}
                                onChange={(e) => {
                                    const parsed = parseInt(e.target.value, 10)
                                    update({
                                        retention_days: Number.isFinite(parsed)
                                            ? Math.max(0, Math.min(3650, parsed))
                                            : 30,
                                    })
                                }}
                                className="input-field font-mono tabular-nums text-xs py-1"
                            />
                            <span className="text-[11px] text-ink-muted block">
                                Set to 0 to retain changelog rows indefinitely.
                            </span>
                        </div>

                        <label
                            className={`p-2.5 rounded-md bg-surface border border-line flex items-start gap-2 ${
                                hasLenses ? 'cursor-pointer hover:border-line-strong' : 'opacity-60 cursor-not-allowed'
                            }`}
                        >
                            <input
                                type="checkbox"
                                checked={Boolean(current.include_lenses && hasLenses)}
                                disabled={!hasLenses}
                                onChange={(e) => update({ include_lenses: e.target.checked })}
                                className="mt-0.5 rounded border-line-strong text-accent"
                            />
                            <div>
                                <span className="font-medium text-ink block">
                                    Include Multi-Lens Views
                                </span>
                                <span className="text-[11px] text-ink-muted block">
                                    {hasLenses
                                        ? `Also emit transitions for ${config.lenses?.length} configured lens(es).`
                                        : 'Configure lenses in Rules to enable per-lens activation deltas.'}
                                </span>
                            </div>
                        </label>

                        <label className="p-2.5 rounded-md bg-surface border border-line hover:border-line-strong flex items-start gap-2 cursor-pointer">
                            <input
                                type="checkbox"
                                checked={Boolean(current.seed_initial_full)}
                                onChange={(e) => update({ seed_initial_full: e.target.checked })}
                                className="mt-0.5 rounded border-line-strong text-accent"
                            />
                            <div>
                                <span className="font-medium text-ink block">
                                    Seed Initial FULL Run
                                </span>
                                <span className="text-[11px] text-ink-muted block">
                                    Leave off if your Reverse-ETL tool backfills Day-1 from golden_profiles.
                                </span>
                            </div>
                        </label>
                    </div>

                    <div className="rounded-md bg-surface border border-line p-2.5 space-y-1.5">
                        <div className="flex items-center justify-between">
                            <span className="text-[11px] font-medium text-ink-secondary">
                                Downstream Reverse-ETL / CRM Sync Query
                            </span>
                            <button
                                type="button"
                                onClick={handleCopySql}
                                className="btn-secondary text-[11px] px-2 py-0.5 min-h-[24px]"
                            >
                                {copied ? (
                                    <>
                                        <Check className="w-3 h-3 text-status-success" />
                                        Copied
                                    </>
                                ) : (
                                    <>
                                        <Copy className="w-3 h-3" />
                                        Copy Sync SQL
                                    </>
                                )}
                            </button>
                        </div>
                        <pre className="text-[11px] font-mono text-ink-secondary overflow-x-auto leading-relaxed">
                            {SYNC_SQL_SNIPPET}
                        </pre>
                    </div>
                </div>
            )}
        </details>
    )
}
