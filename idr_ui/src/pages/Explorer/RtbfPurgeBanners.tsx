import { Loader2, Trash2, X } from 'lucide-react'
import { PrivacyForgetResponse } from './types'

export function RtbfReceiptBanner({
    receipt,
    onDismiss,
}: {
    receipt: PrivacyForgetResponse | null
    onDismiss: () => void
}) {
    if (!receipt) return null
    return (
        <div className="p-3.5 rounded-md bg-status-success-soft border border-status-success-border text-xs space-y-1.5">
            <div className="flex items-center justify-between font-semibold text-status-success">
                <span>GDPR / CCPA Purge Complete ({receipt.request_id})</span>
                <button
                    type="button"
                    onClick={onDismiss}
                    aria-label="Dismiss purge receipt"
                    className="p-1 rounded hover:bg-surface/50"
                >
                    <X className="w-3.5 h-3.5" />
                </button>
            </div>
            <p className="text-ink-secondary">
                Purged <strong className="font-mono tabular-nums text-ink">{receipt.entities_purged}</strong> entities and{' '}
                <strong className="font-mono tabular-nums text-ink">{receipt.edges_purged}</strong> edges across{' '}
                <code className="font-mono">idr_out.*</code> and <code className="font-mono">idr_work.*</code>.
            </p>
            <p className="font-mono text-[11px] text-ink-muted break-all">
                SHA-256 Audit Receipt: {receipt.target_hash}
            </p>
        </div>
    )
}

export function RtbfPreviewBanner({
    selectedCluster,
    preview,
    purging,
    disabled,
    onConfirm,
    onCancel,
}: {
    selectedCluster: string
    preview: PrivacyForgetResponse | null
    purging: boolean
    disabled: boolean
    onConfirm: () => void
    onCancel: () => void
}) {
    if (!preview) return null
    return (
        <div className="p-4 rounded-md bg-status-danger-soft border border-status-danger-border text-xs space-y-2.5">
            <div className="flex items-center justify-between gap-2">
                <span className="font-semibold text-status-danger flex items-center gap-1.5">
                    <Trash2 className="w-4 h-4" />
                    Confirm GDPR / CCPA Cascading Purge
                </span>
                <button
                    type="button"
                    onClick={onCancel}
                    className="text-ink-muted hover:text-ink font-medium"
                >
                    Cancel
                </button>
            </div>
            <p className="text-ink-secondary leading-relaxed">
                Permanently purges cluster <code className="font-mono text-ink">{selectedCluster}</code> (
                <strong className="font-mono tabular-nums text-ink">{preview.entities_purged}</strong> entities,{' '}
                <strong className="font-mono tabular-nums text-ink">{preview.edges_purged}</strong> edges),
                writes SHA-256 suppression hashes to <code className="font-mono">idr_meta.privacy_suppression</code>,
                and logs an audit receipt.
            </p>
            {preview.entity_keys && preview.entity_keys.length > 0 && (
                <div className="flex flex-wrap gap-1.5 p-2 rounded bg-surface border border-line">
                    {preview.entity_keys.slice(0, 6).map((ek) => (
                        <span key={ek} className="font-mono text-[11px] text-ink-secondary px-1.5 py-0.5 rounded bg-subtle border border-line">
                            {ek}
                        </span>
                    ))}
                    {preview.entity_keys.length > 6 && (
                        <span className="font-mono text-[11px] text-ink-muted px-1 py-0.5">
                            +{preview.entity_keys.length - 6} more
                        </span>
                    )}
                </div>
            )}
            <div className="flex items-center gap-2 pt-0.5">
                <button
                    type="button"
                    onClick={onConfirm}
                    disabled={disabled}
                    className="btn-danger"
                >
                    {purging && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
                    {purging ? 'Purging…' : 'Confirm & Purge Cluster'}
                </button>
                <button
                    type="button"
                    onClick={onCancel}
                    disabled={disabled}
                    className="btn-secondary"
                >
                    Cancel
                </button>
            </div>
        </div>
    )
}
