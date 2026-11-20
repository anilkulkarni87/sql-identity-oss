export interface ColorScheme {
    id: string
    name: string
    bg: string
    border: string
    glow: string
    badgeBg: string
    badgeText: string
    badgeBorder: string
    dotClass: string
}

export const ENTITY_PALETTE: ColorScheme[] = [
    {
        id: 'cobalt',
        name: 'Cobalt',
        bg: '#1d4ed8',
        border: '#1e40af',
        glow: 'rgba(29, 78, 216, 0.25)',
        badgeBg: 'bg-surface',
        badgeText: 'text-ink-secondary',
        badgeBorder: 'border-line-strong',
        dotClass: 'bg-[#1d4ed8]'
    },
    {
        id: 'teal',
        name: 'Teal',
        bg: '#0f766e',
        border: '#115e59',
        glow: 'rgba(15, 118, 110, 0.25)',
        badgeBg: 'bg-surface',
        badgeText: 'text-ink-secondary',
        badgeBorder: 'border-line-strong',
        dotClass: 'bg-[#0f766e]'
    },
    {
        id: 'amber',
        name: 'Amber',
        bg: '#b45309',
        border: '#92400e',
        glow: 'rgba(180, 83, 9, 0.25)',
        badgeBg: 'bg-surface',
        badgeText: 'text-ink-secondary',
        badgeBorder: 'border-line-strong',
        dotClass: 'bg-[#d97706]'
    },
    {
        id: 'violet',
        name: 'Violet',
        bg: '#6d28d9',
        border: '#5b21b6',
        glow: 'rgba(109, 40, 217, 0.25)',
        badgeBg: 'bg-surface',
        badgeText: 'text-ink-secondary',
        badgeBorder: 'border-line-strong',
        dotClass: 'bg-[#7c3aed]'
    },
    {
        id: 'rose',
        name: 'Rose',
        bg: '#be123c',
        border: '#9f1239',
        glow: 'rgba(190, 18, 60, 0.25)',
        badgeBg: 'bg-surface',
        badgeText: 'text-ink-secondary',
        badgeBorder: 'border-line-strong',
        dotClass: 'bg-[#e11d48]'
    },
    {
        id: 'slate',
        name: 'Slate',
        bg: '#475569',
        border: '#334155',
        glow: 'rgba(71, 85, 105, 0.25)',
        badgeBg: 'bg-surface',
        badgeText: 'text-ink-secondary',
        badgeBorder: 'border-line-strong',
        dotClass: 'bg-[#64748b]'
    }
]

function hashString(str: string): number {
    let hash = 0
    for (let i = 0; i < str.length; i++) {
        hash = (hash << 5) - hash + str.charCodeAt(i)
        hash |= 0
    }
    return Math.abs(hash)
}

export function getEntityColor(sourceId: string, allSources?: string[]): ColorScheme {
    if (!sourceId) {
        return ENTITY_PALETTE[0]
    }
    if (allSources && allSources.length > 0) {
        const sorted = [...new Set(allSources)].sort()
        const idx = sorted.indexOf(sourceId)
        if (idx !== -1) {
            return ENTITY_PALETTE[idx % ENTITY_PALETTE.length]
        }
    }
    const hash = hashString(sourceId)
    return ENTITY_PALETTE[hash % ENTITY_PALETTE.length]
}

export function formatSourceName(sourceId: string): string {
    if (!sourceId) return 'Entity'
    let cleaned = sourceId.includes('.') ? sourceId.split('.').pop()! : sourceId
    const commonPrefixes = ['ecommerce_synth_', 'synth_', 'raw_', 'stg_', 'dim_', 'fact_', 'tbl_']
    for (const prefix of commonPrefixes) {
        if (cleaned.toLowerCase().startsWith(prefix) && cleaned.length > prefix.length) {
            cleaned = cleaned.slice(prefix.length)
            break
        }
    }
    const words = cleaned.replace(/[_-]/g, ' ').trim().split(/\s+/)
    return words
        .map(w => {
            const upper = w.toUpperCase()
            if (['CRM', 'ID', 'UUID', 'IP', 'POS', 'API', 'SSN', 'ERP'].includes(upper)) {
                return upper
            }
            return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()
        })
        .join(' ')
}

export function getSourceInitials(sourceId: string): string {
    const formatted = formatSourceName(sourceId)
    const words = formatted.split(/\s+/).filter(Boolean)
    if (words.length >= 2) {
        return (words[0][0] + words[1][0]).toUpperCase()
    }
    return formatted.slice(0, 2).toUpperCase()
}

export function getIdentifierBadge(type: string): {
    badgeBg: string
    badgeText: string
    border: string
} {
    const lower = (type || '').toLowerCase()
    if (lower.includes('fuzzy') || lower.includes('prob')) {
        return {
            badgeBg: 'bg-status-warning-soft',
            badgeText: 'text-status-warning',
            border: 'border-status-warning-border border-dashed'
        }
    }
    if (lower.includes('email') || lower.includes('phone') || lower.includes('customer') || lower.includes('user')) {
        return {
            badgeBg: 'bg-accent-soft',
            badgeText: 'text-accent',
            border: 'border-accent-border'
        }
    }
    return {
        badgeBg: 'bg-subtle',
        badgeText: 'text-ink-secondary',
        border: 'border-line-strong'
    }
}

export function formatAttributeKey(key: string): string {
    if (!key) return ''
    return key
        .replace(/[_-]/g, ' ')
        .split(/\s+/)
        .map(w => {
            const upper = w.toUpperCase()
            if (['ID', 'IP', 'URL', 'VIP', 'DOB', 'SSN', 'OS'].includes(upper)) {
                return upper
            }
            return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()
        })
        .join(' ')
}
