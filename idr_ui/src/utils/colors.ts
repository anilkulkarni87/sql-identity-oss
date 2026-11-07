/**
 * Dynamic Entity & Identifier Color Utility
 * 
 * Generates deterministic, accessible, high-contrast color palettes for arbitrary
 * user-defined source tables and entity types without hardcoding table names.
 */

export interface ColorScheme {
    id: string
    name: string
    bg: string            // Main node background hex
    border: string        // Node border hex
    glow: string          // Soft highlight glow
    badgeBg: string       // Tailwind badge bg
    badgeText: string     // Tailwind badge text
    badgeBorder: string   // Tailwind badge border
    dotClass: string      // Tailwind dot color
}

export const ENTITY_PALETTE: ColorScheme[] = [
    {
        id: 'emerald',
        name: 'Emerald',
        bg: '#10b981',
        border: '#059669',
        glow: 'rgba(16, 185, 129, 0.4)',
        badgeBg: 'bg-emerald-500/15',
        badgeText: 'text-emerald-300',
        badgeBorder: 'border-emerald-500/30',
        dotClass: 'bg-emerald-400'
    },
    {
        id: 'blue',
        name: 'Blue',
        bg: '#3b82f6',
        border: '#2563eb',
        glow: 'rgba(59, 130, 246, 0.4)',
        badgeBg: 'bg-blue-500/15',
        badgeText: 'text-blue-300',
        badgeBorder: 'border-blue-500/30',
        dotClass: 'bg-blue-400'
    },
    {
        id: 'purple',
        name: 'Purple',
        bg: '#8b5cf6',
        border: '#7c3aed',
        glow: 'rgba(139, 92, 246, 0.4)',
        badgeBg: 'bg-purple-500/15',
        badgeText: 'text-purple-300',
        badgeBorder: 'border-purple-500/30',
        dotClass: 'bg-purple-400'
    },
    {
        id: 'amber',
        name: 'Amber',
        bg: '#f59e0b',
        border: '#d97706',
        glow: 'rgba(245, 158, 11, 0.4)',
        badgeBg: 'bg-amber-500/15',
        badgeText: 'text-amber-300',
        badgeBorder: 'border-amber-500/30',
        dotClass: 'bg-amber-400'
    },
    {
        id: 'rose',
        name: 'Rose',
        bg: '#f43f5e',
        border: '#e11d48',
        glow: 'rgba(244, 63, 94, 0.4)',
        badgeBg: 'bg-rose-500/15',
        badgeText: 'text-rose-300',
        badgeBorder: 'border-rose-500/30',
        dotClass: 'bg-rose-400'
    },
    {
        id: 'cyan',
        name: 'Cyan',
        bg: '#06b6d4',
        border: '#0891b2',
        glow: 'rgba(6, 182, 212, 0.4)',
        badgeBg: 'bg-cyan-500/15',
        badgeText: 'text-cyan-300',
        badgeBorder: 'border-cyan-500/30',
        dotClass: 'bg-cyan-400'
    },
    {
        id: 'orange',
        name: 'Orange',
        bg: '#f97316',
        border: '#ea580c',
        glow: 'rgba(249, 115, 22, 0.4)',
        badgeBg: 'bg-orange-500/15',
        badgeText: 'text-orange-300',
        badgeBorder: 'border-orange-500/30',
        dotClass: 'bg-orange-400'
    },
    {
        id: 'teal',
        name: 'Teal',
        bg: '#14b8a6',
        border: '#0d9488',
        glow: 'rgba(20, 184, 166, 0.4)',
        badgeBg: 'bg-teal-500/15',
        badgeText: 'text-teal-300',
        badgeBorder: 'border-teal-500/30',
        dotClass: 'bg-teal-400'
    },
    {
        id: 'fuchsia',
        name: 'Fuchsia',
        bg: '#d946ef',
        border: '#c026d3',
        glow: 'rgba(217, 70, 239, 0.4)',
        badgeBg: 'bg-fuchsia-500/15',
        badgeText: 'text-fuchsia-300',
        badgeBorder: 'border-fuchsia-500/30',
        dotClass: 'bg-fuchsia-400'
    },
    {
        id: 'lime',
        name: 'Lime',
        bg: '#84cc16',
        border: '#65a30d',
        glow: 'rgba(132, 204, 22, 0.4)',
        badgeBg: 'bg-lime-500/15',
        badgeText: 'text-lime-300',
        badgeBorder: 'border-lime-500/30',
        dotClass: 'bg-lime-400'
    },
    {
        id: 'indigo',
        name: 'Indigo',
        bg: '#6366f1',
        border: '#4f46e5',
        glow: 'rgba(99, 102, 241, 0.4)',
        badgeBg: 'bg-indigo-500/15',
        badgeText: 'text-indigo-300',
        badgeBorder: 'border-indigo-500/30',
        dotClass: 'bg-indigo-400'
    },
    {
        id: 'pink',
        name: 'Pink',
        bg: '#ec4899',
        border: '#db2777',
        glow: 'rgba(236, 72, 153, 0.4)',
        badgeBg: 'bg-pink-500/15',
        badgeText: 'text-pink-300',
        badgeBorder: 'border-pink-500/30',
        dotClass: 'bg-pink-400'
    }
]

/**
 * Deterministic hash of any string to an integer
 */
function hashString(str: string): number {
    let hash = 0
    for (let i = 0; i < str.length; i++) {
        hash = (hash << 5) - hash + str.charCodeAt(i)
        hash |= 0
    }
    return Math.abs(hash)
}

/**
 * Get color scheme for any arbitrary source_id or entity type.
 * If allSources is provided, uses index-based assignment so every entity type in the cluster
 * gets a distinct color from the palette.
 */
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

/**
 * Formats a source table/entity identifier into a human-friendly title.
 * Examples:
 * - "ecommerce_synth_loyalty_transactions" -> "Loyalty Transactions"
 * - "ecommerce_synth_orders_online" -> "Orders Online"
 * - "crm_customers" -> "CRM Customers"
 * - "salesforce.contacts" -> "Contacts"
 */
export function formatSourceName(sourceId: string): string {
    if (!sourceId) return 'Entity'

    // Remove table/catalog schema qualifiers if present
    let cleaned = sourceId.includes('.') ? sourceId.split('.').pop()! : sourceId

    // Remove common generic prefixes if there is more left
    const commonPrefixes = ['ecommerce_synth_', 'synth_', 'raw_', 'stg_', 'dim_', 'fact_', 'tbl_']
    for (const prefix of commonPrefixes) {
        if (cleaned.toLowerCase().startsWith(prefix) && cleaned.length > prefix.length) {
            cleaned = cleaned.slice(prefix.length)
            break
        }
    }

    // Replace underscores and hyphens with spaces
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

/**
 * Get a short 1-2 letter acronym badge for a source (e.g. Loyalty Transactions -> LT)
 */
export function getSourceInitials(sourceId: string): string {
    const formatted = formatSourceName(sourceId)
    const words = formatted.split(/\s+/).filter(Boolean)
    if (words.length >= 2) {
        return (words[0][0] + words[1][0]).toUpperCase()
    }
    return formatted.slice(0, 2).toUpperCase()
}

/**
 * Get badge styling for identifier types (email, phone, etc.)
 */
export function getIdentifierBadge(type: string): {
    badgeBg: string
    badgeText: string
    border: string
} {
    const lower = (type || '').toLowerCase()
    if (lower.includes('email')) {
        return {
            badgeBg: 'bg-purple-900/40',
            badgeText: 'text-purple-300',
            border: 'border-purple-700/60'
        }
    }
    if (lower.includes('phone')) {
        return {
            badgeBg: 'bg-emerald-900/40',
            badgeText: 'text-emerald-300',
            border: 'border-emerald-700/60'
        }
    }
    if (lower.includes('customer') || lower.includes('user') || lower.includes('client')) {
        return {
            badgeBg: 'bg-blue-900/40',
            badgeText: 'text-blue-300',
            border: 'border-blue-700/60'
        }
    }
    if (lower.includes('device') || lower.includes('session') || lower.includes('cookie')) {
        return {
            badgeBg: 'bg-amber-900/40',
            badgeText: 'text-amber-300',
            border: 'border-amber-700/60'
        }
    }
    if (lower.includes('account') || lower.includes('order')) {
        return {
            badgeBg: 'bg-cyan-900/40',
            badgeText: 'text-cyan-300',
            border: 'border-cyan-700/60'
        }
    }
    return {
        badgeBg: 'bg-gray-800',
        badgeText: 'text-gray-300',
        border: 'border-gray-700'
    }
}

/**
 * Formats attribute keys into Title Case.
 * Example: CUSTOMER_SEGMENT -> Customer Segment
 */
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

