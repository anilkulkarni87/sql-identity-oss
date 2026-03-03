const TOKEN_PATTERNS: RegExp[] = [
    /(Bearer\s+)[A-Za-z0-9\-._~+/]+=*/gi,
    /\b(access_token|id_token|refresh_token)\b(["'=:\s]+)([^\s,;"']+)/gi,
    /\b[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/g,
]

export function redactSensitiveText(value: string): string {
    let sanitized = value
    for (const pattern of TOKEN_PATTERNS) {
        sanitized = sanitized.replace(pattern, (_match, ...groups: unknown[]) => {
            if (groups.length >= 2 && typeof groups[0] === 'string' && typeof groups[1] === 'string') {
                return `${groups[0]}${groups[1]}[REDACTED]`
            }
            return '[REDACTED]'
        })
    }
    return sanitized
}

export function safeErrorMessage(error: unknown, fallback: string): string {
    if (typeof error === 'string' && error.trim()) {
        return redactSensitiveText(error.trim())
    }

    if (error instanceof Error && error.message.trim()) {
        return redactSensitiveText(error.message.trim())
    }

    return fallback
}
