import { afterEach, describe, expect, it, vi } from 'vitest'
import { api, APIError, apiRequest, onAPIAuthFailure, setTokenGetter } from './client'

function mockFetchResponse(response: Response): void {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response))
}

describe('api client', () => {
    afterEach(() => {
        setTokenGetter(() => undefined)
        vi.unstubAllGlobals()
        vi.restoreAllMocks()
    })

    it('injects bearer auth and JSON content-type headers when token is available', async () => {
        setTokenGetter(() => 'token-123')
        mockFetchResponse(new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } }))

        await apiRequest('/health', {
            method: 'POST',
            body: JSON.stringify({ ping: true }),
        })

        const fetchMock = vi.mocked(fetch)
        expect(fetchMock).toHaveBeenCalledTimes(1)
        const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit]
        expect(url).toBe('/api/health')

        const headers = init.headers as Headers
        expect(headers.get('Authorization')).toBe('Bearer token-123')
        expect(headers.get('Content-Type')).toBe('application/json')
    })

    it('parses JSON API errors and raises APIError', async () => {
        mockFetchResponse(
            new Response(JSON.stringify({ detail: 'Database not connected' }), {
                status: 400,
                headers: { 'content-type': 'application/json' },
            })
        )

        await expect(api.getHealth()).rejects.toMatchObject({
            name: 'APIError',
            status: 400,
            detail: 'Database not connected',
        })
    })

    it('redacts sensitive values in error payloads', async () => {
        mockFetchResponse(
            new Response(JSON.stringify({ message: 'Bearer super-secret-token-value' }), {
                status: 500,
                headers: { 'content-type': 'application/json' },
            })
        )

        await expect(api.getHealth()).rejects.toSatisfy((error: unknown) => {
            if (!(error instanceof APIError)) return false
            expect(error.detail).toContain('[REDACTED]')
            expect(error.detail).not.toContain('super-secret-token-value')
            return true
        })
    })

    it('emits auth failure events for 401 and 403 responses', async () => {
        const listener = vi.fn()
        const unsubscribe = onAPIAuthFailure(listener)

        mockFetchResponse(
            new Response(JSON.stringify({ detail: 'Unauthorized' }), {
                status: 401,
                headers: { 'content-type': 'application/json' },
            })
        )
        await expect(api.getHealth()).rejects.toBeInstanceOf(APIError)

        mockFetchResponse(
            new Response(JSON.stringify({ detail: 'Forbidden' }), {
                status: 403,
                headers: { 'content-type': 'application/json' },
            })
        )
        await expect(api.getHealth()).rejects.toBeInstanceOf(APIError)

        expect(listener).toHaveBeenCalledTimes(2)
        expect(listener.mock.calls[0][0]).toMatchObject({ status: 401, path: '/health' })
        expect(listener.mock.calls[1][0]).toMatchObject({ status: 403, path: '/health' })

        unsubscribe()
    })

    it('does not emit auth failure after listener unsubscribe', async () => {
        const listener = vi.fn()
        const unsubscribe = onAPIAuthFailure(listener)
        unsubscribe()

        mockFetchResponse(
            new Response(JSON.stringify({ detail: 'Unauthorized' }), {
                status: 401,
                headers: { 'content-type': 'application/json' },
            })
        )
        await expect(api.getHealth()).rejects.toBeInstanceOf(APIError)

        expect(listener).not.toHaveBeenCalled()
    })
})
