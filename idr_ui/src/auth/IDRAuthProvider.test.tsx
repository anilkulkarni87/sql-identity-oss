import { render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

interface LoadedAuthModule {
    IDRAuthProvider: ({ children }: { children: React.ReactNode }) => JSX.Element
    useIDRAuth: () => {
        isAuthenticated: boolean
        hasPermission: (permission: string) => boolean
        principal: { authType: string } | null
        error?: Error
    }
}

async function loadAuthModule(): Promise<LoadedAuthModule> {
    vi.resetModules()
    return import('./IDRAuthProvider') as Promise<LoadedAuthModule>
}

afterEach(() => {
    vi.unstubAllEnvs()
    vi.resetModules()
})

describe('IDRAuthProvider', () => {
    it('fails closed when auth is not configured', async () => {
        vi.stubEnv('VITE_AUTH_AUTHORITY', '')
        vi.stubEnv('VITE_AUTH_CLIENT_ID', '')
        vi.stubEnv('VITE_ALLOW_INSECURE_DEV_AUTH', 'false')

        const { IDRAuthProvider, useIDRAuth } = await loadAuthModule()

        const Probe = () => {
            const auth = useIDRAuth()
            return (
                <div>
                    <p data-testid="is-auth">{String(auth.isAuthenticated)}</p>
                    <p data-testid="has-runs-read">{String(auth.hasPermission('runs.read'))}</p>
                    <p data-testid="error">{auth.error?.message || ''}</p>
                </div>
            )
        }

        render(
            <IDRAuthProvider>
                <Probe />
            </IDRAuthProvider>
        )

        expect(screen.getByTestId('is-auth')).toHaveTextContent('false')
        expect(screen.getByTestId('has-runs-read')).toHaveTextContent('false')
        expect(screen.getByTestId('error')).toHaveTextContent(/Authentication is not configured/i)
    })

    it('enables localhost-only insecure dev auth bypass when explicitly configured', async () => {
        vi.stubEnv('VITE_AUTH_AUTHORITY', '')
        vi.stubEnv('VITE_AUTH_CLIENT_ID', '')
        vi.stubEnv('VITE_ALLOW_INSECURE_DEV_AUTH', 'true')

        const { IDRAuthProvider, useIDRAuth } = await loadAuthModule()

        const Probe = () => {
            const auth = useIDRAuth()
            return (
                <div>
                    <p data-testid="is-auth">{String(auth.isAuthenticated)}</p>
                    <p data-testid="has-runs-read">{String(auth.hasPermission('runs.read'))}</p>
                    <p data-testid="auth-type">{auth.principal?.authType || ''}</p>
                </div>
            )
        }

        render(
            <IDRAuthProvider>
                <Probe />
            </IDRAuthProvider>
        )

        expect(screen.getByTestId('is-auth')).toHaveTextContent('true')
        expect(screen.getByTestId('has-runs-read')).toHaveTextContent('true')
        expect(screen.getByTestId('auth-type')).toHaveTextContent('dev_mode')
    })
})
