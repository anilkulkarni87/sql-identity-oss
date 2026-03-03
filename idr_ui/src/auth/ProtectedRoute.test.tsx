import { act, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import type { ReactNode } from 'react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { APIError } from '../api/client'
import type { APIAuthFailureEvent } from '../api/client'
import type { IDRAuthContextType } from './IDRAuthProvider'
import { ProtectedRoute } from './ProtectedRoute'

const useIDRAuthMock = vi.fn()
let authFailureListener: ((event: APIAuthFailureEvent) => void) | null = null

vi.mock('./IDRAuthProvider', () => ({
    useIDRAuth: () => useIDRAuthMock(),
}))

vi.mock('../api/client', async () => {
    const actual = await vi.importActual<typeof import('../api/client')>('../api/client')
    return {
        ...actual,
        onAPIAuthFailure: vi.fn((listener: (event: APIAuthFailureEvent) => void) => {
            authFailureListener = listener
            return () => {
                authFailureListener = null
            }
        }),
    }
})

function buildAuthContext(overrides: Partial<IDRAuthContextType> = {}): IDRAuthContextType {
    return {
        isAuthenticated: true,
        isLoading: false,
        error: undefined,
        user: null,
        signinRedirect: vi.fn().mockResolvedValue(undefined),
        signoutRedirect: vi.fn().mockResolvedValue(undefined),
        activeNavigator: undefined,
        principal: null,
        resolvedRoles: [],
        resolvedPermissions: [],
        isAuthorizing: false,
        authorizationError: undefined,
        hasPermission: () => false,
        hasAnyPermission: () => false,
        hasAllPermissions: () => false,
        ...overrides,
    }
}

afterEach(() => {
    vi.clearAllMocks()
    vi.unstubAllEnvs()
    authFailureListener = null
})

function renderProtected(children: ReactNode): void {
    render(
        <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
            <ProtectedRoute>{children}</ProtectedRoute>
        </MemoryRouter>
    )
}

describe('ProtectedRoute', () => {
    it('shows loading state while authenticating', () => {
        useIDRAuthMock.mockReturnValue(buildAuthContext({ isLoading: true }))

        renderProtected(<div>Protected Content</div>)

        expect(screen.getByText(/Authenticating/i)).toBeInTheDocument()
    })

    it('triggers signin redirect when unauthenticated and OIDC is configured', async () => {
        vi.stubEnv('VITE_AUTH_AUTHORITY', 'https://idp.example.com')
        vi.stubEnv('VITE_AUTH_CLIENT_ID', 'idr-ui')
        vi.stubEnv('VITE_ALLOW_INSECURE_DEV_AUTH', 'false')

        const signinRedirect = vi.fn().mockResolvedValue(undefined)
        useIDRAuthMock.mockReturnValue(
            buildAuthContext({
                isAuthenticated: false,
                signinRedirect,
            })
        )

        renderProtected(<div>Protected Content</div>)

        await waitFor(() => {
            expect(signinRedirect).toHaveBeenCalledTimes(1)
        })
        expect(screen.queryByText('Protected Content')).not.toBeInTheDocument()
    })

    it('renders 403 fallback when auth failure event is emitted', async () => {
        vi.stubEnv('VITE_AUTH_AUTHORITY', 'https://idp.example.com')
        vi.stubEnv('VITE_AUTH_CLIENT_ID', 'idr-ui')
        vi.stubEnv('VITE_ALLOW_INSECURE_DEV_AUTH', 'false')

        useIDRAuthMock.mockReturnValue(buildAuthContext({ hasAllPermissions: () => true }))

        renderProtected(<div>Protected Content</div>)

        act(() => {
            authFailureListener?.({
                status: 403,
                path: '/runs',
                error: new APIError(403, 'Forbidden by policy'),
            })
        })

        expect(await screen.findByRole('heading', { name: /Forbidden \(403\)/i })).toBeInTheDocument()
        expect(screen.getByText(/Forbidden by policy/i)).toBeInTheDocument()
        expect(screen.getByText(/\/runs/i)).toBeInTheDocument()
    })

    it('attempts re-authentication on 401 auth failure event', async () => {
        vi.stubEnv('VITE_AUTH_AUTHORITY', 'https://idp.example.com')
        vi.stubEnv('VITE_AUTH_CLIENT_ID', 'idr-ui')
        vi.stubEnv('VITE_ALLOW_INSECURE_DEV_AUTH', 'false')

        const signinRedirect = vi.fn().mockResolvedValue(undefined)
        useIDRAuthMock.mockReturnValue(
            buildAuthContext({
                signinRedirect,
            })
        )

        renderProtected(<div>Protected Content</div>)

        act(() => {
            authFailureListener?.({
                status: 401,
                path: '/metrics/summary',
                error: new APIError(401, 'Token expired'),
            })
        })

        await waitFor(() => {
            expect(signinRedirect).toHaveBeenCalled()
        })
        expect(screen.getByText(/session has expired/i)).toBeInTheDocument()
    })
})
