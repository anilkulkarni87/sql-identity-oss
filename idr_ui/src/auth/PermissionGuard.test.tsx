import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { APIError } from '../api/client'
import { PermissionGuard } from './PermissionGuard'

const useIDRAuthMock = vi.fn()

vi.mock('./IDRAuthProvider', () => ({
    useIDRAuth: () => useIDRAuthMock(),
}))

describe('PermissionGuard', () => {
    it('shows resolving state while authorization is in progress', () => {
        useIDRAuthMock.mockReturnValue({
            isAuthorizing: true,
            authorizationError: undefined,
            hasAllPermissions: vi.fn(),
        })

        render(
            <PermissionGuard requiredPermissions={['runs.read']}>
                <div>Visible Content</div>
            </PermissionGuard>
        )

        expect(screen.getByText(/Resolving access permissions/i)).toBeInTheDocument()
    })

    it('renders session-expired state on 401 authorization error', () => {
        useIDRAuthMock.mockReturnValue({
            isAuthorizing: false,
            authorizationError: new APIError(401, 'Unauthorized'),
            hasAllPermissions: vi.fn(),
        })

        render(
            <PermissionGuard requiredPermissions={['runs.read']}>
                <div>Visible Content</div>
            </PermissionGuard>
        )

        expect(screen.getByRole('heading', { name: /Session Expired/i })).toBeInTheDocument()
    })

    it('renders denied state when required permissions are missing', () => {
        useIDRAuthMock.mockReturnValue({
            isAuthorizing: false,
            authorizationError: undefined,
            hasAllPermissions: vi.fn().mockReturnValue(false),
        })

        render(
            <PermissionGuard
                requiredPermissions={['runs.read']}
                title="Runs Denied"
                message="You need runs.read."
            >
                <div>Visible Content</div>
            </PermissionGuard>
        )

        expect(screen.getByRole('heading', { name: 'Runs Denied' })).toBeInTheDocument()
        expect(screen.getByText('You need runs.read.')).toBeInTheDocument()
        expect(screen.queryByText('Visible Content')).not.toBeInTheDocument()
    })

    it('renders children when all permissions are present', () => {
        useIDRAuthMock.mockReturnValue({
            isAuthorizing: false,
            authorizationError: undefined,
            hasAllPermissions: vi.fn().mockReturnValue(true),
        })

        render(
            <PermissionGuard requiredPermissions={['runs.read']}>
                <div>Visible Content</div>
            </PermissionGuard>
        )

        expect(screen.getByText('Visible Content')).toBeInTheDocument()
    })
})
