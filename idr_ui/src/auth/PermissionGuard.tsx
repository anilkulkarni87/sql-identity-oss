import React from 'react'
import { APIError } from '../api/client'
import AccessDenied from '../components/AccessDenied'
import { useIDRAuth } from './IDRAuthProvider'

interface PermissionGuardProps {
    children: React.ReactNode
    requiredPermissions: string[]
    title?: string
    message?: string
}

export function PermissionGuard({
    children,
    requiredPermissions,
    title,
    message,
}: PermissionGuardProps) {
    const auth = useIDRAuth()

    if (auth.isAuthorizing) {
        return (
            <div className="flex items-center justify-center min-h-[240px] text-gray-300">
                Resolving access permissions...
            </div>
        )
    }

    if (auth.authorizationError) {
        if (auth.authorizationError instanceof APIError && auth.authorizationError.status === 401) {
            return (
                <AccessDenied
                    title="Session Expired"
                    message="Your session is no longer valid. Please sign in again."
                    requiredPermissions={requiredPermissions}
                />
            )
        }

        if (auth.authorizationError instanceof APIError && auth.authorizationError.status === 403) {
            return (
                <AccessDenied
                    title="Authorization Denied"
                    message={auth.authorizationError.detail}
                    requiredPermissions={requiredPermissions}
                />
            )
        }

        return (
            <AccessDenied
                title="Authorization Error"
                message={auth.authorizationError.message}
                requiredPermissions={requiredPermissions}
            />
        )
    }

    if (!auth.hasAllPermissions(requiredPermissions)) {
        return (
            <AccessDenied
                title={title}
                message={message}
                requiredPermissions={requiredPermissions}
            />
        )
    }

    return <>{children}</>
}
