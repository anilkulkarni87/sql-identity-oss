import { useEffect } from 'react'
import { Loader2 } from 'lucide-react'
import { useIDRAuth } from './IDRAuthProvider'

export const ProtectedRoute = ({ children }: { children: React.ReactNode }) => {
    const auth = useIDRAuth()
    const authority = import.meta.env.VITE_AUTH_AUTHORITY?.trim()
    const allowInsecureDevAuth = import.meta.env.VITE_ALLOW_INSECURE_DEV_AUTH === 'true'
    const isDevBypass = !authority && allowInsecureDevAuth

    useEffect(() => {
        if (!!authority && !auth.isAuthenticated && !auth.activeNavigator && !auth.isLoading && !auth.error) {
            auth.signinRedirect()
        }
    }, [auth, authority])

    if (isDevBypass) {
        return <>{children}</>
    }

    if (auth.isLoading) {
        return (
            <div className="flex items-center justify-center min-h-screen bg-canvas text-ink gap-2 text-xs">
                <Loader2 className="w-4 h-4 animate-spin text-accent" />
                <span>Authenticating…</span>
            </div>
        )
    }

    if (auth.error) {
        return (
            <div className="flex items-center justify-center min-h-screen bg-canvas text-ink p-4">
                <div role="alert" className="surface-panel p-5 max-w-md w-full space-y-3 border-status-danger-border">
                    <h1 className="text-sm font-semibold text-status-danger">Authentication Error</h1>
                    <p className="text-xs text-ink-secondary">{auth.error.message}</p>
                    {!!authority && (
                        <button
                            type="button"
                            onClick={() => auth.signinRedirect()}
                            className="btn-primary text-xs"
                        >
                            Retry Login
                        </button>
                    )}
                </div>
            </div>
        )
    }

    if (!auth.isAuthenticated) {
        return null
    }

    return <>{children}</>
}
