
import { useEffect, useRef, useState } from "react";
import { useIDRAuth } from "./IDRAuthProvider";
import { onAPIAuthFailure } from "../api/client";
import AccessDenied from "../components/AccessDenied";
import { safeErrorMessage } from "../security/redaction";

export const ProtectedRoute = ({ children }: { children: React.ReactNode }) => {
    const auth = useIDRAuth();
    const authority = import.meta.env.VITE_AUTH_AUTHORITY?.trim();
    const clientId = import.meta.env.VITE_AUTH_CLIENT_ID?.trim();
    const allowInsecureDevAuth = import.meta.env.VITE_ALLOW_INSECURE_DEV_AUTH === 'true';
    const isLocalDevHost = ['localhost', '127.0.0.1', '::1'].includes(window.location.hostname);
    const isDevBypass = (!authority || !clientId) && allowInsecureDevAuth && isLocalDevHost;
    const [sessionExpiredMessage, setSessionExpiredMessage] = useState<string | null>(null);
    const [forbiddenState, setForbiddenState] = useState<{ detail: string; path: string } | null>(null);
    const redirectInFlight = useRef(false);

    useEffect(() => {
        if (!!authority && !auth.isAuthenticated && !auth.activeNavigator && !auth.isLoading && !auth.error) {
            auth.signinRedirect();
        }
    }, [auth, authority]);

    useEffect(() => {
        const unsubscribe = onAPIAuthFailure(({ status, path, error }) => {
            if (status === 401) {
                setForbiddenState(null);
                setSessionExpiredMessage("Your session has expired. Redirecting to sign in...");

                if (
                    !!authority &&
                    !isDevBypass &&
                    !redirectInFlight.current &&
                    !auth.activeNavigator &&
                    !auth.isLoading
                ) {
                    redirectInFlight.current = true;
                    auth.signinRedirect()
                        .catch((reauthError: unknown) => {
                            const message = safeErrorMessage(reauthError, "Automatic sign-in failed.");
                            setSessionExpiredMessage(`Session expired and re-authentication failed: ${message}`);
                        })
                        .finally(() => {
                            window.setTimeout(() => {
                                redirectInFlight.current = false;
                            }, 1500);
                        });
                }
                return;
            }

            if (status === 403 && path !== '/auth/whoami') {
                setSessionExpiredMessage(null);
                setForbiddenState({
                    detail: error.detail || "You do not have permission to perform this action.",
                    path,
                });
            }
        });

        return unsubscribe;
    }, [auth, authority, isDevBypass]);

    useEffect(() => {
        if (auth.isAuthenticated && !auth.isLoading) {
            setSessionExpiredMessage(null);
        }
    }, [auth.isAuthenticated, auth.isLoading]);

    if (isDevBypass) {
        return <>{children}</>;
    }

    if (forbiddenState) {
        return (
            <div className="min-h-screen bg-gray-900 text-white px-4 py-10">
                <AccessDenied
                    title="Forbidden (403)"
                    message={forbiddenState.detail}
                />
                <div className="max-w-3xl mx-auto mt-4 space-y-3">
                    <p className="text-xs text-gray-400">
                        Request: <span className="font-mono">{forbiddenState.path}</span>
                    </p>
                    <div className="flex gap-3">
                        <button
                            onClick={() => setForbiddenState(null)}
                            className="px-4 py-2 rounded bg-gray-700 hover:bg-gray-600 transition-colors text-sm"
                        >
                            Dismiss
                        </button>
                        {!!authority && (
                            <button
                                onClick={() => auth.signinRedirect()}
                                className="px-4 py-2 rounded bg-blue-600 hover:bg-blue-700 transition-colors text-sm"
                            >
                                Re-authenticate
                            </button>
                        )}
                    </div>
                </div>
            </div>
        );
    }

    if (auth.isLoading) {
        return (
            <div className="flex items-center justify-center min-h-screen bg-gray-900 text-white">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
                <span className="ml-2">{sessionExpiredMessage || "Authenticating..."}</span>
            </div>
        );
    }

    if (auth.error) {
        return (
            <div className="flex items-center justify-center min-h-screen bg-gray-900 text-white">
                <div className="bg-red-900/50 p-6 rounded-lg border border-red-700">
                    <h2 className="text-xl font-bold mb-2">Authentication Error</h2>
                    <p>{safeErrorMessage(auth.error, "Authentication failed.")}</p>
                    {!!authority && (
                        <button
                            onClick={() => auth.signinRedirect()}
                            className="mt-4 px-4 py-2 bg-red-600 hover:bg-red-700 rounded transition-colors"
                        >
                            Retry Login
                        </button>
                    )}
                </div>
            </div>
        );
    }

    if (!auth.isAuthenticated) {
        return null; // Will redirect via useEffect
    }

    return (
        <>
            {sessionExpiredMessage && (
                <div className="fixed top-0 left-0 right-0 z-50 bg-yellow-500/20 border-b border-yellow-500/40 text-yellow-100 text-sm px-4 py-2 text-center">
                    {sessionExpiredMessage}
                </div>
            )}
            {children}
        </>
    );
};
