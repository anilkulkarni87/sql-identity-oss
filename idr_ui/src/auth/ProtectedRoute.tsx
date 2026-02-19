
import { useEffect } from "react";
import { useIDRAuth } from "./IDRAuthProvider";

export const ProtectedRoute = ({ children }: { children: React.ReactNode }) => {
    const auth = useIDRAuth();
    const authority = import.meta.env.VITE_AUTH_AUTHORITY?.trim();
    const allowInsecureDevAuth = import.meta.env.VITE_ALLOW_INSECURE_DEV_AUTH === 'true';
    const isDevBypass = !authority && allowInsecureDevAuth;

    useEffect(() => {
        if (!!authority && !auth.isAuthenticated && !auth.activeNavigator && !auth.isLoading && !auth.error) {
            auth.signinRedirect();
        }
    }, [auth, authority]);

    if (isDevBypass) {
        return <>{children}</>;
    }

    if (auth.isLoading) {
        return (
            <div className="flex items-center justify-center min-h-screen bg-gray-900 text-white">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
                <span className="ml-2">Authenticating...</span>
            </div>
        );
    }

    if (auth.error) {
        return (
            <div className="flex items-center justify-center min-h-screen bg-gray-900 text-white">
                <div className="bg-red-900/50 p-6 rounded-lg border border-red-700">
                    <h2 className="text-xl font-bold mb-2">Authentication Error</h2>
                    <p>{auth.error.message}</p>
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

    return <>{children}</>;
};
