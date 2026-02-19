
import React, { useContext } from 'react';
import { AuthProvider, AuthProviderProps, useAuth } from "react-oidc-context";
import { User } from "oidc-client-ts";

const authority = import.meta.env.VITE_AUTH_AUTHORITY?.trim();
const clientId = import.meta.env.VITE_AUTH_CLIENT_ID?.trim();
const allowInsecureDevAuth = import.meta.env.VITE_ALLOW_INSECURE_DEV_AUTH === 'true';

const oidcConfig: AuthProviderProps = {
    authority: authority || "",
    client_id: clientId || "",
    redirect_uri: window.location.origin,
    onSigninCallback: (_user: User | void) => {
        window.history.replaceState({}, document.title, window.location.pathname);
    },
    automaticSilentRenew: true,
};

// Define an interface that matches what we need from auth
export interface IDRAuthContextType {
    isAuthenticated: boolean;
    isLoading: boolean;
    error?: Error;
    user?: User | null;
    signinRedirect: () => Promise<void>;
    signoutRedirect: () => Promise<void>;
    activeNavigator?: string;
}

const IDRAuthContext = React.createContext<IDRAuthContextType | null>(null);

export const useIDRAuth = () => {
    const context = useContext(IDRAuthContext);
    if (!context) {
        throw new Error("useIDRAuth must be used within IDRAuthProvider");
    }
    return context;
};

// Wrapper to extract useAuth from oidc-context and pass it to our context
const AuthBridge = ({ children }: { children: React.ReactNode }) => {
    const auth = useAuth();
    return (
        <IDRAuthContext.Provider value={auth as IDRAuthContextType}>
            {children}
        </IDRAuthContext.Provider>
    );
};

export const IDRAuthProvider = ({ children }: { children: React.ReactNode }) => {
    // Explicit insecure local override only.
    if ((!authority || !clientId) && allowInsecureDevAuth) {
        const devAuth: IDRAuthContextType = {
            isAuthenticated: true,
            isLoading: false,
            user: { access_token: "dev-token" } as User,
            signinRedirect: async () => { },
            signoutRedirect: async () => { },
        };
        return (
            <IDRAuthContext.Provider value={devAuth}>
                {children}
            </IDRAuthContext.Provider>
        );
    }

    // Fail closed when auth is not configured.
    if (!authority || !clientId) {
        const authConfigError: IDRAuthContextType = {
            isAuthenticated: false,
            isLoading: false,
            user: null,
            error: new Error(
                "Authentication is not configured. Set VITE_AUTH_AUTHORITY and VITE_AUTH_CLIENT_ID, or explicitly enable VITE_ALLOW_INSECURE_DEV_AUTH=true for local development."
            ),
            signinRedirect: async () => { },
            signoutRedirect: async () => { },
        };
        return (
            <IDRAuthContext.Provider value={authConfigError}>
                {children}
            </IDRAuthContext.Provider>
        );
    }

    return (
        <AuthProvider {...oidcConfig}>
            <AuthBridge>{children}</AuthBridge>
        </AuthProvider>
    );
};
