import React, { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { AuthProvider, AuthProviderProps, useAuth } from "react-oidc-context";
import { User } from "oidc-client-ts";
import { api, setTokenGetter, WhoAmIResponse } from "../api/client";

const authority = import.meta.env.VITE_AUTH_AUTHORITY?.trim();
const clientId = import.meta.env.VITE_AUTH_CLIENT_ID?.trim();
const allowInsecureDevAuth = import.meta.env.VITE_ALLOW_INSECURE_DEV_AUTH === 'true';
const isLocalDevHost = ['localhost', '127.0.0.1', '::1'].includes(window.location.hostname);
const enableInsecureDevAuth = allowInsecureDevAuth && isLocalDevHost;

const oidcConfig: AuthProviderProps = {
    authority: authority || "",
    client_id: clientId || "",
    redirect_uri: window.location.origin,
    onSigninCallback: (_user: User | void) => {
        window.history.replaceState({}, document.title, window.location.pathname);
    },
    automaticSilentRenew: true,
    response_type: "code",
    revokeTokensOnSignout: true,
};

// Define an interface that matches what we need from auth
export interface IDRPrincipal {
    sub: string | null;
    authType: string;
    roles: string[];
    permissions: string[];
    scope: string | null;
}

export interface IDRAuthContextType {
    isAuthenticated: boolean;
    isLoading: boolean;
    error?: Error;
    user?: User | null;
    signinRedirect: () => Promise<void>;
    signoutRedirect: () => Promise<void>;
    activeNavigator?: string;
    principal: IDRPrincipal | null;
    resolvedRoles: string[];
    resolvedPermissions: string[];
    isAuthorizing: boolean;
    authorizationError?: Error;
    hasPermission: (permission: string) => boolean;
    hasAnyPermission: (permissions: string[]) => boolean;
    hasAllPermissions: (permissions: string[]) => boolean;
}

const IDRAuthContext = React.createContext<IDRAuthContextType | null>(null);

const toStringArray = (value: unknown): string[] => {
    if (!Array.isArray(value)) return [];
    const values = value.map((item) => String(item).trim()).filter(Boolean);
    return Array.from(new Set(values)).sort();
};

const normalizePrincipal = (payload: WhoAmIResponse): IDRPrincipal => {
    return {
        sub: payload.sub ?? null,
        authType: payload.auth_type || "oidc_or_dev",
        roles: toStringArray(payload.roles),
        permissions: toStringArray(payload.permissions),
        scope: payload.scope ?? null,
    };
};

const permissionMatches = (grantedPermissions: string[], requiredPermission: string): boolean => {
    if (!requiredPermission.trim()) {
        return false;
    }

    if (grantedPermissions.includes("*") || grantedPermissions.includes(requiredPermission)) {
        return true;
    }

    return grantedPermissions.some((candidate) => {
        if (!candidate.endsWith(".*")) return false;
        const prefix = candidate.slice(0, -2);
        return requiredPermission === prefix || requiredPermission.startsWith(`${prefix}.`);
    });
};

const noop = async () => { };

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
    const [principal, setPrincipal] = useState<IDRPrincipal | null>(null);
    const [isAuthorizing, setIsAuthorizing] = useState(false);
    const [authorizationError, setAuthorizationError] = useState<Error | undefined>(undefined);

    useEffect(() => {
        if (auth.isAuthenticated && auth.user?.access_token) {
            setTokenGetter(() => auth.user?.access_token);
            return;
        }
        setTokenGetter(() => undefined);
    }, [auth.isAuthenticated, auth.user?.access_token]);

    useEffect(() => {
        if (!auth.isAuthenticated || !auth.user?.access_token) {
            setPrincipal(null);
            setIsAuthorizing(false);
            setAuthorizationError(undefined);
            return;
        }

        let cancelled = false;
        setIsAuthorizing(true);
        setAuthorizationError(undefined);

        api.getWhoAmI()
            .then((payload) => {
                if (!cancelled) {
                    setPrincipal(normalizePrincipal(payload));
                }
            })
            .catch((err: unknown) => {
                if (cancelled) return;
                setPrincipal(null);
                setAuthorizationError(
                    err instanceof Error ? err : new Error("Unable to resolve user permissions.")
                );
            })
            .finally(() => {
                if (!cancelled) {
                    setIsAuthorizing(false);
                }
            });

        return () => {
            cancelled = true;
        };
    }, [auth.isAuthenticated, auth.user?.access_token]);

    const resolvedPermissions = useMemo(() => principal?.permissions || [], [principal]);
    const resolvedRoles = useMemo(() => principal?.roles || [], [principal]);

    const hasPermission = useCallback(
        (permission: string) => permissionMatches(resolvedPermissions, permission),
        [resolvedPermissions]
    );

    const hasAnyPermission = useCallback(
        (permissions: string[]) => permissions.some((permission) => hasPermission(permission)),
        [hasPermission]
    );

    const hasAllPermissions = useCallback(
        (permissions: string[]) => permissions.every((permission) => hasPermission(permission)),
        [hasPermission]
    );

    const contextValue: IDRAuthContextType = {
        isAuthenticated: auth.isAuthenticated,
        isLoading: auth.isLoading,
        error: auth.error,
        user: auth.user,
        signinRedirect: auth.signinRedirect as () => Promise<void>,
        signoutRedirect: auth.signoutRedirect as () => Promise<void>,
        activeNavigator: auth.activeNavigator,
        principal,
        resolvedRoles,
        resolvedPermissions,
        isAuthorizing,
        authorizationError,
        hasPermission,
        hasAnyPermission,
        hasAllPermissions,
    };

    return (
        <IDRAuthContext.Provider value={contextValue}>
            {children}
        </IDRAuthContext.Provider>
    );
};

const StaticAuthContextProvider = ({
    value,
    token,
    children,
}: {
    value: IDRAuthContextType;
    token?: string;
    children: React.ReactNode;
}) => {
    useEffect(() => {
        if (token) {
            setTokenGetter(() => token);
            return;
        }
        setTokenGetter(() => undefined);
    }, [token]);

    return <IDRAuthContext.Provider value={value}>{children}</IDRAuthContext.Provider>;
};

export const IDRAuthProvider = ({ children }: { children: React.ReactNode }) => {
    // Explicit insecure local override only.
    if ((!authority || !clientId) && enableInsecureDevAuth) {
        const devAuth: IDRAuthContextType = {
            isAuthenticated: true,
            isLoading: false,
            user: null,
            signinRedirect: noop,
            signoutRedirect: noop,
            principal: {
                sub: "dev-user",
                authType: "dev_mode",
                roles: ["admin"],
                permissions: ["*"],
                scope: "*",
            },
            resolvedRoles: ["admin"],
            resolvedPermissions: ["*"],
            isAuthorizing: false,
            hasPermission: () => true,
            hasAnyPermission: () => true,
            hasAllPermissions: () => true,
        };
        return (
            <StaticAuthContextProvider value={devAuth}>
                {children}
            </StaticAuthContextProvider>
        );
    }

    // Fail closed when auth is not configured.
    if (!authority || !clientId) {
        const authConfigError: IDRAuthContextType = {
            isAuthenticated: false,
            isLoading: false,
            user: null,
            error: new Error(
                "Authentication is not configured. Set VITE_AUTH_AUTHORITY and VITE_AUTH_CLIENT_ID, or explicitly enable VITE_ALLOW_INSECURE_DEV_AUTH=true on localhost for local development."
            ),
            signinRedirect: noop,
            signoutRedirect: noop,
            principal: null,
            resolvedRoles: [],
            resolvedPermissions: [],
            isAuthorizing: false,
            hasPermission: () => false,
            hasAnyPermission: () => false,
            hasAllPermissions: () => false,
        };
        return (
            <StaticAuthContextProvider value={authConfigError}>
                {children}
            </StaticAuthContextProvider>
        );
    }

    return (
        <AuthProvider {...oidcConfig}>
            <AuthBridge>{children}</AuthBridge>
        </AuthProvider>
    );
};
