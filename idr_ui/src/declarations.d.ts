/// <reference types="vite/client" />

interface ImportMetaEnv {
    readonly VITE_API_BASE_URL?: string
    readonly VITE_AUTH_AUTHORITY?: string
    readonly VITE_AUTH_CLIENT_ID?: string
    readonly VITE_ALLOW_INSECURE_DEV_AUTH?: string
}

interface ImportMeta {
    readonly env: ImportMetaEnv
}

declare module 'react-cytoscapejs';
