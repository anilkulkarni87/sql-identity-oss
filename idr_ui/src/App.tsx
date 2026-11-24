import { lazy, Suspense, useEffect, useRef, useState } from 'react'
import { Routes, Route, Link, useLocation } from 'react-router-dom'
import {
    LayoutDashboard, Network, History, Settings as SettingsIcon,
    Database, SlidersHorizontal, PanelLeftClose, PanelLeftOpen,
    Menu, X, Sun, Moon, Play, Bot, Loader2
} from 'lucide-react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { api, setTokenGetter } from './api/client'
import { useIDRAuth } from './auth/IDRAuthProvider'
import { ToastProvider } from './components/Toast'
import { ThemeProvider, useTheme } from './utils/theme'
import { RunPipelineDrawerProvider, useRunDrawer } from './components/RunPipelineDrawer'
import AICopilotDrawer from './components/AICopilotDrawer'

const Dashboard = lazy(() => import('./pages/Dashboard'))
const Explorer = lazy(() => import('./pages/Explorer'))
const Runs = lazy(() => import('./pages/Runs'))
const Settings = lazy(() => import('./pages/Settings'))
const SetupWizard = lazy(() => import('./pages/Setup/SetupWizard'))
const DataModel = lazy(() => import('./pages/DataModel'))

const NAV_ITEMS = [
    { to: '/', label: 'Dashboard', icon: LayoutDashboard, section: 'Observe' },
    { to: '/explorer', label: 'Explorer', icon: Network, section: 'Observe' },
    { to: '/runs', label: 'Run History', icon: History, section: 'Observe' },
    { to: '/model', label: 'Data Model', icon: Database, section: 'Configure' },
    { to: '/setup', label: 'Identity Studio', icon: SlidersHorizontal, section: 'Configure' },
    { to: '/settings', label: 'Settings', icon: SettingsIcon, section: 'Configure' },
] as const

function RouteFallback() {
    return (
        <div className="space-y-4 py-2" aria-busy="true" aria-label="Loading view">
            <div className="h-6 w-44 rounded bg-subtle animate-pulse" />
            <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
                {[0, 1, 2, 3].map((i) => (
                    <div key={i} className="h-20 rounded-md bg-subtle border border-line animate-pulse" />
                ))}
            </div>
            <div className="h-64 rounded-md bg-subtle border border-line animate-pulse" />
        </div>
    )
}

function AppShell() {
    const location = useLocation()
    const isActive = (path: string) => location.pathname === path
    const auth = useIDRAuth()
    const { theme, toggleTheme } = useTheme()
    const { openRunDrawer } = useRunDrawer()

    const [collapsed, setCollapsed] = useState(false)
    const [mobileOpen, setMobileOpen] = useState(false)
    const [copilotOpen, setCopilotOpen] = useState(false)

    useEffect(() => {
        const onKeyDown = (e: KeyboardEvent) => {
            if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
                e.preventDefault()
                setCopilotOpen((prev) => !prev)
            }
        }
        window.addEventListener('keydown', onKeyDown)
        return () => window.removeEventListener('keydown', onKeyDown)
    }, [])

    useEffect(() => {
        if (auth.isAuthenticated && auth.user?.access_token) {
            setTokenGetter(() => auth.user?.access_token)
        } else {
            setTokenGetter(() => undefined)
        }
    }, [auth.isAuthenticated, auth.user])

    useEffect(() => {
        setMobileOpen(false)
    }, [location.pathname])

    const queryClient = useQueryClient()
    const { data: connectionStatus } = useQuery({
        queryKey: ['setupStatus'],
        queryFn: () => api.getSetupStatus(),
        refetchInterval: 30000,
    })

    // Global live run progress (persists across all pages and multi-user sessions)
    const { data: activeRunProgress } = useQuery({
        queryKey: ['globalRunProgress'],
        queryFn: () => api.getRunProgress(),
        enabled: Boolean(connectionStatus?.connected),
        refetchInterval: (query) => {
            const st = String((query.state.data as any)?.status || '').toUpperCase()
            return st === 'RUNNING' ? 3000 : 12000
        },
    })
    const isRunActive = String(activeRunProgress?.status || '').toUpperCase() === 'RUNNING'
    const wasRunningRef = useRef(false)
    useEffect(() => {
        if (wasRunningRef.current && !isRunActive) {
            queryClient.invalidateQueries({ queryKey: ['metrics'] })
            queryClient.invalidateQueries({ queryKey: ['runs'] })
            queryClient.invalidateQueries({ queryKey: ['search'] })
            queryClient.invalidateQueries({ queryKey: ['cluster'] })
            queryClient.invalidateQueries({ queryKey: ['distribution'] })
            queryClient.invalidateQueries({ queryKey: ['rules'] })
        }
        wasRunningRef.current = isRunActive
    }, [isRunActive, queryClient])

    const activeCompletedStages = (activeRunProgress?.stages || []).filter(
        (s: any) => s.status === 'completed'
    ).length
    const activeTotalStages = (activeRunProgress?.stages || []).length || 8
    const activeStageLabel =
        activeRunProgress?.current_stage
            ? String(activeRunProgress.current_stage).replace(/_/g, ' ')
            : 'running'

    const currentItem = NAV_ITEMS.find((n) => isActive(n.to))

    return (
        <div className="min-h-screen bg-canvas text-ink flex">
            {mobileOpen && (
                <div
                    className="fixed inset-0 bg-ink/30 z-40 lg:hidden"
                    onClick={() => setMobileOpen(false)}
                    aria-hidden="true"
                />
            )}

            {/* Collapsible Left Navigation Rail */}
            <aside
                aria-label="Primary navigation"
                className={`fixed lg:sticky top-0 left-0 z-50 h-screen bg-subtle border-r border-line flex flex-col transition-transform duration-150 ${
                    collapsed ? 'w-[56px]' : 'w-[224px]'
                } ${mobileOpen ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'}`}
            >
                <div className="h-12 px-3 flex items-center justify-between border-b border-line shrink-0">
                    <Link
                        to="/"
                        className="flex items-center gap-2 min-w-0 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent rounded"
                    >
                        <div className="w-7 h-7 rounded-md bg-surface border border-line-strong flex items-center justify-center shrink-0">
                            <Network className="w-4 h-4 text-accent" />
                        </div>
                        {!collapsed && (
                            <div className="min-w-0 leading-tight">
                                <div className="text-[13px] font-semibold tracking-tight text-ink truncate">
                                    SQL-IDR
                                </div>
                                <div className="text-[10px] font-mono text-ink-muted truncate">
                                    v0.7.0
                                </div>
                            </div>
                        )}
                    </Link>
                    <button
                        type="button"
                        onClick={() => setCollapsed((c) => !c)}
                        className="hidden lg:inline-flex items-center justify-center w-7 h-7 rounded-md text-ink-muted hover:text-ink hover:bg-hover transition-colors"
                        aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
                        title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
                    >
                        {collapsed ? (
                            <PanelLeftOpen className="w-4 h-4" />
                        ) : (
                            <PanelLeftClose className="w-4 h-4" />
                        )}
                    </button>
                    <button
                        type="button"
                        onClick={() => setMobileOpen(false)}
                        aria-label="Close navigation"
                        className="lg:hidden inline-flex items-center justify-center w-7 h-7 rounded-md text-ink-muted hover:text-ink hover:bg-hover"
                    >
                        <X className="w-4 h-4" />
                    </button>
                </div>

                <nav className="flex-1 overflow-y-auto px-2 py-3 space-y-4">
                    {(['Observe', 'Configure'] as const).map((section) => (
                        <div key={section} className="space-y-0.5">
                            {!collapsed && (
                                <div className="px-2.5 pb-1 text-[11px] font-semibold uppercase tracking-[0.05em] text-ink-muted">
                                    {section}
                                </div>
                            )}
                            {NAV_ITEMS.filter((item) => item.section === section).map((item) => {
                                const Icon = item.icon
                                const active = isActive(item.to)
                                return (
                                    <Link
                                        key={item.to}
                                        to={item.to}
                                        aria-current={active ? 'page' : undefined}
                                        title={collapsed ? item.label : undefined}
                                        className={`flex items-center gap-2.5 px-2.5 py-1.5 rounded-md text-[13px] font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent ${
                                            active
                                                ? 'bg-selected text-accent border border-accent-border'
                                                : 'text-ink-secondary hover:text-ink hover:bg-hover border border-transparent'
                                        } ${collapsed ? 'justify-center px-0' : ''}`}
                                    >
                                        <Icon className={`w-4 h-4 shrink-0 ${active ? 'text-accent' : 'text-ink-muted'}`} />
                                        {!collapsed && <span className="truncate">{item.label}</span>}
                                    </Link>
                                )
                            })}
                        </div>
                    ))}
                </nav>

                <div className="p-2 border-t border-line space-y-1.5 shrink-0">
                    {connectionStatus?.connected ? (
                        <Link
                            to="/settings"
                            className={`flex items-center gap-2 px-2.5 py-1.5 rounded-md bg-surface border border-line hover:border-line-strong transition-colors ${
                                collapsed ? 'justify-center px-0' : ''
                            }`}
                            title={`Connected: ${connectionStatus.platform || 'Warehouse'} (${connectionStatus.configured ? 'Configured' : 'Needs Setup'})`}
                        >
                            <span className="w-2 h-2 rounded-full bg-status-success shrink-0" />
                            {!collapsed && (
                                <div className="min-w-0 flex-1 flex items-center justify-between gap-1">
                                    <span className="text-xs font-mono font-medium text-ink uppercase truncate">
                                        {connectionStatus.platform || 'Connected'}
                                    </span>
                                    <span className="text-[10px] text-ink-muted shrink-0">
                                        {connectionStatus.configured ? 'Ready' : 'Setup'}
                                    </span>
                                </div>
                            )}
                        </Link>
                    ) : (
                        <Link
                            to="/setup"
                            className={`flex items-center gap-2 px-2.5 py-1.5 rounded-md bg-status-danger-soft border border-status-danger-border transition-colors ${
                                collapsed ? 'justify-center px-0' : ''
                            }`}
                            title="Warehouse disconnected — open Identity Studio"
                        >
                            <span className="w-2 h-2 rounded-full bg-status-danger shrink-0" />
                            {!collapsed && (
                                <span className="text-xs font-mono font-medium text-status-danger truncate">
                                    Disconnected
                                </span>
                            )}
                        </Link>
                    )}

                    <button
                        type="button"
                        onClick={toggleTheme}
                        aria-label={theme === 'light' ? 'Switch to dark theme' : 'Switch to light theme'}
                        title={theme === 'light' ? 'Switch to dark theme' : 'Switch to light theme'}
                        className={`w-full flex items-center gap-2 px-2.5 py-1.5 rounded-md text-xs font-medium text-ink-muted hover:text-ink hover:bg-hover transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent ${
                            collapsed ? 'justify-center px-0' : ''
                        }`}
                    >
                        {theme === 'light' ? (
                            <Moon className="w-3.5 h-3.5 shrink-0" />
                        ) : (
                            <Sun className="w-3.5 h-3.5 shrink-0" />
                        )}
                        {!collapsed && (
                            <span>{theme === 'light' ? 'Dark appearance' : 'Daylight appearance'}</span>
                        )}
                    </button>
                </div>
            </aside>

            {/* Main Workspace Column */}
            <div className="flex-1 flex flex-col min-w-0">
                <header className="sticky top-0 z-30 h-12 bg-surface border-b border-line px-4 sm:px-6 flex items-center justify-between gap-3">
                    <div className="flex items-center gap-2.5 min-w-0">
                        <button
                            type="button"
                            onClick={() => setMobileOpen(true)}
                            className="lg:hidden inline-flex items-center justify-center w-8 h-8 rounded-md text-ink-muted hover:text-ink hover:bg-hover"
                            aria-label="Open navigation"
                        >
                            <Menu className="w-4 h-4" />
                        </button>
                        <div className="flex items-center gap-1.5 text-[13px] min-w-0">
                            <span className="text-ink-muted hidden sm:inline">Workspace</span>
                            <span className="text-line-strong hidden sm:inline">/</span>
                            <span className="font-semibold text-ink truncate">
                                {currentItem?.label || 'SQL-IDR'}
                            </span>
                        </div>
                    </div>

                    <div className="flex items-center gap-2 shrink-0">
                        {isRunActive && (
                            <Link
                                to="/runs"
                                className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-accent-soft border border-accent-border text-accent text-[11px] font-medium hover:bg-accent/20 transition-colors"
                                title={activeRunProgress?.current_detail || 'Pipeline run in progress'}
                            >
                                <Loader2 className="w-3 h-3 animate-spin shrink-0" />
                                <span className="truncate max-w-[220px]">
                                    Running ({activeCompletedStages}/{activeTotalStages} · {activeStageLabel}
                                    {typeof activeRunProgress?.elapsed_seconds === 'number'
                                        ? ` · ${activeRunProgress.elapsed_seconds.toFixed(0)}s`
                                        : ''})
                                </span>
                            </Link>
                        )}
                        <button
                            type="button"
                            onClick={() => setCopilotOpen(true)}
                            className="btn-secondary text-xs"
                            title="Open AI Copilot (Cmd+K)"
                        >
                            <Bot className="w-3.5 h-3.5 text-accent" />
                            <span>AI Copilot</span>
                            <kbd className="hidden sm:inline text-[10px] font-mono text-ink-muted bg-subtle border border-line px-1.5 py-0.2 rounded">
                                ⌘K
                            </kbd>
                        </button>

                        {location.pathname !== '/setup' && (
                            connectionStatus?.configured ? (
                                <button
                                    type="button"
                                    onClick={openRunDrawer}
                                    className="btn-primary text-xs"
                                >
                                    <Play className="w-3.5 h-3.5" />
                                    Run Pipeline
                                </button>
                            ) : (
                                <Link to="/setup" className="btn-primary text-xs">
                                    Configure Pipeline
                                </Link>
                            )
                        )}
                    </div>
                </header>

                <main className="flex-1 max-w-[1440px] w-full mx-auto px-4 sm:px-6 py-5">
                    <Suspense fallback={<RouteFallback />}>
                        <Routes>
                            <Route path="/" element={<Dashboard />} />
                            <Route path="/setup" element={<SetupWizard />} />
                            <Route path="/explorer" element={<Explorer />} />
                            <Route path="/runs" element={<Runs />} />
                            <Route path="/model" element={<DataModel />} />
                            <Route path="/settings" element={<Settings />} />
                        </Routes>
                    </Suspense>
                </main>
            </div>

            <AICopilotDrawer open={copilotOpen} onClose={() => setCopilotOpen(false)} />
        </div>
    )
}

function AppWithProviders() {
    const { data: connectionStatus } = useQuery({
        queryKey: ['setupStatus'],
        queryFn: () => api.getSetupStatus(),
        refetchInterval: 30000,
    })

    return (
        <RunPipelineDrawerProvider isConfigured={Boolean(connectionStatus?.configured)}>
            <AppShell />
        </RunPipelineDrawerProvider>
    )
}

export default function App() {
    return (
        <ThemeProvider>
            <ToastProvider>
                <AppWithProviders />
            </ToastProvider>
        </ThemeProvider>
    )
}
