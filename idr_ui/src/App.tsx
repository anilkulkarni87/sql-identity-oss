import { Routes, Route, Link, useLocation } from 'react-router-dom'
import {
    LayoutDashboard, Network, History, Settings as SettingsIcon,
    Database, Wand2, PanelLeftClose, PanelLeftOpen, Menu, X
} from 'lucide-react'
import Dashboard from './pages/Dashboard'
import Explorer from './pages/Explorer'
import Runs from './pages/Runs'
import Settings from './pages/Settings'
import SetupWizard from './pages/Setup/SetupWizard'
import DataModel from './pages/DataModel'

import { useQuery } from '@tanstack/react-query'
import { api, setTokenGetter } from './api/client'
import { useIDRAuth } from './auth/IDRAuthProvider'
import { useEffect, useState } from 'react'
import { ToastProvider } from './components/Toast'

const NAV_ITEMS = [
    { to: '/', label: 'Dashboard', icon: LayoutDashboard, section: 'Observe' },
    { to: '/explorer', label: 'Explorer', icon: Network, section: 'Observe' },
    { to: '/runs', label: 'Run History', icon: History, section: 'Observe' },
    { to: '/model', label: 'Data Model', icon: Database, section: 'Configure' },
    { to: '/setup', label: 'Setup Wizard', icon: Wand2, section: 'Configure' },
    { to: '/settings', label: 'Settings', icon: SettingsIcon, section: 'Configure' },
]

export default function App() {
    const location = useLocation()
    const isActive = (path: string) => location.pathname === path
    const auth = useIDRAuth()
    const [collapsed, setCollapsed] = useState(false)
    const [mobileOpen, setMobileOpen] = useState(false)

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

    const { data: connectionStatus } = useQuery({
        queryKey: ['setupStatus'],
        queryFn: () => api.getSetupStatus(),
        refetchInterval: 30000
    })

    const currentItem = NAV_ITEMS.find((n) => isActive(n.to))

    return (
        <ToastProvider>
            <div className="min-h-screen bg-slate-950 text-slate-100 flex">
                {/* Mobile Backdrop */}
                {mobileOpen && (
                    <div
                        className="fixed inset-0 bg-black/60 backdrop-blur-sm z-40 lg:hidden"
                        onClick={() => setMobileOpen(false)}
                    />
                )}

                {/* Collapsible Left Sidebar */}
                <aside
                    className={`fixed lg:sticky top-0 left-0 z-50 h-screen bg-slate-900/95 border-r border-slate-800/90 flex flex-col transition-all duration-200 ${
                        collapsed ? 'w-[72px]' : 'w-64'
                    } ${mobileOpen ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'}`}
                >
                    {/* Brand Header */}
                    <div className="h-16 px-4 flex items-center justify-between border-b border-slate-800/90 shrink-0">
                        <Link to="/" className="flex items-center gap-2.5 min-w-0">
                            <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-500/20 to-indigo-500/20 border border-blue-500/30 flex items-center justify-center shrink-0">
                                <Network className="w-5 h-5 text-blue-400" />
                            </div>
                            {!collapsed && (
                                <div className="min-w-0">
                                    <div className="text-sm font-bold tracking-tight text-white truncate">
                                        SQL-IDR
                                    </div>
                                    <div className="text-[11px] text-slate-400 truncate">
                                        Identity Resolution
                                    </div>
                                </div>
                            )}
                        </Link>
                        <button
                            onClick={() => setCollapsed((c) => !c)}
                            className="hidden lg:flex p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition-colors"
                            title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
                        >
                            {collapsed ? (
                                <PanelLeftOpen className="w-4 h-4" />
                            ) : (
                                <PanelLeftClose className="w-4 h-4" />
                            )}
                        </button>
                        <button
                            onClick={() => setMobileOpen(false)}
                            className="lg:hidden p-1.5 rounded-lg text-slate-400 hover:text-white"
                        >
                            <X className="w-5 h-5" />
                        </button>
                    </div>

                    {/* Nav Links */}
                    <nav className="flex-1 overflow-y-auto px-3 py-4 space-y-5">
                        {(['Observe', 'Configure'] as const).map((section) => (
                            <div key={section} className="space-y-1">
                                {!collapsed && (
                                    <div className="px-3 pb-1 text-[10px] font-semibold uppercase tracking-wider text-slate-500">
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
                                            title={collapsed ? item.label : undefined}
                                            className={`flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-all ${
                                                active
                                                    ? 'bg-blue-600/15 text-blue-300 border border-blue-500/30 shadow-sm'
                                                    : 'text-slate-400 hover:text-slate-100 hover:bg-slate-800/70 border border-transparent'
                                            } ${collapsed ? 'justify-center px-2' : ''}`}
                                        >
                                            <Icon className={`w-4 h-4 shrink-0 ${active ? 'text-blue-400' : ''}`} />
                                            {!collapsed && <span className="truncate">{item.label}</span>}
                                        </Link>
                                    )
                                })}
                            </div>
                        ))}
                    </nav>

                    {/* Warehouse Connection Footer */}
                    <div className="p-3 border-t border-slate-800/90 shrink-0">
                        {connectionStatus?.connected ? (
                            <div
                                className={`flex items-center gap-2.5 px-3 py-2 rounded-xl bg-emerald-500/10 border border-emerald-500/20 ${
                                    collapsed ? 'justify-center px-2' : ''
                                }`}
                                title={`Connected: ${connectionStatus.platform || 'Warehouse'}`}
                            >
                                <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse shrink-0" />
                                {!collapsed && (
                                    <div className="min-w-0">
                                        <div className="text-xs font-semibold text-emerald-300 uppercase font-mono truncate">
                                            {connectionStatus.platform || 'Connected'}
                                        </div>
                                        <div className="text-[10px] text-emerald-400/70 truncate">
                                            {connectionStatus.configured ? 'Configured' : 'Needs Setup'}
                                        </div>
                                    </div>
                                )}
                            </div>
                        ) : (
                            <Link
                                to="/setup"
                                className={`flex items-center gap-2.5 px-3 py-2 rounded-xl bg-rose-500/10 hover:bg-rose-500/20 border border-rose-500/25 transition-colors ${
                                    collapsed ? 'justify-center px-2' : ''
                                }`}
                                title="Warehouse not connected. Click to run Setup Wizard."
                            >
                                <span className="w-2 h-2 rounded-full bg-rose-400 shrink-0" />
                                {!collapsed && (
                                    <div className="min-w-0">
                                        <div className="text-xs font-semibold text-rose-300 uppercase font-mono truncate">
                                            Disconnected
                                        </div>
                                        <div className="text-[10px] text-rose-300/70 truncate">
                                            Click to connect
                                        </div>
                                    </div>
                                )}
                            </Link>
                        )}
                    </div>
                </aside>

                {/* Main Content Area */}
                <div className="flex-1 flex flex-col min-w-0">
                    {/* Sticky Top Context Bar */}
                    <header className="sticky top-0 z-30 h-14 bg-slate-950/80 backdrop-blur-md border-b border-slate-800/80 px-4 sm:px-6 flex items-center justify-between">
                        <div className="flex items-center gap-3">
                            <button
                                onClick={() => setMobileOpen(true)}
                                className="lg:hidden p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800"
                                aria-label="Open navigation"
                            >
                                <Menu className="w-5 h-5" />
                            </button>
                            <div className="flex items-center gap-2 text-sm">
                                <span className="text-slate-500 hidden sm:inline">Workspace</span>
                                <span className="text-slate-700 hidden sm:inline">/</span>
                                <span className="font-semibold text-slate-100">
                                    {currentItem?.label || 'IDR'}
                                </span>
                            </div>
                        </div>

                        <div className="flex items-center gap-2">
                            {connectionStatus?.connected && (
                                <span className="hidden sm:inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-slate-900 border border-slate-800 text-xs text-slate-300 font-mono">
                                    <span className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
                                    {connectionStatus.platform?.toUpperCase()}
                                </span>
                            )}
                        </div>
                    </header>

                    {/* Page Canvas */}
                    <main className="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 py-6">
                        <Routes>
                            <Route path="/" element={<Dashboard />} />
                            <Route path="/setup" element={<SetupWizard />} />
                            <Route path="/explorer" element={<Explorer />} />
                            <Route path="/runs" element={<Runs />} />
                            <Route path="/model" element={<DataModel />} />
                            <Route path="/settings" element={<Settings />} />
                        </Routes>
                    </main>
                </div>
            </div>
        </ToastProvider>
    )
}
