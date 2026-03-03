import { ReactNode } from 'react'
import { Routes, Route, Link, useLocation } from 'react-router-dom'
import { LayoutDashboard, Network, History, Settings as SettingsIcon, Database, Lock } from 'lucide-react'
import Dashboard from './pages/Dashboard'
import Explorer from './pages/Explorer'
import Runs from './pages/Runs'
import Settings from './pages/Settings'
import SetupWizard from './pages/Setup/SetupWizard'
import DataModel from './pages/DataModel'

import { useQuery } from '@tanstack/react-query'
import { api } from './api/client'
import { useIDRAuth } from './auth/IDRAuthProvider'
import { PermissionGuard } from './auth/PermissionGuard'

export default function App() {
    const location = useLocation()
    const isActive = (path: string) => location.pathname === path
    const auth = useIDRAuth()

    const canReadConnection = !auth.isAuthorizing && auth.hasPermission('connection.read')
    const canReadConfig = !auth.isAuthorizing && auth.hasPermission('config.read')
    const canReadMetrics = !auth.isAuthorizing && auth.hasPermission('metrics.read')
    const canReadExplorer = !auth.isAuthorizing && auth.hasPermission('explorer.read')
    const canReadRuns = !auth.isAuthorizing && auth.hasPermission('runs.read')
    const canReadSchema = !auth.isAuthorizing && auth.hasPermission('schema.read')
    const canViewSetup = canReadConnection && canReadConfig
    const canViewSettings = canReadConnection

    const { data: connectionStatus } = useQuery({
        queryKey: ['setupStatus'],
        queryFn: () => api.getSetupStatus(),
        enabled: canReadConnection,
        refetchInterval: canReadConnection ? 30000 : false, // Check every 30s
    })

    return (
        <div className="min-h-screen bg-gray-900 text-white">
            {/* Top Navigation */}
            <nav className="bg-gray-800 border-b border-gray-700">
                <div className="max-w-7xl mx-auto px-4">
                    <div className="flex items-center justify-between h-16">
                        <div className="flex items-center gap-4">
                            <Link to="/" className="flex items-center gap-2">
                                <Network className="w-8 h-8 text-blue-400" />
                                <span className="text-xl font-bold">IDR</span>
                                <span className="text-sm text-gray-400 hidden sm:inline">Identity Resolution</span>
                            </Link>

                            {/* Connection Badge */}
                            {!canReadConnection ? (
                                <span className="px-3 py-1 bg-yellow-500/10 text-yellow-300 border border-yellow-500/30 rounded-full text-xs font-mono uppercase">
                                    ● Restricted
                                </span>
                            ) : connectionStatus?.connected ? (
                                <span className="px-3 py-1 bg-green-500/10 text-green-400 border border-green-500/20 rounded-full text-xs font-mono uppercase">
                                    ● {connectionStatus.platform || 'Connected'}
                                </span>
                            ) : (
                                <span className="px-3 py-1 bg-red-500/10 text-red-400 border border-red-500/20 rounded-full text-xs font-mono uppercase">
                                    ● Disconnected
                                </span>
                            )}

                            {/* Setup Link */}
                            <NavLink
                                to="/setup"
                                active={isActive('/setup')}
                                disabled={!canViewSetup}
                                disabledReason="Setup Wizard requires connection.read and config.read permissions."
                            >
                                <span className="text-xs font-semibold px-2 py-1 bg-blue-500/20 text-blue-300 rounded hover:bg-blue-500/30">
                                    Setup Wizard
                                </span>
                            </NavLink>
                        </div>

                        <div className="flex gap-1">
                            <NavLink
                                to="/"
                                active={isActive('/')}
                                disabled={!canReadMetrics || !canReadConnection}
                                disabledReason="Dashboard requires metrics.read and connection.read permissions."
                            >
                                <LayoutDashboard className="w-4 h-4" />
                                Dashboard
                            </NavLink>
                            <NavLink
                                to="/explorer"
                                active={isActive('/explorer')}
                                disabled={!canReadExplorer}
                                disabledReason="Explorer requires explorer.read permission."
                            >
                                <Network className="w-4 h-4" />
                                Explorer
                            </NavLink>
                            <NavLink
                                to="/runs"
                                active={isActive('/runs')}
                                disabled={!canReadRuns}
                                disabledReason="Runs view requires runs.read permission."
                            >
                                <History className="w-4 h-4" />
                                Runs
                            </NavLink>
                            <NavLink
                                to="/model"
                                active={isActive('/model')}
                                disabled={!canReadSchema}
                                disabledReason="Data Model requires schema.read permission."
                            >
                                <Database className="w-4 h-4" />
                                Data Model
                            </NavLink>
                            <NavLink
                                to="/settings"
                                active={isActive('/settings')}
                                disabled={!canViewSettings}
                                disabledReason="Settings requires connection.read permission."
                            >
                                <SettingsIcon className="w-4 h-4" />
                                Settings
                            </NavLink>
                        </div>
                    </div>
                </div>
            </nav>

            {/* Main Content */}
            <main className="max-w-7xl mx-auto px-4 py-6">
                <Routes>
                    <Route path="/" element={
                        <PermissionGuard
                            requiredPermissions={['metrics.read', 'connection.read']}
                            title="Dashboard Access Denied"
                            message="You need metrics.read and connection.read to view dashboard metrics."
                        >
                            <Dashboard />
                        </PermissionGuard>
                    } />
                    <Route path="/setup" element={
                        <PermissionGuard
                            requiredPermissions={['connection.read', 'config.read']}
                            title="Setup Access Denied"
                            message="You need connection.read and config.read to open the setup wizard."
                        >
                            <SetupWizard />
                        </PermissionGuard>
                    } />
                    <Route path="/explorer" element={
                        <PermissionGuard
                            requiredPermissions={['explorer.read']}
                            title="Explorer Access Denied"
                            message="You need explorer.read to search and inspect identity clusters."
                        >
                            <Explorer />
                        </PermissionGuard>
                    } />
                    <Route path="/runs" element={
                        <PermissionGuard
                            requiredPermissions={['runs.read']}
                            title="Run History Access Denied"
                            message="You need runs.read to view pipeline run history."
                        >
                            <Runs />
                        </PermissionGuard>
                    } />
                    <Route path="/model" element={
                        <PermissionGuard
                            requiredPermissions={['schema.read']}
                            title="Data Model Access Denied"
                            message="You need schema.read to view schema documentation."
                        >
                            <DataModel />
                        </PermissionGuard>
                    } />
                    <Route path="/settings" element={
                        <PermissionGuard
                            requiredPermissions={['connection.read']}
                            title="Settings Access Denied"
                            message="You need connection.read to view connection settings."
                        >
                            <Settings />
                        </PermissionGuard>
                    } />
                </Routes>
            </main>
        </div>
    )
}

function NavLink({
    to,
    children,
    active,
    disabled = false,
    disabledReason,
}: {
    to: string
    children: ReactNode
    active: boolean
    disabled?: boolean
    disabledReason?: string
}) {
    if (disabled) {
        return (
            <span
                title={disabledReason}
                aria-disabled="true"
                className="flex items-center gap-2 px-4 py-2 rounded-lg text-gray-500 bg-gray-800/50 cursor-not-allowed"
            >
                <Lock className="w-3.5 h-3.5" />
                {children}
            </span>
        )
    }

    return (
        <Link
            to={to}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${active
                ? 'bg-blue-600 text-white'
                : 'text-gray-300 hover:bg-gray-700'
                }`}
        >
            {children}
        </Link>
    )
}
