import { Routes, Route, Link, useLocation } from 'react-router-dom'
import {
    LayoutDashboard, Network, History, Settings as SettingsIcon,
    Database, Wand2
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
import { useEffect } from 'react'

export default function App() {
    const location = useLocation()
    const isActive = (path: string) => location.pathname === path
    const auth = useIDRAuth()

    useEffect(() => {
        if (auth.isAuthenticated && auth.user?.access_token) {
            setTokenGetter(() => auth.user?.access_token)
            console.log("Auth token set for API calls")
        } else {
            setTokenGetter(() => undefined)
        }
    }, [auth.isAuthenticated, auth.user])

    const { data: connectionStatus } = useQuery({
        queryKey: ['setupStatus'],
        queryFn: () => api.getSetupStatus(),
        refetchInterval: 30000 // Check every 30s
    })

    return (
        <div className="min-h-screen bg-gray-900 text-white">
            {/* Top Navigation */}
            <nav className="bg-gray-800 border-b border-gray-700">
                <div className="max-w-7xl mx-auto px-4">
                    <div className="flex items-center justify-between h-16">
                        {/* Left: Brand & Connection Status */}
                        <div className="flex items-center gap-3">
                            <Link to="/" className="flex items-center gap-2">
                                <Network className="w-8 h-8 text-blue-400" />
                                <span className="text-xl font-bold">IDR</span>
                                <span className="text-sm text-gray-400 hidden sm:inline">Identity Resolution</span>
                            </Link>

                            {/* Connection Badge */}
                            {connectionStatus?.connected ? (
                                <span className="px-2.5 py-0.5 bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 rounded-full text-xs font-mono uppercase">
                                    ● {connectionStatus.platform || 'Connected'}
                                </span>
                            ) : (
                                <Link
                                    to="/setup"
                                    className="px-2.5 py-0.5 bg-rose-500/10 hover:bg-rose-500/20 text-rose-400 border border-rose-500/20 rounded-full text-xs font-mono uppercase transition-colors"
                                    title="Warehouse not connected. Click to run Setup Wizard."
                                >
                                    ● Disconnected
                                </Link>
                            )}
                        </div>

                        {/* Right: Primary Navigation Links */}
                        <div className="flex gap-1 items-center">
                            <NavLink to="/" active={isActive('/')}>
                                <LayoutDashboard className="w-4 h-4" />
                                Dashboard
                            </NavLink>
                            <NavLink to="/explorer" active={isActive('/explorer')}>
                                <Network className="w-4 h-4" />
                                Explorer
                            </NavLink>
                            <NavLink to="/runs" active={isActive('/runs')}>
                                <History className="w-4 h-4" />
                                Runs
                            </NavLink>
                            <NavLink to="/model" active={isActive('/model')}>
                                <Database className="w-4 h-4" />
                                Data Model
                            </NavLink>
                            <NavLink to="/setup" active={isActive('/setup')}>
                                <Wand2 className="w-4 h-4" />
                                Setup Wizard
                            </NavLink>
                            <NavLink to="/settings" active={isActive('/settings')}>
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
                    <Route path="/" element={<Dashboard />} />
                    <Route path="/setup" element={<SetupWizard />} />
                    <Route path="/explorer" element={<Explorer />} />
                    <Route path="/runs" element={<Runs />} />
                    <Route path="/model" element={<DataModel />} />
                    <Route path="/settings" element={<Settings />} />
                </Routes>
            </main>
        </div>
    )
}

function NavLink({ to, children, active }: { to: string; children: React.ReactNode; active: boolean }) {
    return (
        <Link
            to={to}
            className={`flex items-center gap-2 px-3.5 py-2 rounded-lg text-sm font-medium transition-colors ${
                active
                    ? 'bg-blue-600 text-white shadow-sm'
                    : 'text-gray-300 hover:bg-gray-700/60 hover:text-white'
            }`}
        >
            {children}
        </Link>
    )
}
