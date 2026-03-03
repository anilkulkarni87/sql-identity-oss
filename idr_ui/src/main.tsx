import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import App from './App'
import './index.css'

const queryClient = new QueryClient()

import { IDRAuthProvider } from './auth/IDRAuthProvider'
import { ProtectedRoute } from './auth/ProtectedRoute'
import { AppErrorBoundary } from './components/AppErrorBoundary'

ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
        <QueryClientProvider client={queryClient}>
            <IDRAuthProvider>
                <BrowserRouter>
                    <AppErrorBoundary>
                        <ProtectedRoute>
                            <Routes>
                                <Route path="/*" element={<App />} />
                            </Routes>
                        </ProtectedRoute>
                    </AppErrorBoundary>
                </BrowserRouter>
            </IDRAuthProvider>
        </QueryClientProvider>
    </React.StrictMode>,
)
