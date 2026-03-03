import React from 'react'
import { AlertOctagon, Home, RefreshCcw, RotateCw } from 'lucide-react'
import { safeErrorMessage } from '../security/redaction'

interface AppErrorBoundaryProps {
    children: React.ReactNode
}

interface AppErrorBoundaryState {
    hasError: boolean
    message: string
}

export class AppErrorBoundary extends React.Component<AppErrorBoundaryProps, AppErrorBoundaryState> {
    state: AppErrorBoundaryState = {
        hasError: false,
        message: '',
    }

    static getDerivedStateFromError(error: unknown): AppErrorBoundaryState {
        return {
            hasError: true,
            message: safeErrorMessage(error, 'The UI encountered an unexpected error.'),
        }
    }

    componentDidCatch(_error: unknown): void {
        // Intentionally no console logging to avoid accidental sensitive data leaks.
    }

    private resetBoundary = (): void => {
        this.setState({ hasError: false, message: '' })
    }

    render() {
        if (this.state.hasError) {
            return (
                <div className="min-h-screen bg-gray-900 text-white px-4 py-10">
                    <div className="max-w-2xl mx-auto bg-red-900/20 border border-red-700 rounded-xl p-6 space-y-5">
                        <div className="flex items-center gap-3 text-red-300">
                            <AlertOctagon className="w-6 h-6" />
                            <h1 className="text-xl font-semibold">Application Error</h1>
                        </div>
                        <p className="text-sm text-red-100/90">
                            {this.state.message}
                        </p>
                        <p className="text-xs text-gray-300">
                            You can retry rendering, go back to dashboard, or reload the app.
                        </p>
                        <div className="flex flex-wrap gap-3">
                            <button
                                onClick={this.resetBoundary}
                                className="inline-flex items-center gap-2 px-4 py-2 rounded bg-blue-600 hover:bg-blue-700 text-sm"
                            >
                                <RefreshCcw className="w-4 h-4" />
                                Try Again
                            </button>
                            <button
                                onClick={() => { window.location.href = '/' }}
                                className="inline-flex items-center gap-2 px-4 py-2 rounded bg-gray-700 hover:bg-gray-600 text-sm"
                            >
                                <Home className="w-4 h-4" />
                                Go to Dashboard
                            </button>
                            <button
                                onClick={() => window.location.reload()}
                                className="inline-flex items-center gap-2 px-4 py-2 rounded bg-gray-700 hover:bg-gray-600 text-sm"
                            >
                                <RotateCw className="w-4 h-4" />
                                Reload App
                            </button>
                        </div>
                    </div>
                </div>
            )
        }

        return this.props.children
    }
}
