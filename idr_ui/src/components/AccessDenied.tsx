import { ShieldAlert } from 'lucide-react'

interface AccessDeniedProps {
    title?: string
    message?: string
    requiredPermissions?: string[]
}

export default function AccessDenied({
    title = 'Access Denied',
    message = 'Your account does not have the required permission for this section.',
    requiredPermissions = [],
}: AccessDeniedProps) {
    return (
        <div className="max-w-3xl mx-auto mt-10">
            <div className="bg-red-900/20 border border-red-700 rounded-xl p-6 space-y-3">
                <div className="flex items-center gap-2 text-red-300">
                    <ShieldAlert className="w-5 h-5" />
                    <h2 className="text-lg font-semibold">{title}</h2>
                </div>
                <p className="text-sm text-red-100/90">{message}</p>
                {requiredPermissions.length > 0 && (
                    <p className="text-xs text-red-200/80">
                        Required permissions: <span className="font-mono">{requiredPermissions.join(', ')}</span>
                    </p>
                )}
            </div>
        </div>
    )
}
