import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import AccessDenied from './AccessDenied'

describe('AccessDenied', () => {
    it('renders default message and permissions', () => {
        render(<AccessDenied requiredPermissions={['runs.read', 'run.execute']} />)

        expect(screen.getByRole('heading', { name: 'Access Denied' })).toBeInTheDocument()
        expect(screen.getByText(/runs\.read, run\.execute/i)).toBeInTheDocument()
    })

    it('renders custom title and message', () => {
        render(
            <AccessDenied
                title="Forbidden"
                message="You cannot access this view."
                requiredPermissions={[]}
            />
        )

        expect(screen.getByRole('heading', { name: 'Forbidden' })).toBeInTheDocument()
        expect(screen.getByText('You cannot access this view.')).toBeInTheDocument()
    })
})
