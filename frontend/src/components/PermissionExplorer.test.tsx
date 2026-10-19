import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { PermissionExplorer } from './PermissionExplorer'

function response(isAllowed: boolean, ok = true, status = 200): Response {
  return {
    ok,
    status,
    json: vi.fn().mockResolvedValue({
      is_allowed: isAllowed,
      steps: ['principal -> readers', 'readers has SELECT on items'],
    }),
  } as unknown as Response
}

function submitForm() {
  render(<PermissionExplorer apiBase="" />)
  fireEvent.change(screen.getByLabelText('Principal'), {
    target: { value: '00000000-0000-0000-0000-000000000001' },
  })
  fireEvent.change(screen.getByLabelText('Resource'), {
    target: { value: 'items' },
  })
  fireEvent.click(screen.getByRole('button', { name: 'Check access' }))
}

describe('PermissionExplorer', () => {
  it('shows and disables the loading control', async () => {
    let resolveFetch!: (value: Response) => void
    vi.spyOn(globalThis, 'fetch').mockReturnValue(
      new Promise<Response>((resolve) => {
        resolveFetch = resolve
      }),
    )

    submitForm()
    expect(screen.getByRole('button', { name: 'Checking…' })).toBeDisabled()

    resolveFetch(response(true))
    expect(await screen.findByText('ALLOW')).toBeVisible()
  })

  it.each([
    { allowed: true, label: 'ALLOW' },
    { allowed: false, label: 'DENY' },
  ])('renders a $label explanation', async ({ allowed, label }) => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(response(allowed))

    submitForm()
    expect(await screen.findByText(label)).toBeVisible()
    expect(screen.getByText('principal -> readers')).toBeVisible()
  })

  it('renders HTTP errors without an authorization result', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(response(false, false, 503))

    submitForm()
    expect(await screen.findByText('Error: HTTP 503')).toBeVisible()
    expect(screen.queryByText('DENY')).not.toBeInTheDocument()
  })
})
