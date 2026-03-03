import { expect, test, type Route } from '@playwright/test'

type JsonValue = string | number | boolean | null | JsonObject | JsonValue[]
interface JsonObject {
    [key: string]: JsonValue
}

async function fulfillJson(route: Route, body: JsonObject | JsonValue[]): Promise<void> {
    await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(body),
    })
}

test.beforeEach(async ({ page }) => {
    await page.route('**/api/**', async (route) => {
        const url = new URL(route.request().url())
        const path = url.pathname

        if (path === '/api/setup/status') {
            return fulfillJson(route, {
                connected: true,
                configured: true,
                platform: 'duckdb',
            })
        }

        if (path === '/api/metrics/summary') {
            return fulfillJson(route, {
                total_clusters: 0,
                total_entities: 0,
                total_edges: 0,
                avg_confidence: 0,
                last_run_id: null,
                last_run_duration: null,
                last_run_started_at: null,
            })
        }

        if (path === '/api/metrics/distribution' || path === '/api/metrics/rules' || path === '/api/alerts') {
            return fulfillJson(route, [])
        }

        if (path === '/api/runs') {
            return fulfillJson(route, [])
        }

        if (path === '/api/setup/config') {
            return fulfillJson(route, { sources: [] })
        }

        if (path === '/api/schema') {
            return fulfillJson(route, [])
        }

        await route.fulfill({
            status: 404,
            contentType: 'application/json',
            body: JSON.stringify({ detail: `Unmocked endpoint: ${path}` }),
        })
    })
})

test('smoke: authenticated shell, setup page, and runs page render', async ({ page }) => {
    await page.goto('/', { waitUntil: 'domcontentloaded' })
    await page.waitForFunction(() => (document.getElementById('root')?.children.length || 0) > 0)

    await expect(page.getByRole('heading', { name: 'Match Quality Dashboard' })).toBeVisible()
    await expect(page.getByRole('link', { name: /Setup Wizard/i })).toBeVisible()

    await page.getByRole('link', { name: /Setup Wizard/i }).click()
    await expect(page.getByRole('heading', { name: 'Setup IDR' })).toBeVisible()

    await page.getByRole('link', { name: /Runs/i }).click()
    await expect(page.getByRole('heading', { name: 'Run History' })).toBeVisible()
    await expect(page.getByText('No runs found')).toBeVisible()
})
