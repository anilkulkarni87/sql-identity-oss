import { defineConfig, devices } from '@playwright/test'

const baseURL = process.env.PLAYWRIGHT_BASE_URL || 'http://127.0.0.1:4173'

export default defineConfig({
    testDir: './tests/e2e',
    timeout: 60_000,
    expect: {
        timeout: 20_000,
    },
    fullyParallel: true,
    retries: process.env.CI ? 1 : 0,
    reporter: process.env.CI
        ? [['github'], ['html', { open: 'never' }]]
        : [['list'], ['html', { open: 'never' }]],
    use: {
        baseURL,
        headless: true,
        trace: 'retain-on-failure',
        screenshot: 'only-on-failure',
        video: 'retain-on-failure',
    },
    webServer: process.env.PLAYWRIGHT_BASE_URL
        ? undefined
        : {
            command: 'npm run build && npm run preview -- --host 127.0.0.1 --port 4173',
            url: baseURL,
            reuseExistingServer: !process.env.CI,
            env: {
                ...process.env,
                VITE_ALLOW_INSECURE_DEV_AUTH: 'true',
            },
        },
    projects: [
        {
            name: 'chromium',
            use: { ...devices['Desktop Chrome'] },
        },
    ],
})
