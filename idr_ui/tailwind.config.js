/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ['"IBM Plex Sans"', 'system-ui', '-apple-system', 'BlinkMacSystemFont', 'sans-serif'],
        mono: ['"JetBrains Mono"', 'ui-monospace', 'SFMono-Regular', 'Menlo', 'Monaco', 'Consolas', 'monospace'],
      },
      colors: {
        canvas: 'var(--bg-canvas)',
        surface: 'var(--bg-surface)',
        subtle: 'var(--bg-subtle)',
        hover: 'var(--bg-hover)',
        selected: 'var(--bg-selected)',
        line: {
          DEFAULT: 'var(--border-subtle)',
          strong: 'var(--border-strong)',
        },
        ink: {
          DEFAULT: 'var(--ink-primary)',
          secondary: 'var(--ink-secondary)',
          muted: 'var(--ink-muted)',
          subtle: 'var(--ink-subtle)',
        },
        accent: {
          DEFAULT: 'var(--accent)',
          hover: 'var(--accent-hover)',
          soft: 'var(--accent-soft)',
          border: 'var(--accent-border)',
          fg: 'var(--accent-fg)',
        },
        status: {
          success: 'var(--status-success)',
          'success-soft': 'var(--status-success-soft)',
          'success-border': 'var(--status-success-border)',
          warning: 'var(--status-warning)',
          'warning-soft': 'var(--status-warning-soft)',
          'warning-border': 'var(--status-warning-border)',
          danger: 'var(--status-danger)',
          'danger-soft': 'var(--status-danger-soft)',
          'danger-border': 'var(--status-danger-border)',
        },
      },
      boxShadow: {
        popover: 'var(--shadow-popover)',
      },
    },
  },
  plugins: [],
}
