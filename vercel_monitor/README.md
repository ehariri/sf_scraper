# Vercel Monitor

`monitor/` is the source of truth for the monitor UI.

This folder contains:

- generated frontend copies for Vercel hosting
- the Vercel-specific API route in `api/status.js`
- the committed snapshot fallback in `status-snapshot.json`

Do not edit `vercel_monitor/index.html`, `vercel_monitor/app.js`, or `vercel_monitor/styles.css` directly. Edit the matching files under `monitor/` and then resync:

```bash
python sync_vercel_monitor.py
```

That copies:

- `monitor/index.html` -> `vercel_monitor/index.html`
- `monitor/app.js` -> `vercel_monitor/app.js`
- `monitor/styles.css` -> `vercel_monitor/styles.css`

It also refreshes the committed `status-snapshot.json` fallback used when the live upstream monitor is unavailable.

Keep the Vercel frontend in sync with the local monitor assets:

```bash
python sync_vercel_monitor.py
```

That copies:

- `monitor/index.html` -> `vercel_monitor/index.html`
- `monitor/app.js` -> `vercel_monitor/app.js`
- `monitor/styles.css` -> `vercel_monitor/styles.css`

Required environment variable:

- `MONITOR_UPSTREAM_ORIGIN`

Public monitor URL:

- `https://vercelmonitor.vercel.app`
