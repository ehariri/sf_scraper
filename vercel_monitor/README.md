# Vercel Monitor

This folder hosts the monitor frontend on Vercel and proxies `/api/status` to a live upstream monitor running on the scraper machine.

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
