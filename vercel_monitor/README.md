# Vercel Monitor

This folder hosts the monitor frontend on Vercel and proxies `/api/status` to a live upstream monitor running on the scraper machine.

Required environment variable:

- `MONITOR_UPSTREAM_ORIGIN`

Example:

- `https://advanced-pork-himself-spray.trycloudflare.com`
