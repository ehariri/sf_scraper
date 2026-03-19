import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));

async function loadSnapshot() {
  const candidates = [
    path.join(MODULE_DIR, "..", "status-snapshot.json"),
    path.join(process.cwd(), "status-snapshot.json"),
    path.join(process.cwd(), "vercel_monitor", "status-snapshot.json"),
  ];

  let lastError = null;
  for (const snapshotPath of candidates) {
    try {
      const raw = await readFile(snapshotPath, "utf8");
      return JSON.parse(raw);
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError || new Error("status snapshot not found");
}

export default async function handler(req, res) {
  const origin = (process.env.MONITOR_UPSTREAM_ORIGIN || "").trim();

  if (!origin) {
    try {
      const snapshot = await loadSnapshot();
      res.setHeader("Cache-Control", "no-store, max-age=0");
      res.status(200).json({
        ...snapshot,
        snapshot_mode: true,
        snapshot_reason: "no_upstream_configured",
      });
      return;
    } catch (error) {
      res.status(500).json({
        error: "MONITOR_UPSTREAM_ORIGIN is not configured and no snapshot fallback is available",
        detail: String(error),
      });
      return;
    }
  }

  const upstream = new URL("/api/status", origin).toString();

  try {
    const response = await fetch(upstream, {
      headers: { accept: "application/json" },
    });

    const text = await response.text();
    res.setHeader("Cache-Control", "no-store, max-age=0");

    if (!response.ok) {
      res.status(response.status).send(text);
      return;
    }

    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.status(200).send(text);
  } catch (error) {
    try {
      const snapshot = await loadSnapshot();
      res.setHeader("Cache-Control", "no-store, max-age=0");
      res.status(200).json({
        ...snapshot,
        snapshot_mode: true,
        snapshot_reason: "upstream_unreachable",
        upstream_error: String(error),
      });
      return;
    } catch (snapshotError) {
      res.status(502).json({
        error: "Failed to reach monitor upstream",
        detail: String(error),
        snapshot_error: String(snapshotError),
      });
    }
  }
}
