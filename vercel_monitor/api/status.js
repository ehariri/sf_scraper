export default async function handler(req, res) {
  const origin = process.env.MONITOR_UPSTREAM_ORIGIN;

  if (!origin) {
    res.status(500).json({
      error: "MONITOR_UPSTREAM_ORIGIN is not configured",
    });
    return;
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
    res.status(502).json({
      error: "Failed to reach monitor upstream",
      detail: String(error),
    });
  }
}
