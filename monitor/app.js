const REFRESH_MS = 15000;
let tabsBound = false;

function fmtInt(value) {
  return new Intl.NumberFormat().format(value ?? 0);
}

function fmtPct(value) {
  return `${(value ?? 0).toFixed(1)}%`;
}

function fmtRelative(iso) {
  if (!iso) return "never";
  const delta = Math.max(0, Date.now() - new Date(iso).getTime());
  const seconds = Math.floor(delta / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function badgeClass(status) {
  return `badge badge-${status}`;
}

function serviceBadgeText(service) {
  if (service.active && service.status === "waiting") return "running / waiting";
  if (service.active && service.status === "healthy") return "running";
  if (service.active && service.status === "degraded") return "running / degraded";
  if (service.active && service.status === "stalled") return "running / stalled";
  if (service.active && service.status === "error") return "running / error";
  return service.status;
}

function progressBar(value, max = 100) {
  const pct = max > 0 ? Math.max(0, Math.min(100, (value / max) * 100)) : 0;
  return `
    <div class="progress-track">
      <div class="progress-fill" style="width:${pct}%"></div>
    </div>
  `;
}

function metricCard(label, value, subtext, pct) {
  return `
    <article class="metric-card">
      <span class="label">${label}</span>
      <strong>${value}</strong>
      <span class="subtext">${subtext}</span>
      ${pct !== undefined ? progressBar(pct, 100) : ""}
    </article>
  `;
}

function renderService(service, containerId, badgeId, extraHtml = "") {
  const badge = document.getElementById(badgeId);
  badge.className = badgeClass(service.status);
  badge.textContent = serviceBadgeText(service);

  const processLines = service.processes.length
    ? service.processes
        .map(
          (p) =>
            `<li><code>${p.pid}</code> <span>${p.elapsed}</span> <span class="dim">${p.command}</span></li>`
        )
        .join("")
    : "<li>No active process</li>";

  const issues = service.issues.length
    ? `<ul class="issues">${service.issues.map((i) => `<li>${i}</li>`).join("")}</ul>`
    : `<p class="dim">No current issues detected.</p>`;

  document.getElementById(containerId).innerHTML = `
    <div class="service-stats">
      <div><span class="label">Processes</span><strong>${service.process_count}</strong></div>
      <div><span class="label">Last activity</span><strong>${fmtRelative(service.latest_activity_at)}</strong></div>
    </div>
    ${extraHtml}
    ${issues}
    <ul class="process-list">${processLines}</ul>
  `;
}

function stageLabel(stage) {
  const labels = {
    idle: "Idle",
    planning: "Planning batches",
    starting_next_batch: "Starting next batch",
    uploading_commit: "Building commit",
    uploading_lfs: "Uploading files",
    finalizing_commit: "Finalizing commit",
    verifying: "Verifying remote files",
    pruning: "Pruning local data",
    retrying: "Retrying after HF error",
    complete: "Complete",
  };
  return labels[stage] || stage || "Unknown";
}

function renderUploadSummary(upload) {
  const batchText =
    upload.batch_index && upload.batch_total
      ? `Batch ${upload.batch_index} / ${upload.batch_total}`
      : "No active batch";
  const detailText =
    upload.files_total
      ? `${fmtInt(upload.files_done)} / ${fmtInt(upload.files_total)} files`
      : upload.batch_days
        ? `${fmtInt(upload.batch_days)} days in batch`
        : "Waiting for upload activity";
  const pct = upload.files_pct ?? 0;

  return `
    <div class="upload-inline">
      <div class="year-header">
        <strong>${stageLabel(upload.stage)}</strong>
        <span>${batchText}</span>
      </div>
      ${progressBar(pct, 100)}
      <div class="year-meta upload-meta">
        <span>${detailText}</span>
        ${upload.current_day ? `<span>Current day ${upload.current_day}</span>` : ""}
        <span>${fmtRelative(upload.updated_at)}</span>
      </div>
      <p class="upload-message">${upload.message || "No current upload activity."}</p>
    </div>
  `;
}

function renderYearRows(years) {
  document.getElementById("year-rows").innerHTML = years
    .map(
      (row) => `
        <div class="year-row">
          <div class="year-header">
            <strong>${row.year}</strong>
            <span>${fmtInt(row.scraped_cases)} / ${fmtInt(row.total_cases)} cases</span>
          </div>
          ${progressBar(row.scraped_cases, row.total_cases)}
          <div class="year-meta">
            <span>${fmtPct(row.coverage_pct)} scraped</span>
            <span>${row.full_days}/${row.days} days complete</span>
            <span>${fmtPct(row.full_day_pct)} full-day completion</span>
          </div>
        </div>
      `
    )
    .join("");
}

function renderPrefixRows(prefixes) {
  document.getElementById("prefix-rows").innerHTML = prefixes
    .map(
      (row) => `
        <div class="prefix-row">
          <div class="year-header">
            <strong>${row.prefix}</strong>
            <span>${fmtInt(row.scraped_cases)} / ${fmtInt(row.discovered_cases)}</span>
          </div>
          ${progressBar(row.scraped_cases, row.discovered_cases)}
          <div class="year-meta">
            <span>${fmtPct(row.coverage_pct)} yield</span>
          </div>
        </div>
      `
    )
    .join("");
}

function renderDayList(containerId, rows) {
  document.getElementById(containerId).innerHTML = rows
    .map(
      (row) => `
        <div class="day-row">
          <div class="year-header">
            <strong>${row.date}</strong>
            <span>${fmtRelative(row.updated_at_iso)}</span>
          </div>
          ${progressBar(row.scraped_cases, row.total_cases)}
          <div class="year-meta">
            <span>${fmtInt(row.scraped_cases)} / ${fmtInt(row.total_cases)} scraped</span>
            <span>${row.failed_case_count} failed queued</span>
            <span>${row.source === "both" ? "HF + local" : row.source === "hf" ? "HF only" : "local only"}</span>
          </div>
        </div>
      `
    )
    .join("");
}

function renderLogs(containerId, logs) {
  document.getElementById(containerId).innerHTML = logs
    .map((log) => {
      const body = log.exists ? log.lines.join("\n") : "Log file not found.";
      return `
        <div class="log-block">
          <div class="year-header">
            <strong>${log.name}</strong>
            <span>${fmtRelative(log.updated_at)}</span>
          </div>
          <pre>${body}</pre>
        </div>
      `;
    })
    .join("");
}

function bindTabs() {
  if (tabsBound) return;
  tabsBound = true;

  const buttons = Array.from(document.querySelectorAll(".tab-button"));
  const panels = Array.from(document.querySelectorAll(".tab-panel"));

  buttons.forEach((button) => {
    button.addEventListener("click", () => {
      const tab = button.dataset.tab;
      buttons.forEach((b) => b.classList.toggle("active", b === button));
      panels.forEach((panel) =>
        panel.classList.toggle("active", panel.dataset.tabPanel === tab)
      );
    });
  });
}

function renderCalendar(calendar) {
  const legend = document.getElementById("calendar-legend");
  legend.innerHTML = calendar.legend
    .map(
      (entry) => `
        <div class="legend-item">
          <span class="calendar-cell legend-swatch ${entry.status}"></span>
          <span>${entry.label}</span>
        </div>
      `
    )
    .join("");

  const weekdayLabels = ["Mon", "Tue", "Wed", "Thu", "Fri"];

  document.getElementById("coverage-calendar").innerHTML = calendar.years
    .map((year) => {
      const monthLabels = year.months
        .map(
          (month) =>
            `<span class="calendar-month" style="grid-column:${month.week_index + 2}">${month.label}</span>`
        )
        .join("");

      const dayCells = year.days
        .map((day) => {
          const tooltip = [
            day.date,
            day.status === "untouched"
              ? "Untouched"
              : day.status === "attempted_error"
                ? "Attempted, unresolved"
              : day.status === "no_cases"
                ? "Known zero-case"
                : `${fmtInt(day.scraped_cases)} / ${fmtInt(day.total_cases)} scraped`,
            day.remaining_cases ? `${fmtInt(day.remaining_cases)} remaining` : null,
            day.source === "hf" ? "HF only" : null,
            day.on_hf && day.source === "local" ? "Also present on HF" : null,
            day.updated_at ? `Updated ${fmtRelative(day.updated_at)}` : null,
          ]
            .filter(Boolean)
            .join(" • ");

          const shadeClass =
            day.status === "touched" || day.status === "complete"
              ? `shade${day.shade}`
              : day.status;

          return `<button class="calendar-cell ${shadeClass}" style="grid-column:${day.week_index + 2}; grid-row:${day.weekday + 2}" title="${tooltip.replace(/"/g, "&quot;")}"></button>`;
        })
        .join("");

      return `
        <div class="calendar-year-block">
          <div class="calendar-year-label">${year.year}</div>
          <div class="calendar-board" style="grid-template-columns: auto repeat(${year.weeks}, 12px);">
            <span></span>
            ${monthLabels}
            ${weekdayLabels.map((label, index) => `<span class="calendar-weekday" style="grid-row:${index + 2}">${label}</span>`).join("")}
            ${dayCells}
          </div>
        </div>
      `;
    })
    .join("");
}

function showStatusBanner(message, tone = "warn") {
  const el = document.getElementById("status-banner");
  if (!el) return;
  if (!message) {
    el.className = "status-banner hidden";
    el.textContent = "";
    return;
  }
  el.className = `status-banner ${tone}`;
  el.textContent = message;
}

async function refresh() {
  const res = await fetch("/api/status");
  const payload = await res.json();
  if (!res.ok || payload.error) {
    const detail = payload.detail ? ` (${payload.detail})` : "";
    showStatusBanner(`Monitor API error: ${payload.error || res.status}${detail}`, "error");
    return;
  }

  if (payload.snapshot_mode) {
    const reason =
      payload.snapshot_reason === "upstream_unreachable"
        ? "live upstream unreachable"
        : "no live upstream configured";
    showStatusBanner(`Showing cached snapshot because ${reason}.`, "warn");
  } else {
    showStatusBanner("");
  }

  document.getElementById("generated-at").textContent = `${fmtRelative(payload.generated_at)}${payload.snapshot_mode ? " (snapshot)" : ""}`;
  document.getElementById("scope-range").textContent = `${payload.scope.start} to ${payload.scope.end}`;
  document.getElementById("data-size").textContent = payload.storage.data_human;

  const corpus = payload.corpus;
  document.getElementById("metric-cards").innerHTML = [
    metricCard(
      "HF + Local Case Coverage",
      `${fmtInt(corpus.scraped_cases)} / ${fmtInt(corpus.total_cases)}`,
      `${fmtPct(corpus.coverage_pct)} of combined discovered cases scraped`,
      corpus.coverage_pct
    ),
    metricCard(
      "HF + Local Day Completion",
      `${fmtInt(corpus.full_days)} / ${fmtInt(corpus.total_days)}`,
      `${fmtPct(corpus.full_day_pct)} of combined filing days fully scraped`,
      corpus.full_day_pct
    ),
    metricCard(
      "Local Failed Queue",
      fmtInt(corpus.failed_case_count),
      "cases still listed in local failed_cases.json files"
    ),
    metricCard(
      "HF-Synced Local Cases",
      fmtInt(corpus.synced_pruned_cases),
      `${corpus.synced_bytes ? `${(corpus.synced_bytes / (1024 ** 3)).toFixed(1)} GB` : "0.0 GB"} uploaded and tracked in local sync metadata`
    ),
    metricCard(
      "Local Scrape Speed",
      `${corpus.recent_cases_per_minute.toFixed(2)} cases/min`,
      `recent average from ${fmtInt(corpus.run_rows)} local run summaries`
    ),
    metricCard(
      "Local Success Rate",
      fmtPct(corpus.recent_success_rate),
      "recent scraped / attempted cases from local run summaries",
      corpus.recent_success_rate
    ),
  ].join("");

  renderService(payload.services.scrape, "scrape-service", "scrape-status-badge");
  renderService(
    payload.services.sync,
    "sync-service",
    "sync-status-badge",
    renderUploadSummary(payload.services.upload)
  );
  renderCalendar(payload.calendar);
  renderYearRows(corpus.years);
  renderPrefixRows(payload.prefixes);
  renderDayList("latest-days", corpus.latest_rows);
  renderDayList("worst-days", corpus.worst_rows);
  renderLogs("scrape-logs", payload.logs.scrape);
  renderLogs("sync-logs", payload.logs.sync);
  bindTabs();
}

refresh().catch((error) => {
  console.error(error);
  showStatusBanner(`Monitor refresh failed: ${error}`, "error");
});
setInterval(() => {
  refresh().catch((error) => {
    console.error(error);
    showStatusBanner(`Monitor refresh failed: ${error}`, "error");
  });
}, REFRESH_MS);
