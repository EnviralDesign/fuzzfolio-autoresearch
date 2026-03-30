const state = {
  overview: null,
  runCache: new Map(),
  attemptCache: new Map(),
};

function formatNumber(value, digits = 1) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return Number(value).toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

function formatInt(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return Number(value).toLocaleString();
}

function formatTime(value) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function scoreTag(score) {
  if (score === null || score === undefined) return `<span class="tag bad">unscored</span>`;
  if (score >= 80) return `<span class="score-pill">${formatNumber(score, 2)}</span>`;
  if (score >= 50) return `<span class="tag warn">${formatNumber(score, 2)}</span>`;
  return `<span class="tag bad">${formatNumber(score, 2)}</span>`;
}

function setStatus(message) {
  document.getElementById("refreshStatus").textContent = message;
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${response.status} ${response.statusText}: ${text}`);
  }
  return response.json();
}

function metricCard(label, value, secondary = "") {
  return `
    <article class="metric-card">
      <div class="metric-label">${escapeHtml(label)}</div>
      <div class="metric-value">${escapeHtml(value)}</div>
      ${secondary ? `<div class="muted">${escapeHtml(secondary)}</div>` : ""}
    </article>
  `;
}

function renderOverviewCards(overview) {
  const container = document.getElementById("overviewCards");
  container.innerHTML = [
    metricCard("Runs", formatInt(overview.runCount), `${formatInt(overview.scoredRunCount)} with scored leaders`),
    metricCard("Attempts", formatInt(overview.attemptCount), "all run-local ledger entries"),
    metricCard("Best score", formatNumber(overview.bestScore, 2), "highest best-per-run quality score"),
    metricCard("Median best", formatNumber(overview.medianBestScore, 2), "typical run leader"),
    metricCard("Profile drops", formatInt(overview.profileDropCount), "runs with rendered card"),
    metricCard("Curve coverage", formatInt(overview.curveCoverageCount), "runs with path detail persisted"),
  ].join("");
}

function renderGallery(images) {
  const gallery = document.getElementById("derivedGallery");
  const rows = [
    ["Aggregate progress", images.aggregatePlotUrl, "All logged scored attempts across runs."],
    ["Best-per-run leaderboard", images.leaderboardPlotUrl, "The current champion from each run."],
    ["Model averages", images.modelLeaderboardPlotUrl, "Average best-per-run score by explorer model."],
    ["Score vs trade rate", images.tradeoffPlotUrl, "Existing derived tradeoff render from the leaderboard pass."],
    ["12m vs 36m validation", images.validationScatterPlotUrl, "How the recent winners hold up under 3-year scrutiny."],
    ["Scrutiny delta", images.validationDeltaPlotUrl, "36m minus 12m score for the currently validated leaders."],
    ["Similarity heatmap", images.similarityHeatmapPlotUrl, "Pairwise 36m sameness across the validated leaders."],
    ["Score vs sameness", images.similarityScatterPlotUrl, "High-scoring leaders that are not obvious clones stand out on the left."],
  ].filter((row) => row[1]);
  gallery.innerHTML = rows
    .map(
      ([title, url, copy]) => `
        <article class="gallery-card">
          <img alt="${escapeHtml(title)}" src="${escapeHtml(url)}&t=${Date.now()}" />
          <div class="gallery-copy">
            <h3>${escapeHtml(title)}</h3>
            <p class="muted">${escapeHtml(copy)}</p>
          </div>
        </article>
      `,
    )
    .join("");
}

function axisTicks(min, max, count = 5) {
  if (min === max) return [min];
  const ticks = [];
  for (let index = 0; index < count; index += 1) {
    const ratio = index / (count - 1);
    ticks.push(min + (max - min) * ratio);
  }
  return ticks;
}

function createTooltip() {
  let tooltip = document.querySelector(".chart-tooltip");
  if (!tooltip) {
    tooltip = document.createElement("div");
    tooltip.className = "chart-tooltip";
    tooltip.hidden = true;
    document.body.appendChild(tooltip);
  }
  return tooltip;
}

function renderScatterChart(container, points, options) {
  if (!points.length) {
    container.innerHTML = `<div class="empty-state">No points yet.</div>`;
    return;
  }
  const tooltip = createTooltip();
  const width = Math.max(container.clientWidth || 640, 320);
  const height = Math.max(options.height || 360, 260);
  const margin = { top: 24, right: 26, bottom: 42, left: 52 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const xValues = points.map((point) => Number(point.x));
  const yValues = points.map((point) => Number(point.y));
  const xMin = options.xDomain ? Number(options.xDomain[0]) : Math.min(...xValues);
  const xMax = options.xDomain ? Number(options.xDomain[1]) : Math.max(...xValues);
  const yMin = options.yDomain ? Number(options.yDomain[0]) : Math.min(...yValues);
  const yMax = options.yDomain ? Number(options.yDomain[1]) : Math.max(...yValues);
  const xPad = xMax === xMin ? 1 : (xMax - xMin) * 0.08;
  const yPad = yMax === yMin ? 1 : (yMax - yMin) * 0.08;
  const xScale = (value) =>
    margin.left + ((value - (xMin - xPad)) / ((xMax + xPad) - (xMin - xPad))) * innerWidth;
  const yScale = (value) =>
    margin.top + innerHeight - ((value - (yMin - yPad)) / ((yMax + yPad) - (yMin - yPad))) * innerHeight;

  const xTicks = axisTicks(xMin, xMax);
  const yTicks = axisTicks(yMin, yMax);
  const svgParts = [
    `<svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(options.title)}">`,
    `<rect x="0" y="0" width="${width}" height="${height}" rx="18" fill="rgba(7,12,22,0.45)"></rect>`,
  ];

  xTicks.forEach((tick) => {
    const x = xScale(tick);
    svgParts.push(`<line x1="${x}" y1="${margin.top}" x2="${x}" y2="${margin.top + innerHeight}" stroke="rgba(147,190,255,0.1)" />`);
    svgParts.push(`<text x="${x}" y="${height - 14}" fill="#99abc4" text-anchor="middle" font-size="11">${escapeHtml(options.xTickFormat(tick))}</text>`);
  });
  yTicks.forEach((tick) => {
    const y = yScale(tick);
    svgParts.push(`<line x1="${margin.left}" y1="${y}" x2="${margin.left + innerWidth}" y2="${y}" stroke="rgba(147,190,255,0.1)" />`);
    svgParts.push(`<text x="${margin.left - 10}" y="${y + 4}" fill="#99abc4" text-anchor="end" font-size="11">${escapeHtml(options.yTickFormat(tick))}</text>`);
  });

  if (Array.isArray(options.polyline) && options.polyline.length > 1) {
    const d = options.polyline
      .map((point, index) => `${index === 0 ? "M" : "L"} ${xScale(point.x)} ${yScale(point.y)}`)
      .join(" ");
    svgParts.push(`<path d="${d}" fill="none" stroke="#60d6c3" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"></path>`);
  }

  points.forEach((point, index) => {
    const x = xScale(Number(point.x));
    const y = yScale(Number(point.y));
    const radius = point.emphasis ? 7 : 5;
    const fill = point.emphasis ? "#ffba6d" : "#60d6c3";
    const stroke = point.outline ? "#dff8f4" : "#10253a";
    svgParts.push(
      `<circle class="chart-point" data-point-index="${index}" cx="${x}" cy="${y}" r="${radius}" fill="${fill}" stroke="${stroke}" stroke-width="${point.outline ? 2 : 1.2}"></circle>`,
    );
  });
  svgParts.push(
    `<text x="${margin.left + innerWidth / 2}" y="${height - 2}" fill="#99abc4" text-anchor="middle" font-size="12">${escapeHtml(options.xLabel)}</text>`,
  );
  svgParts.push(
    `<text x="14" y="${margin.top + innerHeight / 2}" fill="#99abc4" text-anchor="middle" font-size="12" transform="rotate(-90 14 ${margin.top + innerHeight / 2})">${escapeHtml(options.yLabel)}</text>`,
  );
  svgParts.push(`</svg>`);
  container.innerHTML = svgParts.join("");

  container.querySelectorAll(".chart-point").forEach((node) => {
    const point = points[Number(node.getAttribute("data-point-index"))];
    node.addEventListener("mouseenter", (event) => {
      tooltip.hidden = false;
      tooltip.innerHTML = point.tooltip;
      tooltip.style.left = `${event.clientX}px`;
      tooltip.style.top = `${event.clientY}px`;
    });
    node.addEventListener("mousemove", (event) => {
      tooltip.style.left = `${event.clientX}px`;
      tooltip.style.top = `${event.clientY}px`;
    });
    node.addEventListener("mouseleave", () => {
      tooltip.hidden = true;
    });
    if (point.onClick) {
      node.style.cursor = "pointer";
      node.addEventListener("click", point.onClick);
    }
  });
}

function renderSimpleTable(containerId, columns, rows) {
  const container = document.getElementById(containerId);
  if (!rows.length) {
    container.innerHTML = `<div class="empty-state">No rows yet.</div>`;
    return;
  }
  container.innerHTML = `
    <div class="table-wrap">
      <table>
        <thead>
          <tr>${columns.map((column) => `<th>${escapeHtml(column.label)}</th>`).join("")}</tr>
        </thead>
        <tbody>
          ${rows.map((row) => `<tr class="${row.className || ""}">${columns.map((column) => `<td>${column.render(row)}</td>`).join("")}</tr>`).join("")}
        </tbody>
      </table>
    </div>
  `;
  rows.forEach((row, index) => {
    if (!row.onClick) return;
    const tr = container.querySelectorAll("tbody tr")[index];
    tr.classList.add("is-clickable");
    tr.addEventListener("click", row.onClick);
  });
}

function renderHorizontalBarChart(container, rows, options) {
  if (!rows.length) {
    container.innerHTML = `<div class="empty-state">No bars yet.</div>`;
    return;
  }
  const width = Math.max(container.clientWidth || 640, 320);
  const rowHeight = 28;
  const height = Math.max(240, rows.length * rowHeight + 72);
  const margin = { top: 22, right: 24, bottom: 34, left: 220 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const values = rows.map((row) => Number(row.value));
  const minValue = Math.min(0, ...values);
  const maxValue = Math.max(0, ...values);
  const padding = maxValue === minValue ? 1 : (maxValue - minValue) * 0.08;
  const domainMin = minValue - padding;
  const domainMax = maxValue + padding;
  const xScale = (value) =>
    margin.left + ((value - domainMin) / (domainMax - domainMin)) * innerWidth;
  const zeroX = xScale(0);
  const svgParts = [
    `<svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(options.title)}">`,
    `<rect x="0" y="0" width="${width}" height="${height}" rx="18" fill="rgba(7,12,22,0.45)"></rect>`,
    `<line x1="${zeroX}" y1="${margin.top}" x2="${zeroX}" y2="${margin.top + innerHeight}" stroke="rgba(216,228,255,0.65)" stroke-width="1.2" />`,
  ];
  rows.forEach((row, index) => {
    const y = margin.top + index * rowHeight + 2;
    const barHeight = rowHeight - 8;
    const valueX = xScale(Number(row.value));
    const x = Math.min(zeroX, valueX);
    const barWidth = Math.abs(valueX - zeroX);
    svgParts.push(`<text x="${margin.left - 10}" y="${y + barHeight * 0.78}" fill="#c7d8ef" text-anchor="end" font-size="11">${escapeHtml(row.label)}</text>`);
    svgParts.push(`<rect x="${x}" y="${y}" width="${barWidth}" height="${barHeight}" rx="8" fill="${escapeHtml(row.color || "#60d6c3")}" opacity="0.88"></rect>`);
    svgParts.push(`<text x="${valueX + (Number(row.value) >= 0 ? 8 : -8)}" y="${y + barHeight * 0.78}" fill="#ebf3ff" text-anchor="${Number(row.value) >= 0 ? "start" : "end"}" font-size="11">${escapeHtml(options.valueFormat(Number(row.value), row))}</text>`);
  });
  svgParts.push(
    `<text x="${margin.left + innerWidth / 2}" y="${height - 8}" fill="#99abc4" text-anchor="middle" font-size="12">${escapeHtml(options.xLabel)}</text>`,
  );
  svgParts.push(`</svg>`);
  container.innerHTML = svgParts.join("");
}

function renderModelTable(payload) {
  renderSimpleTable(
    "modelTable",
    [
      { label: "Model", render: (row) => escapeHtml(row.modelLabel) },
      { label: "Runs", render: (row) => formatInt(row.runCount) },
      { label: "Avg", render: (row) => formatNumber(row.averageScore, 2) },
      { label: "Median", render: (row) => formatNumber(row.medianScore, 2) },
      { label: "Best", render: (row) => formatNumber(row.bestScore, 2) },
      { label: "70+", render: (row) => `${Math.round((row.score70PlusRate || 0) * 100)}%` },
      { label: "80+", render: (row) => `${Math.round((row.score80PlusRate || 0) * 100)}%` },
    ],
    payload.modelConsistency || [],
  );
}

function setHashForRun(runId, attemptId = null) {
  window.location.hash = attemptId ? `#run/${encodeURIComponent(runId)}/attempt/${encodeURIComponent(attemptId)}` : `#run/${encodeURIComponent(runId)}`;
}

function renderRunsTable(payload) {
  renderSimpleTable(
    "runsTable",
    [
      { label: "Run", render: (row) => `<strong>${escapeHtml(row.runId)}</strong><div class="muted">${escapeHtml(row.explorerModel || row.explorerProfile || "unknown")}</div>` },
      { label: "Best", render: (row) => scoreTag(row.bestAttempt?.score) },
      { label: "Attempts", render: (row) => formatInt(row.attemptCount) },
      { label: "Step", render: (row) => formatInt(row.latestStep) },
      { label: "Advisors", render: (row) => formatInt(row.advisorGuidanceCount) },
      { label: "Updated", render: (row) => formatTime(row.latestLogTimestamp || row.latestAttemptAt) },
    ],
    (payload.runs || []).map((row) => ({
      ...row,
      onClick: () => setHashForRun(row.runId),
    })),
  );
}

function renderTradeoffChart(payload) {
  const visibleRows = (payload.tradeoff || []).filter(
    (row) => Number(row.composite_score) >= 15 && Number(row.trades_per_month) <= 200,
  );
  const points = visibleRows.map((row) => ({
    x: row.trades_per_month,
    y: row.composite_score,
    emphasis: row.is_trade_envelope,
    outline: row.is_frontier,
    tooltip: `<strong>${escapeHtml(row.run_id)}</strong><br>${escapeHtml(row.candidate_name)}<br>score ${formatNumber(row.composite_score, 2)}<br>${formatNumber(row.trades_per_month, 1)} trades/mo`,
    onClick: () => setHashForRun(row.run_id, row.attempt_id),
  }));
  const envelope = visibleRows
    .filter((row) => row.is_trade_envelope)
    .map((row) => ({ x: row.trades_per_month, y: row.composite_score }))
    .sort((left, right) => left.x - right.x);
  renderScatterChart(document.getElementById("tradeoffChart"), points, {
    title: "Score vs trade rate",
    xLabel: "Average resolved trades / month",
    yLabel: "Quality score",
    xTickFormat: (value) => formatNumber(value, value < 10 ? 1 : 0),
    yTickFormat: (value) => formatNumber(value, 0),
    polyline: envelope,
    xDomain: [0, 200],
  });
}

function renderDrawdownChart(payload) {
  const points = (payload.scoreVsDrawdown || []).map((row) => ({
    x: row.maxDrawdownR,
    y: row.score,
    emphasis: row.score >= 80,
    outline: false,
    tooltip: `<strong>${escapeHtml(row.runId)}</strong><br>${escapeHtml(row.label)}<br>score ${formatNumber(row.score, 2)}<br>max DD ${formatNumber(row.maxDrawdownR, 1)}R`,
    onClick: () => setHashForRun(row.runId, row.attemptId),
  }));
  renderScatterChart(document.getElementById("drawdownChart"), points, {
    title: "Score vs drawdown",
    xLabel: "Max drawdown (R)",
    yLabel: "Quality score",
    xTickFormat: (value) => formatNumber(value, 0),
    yTickFormat: (value) => formatNumber(value, 0),
  });
}

function renderValidationScatterChart(payload) {
  const points = (payload.validation || []).map((row) => ({
    x: row.score_36m,
    y: row.score_12m,
    emphasis: Number(row.trades_per_month_36m || 0) >= 20,
    outline: false,
    tooltip: `<strong>${escapeHtml(row.run_id)}</strong><br>${escapeHtml(row.candidate_name || "candidate")}<br>12m ${formatNumber(row.score_12m, 2)}<br>36m ${formatNumber(row.score_36m, 2)}<br>${formatNumber(row.trades_per_month_36m, 1)} trades/mo on 36m`,
    onClick: () => setHashForRun(row.run_id, row.attempt_id),
  }));
  const diagonal = points.length
    ? (() => {
        const values = points.flatMap((point) => [Number(point.x), Number(point.y)]);
        const min = Math.min(...values);
        const max = Math.max(...values);
        return [{ x: min, y: min }, { x: max, y: max }];
      })()
    : [];
  renderScatterChart(document.getElementById("validationScatterChart"), points, {
    title: "12m vs 36m validation",
    xLabel: "36m quality score",
    yLabel: "12m quality score",
    xTickFormat: (value) => formatNumber(value, 0),
    yTickFormat: (value) => formatNumber(value, 0),
    polyline: diagonal,
  });
}

function renderValidationDeltaChart(payload) {
  const rows = (payload.validation || [])
    .map((row) => ({
      label: row.leaderboard_label || row.run_id,
      value: Number(row.score_delta),
      runId: row.run_id,
      attemptId: row.attempt_id,
      score12m: row.score_12m,
      score36m: row.score_36m,
      color: Number(row.score_delta) >= 0 ? "#60d6c3" : "#ff9a76",
    }))
    .sort((left, right) => right.value - left.value);
  renderHorizontalBarChart(document.getElementById("validationDeltaChart"), rows, {
    title: "36m minus 12m score",
    xLabel: "36m - 12m quality score",
    valueFormat: (value, row) => `${value >= 0 ? "+" : ""}${formatNumber(value, 2)} (${formatNumber(row.score12m, 1)} → ${formatNumber(row.score36m, 1)})`,
  });
}

function renderSimilarityScatterChart(payload) {
  const points = (payload.similarity || []).map((row) => ({
    x: Number(row.max_sameness || 0),
    y: Number(row.score_36m),
    emphasis: Number(row.trades_per_month_36m || 0) >= 20,
    outline: false,
    tooltip:
      `<strong>${escapeHtml(row.run_id)}</strong><br>` +
      `${escapeHtml(row.candidate_name || "candidate")}<br>` +
      `36m ${formatNumber(row.score_36m, 2)}<br>` +
      `closest-match sameness ${formatNumber(row.max_sameness, 2)}<br>` +
      `${formatNumber(row.trades_per_month_36m, 1)} trades/mo<br>` +
      `${escapeHtml(row.closest_match_label || "no close match found")}`,
    onClick: () => setHashForRun(row.run_id, row.attempt_id),
  }));
  renderScatterChart(document.getElementById("similarityScatterChart"), points, {
    title: "Score vs closest-match sameness",
    xLabel: "Closest-match sameness",
    yLabel: "36m quality score",
    xTickFormat: (value) => formatNumber(value, 2),
    yTickFormat: (value) => formatNumber(value, 0),
    xDomain: [0, 1],
  });
}

function renderSimilarityPairsTable(payload) {
  renderSimpleTable(
    "similarityPairsTable",
    [
      {
        label: "Pair",
        render: (row) =>
          `<strong>${escapeHtml(row.left_run_id)}</strong><div class="muted">${escapeHtml(row.right_run_id)}</div>`,
      },
      { label: "Sameness", render: (row) => formatNumber(row.similarity_score, 2) },
      { label: "Corr", render: (row) => formatNumber(row.positive_correlation, 2) },
      { label: "Overlap", render: (row) => `${Math.round(Number(row.shared_active_ratio || 0) * 100)}%` },
    ],
    (payload.similarityPairs || []).map((row) => ({
      ...row,
      onClick: () => setHashForRun(row.left_run_id, row.left_attempt_id),
    })),
  );
}

function parseHash() {
  const raw = window.location.hash.replace(/^#/, "");
  const parts = raw.split("/").filter(Boolean).map(decodeURIComponent);
  if (parts[0] !== "run" || !parts[1]) return {};
  if (parts[2] === "attempt" && parts[3]) {
    return { runId: parts[1], attemptId: parts[3] };
  }
  return { runId: parts[1] };
}

async function ensureRunDetail(runId) {
  if (!runId) return null;
  if (state.runCache.has(runId)) return state.runCache.get(runId);
  const payload = await fetchJson(`/api/runs/${encodeURIComponent(runId)}`);
  state.runCache.set(runId, payload);
  return payload;
}

async function ensureAttemptDetail(runId, attemptId) {
  if (!runId || !attemptId) return null;
  const key = `${runId}::${attemptId}`;
  if (state.attemptCache.has(key)) return state.attemptCache.get(key);
  const payload = await fetchJson(`/api/runs/${encodeURIComponent(runId)}/attempts/${encodeURIComponent(attemptId)}`);
  state.attemptCache.set(key, payload);
  return payload;
}

function renderRunTimeline(runDetail) {
  const points = (runDetail.attempts || [])
    .filter((row) => row.score !== null && row.score !== undefined)
    .sort((left, right) => Number(left.sequence) - Number(right.sequence))
    .map((row) => ({
      x: Number(row.sequence),
      y: Number(row.score),
      emphasis: row.attemptId === runDetail.run.bestAttempt?.attemptId,
      outline: false,
      tooltip: `<strong>${escapeHtml(row.candidateName)}</strong><br>sequence ${escapeHtml(row.sequence)}<br>score ${formatNumber(row.score, 2)}`,
      onClick: () => setHashForRun(runDetail.run.runId, row.attemptId),
    }));
  renderScatterChart(document.getElementById("runTimelineChart"), points, {
    title: "Run score trace",
    xLabel: "Attempt sequence",
    yLabel: "Quality score",
    xTickFormat: (value) => formatNumber(value, 0),
    yTickFormat: (value) => formatNumber(value, 0),
    polyline: points.map((row) => ({ x: row.x, y: row.y })),
    height: 280,
  });
}

function renderCurve(detail) {
  const points = (((detail.curve || {}).curve || {}).points || []).map((point) => ({
    x: Number(point.time),
    y: Number(point.equity_r),
    drawdown: Number(point.drawdown_r),
    date: point.date,
    closedTradeCount: point.closed_trade_count,
  }));
  const container = document.getElementById("curveChart");
  if (!points.length) {
    container.innerHTML = `<div class="empty-state">No best-cell path detail for this attempt.</div>`;
    document.getElementById("drawdownCurveChart").innerHTML = "";
    return;
  }
  const equityPoints = points.map((point) => ({
    x: point.x,
    y: point.y,
    tooltip: `<strong>${escapeHtml(point.date)}</strong><br>equity ${formatNumber(point.y, 1)}R<br>drawdown ${formatNumber(point.drawdown, 1)}R<br>closed trades ${formatInt(point.closedTradeCount)}`,
  }));
  renderScatterChart(container, equityPoints.map((point) => ({ ...point, emphasis: false, outline: false })), {
    title: "Equity curve",
    xLabel: "Backtest timeline",
    yLabel: "Equity (R)",
    xTickFormat: (value) => new Date(Number(value) * 1000).toLocaleDateString(undefined, { month: "short", day: "numeric" }),
    yTickFormat: (value) => formatNumber(value, 0),
    polyline: points.map((point) => ({ x: point.x, y: point.y })),
    height: 340,
  });

  const drawdownContainer = document.getElementById("drawdownCurveChart");
  const drawdownPoints = points.map((point) => ({
    x: point.x,
    y: point.drawdown,
    tooltip: `<strong>${escapeHtml(point.date)}</strong><br>drawdown ${formatNumber(point.drawdown, 1)}R`,
  }));
  renderScatterChart(drawdownContainer, drawdownPoints.map((point) => ({ ...point, emphasis: false, outline: false })), {
    title: "Drawdown curve",
    xLabel: "Backtest timeline",
    yLabel: "Drawdown (R)",
    xTickFormat: (value) => new Date(Number(value) * 1000).toLocaleDateString(undefined, { month: "short", day: "numeric" }),
    yTickFormat: (value) => formatNumber(value, 0),
    polyline: points.map((point) => ({ x: point.x, y: point.drawdown })),
    height: 260,
  });
}

function renderAttemptDetail(detail) {
  const attempt = detail.attempt;
  const container = document.getElementById("attemptDetail");
  container.innerHTML = `
    <div class="attempt-detail-grid">
      <article class="mini-card span-3">
        <h3>Score</h3>
        <div class="metric-value">${attempt.score !== null && attempt.score !== undefined ? formatNumber(attempt.score, 2) : "—"}</div>
        <p>${escapeHtml(attempt.scoreBasis || "quality score")}</p>
      </article>
      <article class="mini-card span-3">
        <h3>Trades / month</h3>
        <div class="metric-value">${formatNumber(attempt.tradesPerMonth, 1)}</div>
        <p>${formatInt(attempt.tradeCount)} resolved trades</p>
      </article>
      <article class="mini-card span-3">
        <h3>Max drawdown</h3>
        <div class="metric-value">${formatNumber(attempt.maxDrawdownR, 1)}R</div>
        <p>effective window ${formatNumber(attempt.effectiveWindowMonths, 2)} months</p>
      </article>
      <article class="mini-card span-3">
        <h3>Expectancy</h3>
        <div class="metric-value">${formatNumber(attempt.expectancyR, 3)}R</div>
        <p>profit factor ${formatNumber(attempt.profitFactor, 2)}</p>
      </article>
      <section class="span-8 curve-panel">
        <div class="curve-meta">
          <span class="score-pill">${escapeHtml(attempt.instrument || "instrument n/a")}</span>
          <span class="tag">${escapeHtml(attempt.timeframe || "timeframe n/a")}</span>
          <span class="tag">${escapeHtml(attempt.signalSelectivity || "selectivity n/a")}</span>
          ${attempt.profileRef ? `<span class="tag">profile ${escapeHtml(attempt.profileRef)}</span>` : ""}
        </div>
        <div id="curveChart" class="chart-frame"></div>
        <div id="drawdownCurveChart" class="chart-frame"></div>
      </section>
      <section class="span-4">
        <div class="payload-grid">
          ${detail.profileDrop12PngUrl ? `<div class="image-frame"><img alt="Profile drop 12mo" src="${escapeHtml(detail.profileDrop12PngUrl)}&t=${Date.now()}" /></div>` : ""}
          ${detail.profileDrop36PngUrl ? `<div class="image-frame"><img alt="Profile drop 36mo" src="${escapeHtml(detail.profileDrop36PngUrl)}&t=${Date.now()}" /></div>` : ""}
          ${!detail.profileDrop12PngUrl && !detail.profileDrop36PngUrl && detail.profileDropPngUrl ? `<div class="image-frame"><img alt="Profile drop" src="${escapeHtml(detail.profileDropPngUrl)}&t=${Date.now()}" /></div>` : ""}
          ${!detail.profileDrop12PngUrl && !detail.profileDrop36PngUrl && !detail.profileDropPngUrl ? `<div class="empty-state">No profile-drop PNG rendered for this run yet.</div>` : ""}
        </div>
      </section>
      <section class="span-12 payload-grid">
        <details open>
          <summary>Profile payload</summary>
          <pre>${escapeHtml(JSON.stringify(detail.profile || {}, null, 2))}</pre>
        </details>
        <details>
          <summary>Deep replay request</summary>
          <pre>${escapeHtml(JSON.stringify(detail.deepReplayJob || {}, null, 2))}</pre>
        </details>
        <details>
          <summary>Best summary</summary>
          <pre>${escapeHtml(JSON.stringify(attempt.bestSummary || {}, null, 2))}</pre>
        </details>
      </section>
    </div>
  `;
  renderCurve(detail);
}

async function renderRunDetailFromHash() {
  const { runId, attemptId } = parseHash();
  const title = document.getElementById("runDetailTitle");
  const meta = document.getElementById("runDetailMeta");
  const container = document.getElementById("runDetail");
  const attemptTitle = document.getElementById("attemptDetailTitle");
  const attemptMeta = document.getElementById("attemptDetailMeta");
  const attemptContainer = document.getElementById("attemptDetail");

  if (!runId) {
    title.textContent = "Select a run";
    meta.textContent = "";
    container.innerHTML = `Pick a run from the table or a point from the charts.`;
    attemptTitle.textContent = "Select an attempt";
    attemptMeta.textContent = "";
    attemptContainer.innerHTML = `Once a run is selected, pick an attempt to inspect its backtest path and profile payload.`;
    return;
  }

  const runDetail = await ensureRunDetail(runId);
  title.textContent = runDetail.run.runId;
  meta.textContent = `${runDetail.run.explorerModel || runDetail.run.explorerProfile || "unknown explorer"} • ${formatTime(runDetail.run.createdAt)} • ${formatInt(runDetail.run.attemptCount)} attempts`;

  container.innerHTML = `
    <div class="run-detail-grid">
      <article class="mini-card span-3">
        <h3>Leader</h3>
        <p>${escapeHtml(runDetail.run.bestAttempt?.candidateName || "none")}</p>
        <div class="metric-value">${runDetail.run.bestAttempt?.score !== null && runDetail.run.bestAttempt?.score !== undefined ? formatNumber(runDetail.run.bestAttempt.score, 2) : "—"}</div>
      </article>
      <article class="mini-card span-3">
        <h3>Advisors seen</h3>
        <div class="metric-value">${formatInt(runDetail.run.advisorGuidanceCount)}</div>
        <p>${escapeHtml(runDetail.run.latestStep || "—")} latest step</p>
      </article>
      <article class="mini-card span-3">
        <h3>Curve coverage</h3>
        <div class="metric-value">${formatInt(runDetail.run.curveAttemptCount)}</div>
        <p>attempts with persisted path detail</p>
      </article>
      <article class="mini-card span-3">
        <h3>Profile drop</h3>
        <div class="metric-value">${runDetail.run.profileDropPngUrl ? "Yes" : "No"}</div>
        <p>${runDetail.run.qualityScorePreset || "profile-drop"}</p>
      </article>
      <section class="span-12">
        <div class="attempt-list-header">
          <div>
            <h3>Attempts in this run</h3>
            <p class="muted">Best scores float to the top. Click through for curve detail.</p>
          </div>
        </div>
        <div id="runAttemptsTable"></div>
      </section>
      <section class="span-12">
        <div id="runTimelineChart" class="chart-frame"></div>
      </section>
    </div>
  `;

  renderSimpleTable(
    "runAttemptsTable",
    [
      { label: "Attempt", render: (row) => `<strong>${escapeHtml(row.candidateName || "candidate")}</strong><div class="muted">#${escapeHtml(row.sequence)}</div>` },
      { label: "Score", render: (row) => scoreTag(row.score) },
      { label: "Trades/mo", render: (row) => formatNumber(row.tradesPerMonth, 1) },
      { label: "DD", render: (row) => `${formatNumber(row.maxDrawdownR, 1)}R` },
      { label: "PF", render: (row) => formatNumber(row.profitFactor, 2) },
      { label: "Style", render: (row) => escapeHtml(row.signalSelectivity || "—") },
      { label: "Instrument", render: (row) => escapeHtml(row.instrument || "—") },
    ],
    (runDetail.attempts || []).map((row) => ({
      ...row,
      onClick: () => setHashForRun(runId, row.attemptId),
    })),
  );
  renderRunTimeline(runDetail);

  const selectedAttemptId = attemptId || runDetail.run.bestAttempt?.attemptId;
  if (!selectedAttemptId) {
    attemptTitle.textContent = "Select an attempt";
    attemptMeta.textContent = "";
    attemptContainer.innerHTML = `This run has no scored attempts yet.`;
    return;
  }

  const attemptDetail = await ensureAttemptDetail(runId, selectedAttemptId);
  attemptTitle.textContent = attemptDetail.attempt.candidateName || selectedAttemptId;
  attemptMeta.textContent = `${formatTime(attemptDetail.attempt.createdAt)} • ${formatNumber(attemptDetail.attempt.score, 2)} score • ${formatNumber(attemptDetail.attempt.tradesPerMonth, 1)} trades/mo`;
  renderAttemptDetail(attemptDetail);
}

async function loadOverview(refresh = false) {
  const refreshButton = document.getElementById("refreshButton");
  refreshButton.disabled = true;
  setStatus(refresh ? "Refreshing source artifacts…" : "Loading dashboard…");
  try {
    const payload = await fetchJson(refresh ? "/api/refresh" : "/api/overview", refresh ? { method: "POST" } : {});
    state.overview = payload;
    document.getElementById("generatedAt").textContent = formatTime(payload.overview.generatedAt);
    renderOverviewCards(payload.overview);
    renderGallery(payload.images);
    renderModelTable(payload);
    renderRunsTable(payload);
    renderTradeoffChart(payload);
    renderDrawdownChart(payload);
    renderValidationScatterChart(payload);
    renderValidationDeltaChart(payload);
    renderSimilarityScatterChart(payload);
    renderSimilarityPairsTable(payload);
    setStatus(refresh ? "Refresh complete." : "Ready.");
    await renderRunDetailFromHash();
  } catch (error) {
    console.error(error);
    setStatus(`Failed: ${error.message}`);
  } finally {
    refreshButton.disabled = false;
  }
}

function wireEvents() {
  document.getElementById("refreshButton").addEventListener("click", () => {
    state.runCache.clear();
    state.attemptCache.clear();
    loadOverview(true);
  });
  window.addEventListener("hashchange", () => {
    renderRunDetailFromHash();
  });
  window.addEventListener("resize", () => {
    if (state.overview) {
      renderTradeoffChart(state.overview);
      renderDrawdownChart(state.overview);
      renderValidationScatterChart(state.overview);
      renderValidationDeltaChart(state.overview);
      renderSimilarityScatterChart(state.overview);
      renderSimilarityPairsTable(state.overview);
      renderRunDetailFromHash();
    }
  });
}

async function init() {
  wireEvents();
  await loadOverview(false);
}

init();
