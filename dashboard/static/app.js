// dashboard/static/app.js
// Auto-refreshes all panels every 5 s via parallel fetch calls to JSON endpoints.
// Never triggers a full page reload.

const REFRESH_MS = 5000;
let logsOpen = true;

// ── Utilities ─────────────────────────────────────────────────────────────────

function esc(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// Smart price formatter: preserves significant digits across instruments.
function fmtPrice(v) {
  const n = Number(v);
  if (v === null || v === undefined || isNaN(n)) return "—";
  if (n > 999)  return n.toFixed(2);   // XAUUSD, BTCUSD
  if (n > 10)   return n.toFixed(4);   // USDJPY
  return n.toFixed(5);                  // EURUSD, GBPUSD
}

function fmtPnl(v) {
  const n = Number(v);
  if (v === null || v === undefined || isNaN(n)) return "—";
  const sign = n >= 0 ? "+" : "−";
  return `${sign}$${Math.abs(n).toFixed(2)}`;
}

function pnlClass(v) {
  const n = Number(v);
  if (isNaN(n) || n === 0) return "muted";
  return n > 0 ? "green" : "red";
}

// "2026-06-04T22:33:25.123456+00:00"  →  "2026-06-04 22:33"
function fmtTs(iso) {
  if (!iso) return "—";
  return String(iso).slice(0, 16).replace("T", " ");
}

function timeAgo(iso) {
  if (!iso) return "—";
  const then = new Date(iso);
  if (isNaN(then)) return "—";
  const ms = Date.now() - then.getTime();
  if (ms < 0) return "just now";
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m ago`;
  return `${Math.floor(h / 24)}d ago`;
}

// Pass-fail icon for risk check columns.
function check(v) {
  return v
    ? '<span class="green">✓</span>'
    : '<span class="red">✗</span>';
}

// Color class for funnel drop between two stages.
function dropClass(dropped, base) {
  if (!base || dropped <= 0) return "muted";
  const pct = dropped / base;
  if (pct > 0.5) return "red";
  if (pct > 0.2) return "amber";
  return "green";
}

// ── Funnel ────────────────────────────────────────────────────────────────────

function updateFunnel(d) {
  const zt = d.zone_touches;
  const sg = d.signals;
  const ra = d.risk_approved;
  const rr = d.risk_rejected;
  const ex = d.executed;
  const ef = d.exec_failed;

  const pct = (n, base) => (base > 0 ? Math.round((n / base) * 100) : 0);

  const stages = [
    { label: "ZONE TOUCH",   count: zt.total, today: zt.today, pct: 100,                        isBase: true },
    { label: "GPT SIGNAL",   count: sg.total, today: sg.today, pct: pct(sg.total, zt.total) },
    { label: "RISK GATE ✓",  count: ra.total, today: ra.today, pct: pct(ra.total, zt.total) },
    { label: "EXECUTED",     count: ex.total, today: ex.today, pct: pct(ex.total, zt.total) },
  ];

  const drops = [
    { n: zt.total - sg.total, base: zt.total },
    { n: sg.total - ra.total, base: sg.total },
    { n: ra.total - ex.total, base: ra.total },
  ];

  let pipeline = '<div class="pipeline">';
  stages.forEach((s, i) => {
    const pctCls = s.isBase ? "amber" : (s.pct >= 70 ? "green" : s.pct >= 35 ? "amber" : "red");
    pipeline += `
      <div class="pipe-stage">
        <div class="pipe-label">${s.label}</div>
        <div class="pipe-count">${s.count}</div>
        <div class="pipe-today muted">+${s.today} today</div>
        <div class="pipe-pct ${pctCls}">${s.isBase ? "base" : s.pct + "%"}</div>
      </div>`;
    if (i < drops.length) {
      const drop = drops[i];
      const cls = dropClass(drop.n, drop.base);
      pipeline += `
        <div class="pipe-connector">
          <span class="drop-count ${cls}">${drop.n > 0 ? "−" + drop.n : "="}</span>
          <span class="arrow-line">──▶</span>
        </div>`;
    }
  });
  pipeline += "</div>";

  // Summary badges — warn on today's rejections / failures.
  let summary = '<div class="funnel-summary">';
  if (rr.today > 0)
    summary += `<span class="badge badge-warn">⚠ ${rr.today} rejection${rr.today > 1 ? "s" : ""} today</span>`;
  if (ef.today > 0)
    summary += `<span class="badge badge-error">✗ ${ef.today} exec failure${ef.today > 1 ? "s" : ""} today</span>`;
  if (rr.total > 0)
    summary += `<span class="badge badge-muted">total rejected: ${rr.total}</span>`;
  if (rr.today === 0 && ef.today === 0)
    summary += `<span class="badge badge-ok">✓ no anomalies today</span>`;
  summary += "</div>";

  document.getElementById("funnel-data").innerHTML = pipeline + summary;
}

// ── P&L ───────────────────────────────────────────────────────────────────────

function updatePnl(d) {
  // Banners for daily limit proximity.
  let banners = "";
  if (d.hit_loss) {
    banners += `<div class="banner banner-error">⛔ DAILY LOSS LIMIT HIT (-$${d.daily_loss_limit}) — new trades blocked</div>`;
  } else if (d.warn_loss) {
    banners += `<div class="banner banner-warn">⚠ Approaching daily loss limit (${d.loss_used_pct}% used)</div>`;
  }
  if (d.hit_target) {
    banners += `<div class="banner banner-ok">✓ Daily profit target reached ($${d.daily_profit_target})</div>`;
  } else if (d.warn_target) {
    banners += `<div class="banner banner-info">↑ Near daily profit target (${d.profit_used_pct}%)</div>`;
  }

  const lossBar   = Math.min(d.loss_used_pct, 100);
  const profitBar = Math.min(d.profit_used_pct, 100);
  const lossBarCls = lossBar >= 100 ? "fill-red" : lossBar >= 70 ? "fill-amber" : "fill-muted";

  const pnlRows = [
    { label: "TODAY",     val: d.today     },
    { label: "THIS WEEK", val: d.week      },
    { label: "ALL TIME",  val: d.all_time  },
  ].map(r => `
    <div class="pnl-row">
      <span class="pnl-label muted">${r.label}</span>
      <span class="pnl-value ${pnlClass(r.val)}">${fmtPnl(r.val)}</span>
    </div>`).join("");

  const bars = `
    <div class="bar-section">
      <div class="bar-row">
        <span class="bar-label muted">LOSS</span>
        <div class="progress-bar">
          <div class="progress-fill ${lossBarCls}" style="width:${lossBar}%"></div>
        </div>
        <span class="bar-val muted">$${Math.abs(Math.min(d.today, 0)).toFixed(2)} / $${d.daily_loss_limit}</span>
      </div>
      <div class="bar-row">
        <span class="bar-label muted">TARGET</span>
        <div class="progress-bar">
          <div class="progress-fill fill-green" style="width:${profitBar}%"></div>
        </div>
        <span class="bar-val muted">$${Math.max(d.today, 0).toFixed(2)} / $${d.daily_profit_target}</span>
      </div>
    </div>`;

  document.getElementById("pnl-data").innerHTML =
    `<div class="pnl-body">${banners}${pnlRows}${bars}</div>`;
}

// ── Agent Health ──────────────────────────────────────────────────────────────

function healthStatus(lastSeen, staleHours) {
  if (!lastSeen) return { cls: "muted", label: "NO DATA", dot: "○" };
  const hoursAgo = (Date.now() - new Date(lastSeen).getTime()) / 3_600_000;
  if (staleHours && hoursAgo > staleHours)
    return { cls: "red", label: "STALLED", dot: "○" };
  return { cls: "green", label: "OK", dot: "●" };
}

function updateHealth(d) {
  // stale threshold only set for SR Mapper (periodic scan every 4 h).
  // Other agents only emit events when something happens so absence ≠ stalled.
  const agents = [
    { key: "sr_mapper",      label: "SR MAPPER",      desc: "Zone scan · refreshes every 4 h",    stale: 5.5 },
    { key: "price_watcher",  label: "PRICE WATCHER",  desc: "Zone touch detector · 2 s tick",     stale: null },
    { key: "analysis_agent", label: "ANALYSIS AGENT", desc: "GPT-4o signal generator",             stale: null },
    { key: "trade_monitor",  label: "TRADE MONITOR",  desc: "MT5 close detector · 30 s poll",     stale: null },
  ];

  const cards = agents.map(a => {
    const last = d[a.key];
    const s = healthStatus(last, a.stale);
    return `
      <div class="health-agent">
        <div class="agent-name">${a.label}</div>
        <div class="agent-desc">${a.desc}</div>
        <div class="agent-last">${timeAgo(last)}</div>
        <div class="agent-status ${s.cls}">${s.dot} ${s.label}</div>
        ${last ? `<div class="agent-ts">${fmtTs(last)}</div>` : ""}
      </div>`;
  }).join("");

  document.getElementById("health-data").innerHTML =
    `<div class="health-grid">${cards}</div>`;
}

// ── Trades ────────────────────────────────────────────────────────────────────

function tradeStatus(r) {
  if (!r.success)    return { label: "FAILED",   cls: "red" };
  if (!r.close_time) return { label: r.dry_run ? "DRY-OPEN" : "OPEN", cls: "amber" };
  const pnl = Number(r.realized_pnl);
  if (pnl > 0)       return { label: "WON",     cls: "green" };
  if (pnl < 0)       return { label: "LOST",    cls: "red" };
  return { label: "CLOSED", cls: "muted" };
}

function updateTrades(rows) {
  if (!rows.length) {
    document.getElementById("trades-data").innerHTML =
      '<div class="empty">No trades recorded yet</div>';
    return;
  }

  const head = `<thead><tr>
    <th>TIME (UTC)</th><th>SYMBOL</th><th>DIR</th>
    <th>ENTRY</th><th>SL</th><th>TP</th>
    <th>FILL</th><th>P&amp;L</th><th>STATUS</th><th>DRY</th>
  </tr></thead>`;

  const body = rows.map(r => {
    const st     = tradeStatus(r);
    const dirCls = r.direction === "BUY" ? "green" : "red";
    return `<tr>
      <td class="muted">${fmtTs(r.created_at)}</td>
      <td>${esc(r.symbol)}</td>
      <td class="${dirCls}">${esc(r.direction)}</td>
      <td>${fmtPrice(r.entry)}</td>
      <td class="red">${fmtPrice(r.stop_loss)}</td>
      <td class="green">${fmtPrice(r.take_profit)}</td>
      <td>${fmtPrice(r.fill_price)}</td>
      <td class="${pnlClass(r.realized_pnl)}">${fmtPnl(r.realized_pnl)}</td>
      <td class="${st.cls}">${st.label}</td>
      <td class="muted">${r.dry_run ? "✓" : ""}</td>
    </tr>`;
  }).join("");

  document.getElementById("trades-data").innerHTML =
    `<div class="table-wrap"><table class="data-table">${head}<tbody>${body}</tbody></table></div>`;
}

// ── Risk Rejections ───────────────────────────────────────────────────────────

function updateRejections(rows) {
  if (!rows.length) {
    document.getElementById("rejections-data").innerHTML =
      '<div class="empty">No rejections recorded yet</div>';
    return;
  }

  const head = `<thead><tr>
    <th>TIME (UTC)</th><th>SYMBOL</th><th>DIR</th>
    <th title="Computed R:R ratio">R:R</th>
    <th>REASON</th>
    <th title="R:R check ≥ 1.5">RR</th>
    <th title="Max open trades check">MT</th>
    <th title="Correlation check">CR</th>
    <th title="Daily loss gate">DL</th>
    <th title="Weekly loss gate">WL</th>
  </tr></thead>`;

  const body = rows.map(r => {
    const dirCls = r.direction === "BUY" ? "green" : "red";
    const rr = (r.rr_ratio !== null && r.rr_ratio !== undefined)
      ? `<span class="${r.rr_ratio < 1.5 ? "red" : "green"}">${Number(r.rr_ratio).toFixed(2)}</span>`
      : "<span class='muted'>—</span>";
    return `<tr>
      <td class="muted">${fmtTs(r.created_at)}</td>
      <td>${esc(r.symbol || "—")}</td>
      <td class="${dirCls}">${esc(r.direction || "—")}</td>
      <td>${rr}</td>
      <td class="muted reason-cell" title="${esc(r.reason || "")}">${esc(r.reason || "—")}</td>
      <td>${check(r.rr_ok)}</td>
      <td>${check(r.max_trades_ok)}</td>
      <td>${check(r.correlation_ok)}</td>
      <td>${check(r.daily_loss_ok)}</td>
      <td>${check(r.weekly_loss_ok)}</td>
    </tr>`;
  }).join("");

  document.getElementById("rejections-data").innerHTML =
    `<div class="table-wrap"><table class="data-table">${head}<tbody>${body}</tbody></table></div>`;
}

// ── Log Tail ──────────────────────────────────────────────────────────────────

function updateLogs(data) {
  const { lines, path } = data;

  // Show the log filename in the header.
  if (path) {
    const label = document.getElementById("log-path-label");
    if (label) label.textContent = path.replace(/.*[\\/]/, "");
  }

  const container = document.getElementById("logs-data");
  // Only auto-scroll if already at the bottom.
  const atBottom = container.scrollHeight - container.scrollTop <= container.clientHeight + 60;

  const html = lines.map(l => {
    const cls = { ERROR: "log-error", WARNING: "log-warn", INFO: "log-info", DEBUG: "log-debug" }[l.level] || "log-other";
    return `<div class="${cls}">${esc(l.text)}</div>`;
  }).join("") || `<span class="muted">No log lines</span>`;

  container.innerHTML = html;
  if (atBottom) container.scrollTop = container.scrollHeight;
}

// ── Log collapse ──────────────────────────────────────────────────────────────

document.getElementById("logs-toggle").addEventListener("click", () => {
  logsOpen = !logsOpen;
  document.getElementById("logs-container").style.display = logsOpen ? "" : "none";
  document.getElementById("logs-chevron").textContent = logsOpen ? "▼" : "▶";
});

// ── Clock ─────────────────────────────────────────────────────────────────────

function updateClock() {
  const n = new Date();
  const pad = v => String(v).padStart(2, "0");
  const el = document.getElementById("clock");
  if (el) el.textContent = `${pad(n.getUTCHours())}:${pad(n.getUTCMinutes())}:${pad(n.getUTCSeconds())} UTC`;
}

// ── Main refresh loop ─────────────────────────────────────────────────────────

async function refreshAll() {
  try {
    const [funnel, pnl, health, trades, rejections, logs] = await Promise.all([
      fetch("/api/funnel").then(r => { if (!r.ok) throw new Error(r.status); return r.json(); }),
      fetch("/api/pnl").then(r => { if (!r.ok) throw new Error(r.status); return r.json(); }),
      fetch("/api/health").then(r => { if (!r.ok) throw new Error(r.status); return r.json(); }),
      fetch("/api/trades").then(r => { if (!r.ok) throw new Error(r.status); return r.json(); }),
      fetch("/api/rejections").then(r => { if (!r.ok) throw new Error(r.status); return r.json(); }),
      fetch("/api/logs").then(r => { if (!r.ok) throw new Error(r.status); return r.json(); }),
    ]);

    updateFunnel(funnel);
    updatePnl(pnl);
    updateHealth(health);
    updateTrades(trades);
    updateRejections(rejections);
    updateLogs(logs);

    document.getElementById("status-dot").className  = "dot green";
    document.getElementById("status-text").textContent = "live";
  } catch (err) {
    document.getElementById("status-dot").className  = "dot red";
    document.getElementById("status-text").textContent = "offline — " + err.message;
  }
}

// Boot.
refreshAll();
setInterval(refreshAll, REFRESH_MS);
setInterval(updateClock, 1000);
updateClock();
