/* ═══════════════════════════════════════════════════════════════════
   HubSpot Big Data Analytics Dashboard — Frontend Engine
   ═══════════════════════════════════════════════════════════════════ */

// ── Chart.js Global Config ─────────────────────────────────────────
Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = 'rgba(99, 102, 241, 0.08)';
Chart.defaults.font.family = "'Inter', -apple-system, sans-serif";
Chart.defaults.font.size = 12;
Chart.defaults.plugins.legend.labels.usePointStyle = true;
Chart.defaults.plugins.legend.labels.pointStyleWidth = 12;
Chart.defaults.plugins.legend.labels.padding = 20;
Chart.defaults.plugins.tooltip.backgroundColor = 'rgba(10, 14, 26, 0.95)';
Chart.defaults.plugins.tooltip.borderColor = 'rgba(99, 102, 241, 0.3)';
Chart.defaults.plugins.tooltip.borderWidth = 1;
Chart.defaults.plugins.tooltip.cornerRadius = 10;
Chart.defaults.plugins.tooltip.padding = 12;
Chart.defaults.plugins.tooltip.titleFont = { weight: '700', size: 13 };
Chart.defaults.plugins.tooltip.bodyFont = { size: 12 };
Chart.defaults.elements.point.radius = 0;
Chart.defaults.elements.point.hoverRadius = 6;
Chart.defaults.elements.point.hoverBorderWidth = 2;

// ── Color Palette ──────────────────────────────────────────────────
const COLORS = {
  blue:   { bg: 'rgba(99, 102, 241, 0.15)',  border: '#6366f1', solid: '#6366f1' },
  cyan:   { bg: 'rgba(34, 211, 238, 0.15)',  border: '#22d3ee', solid: '#22d3ee' },
  green:  { bg: 'rgba(16, 185, 129, 0.15)',  border: '#10b981', solid: '#10b981' },
  orange: { bg: 'rgba(245, 158, 11, 0.15)',  border: '#f59e0b', solid: '#f59e0b' },
  pink:   { bg: 'rgba(236, 72, 153, 0.15)',  border: '#ec4899', solid: '#ec4899' },
  purple: { bg: 'rgba(167, 139, 250, 0.15)', border: '#a78bfa', solid: '#a78bfa' },
  red:    { bg: 'rgba(239, 68, 68, 0.15)',   border: '#ef4444', solid: '#ef4444' },
  teal:   { bg: 'rgba(20, 184, 166, 0.15)',  border: '#14b8a6', solid: '#14b8a6' },
};

const PALETTE = [COLORS.blue, COLORS.cyan, COLORS.green, COLORS.orange, COLORS.pink, COLORS.purple, COLORS.red, COLORS.teal];

// ── Utilities ──────────────────────────────────────────────────────
function fmt(n, decimals = 0) {
  if (n == null) return '—';
  if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(1) + 'B';
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return decimals > 0 ? n.toFixed(decimals) : n.toLocaleString();
}

function fmtDollar(n) {
  if (n == null) return '—';
  if (Math.abs(n) >= 1e9) return '$' + (n / 1e9).toFixed(2) + 'B';
  if (Math.abs(n) >= 1e6) return '$' + (n / 1e6).toFixed(1) + 'M';
  if (Math.abs(n) >= 1e3) return '$' + (n / 1e3).toFixed(1) + 'K';
  return '$' + n.toLocaleString();
}

function pct(n) {
  return n != null ? n.toFixed(1) + '%' : '—';
}

const charts = {};

function createChart(canvasId, config) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return null;
  if (charts[canvasId]) charts[canvasId].destroy();
  const chart = new Chart(canvas, config);
  charts[canvasId] = chart;
  return chart;
}

function createGradient(ctx, c1, c2) {
  const g = ctx.createLinearGradient(0, 0, 0, ctx.canvas.height);
  g.addColorStop(0, c1);
  g.addColorStop(1, c2);
  return g;
}

// ── Navigation ─────────────────────────────────────────────────────
function initNav() {
  const tabs = document.querySelectorAll('.nav-tab');
  const sections = document.querySelectorAll('.section');

  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      const target = tab.dataset.section;
      tabs.forEach(t => t.classList.remove('active'));
      sections.forEach(s => s.classList.remove('active'));
      tab.classList.add('active');
      document.getElementById(target).classList.add('active');

      // Resize charts in the newly visible section
      setTimeout(() => {
        Object.values(charts).forEach(c => c.resize());
      }, 50);
    });
  });
}

// ── KPI Rendering ──────────────────────────────────────────────────
function renderKPIs(kpis) {
  const el = (id) => document.getElementById(id);
  el('kpi-revenue').textContent = fmtDollar(kpis.won_revenue);
  el('kpi-pipeline').textContent = fmtDollar(kpis.pipeline_value);
  el('kpi-deals').textContent = fmt(kpis.total_deals);
  el('kpi-winrate').textContent = pct(kpis.win_rate);
  el('kpi-avgdeal').textContent = fmtDollar(kpis.avg_deal_size);
  el('kpi-contacts').textContent = fmt(kpis.total_contacts);
  el('kpi-companies').textContent = fmt(kpis.total_companies);
  el('kpi-sessions').textContent = fmt(kpis.total_sessions);
  el('kpi-conversion').textContent = pct(kpis.conversion_rate);
  el('kpi-bounce').textContent = pct(kpis.bounce_rate);
  el('kpi-csat').textContent = kpis.avg_csat + '/5';
  el('kpi-sla').textContent = pct(kpis.sla_compliance);
  el('kpi-openrate').textContent = pct(kpis.email_open_rate);
  el('kpi-clickrate').textContent = pct(kpis.email_click_rate);
  el('kpi-tickets').textContent = fmt(kpis.total_tickets);

  el('header-records').textContent = fmt(
    kpis.total_contacts + kpis.total_deals + kpis.total_sessions + kpis.total_tickets + kpis.total_companies
  );
}

// ── Revenue & Deals Charts ─────────────────────────────────────────
function renderRevenueTrend(data) {
  const ctx = document.getElementById('chart-revenue-trend');
  if (!ctx || !data.labels) return;

  createChart('chart-revenue-trend', {
    type: 'line',
    data: {
      labels: data.labels,
      datasets: [
        {
          label: 'Closed Revenue',
          data: data.datasets[0].data,
          borderColor: COLORS.blue.border,
          backgroundColor: createGradient(ctx.getContext('2d'), 'rgba(99,102,241,0.25)', 'rgba(99,102,241,0)'),
          fill: true,
          tension: 0.4,
          borderWidth: 2.5,
        },
        {
          label: 'Weighted Pipeline',
          data: data.datasets[1].data,
          borderColor: COLORS.cyan.border,
          backgroundColor: createGradient(ctx.getContext('2d'), 'rgba(34,211,238,0.15)', 'rgba(34,211,238,0)'),
          fill: true,
          tension: 0.4,
          borderWidth: 2.5,
          borderDash: [6, 4],
        },
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      scales: {
        y: {
          ticks: { callback: v => fmtDollar(v) },
          grid: { color: 'rgba(99,102,241,0.06)' }
        },
        x: {
          ticks: { maxTicksLimit: 12 },
          grid: { display: false }
        }
      },
      plugins: {
        tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + fmtDollar(ctx.raw) } }
      }
    }
  });
}

function renderDealsByStage(data) {
  if (!data.labels) return;
  const maxCount = Math.max(...data.counts);

  const funnel = document.getElementById('funnel-stages');
  if (!funnel) return;

  const colors = ['#6366f1', '#8b5cf6', '#a78bfa', '#f59e0b', '#10b981', '#ef4444'];
  funnel.innerHTML = data.labels.map((label, i) => {
    const pctWidth = Math.max((data.counts[i] / maxCount) * 100, 8);
    return `
      <div class="funnel-step">
        <span class="funnel-step-label">${label}</span>
        <div class="funnel-step-bar-bg">
          <div class="funnel-step-bar" style="width:${pctWidth}%;background:${colors[i]}">
            ${fmt(data.counts[i])}
          </div>
        </div>
        <span class="funnel-step-value" style="color:${colors[i]}">${fmtDollar(data.values[i])}</span>
      </div>`;
  }).join('');

  // Animate bars
  setTimeout(() => {
    funnel.querySelectorAll('.funnel-step-bar').forEach(bar => {
      bar.style.width = bar.style.width;
    });
  }, 100);
}

function renderDealsByRegion(data) {
  if (!data.labels) return;
  createChart('chart-deals-region', {
    type: 'bar',
    data: {
      labels: data.labels,
      datasets: [{
        label: 'Revenue',
        data: data.revenue,
        backgroundColor: PALETTE.map(c => c.bg),
        borderColor: PALETTE.map(c => c.border),
        borderWidth: 1.5,
        borderRadius: 8,
        borderSkipped: false,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: 'y',
      scales: {
        x: { ticks: { callback: v => fmtDollar(v) }, grid: { color: 'rgba(99,102,241,0.06)' } },
        y: { grid: { display: false } }
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            afterLabel: (ctx) => `Win Rate: ${pct(data.win_rate[ctx.dataIndex])}\nDeals: ${fmt(data.deal_count[ctx.dataIndex])}`
          }
        }
      }
    }
  });
}

function renderDealsByIndustry(data) {
  if (!data.labels) return;
  const top10 = {
    labels: data.labels.slice(0, 10),
    revenue: data.revenue.slice(0, 10),
    win_rate: data.win_rate.slice(0, 10),
  };
  createChart('chart-deals-industry', {
    type: 'bar',
    data: {
      labels: top10.labels,
      datasets: [{
        label: 'Revenue',
        data: top10.revenue,
        backgroundColor: PALETTE.map(c => c.bg).concat(PALETTE.map(c => c.bg)),
        borderColor: PALETTE.map(c => c.border).concat(PALETTE.map(c => c.border)),
        borderWidth: 1.5,
        borderRadius: 8,
        borderSkipped: false,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: { ticks: { callback: v => fmtDollar(v) }, grid: { color: 'rgba(99,102,241,0.06)' } },
        x: { grid: { display: false }, ticks: { maxRotation: 45 } }
      },
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: ctx => 'Revenue: ' + fmtDollar(ctx.raw) + ' | Win: ' + pct(top10.win_rate[ctx.dataIndex]) } }
      }
    }
  });
}

function renderDealsByPipeline(data) {
  if (!data.labels) return;
  createChart('chart-deals-pipeline', {
    type: 'doughnut',
    data: {
      labels: data.labels,
      datasets: [{
        data: data.revenue,
        backgroundColor: [COLORS.blue.solid, COLORS.cyan.solid, COLORS.green.solid, COLORS.orange.solid],
        borderWidth: 0,
        hoverOffset: 8,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '65%',
      plugins: {
        legend: { position: 'bottom' },
        tooltip: { callbacks: { label: ctx => ctx.label + ': ' + fmtDollar(ctx.raw) + ' (' + pct(data.win_rate[ctx.dataIndex]) + ' win)' } }
      }
    }
  });
}

// ── Marketing Charts ───────────────────────────────────────────────
function renderMarketingChannels(data) {
  if (!data.labels) return;
  createChart('chart-mktg-channels', {
    type: 'radar',
    data: {
      labels: data.labels,
      datasets: [
        {
          label: 'Volume (K)',
          data: data.event_count.map(v => v / 1000),
          borderColor: COLORS.blue.border,
          backgroundColor: COLORS.blue.bg,
          pointBackgroundColor: COLORS.blue.solid,
          borderWidth: 2,
        },
        {
          label: 'Engagement %',
          data: data.engagement_rate,
          borderColor: COLORS.pink.border,
          backgroundColor: COLORS.pink.bg,
          pointBackgroundColor: COLORS.pink.solid,
          borderWidth: 2,
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        r: {
          grid: { color: 'rgba(99,102,241,0.1)' },
          angleLines: { color: 'rgba(99,102,241,0.1)' },
          ticks: { display: false },
        }
      }
    }
  });
}

function renderMarketingTrend(data) {
  if (!data.labels) return;
  const ctx = document.getElementById('chart-mktg-trend');
  if (!ctx) return;
  createChart('chart-mktg-trend', {
    type: 'line',
    data: {
      labels: data.labels,
      datasets: [
        {
          label: 'Events',
          data: data.datasets[0].data,
          borderColor: COLORS.purple.border,
          backgroundColor: createGradient(ctx.getContext('2d'), 'rgba(167,139,250,0.2)', 'rgba(167,139,250,0)'),
          fill: true,
          tension: 0.4,
          borderWidth: 2.5,
          yAxisID: 'y',
        },
        {
          label: 'Engagement %',
          data: data.datasets[1].data,
          borderColor: COLORS.green.border,
          tension: 0.4,
          borderWidth: 2.5,
          borderDash: [6, 4],
          yAxisID: 'y1',
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      scales: {
        y: { position: 'left', ticks: { callback: v => fmt(v) }, grid: { color: 'rgba(99,102,241,0.06)' } },
        y1: { position: 'right', ticks: { callback: v => v + '%' }, grid: { drawOnChartArea: false } },
        x: { ticks: { maxTicksLimit: 12 }, grid: { display: false } },
      }
    }
  });
}

function renderMarketingEventTypes(data) {
  if (!data.labels) return;
  createChart('chart-mktg-events', {
    type: 'polarArea',
    data: {
      labels: data.labels,
      datasets: [{
        data: data.event_count,
        backgroundColor: PALETTE.map(c => c.bg).concat(PALETTE.map(c => c.bg)),
        borderColor: PALETTE.map(c => c.border).concat(PALETTE.map(c => c.border)),
        borderWidth: 1.5,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        r: { grid: { color: 'rgba(99,102,241,0.08)' }, ticks: { display: false } }
      },
      plugins: {
        legend: { position: 'right', labels: { font: { size: 11 } } },
        tooltip: { callbacks: { label: ctx => ctx.label + ': ' + fmt(ctx.raw) } }
      }
    }
  });
}

// ── Email Charts ───────────────────────────────────────────────────
function renderEmailPerformance(data) {
  if (!data.labels) return;
  createChart('chart-email-perf', {
    type: 'bar',
    data: {
      labels: data.labels,
      datasets: [
        { label: 'Open Rate %', data: data.open_rate, backgroundColor: COLORS.blue.bg, borderColor: COLORS.blue.border, borderWidth: 1.5, borderRadius: 6 },
        { label: 'Click Rate %', data: data.click_rate, backgroundColor: COLORS.green.bg, borderColor: COLORS.green.border, borderWidth: 1.5, borderRadius: 6 },
        { label: 'Bounce Rate %', data: data.bounce_rate, backgroundColor: COLORS.orange.bg, borderColor: COLORS.orange.border, borderWidth: 1.5, borderRadius: 6 },
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: { ticks: { callback: v => v + '%' }, grid: { color: 'rgba(99,102,241,0.06)' } },
        x: { grid: { display: false } },
      },
      plugins: {
        tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + ctx.raw + '%' } }
      }
    }
  });
}

function renderEmailTrend(data) {
  if (!data.labels) return;
  const ctx = document.getElementById('chart-email-trend');
  if (!ctx) return;
  createChart('chart-email-trend', {
    type: 'line',
    data: {
      labels: data.labels,
      datasets: [
        {
          label: 'Open Rate %',
          data: data.datasets[0].data,
          borderColor: COLORS.blue.border,
          backgroundColor: createGradient(ctx.getContext('2d'), 'rgba(99,102,241,0.2)', 'rgba(99,102,241,0)'),
          fill: true,
          tension: 0.4,
          borderWidth: 2.5,
        },
        {
          label: 'Click Rate %',
          data: data.datasets[1].data,
          borderColor: COLORS.green.border,
          tension: 0.4,
          borderWidth: 2.5,
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      scales: {
        y: { ticks: { callback: v => v + '%' }, grid: { color: 'rgba(99,102,241,0.06)' } },
        x: { ticks: { maxTicksLimit: 12 }, grid: { display: false } },
      }
    }
  });
}

function renderEmailByHour(data) {
  if (!data.labels) return;
  createChart('chart-email-hour', {
    type: 'bar',
    data: {
      labels: data.labels,
      datasets: [
        { label: 'Open Rate %', data: data.open_rate, backgroundColor: COLORS.cyan.bg, borderColor: COLORS.cyan.border, borderWidth: 1.5, borderRadius: 4 },
        { label: 'Click Rate %', data: data.click_rate, backgroundColor: COLORS.pink.bg, borderColor: COLORS.pink.border, borderWidth: 1.5, borderRadius: 4 },
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: { ticks: { callback: v => v + '%' }, grid: { color: 'rgba(99,102,241,0.06)' } },
        x: { grid: { display: false } },
      }
    }
  });
}

// ── Contact Charts ─────────────────────────────────────────────────
function renderContactsLifecycle(data) {
  if (!data.labels) return;
  const maxCount = Math.max(...data.count);
  const colors = ['#94a3b8', '#a78bfa', '#8b5cf6', '#6366f1', '#22d3ee', '#10b981', '#f59e0b'];

  const el = document.getElementById('funnel-lifecycle');
  if (!el) return;

  el.innerHTML = data.labels.map((label, i) => {
    const pctWidth = Math.max((data.count[i] / maxCount) * 100, 8);
    return `
      <div class="funnel-step">
        <span class="funnel-step-label">${label}</span>
        <div class="funnel-step-bar-bg">
          <div class="funnel-step-bar" style="width:${pctWidth}%;background:${colors[i]}">${fmt(data.count[i])}</div>
        </div>
        <span class="funnel-step-value" style="color:${colors[i]}">Score: ${data.avg_score[i]}</span>
      </div>`;
  }).join('');
}

function renderContactsBySource(data) {
  if (!data.labels) return;
  createChart('chart-contacts-source', {
    type: 'doughnut',
    data: {
      labels: data.labels,
      datasets: [{
        data: data.count,
        backgroundColor: PALETTE.map(c => c.solid).concat(PALETTE.map(c => c.solid)),
        borderWidth: 0,
        hoverOffset: 8,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '60%',
      plugins: {
        legend: { position: 'right', labels: { font: { size: 11 } } },
        tooltip: { callbacks: { label: ctx => ctx.label + ': ' + fmt(ctx.raw) + ' (Score: ' + data.avg_score[ctx.dataIndex] + ')' } }
      }
    }
  });
}

// ── Support Charts ─────────────────────────────────────────────────
function renderSupportTable(data) {
  if (!data.labels) return;
  const tbody = document.getElementById('support-table-body');
  if (!tbody) return;

  tbody.innerHTML = data.labels.map((label, i) => {
    const sla = data.sla_compliance[i];
    const slaColor = sla >= 70 ? '#10b981' : sla >= 50 ? '#f59e0b' : '#ef4444';
    return `<tr>
      <td>${label}</td>
      <td class="num">${fmt(data.ticket_count[i])}</td>
      <td class="num">${data.avg_resolution[i]}h</td>
      <td class="num" style="color:${slaColor};font-weight:700">${pct(sla)}</td>
      <td class="num">${data.satisfaction[i]}/5</td>
    </tr>`;
  }).join('');
}

function renderSupportByPriority(data) {
  if (!data.labels) return;
  createChart('chart-support-priority', {
    type: 'bar',
    data: {
      labels: data.labels,
      datasets: [
        {
          label: 'Tickets',
          data: data.ticket_count,
          backgroundColor: [COLORS.red.bg, COLORS.orange.bg, COLORS.blue.bg, COLORS.green.bg],
          borderColor: [COLORS.red.border, COLORS.orange.border, COLORS.blue.border, COLORS.green.border],
          borderWidth: 1.5,
          borderRadius: 8,
          yAxisID: 'y',
        },
        {
          label: 'SLA Compliance %',
          data: data.sla_compliance,
          type: 'line',
          borderColor: COLORS.cyan.border,
          backgroundColor: COLORS.cyan.bg,
          borderWidth: 2.5,
          tension: 0.4,
          pointRadius: 5,
          pointBackgroundColor: COLORS.cyan.solid,
          yAxisID: 'y1',
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: { position: 'left', grid: { color: 'rgba(99,102,241,0.06)' } },
        y1: { position: 'right', ticks: { callback: v => v + '%' }, grid: { drawOnChartArea: false }, max: 100 },
        x: { grid: { display: false } },
      }
    }
  });
}

// ── Web Analytics Charts ───────────────────────────────────────────
function renderWebTopPages(data) {
  if (!data.labels) return;
  const tbody = document.getElementById('web-pages-body');
  if (!tbody) return;

  tbody.innerHTML = data.labels.map((label, i) => {
    const maxSessions = Math.max(...data.sessions);
    const barPct = (data.sessions[i] / maxSessions) * 100;
    return `<tr>
      <td><code style="color:var(--accent-cyan)">${label}</code></td>
      <td class="num highlight">${fmt(data.sessions[i])}</td>
      <td>
        <div class="metric-bar-bg" style="width:120px;display:inline-block;vertical-align:middle">
          <div class="metric-bar blue" style="width:${barPct}%"></div>
        </div>
      </td>
      <td class="num">${pct(data.bounce_rate[i])}</td>
      <td class="num" style="color:var(--accent-green)">${pct(data.conversion_rate[i])}</td>
    </tr>`;
  }).join('');
}

function renderWebByCountry(data) {
  if (!data.labels) return;
  createChart('chart-web-country', {
    type: 'bar',
    data: {
      labels: data.labels,
      datasets: [{
        label: 'Sessions',
        data: data.sessions,
        backgroundColor: PALETTE.map(c => c.bg).concat(PALETTE.map(c => c.bg)),
        borderColor: PALETTE.map(c => c.border).concat(PALETTE.map(c => c.border)),
        borderWidth: 1.5,
        borderRadius: 6,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: 'y',
      scales: {
        x: { grid: { color: 'rgba(99,102,241,0.06)' } },
        y: { grid: { display: false } },
      },
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { afterLabel: ctx => 'Conversion: ' + pct(data.conversion_rate[ctx.dataIndex]) } }
      }
    }
  });
}

function renderWebByDevice(data) {
  if (!data.labels) return;
  createChart('chart-web-device', {
    type: 'doughnut',
    data: {
      labels: data.labels,
      datasets: [{
        data: data.sessions,
        backgroundColor: [COLORS.blue.solid, COLORS.cyan.solid, COLORS.purple.solid],
        borderWidth: 0,
        hoverOffset: 8,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '65%',
      plugins: {
        legend: { position: 'bottom' },
        tooltip: { callbacks: { label: ctx => ctx.label + ': ' + fmt(ctx.raw) + ' (' + pct(data.bounce_rate[ctx.dataIndex]) + ' bounce)' } }
      }
    }
  });
}

function renderWebTrend(data) {
  if (!data.labels) return;
  const ctx = document.getElementById('chart-web-trend');
  if (!ctx) return;
  createChart('chart-web-trend', {
    type: 'line',
    data: {
      labels: data.labels,
      datasets: [
        {
          label: 'Sessions',
          data: data.datasets[0].data,
          borderColor: COLORS.blue.border,
          backgroundColor: createGradient(ctx.getContext('2d'), 'rgba(99,102,241,0.2)', 'rgba(99,102,241,0)'),
          fill: true,
          tension: 0.4,
          borderWidth: 2.5,
          yAxisID: 'y',
        },
        {
          label: 'Conversion %',
          data: data.datasets[1].data,
          borderColor: COLORS.green.border,
          tension: 0.4,
          borderWidth: 2.5,
          borderDash: [6, 4],
          yAxisID: 'y1',
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      scales: {
        y: { position: 'left', ticks: { callback: v => fmt(v) }, grid: { color: 'rgba(99,102,241,0.06)' } },
        y1: { position: 'right', ticks: { callback: v => v + '%' }, grid: { drawOnChartArea: false } },
        x: { ticks: { maxTicksLimit: 12 }, grid: { display: false } },
      }
    }
  });
}

// ── Company Charts ─────────────────────────────────────────────────
function renderCompaniesByIndustry(data) {
  if (!data.labels) return;
  createChart('chart-companies-industry', {
    type: 'bar',
    data: {
      labels: data.labels,
      datasets: [{
        label: 'Companies',
        data: data.count,
        backgroundColor: PALETTE.map(c => c.bg).concat(PALETTE.map(c => c.bg)).concat(PALETTE.map(c => c.bg)),
        borderColor: PALETTE.map(c => c.border).concat(PALETTE.map(c => c.border)).concat(PALETTE.map(c => c.border)),
        borderWidth: 1.5,
        borderRadius: 6,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: { grid: { color: 'rgba(99,102,241,0.06)' } },
        x: { grid: { display: false }, ticks: { maxRotation: 45, font: { size: 10 } } },
      },
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { afterLabel: ctx => 'Avg Revenue: ' + fmtDollar(data.avg_revenue[ctx.dataIndex]) } }
      }
    }
  });
}

function renderCompaniesByRegion(data) {
  if (!data.labels) return;
  createChart('chart-companies-region', {
    type: 'doughnut',
    data: {
      labels: data.labels,
      datasets: [{
        data: data.count,
        backgroundColor: PALETTE.map(c => c.solid),
        borderWidth: 0,
        hoverOffset: 8,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '60%',
      plugins: {
        legend: { position: 'right' },
        tooltip: { callbacks: { label: ctx => ctx.label + ': ' + fmt(ctx.raw) + ' companies' } }
      }
    }
  });
}

// ── Main Load ──────────────────────────────────────────────────────
async function loadDashboard() {
  try {
    const res = await fetch('/api/dashboard');
    const data = await res.json();

    renderKPIs(data.kpis);

    // Revenue & Deals
    renderRevenueTrend(data.revenue_trend);
    renderDealsByStage(data.deals_by_stage);
    renderDealsByRegion(data.deals_by_region);
    renderDealsByIndustry(data.deals_by_industry);
    renderDealsByPipeline(data.deals_by_pipeline);

    // Marketing
    renderMarketingChannels(data.marketing_channels);
    renderMarketingTrend(data.marketing_trend);
    renderMarketingEventTypes(data.marketing_event_types);

    // Email
    renderEmailPerformance(data.email_performance);
    renderEmailTrend(data.email_trend);
    renderEmailByHour(data.email_by_hour);

    // Contacts
    renderContactsLifecycle(data.contacts_lifecycle);
    renderContactsBySource(data.contacts_by_source);

    // Support
    renderSupportTable(data.support_by_category);
    renderSupportByPriority(data.support_by_priority);

    // Web
    renderWebTopPages(data.web_top_pages);
    renderWebByCountry(data.web_by_country);
    renderWebByDevice(data.web_by_device);
    renderWebTrend(data.web_trend);

    // Companies
    renderCompaniesByIndustry(data.companies_by_industry);
    renderCompaniesByRegion(data.companies_by_region);

    // Hide loader
    setTimeout(() => {
      document.getElementById('loading').classList.add('hidden');
    }, 600);

  } catch (err) {
    console.error('Dashboard load failed:', err);
    document.querySelector('.loading-text').textContent = 'Error loading data';
    document.querySelector('.loading-sub').textContent = err.message;
  }
}

// ── Init ───────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  initNav();
  loadDashboard();
});
