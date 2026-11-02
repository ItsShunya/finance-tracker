// Charts for dashboards.yaml.
//
// fava-dashboards runs this file as a function body, so it has to end with a
// `return`. Every panel calls `utils.render(...)`, which looks up the panel's
// `chart:` name in PANELS at the bottom of the file.

const CONFIG = {
  // First matching regex wins, anything else ends up in "Other".
  assetGroups: [
    // Zero-sum accounts are money in transit between my own accounts.
    ["Cash", /^Assets:([^:]+:[^:]+:Checking|Zero-Sum-Accounts:.*|ZSA-Matched:.*)$/],
    ["Savings", /^Assets:[^:]+:[^:]+:Saving$/],
    ["Real Estate", /^Assets:RealEstate(:|$)/],
    ["Crypto", /^Assets:Crypto(:|$)/],
  ],
  liabilityGroups: [
    ["Mortgage", /^Liabilities:House:Mortgage$/],
    ["House", /^Liabilities:(House|Rent)(:|$)/],
    ["Family & Friends", /^Liabilities:(Papa|Tata|Dario|Jhuseidi)$/],
  ],
  // Expense categories with a fixed color (max 8). The rest are grey or "Other".
  pinnedCategories: ["House", "Travel", "Food", "Health", "Hardware", "Gifts", "Leisure", "Sports"],
  baselineMonths: 12, // history the Monthly dashboard compares against
  zoomMonths: 36, // long charts start zoomed in on the last N months
  topN: 12,
  sankeyMinShare: 0.01, // hide flows smaller than 1% of income
};

// ---------------------------------------------------------------------------
// Colors and formatting
// ---------------------------------------------------------------------------

const DARK = window.matchMedia?.("(prefers-color-scheme: dark)").matches ?? false;

const THEME = DARK
  ? {
      series: ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"],
      text: "#f0efec",
      textMuted: "#c3c2b7",
      axis: "#383835",
      grid: "#2c2c2a",
      muted: "#898781",
      neutral: "#383835",
      good: "#0ca30c",
      bad: "#e66767",
    }
  : {
      series: ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"],
      text: "#0b0b0b",
      textMuted: "#52514e",
      axis: "#c3c2b7",
      grid: "#e1e0d9",
      muted: "#898781",
      neutral: "#f0efec",
      good: "#006300",
      bad: "#d03b3b",
    };

const [BLUE, ORANGE, AQUA, , , , , RED] = THEME.series;
const COLOR = { up: BLUE, down: RED, assets: AQUA, liabilities: ORANGE, muted: THEME.muted };

// Page background, used for the small gaps between stacked areas/bars.
const SURFACE =
  getComputedStyle(document.documentElement).getPropertyValue("--background").trim() || (DARK ? "#1a1a19" : "#ffffff");

const categoryColor = (name) => THEME.series[CONFIG.pinnedCategories.indexOf(name)] ?? COLOR.muted;

function currencyFormatter(currency, digits = 0) {
  const format = new Intl.NumberFormat(undefined, {
    style: "currency",
    currency,
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
  return (value) => (Number.isFinite(value) ? format.format(value) : "–");
}

function compactFormatter(currency) {
  const format = new Intl.NumberFormat(undefined, { style: "currency", currency, notation: "compact", maximumFractionDigits: 1 });
  return (value) => format.format(value);
}

const percentFormat = new Intl.NumberFormat(undefined, { style: "percent", maximumFractionDigits: 0 });
const formatPercent = (value) => (Number.isFinite(value) ? percentFormat.format(value) : "–");

const escapeHtml = (s) =>
  String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);

// ---------------------------------------------------------------------------
// Months, as "YYYY-MM" strings
// ---------------------------------------------------------------------------

const pad2 = (n) => String(n).padStart(2, "0");
const monthKey = (year, month) => `${year}-${pad2(month)}`;
const toMonth = (isoDate) => isoDate.slice(0, 7);
const parseMonth = (key) => key.split("-").map(Number);

function addMonths(key, n) {
  const [year, month] = parseMonth(key);
  const index = year * 12 + month - 1 + n;
  return monthKey(Math.floor(index / 12), (index % 12) + 1);
}

function monthRange(first, last) {
  const months = [];
  for (let m = first; m <= last; m = addMonths(m, 1)) months.push(m);
  return months;
}

const daysInMonth = (key) => {
  const [year, month] = parseMonth(key);
  return new Date(Date.UTC(year, month, 0)).getUTCDate();
};
const monthStart = (key) => `${key}-01`;
const monthEnd = (key) => `${key}-${pad2(daysInMonth(key))}`;

function monthLabel(key, style = "short") {
  const [year, month] = parseMonth(key);
  return new Intl.DateTimeFormat(undefined, { month: style, year: "numeric" }).format(new Date(year, month - 1, 1));
}

const shortMonthName = (month) => new Intl.DateTimeFormat(undefined, { month: "short" }).format(new Date(2000, month - 1, 1));

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

// Results are cached per page, so panels that need the same data share one request.
const queryCache = new Map();

// Runs a query with the same filters as the Fava filter bar.
function query(ext, bql, { ignoreTime = false } = {}) {
  const url = new URL(window.location.href);
  const params = { bql };
  for (const name of ["time", "account", "filter"]) {
    if (url.searchParams.has(name) && !(ignoreTime && name === "time")) params[name] = url.searchParams.get(name);
  }
  const key = JSON.stringify(params);
  if (!queryCache.has(key)) {
    const request = ext.api.get("query", params).then((res) => {
      if (!res.success) throw new Error(`Query failed: ${res.error}`);
      return res.data.result;
    });
    request.catch(() => queryCache.delete(key));
    queryCache.set(key, request);
  }
  return queryCache.get(key);
}

const BQL = {
  units: `
    SELECT year, month, account, currency, SUM(number) AS units
    WHERE account ~ '^(Assets|Liabilities):'
    GROUP BY year, month, account, currency`,

  prices: (ccy) => `
    SELECT date, currency, number(amount) AS price
    FROM #prices WHERE currency(amount) = '${ccy}'`,

  flows: (ccy, where = "") => `
    SELECT year, month, account, CONVERT(SUM(position), '${ccy}', LAST(date)) AS value
    WHERE account ~ '^(Income|Expenses):' ${where}
    GROUP BY year, month, account`,

  dailyExpenses: (ccy, from, to) => `
    SELECT date, CONVERT(SUM(position), '${ccy}', LAST(date)) AS value
    WHERE account ~ '^Expenses:' AND date >= ${from} AND date < ${to}
    GROUP BY date`,

  expenses: (ccy, from, to) => `
    SELECT date, payee, narration, account, number(CONVERT(position, '${ccy}', date)) AS value
    WHERE account ~ '^Expenses:' AND date >= ${from} AND date < ${to}`,
};

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

// Converted amounts come back as {EUR: 12.3}.
const amount = (value, ccy) => (typeof value === "number" ? value : (value?.[ccy] ?? 0));
const sum = (values) => values.reduce((a, b) => a + (b ?? 0), 0);
const median = (values) => {
  if (!values.length) return null;
  const s = [...values].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
};
const rollingSum = (values, n) => values.map((_, i) => (i + 1 < n ? null : sum(values.slice(i + 1 - n, i + 1))));
const rootAccount = (account, depth) => account.split(":").slice(0, depth).join(":");
const leafName = (account) => account.split(":").slice(1).join(":");

// (currency, date) -> latest price on or before date. Before the first price
// the earliest one is used. Currencies without a price count as 0.
function makePricer(rows, ccy) {
  const table = {};
  for (const row of [...rows].sort((a, b) => (a.date < b.date ? -1 : 1))) (table[row.currency] ??= []).push(row);
  return (currency, date) => {
    if (currency === ccy) return 1;
    const history = table[currency];
    if (!history) return 0;
    return (history.findLast((row) => row.date <= date) ?? history[0]).price;
  };
}

// Month-end value of every Assets/Liabilities account. Units are valued with
// that month's price, so the flat gets revalued even without new postings.
async function balances({ ext, ledger }) {
  const [unitRows, priceRows] = await Promise.all([query(ext, BQL.units), query(ext, BQL.prices(ledger.ccy))]);
  const price = makePricer(priceRows, ledger.ccy);
  const months = monthRange(toMonth(ledger.dateFirst), toMonth(ledger.dateLast));
  const rows = unitRows.map((r) => ({ ...r, key: monthKey(r.year, r.month) })).sort((a, b) => (a.key < b.key ? -1 : 1));

  const holdings = {}; // account -> currency -> units
  const accounts = {}; // account -> value per month
  let next = 0;
  months.forEach((month, i) => {
    for (; next < rows.length && rows[next].key <= month; next++) {
      const { account, currency, units } = rows[next];
      holdings[account] ??= {};
      holdings[account][currency] = (holdings[account][currency] ?? 0) + units;
    }
    for (const [account, inventory] of Object.entries(holdings)) {
      accounts[account] ??= new Array(months.length).fill(0);
      accounts[account][i] = sum(Object.entries(inventory).map(([currency, units]) => units * price(currency, monthEnd(month))));
    }
  });
  return { months, accounts };
}

// Sums account series into the groups from CONFIG, dropping empty groups.
function groupSeries(accounts, prefix, groups, sign = 1) {
  const result = {};
  for (const [account, values] of Object.entries(accounts)) {
    if (!account.startsWith(prefix)) continue;
    const group = groups.find(([, regex]) => regex.test(account))?.[0] ?? "Other";
    result[group] ??= new Array(values.length).fill(0);
    values.forEach((v, i) => (result[group][i] += sign * v));
  }
  return [...groups.map(([name]) => name), "Other"]
    .filter((name) => result[name]?.some((v) => Math.abs(v) >= 0.5))
    .map((name) => ({ name, values: result[name] }));
}

async function netWorth(ctx) {
  const { months, accounts } = await balances(ctx);
  const total = (prefix, sign) => months.map((_, i) => sign * sum(Object.keys(accounts).filter((a) => a.startsWith(prefix)).map((a) => accounts[a][i])));
  const assets = total("Assets:", 1);
  const liabilities = total("Liabilities:", -1); // positive = debt
  return { months, accounts, assets, liabilities, netWorth: assets.map((a, i) => a - liabilities[i]) };
}

// Income (positive), expenses and net savings per month.
function summarizeFlows(rows, months, ccy) {
  const index = new Map(months.map((m, i) => [m, i]));
  const income = new Array(months.length).fill(0);
  const expenses = new Array(months.length).fill(0);
  for (const row of rows) {
    const i = index.get(monthKey(row.year, row.month));
    if (i === undefined) continue;
    if (row.account.startsWith("Income:")) income[i] -= amount(row.value, ccy);
    else expenses[i] += amount(row.value, ccy);
  }
  return { months, income, expenses, net: income.map((v, i) => v - expenses[i]), rows };
}

async function flows({ ext, ledger }) {
  const rows = await query(ext, BQL.flows(ledger.ccy));
  return summarizeFlows(rows, monthRange(toMonth(ledger.dateFirst), toMonth(ledger.dateLast)), ledger.ccy);
}

// Everything the Monthly dashboard needs. The month shown is the last month of
// the time filter (the latest month without one). It is compared against the
// previous months: totals against the median, so a one-off bonus doesn't skew
// it, and categories against the mean. The time filter is ignored here so the
// history is still there when the filter is a single month.
async function monthContext({ ext, ledger }) {
  const month = toMonth(ledger.dateLast);
  const to = addMonths(month, 1);
  const from = addMonths(month, -CONFIG.baselineMonths);
  const rows = await query(ext, BQL.flows(ledger.ccy, `AND date >= ${monthStart(from)} AND date < ${monthStart(to)}`), { ignoreTime: true });

  // Skip months before the ledger starts.
  const first = rows.reduce((min, r) => (monthKey(r.year, r.month) < min ? monthKey(r.year, r.month) : min), month);
  const history = monthRange(first, addMonths(month, -1));
  const n = history.length;
  const summary = summarizeFlows(rows, [...history, month], ledger.ccy);
  const typical = (values) => (n ? median(values.slice(0, n)) : null);

  const current = {};
  const average = {};
  for (const row of rows) {
    const value = amount(row.value, ledger.ccy);
    if (monthKey(row.year, row.month) === month) current[row.account] = (current[row.account] ?? 0) + value;
    else average[row.account] = (average[row.account] ?? 0) + value / n;
  }

  return {
    month,
    label: monthLabel(month, "long"),
    from: monthStart(month),
    to: monthStart(to),
    historyMonths: n,
    totals: { income: summary.income.at(-1), expenses: summary.expenses.at(-1), net: summary.net.at(-1) },
    typical: { income: typical(summary.income), expenses: typical(summary.expenses), net: typical(summary.net) },
    accounts: current,
    averageAccounts: average,
  };
}

// {"Expenses:Food:Cafe": 10, "Expenses:Food:Groceries": 20} -> {"Expenses:Food": 30}
function rollUp(totals, prefix, depth) {
  const result = {};
  for (const [account, value] of Object.entries(totals)) {
    if (account.startsWith(prefix)) result[rootAccount(account, depth)] = (result[rootAccount(account, depth)] ?? 0) + value;
  }
  return result;
}

// Nested [{name, path, value, children}] for the treemap and the sankey.
function accountTree(totals, prefix) {
  const root = { children: [] };
  for (const [account, value] of Object.entries(totals)) {
    if (!account.startsWith(prefix) || value <= 0) continue;
    const parts = account.split(":");
    let node = root;
    for (let depth = 2; depth <= parts.length; depth++) {
      const path = parts.slice(0, depth).join(":");
      let child = node.children.find((c) => c.path === path);
      if (!child) node.children.push((child = { name: parts[depth - 1], path, value: 0, children: [] }));
      child.value += value;
      node = child;
    }
  }
  const sort = (node) => {
    node.children.sort((a, b) => b.value - a.value).forEach(sort);
    if (!node.children.length) delete node.children;
  };
  sort(root);
  return root.children ?? [];
}

// Bank payees look like "MERCADONA - AVDA.  | 04000022SIO"; keep the name.
const cleanPayee = (row) =>
  (row.payee || row.narration || "Unknown")
    .split(" | ")[0]
    .replace(/\s{2,}.*$/, "")
    .trim();

// ---------------------------------------------------------------------------
// Chart building blocks
// ---------------------------------------------------------------------------

const openLink = (helpers, path) => window.open(helpers.urlFor(path));
const accountLink = (account, time) => `../../account/${account}/?time=${time}`;

const categoryAxis = (data, extra = {}) => ({
  type: "category",
  data,
  axisLine: { lineStyle: { color: THEME.axis } },
  axisTick: { show: false },
  axisLabel: { color: THEME.textMuted },
  ...extra,
});

const valueAxis = (formatter, extra = {}) => ({
  type: "value",
  axisLabel: { color: THEME.textMuted, formatter },
  splitLine: { lineStyle: { color: THEME.grid } },
  ...extra,
});

function baseChart(ledger, { legend = true } = {}) {
  return {
    textStyle: { fontFamily: "inherit" },
    color: THEME.series,
    grid: { left: 8, right: 16, top: legend ? 36 : 12, bottom: 8, containLabel: true },
    legend: legend ? { top: 0, icon: "roundRect", itemWidth: 12, itemHeight: 12, textStyle: { color: THEME.textMuted } } : undefined,
    tooltip: { trigger: "axis", valueFormatter: currencyFormatter(ledger.ccy) },
  };
}

// One value per month, with a zoom slider for long ranges.
// onClick receives the clicked month ("YYYY-MM") and the ECharts event.
function monthlyChart(ledger, months, series, { onClick, yFormatter, yAxis, legend } = {}) {
  const base = baseChart(ledger, { legend });
  const zoom = months.length > CONFIG.zoomMonths;
  const startValue = months.length - CONFIG.zoomMonths;
  return {
    ...base,
    grid: { ...base.grid, bottom: zoom ? 48 : 8 },
    xAxis: categoryAxis(months.map((m) => monthLabel(m))),
    yAxis: valueAxis(yFormatter ?? compactFormatter(ledger.ccy), yAxis),
    dataZoom: zoom ? [{ type: "inside", startValue }, { type: "slider", startValue, height: 18, bottom: 8 }] : undefined,
    series,
    onClick: onClick && ((event) => onClick(months[event.dataIndex], event)),
  };
}

// Horizontal bars, biggest first. onClick receives the row index.
function rankedBars(ledger, labels, series, { onClick } = {}) {
  const base = baseChart(ledger, { legend: series.length > 1 });
  return {
    ...base,
    tooltip: { ...base.tooltip, axisPointer: { type: "shadow" } },
    xAxis: valueAxis(compactFormatter(ledger.ccy)),
    yAxis: categoryAxis(labels, { inverse: true, axisLabel: { color: THEME.textMuted, width: 140, overflow: "truncate" } }),
    series: series.map((s) => ({ ...s, itemStyle: { ...s.itemStyle, borderRadius: [0, 4, 4, 0] } })),
    onClick: onClick && ((event) => onClick(event.dataIndex)),
  };
}

const line = (name, data, color, extra = {}) => ({
  type: "line",
  name,
  data,
  showSymbol: false,
  lineStyle: { width: 2, color },
  itemStyle: { color },
  emphasis: { focus: "series" },
  ...extra,
});

const dashedLine = (name, data) => line(name, data, COLOR.muted, { lineStyle: { width: 2, type: "dashed", color: COLOR.muted } });

const bar = (name, data, color, extra = {}) => ({
  type: "bar",
  name,
  data,
  barMaxWidth: 28,
  itemStyle: { color, borderRadius: [4, 4, 0, 0] },
  ...extra,
});

// Positive bars in `color`, negative ones in red.
const signedBar = (name, values, color = COLOR.up) =>
  bar(
    name,
    values.map((value) => ({
      value,
      itemStyle: { color: value >= 0 ? color : COLOR.down, borderRadius: value >= 0 ? [4, 4, 0, 0] : [0, 0, 4, 4] },
    })),
    color,
  );

const stacked = (type, groups, colorOf, stack) =>
  groups.map(({ name, values }) => ({
    type,
    name,
    stack,
    data: values,
    showSymbol: false,
    barMaxWidth: 28,
    itemStyle: { color: colorOf(name), borderColor: SURFACE, borderWidth: type === "bar" ? 1 : 0 },
    lineStyle: { width: 1.5, color: colorOf(name) },
    areaStyle: type === "line" ? { color: colorOf(name), opacity: 0.85 } : undefined,
    emphasis: { focus: "series" },
  }));

// Note like "▲ +€500 (12%) vs typical month", green when the change is good.
function compare(value, base, format, label, higherIsBetter = true) {
  if (!Number.isFinite(value) || !Number.isFinite(base) || Math.abs(value - base) < 0.5) return {};
  const diff = value - base;
  const ratio = Math.abs(diff / base);
  const pct = ratio <= 5 ? ` (${formatPercent(ratio)})` : "";
  return {
    note: `${diff > 0 ? "▲ +" : "▼ −"}${format(Math.abs(diff))}${pct} ${label}`,
    tone: diff > 0 === higherIsBetter ? "good" : "bad",
  };
}

// HTML row of KPI tiles: [{label, value, note?, tone?}]
function kpiTiles(tiles, header = "") {
  const muted = `var(--text-color-lighter, ${THEME.textMuted})`;
  const tile = ({ label, value, note, tone }) => `
    <div style="flex:1 1 160px; padding:6px 12px; border-left:3px solid ${THEME.grid};">
      <div style="font-size:12px; text-transform:uppercase; color:${muted};">${escapeHtml(label)}</div>
      <div style="font-size:26px; font-weight:600;">${escapeHtml(value)}</div>
      ${note ? `<div style="font-size:12px; color:${THEME[tone] ?? muted};">${escapeHtml(note)}</div>` : ""}
    </div>`;
  return `${header}<div style="display:flex; flex-wrap:wrap; gap:8px;">${tiles.map(tile).join("")}</div>`;
}

function htmlTable(columns, rows) {
  const cell = (tag, c, content) => `<${tag} style="text-align:${c.align ?? "left"}">${content}</${tag}>`;
  const head = columns.map((c) => cell("th", c, c.label)).join("");
  const body = rows.map((row) => `<tr>${columns.map((c) => cell("td", c, c.html(row))).join("")}</tr>`).join("");
  return `<div style="height:100%; overflow:auto;"><table style="width:100%"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

// ---------------------------------------------------------------------------
// Panels, referenced by `chart:` in dashboards.yaml
// ---------------------------------------------------------------------------

const PANELS = {
  // Overview

  async overviewKpis(ctx) {
    const money = currencyFormatter(ctx.ledger.ccy);
    const [nw, fl] = await Promise.all([netWorth(ctx), flows(ctx)]);
    const yearAgo = (series, higherIsBetter = true) => compare(series.at(-1), series.at(-13), money, "vs 12 months ago", higherIsBetter);
    const income = sum(fl.income);
    const saved = sum(fl.net);
    return kpiTiles([
      { label: "Net worth", value: money(nw.netWorth.at(-1)), ...yearAgo(nw.netWorth) },
      { label: "Assets", value: money(nw.assets.at(-1)), ...yearAgo(nw.assets) },
      { label: "Liabilities", value: money(nw.liabilities.at(-1)), ...yearAgo(nw.liabilities, false) },
      { label: "Avg. monthly savings", value: money(saved / fl.months.length), note: `over ${fl.months.length} months` },
      { label: "Savings rate", value: formatPercent(saved / income), note: "of all income in this period" },
    ]);
  },

  async netWorthTrend(ctx) {
    const { months, assets, liabilities, netWorth: total } = await netWorth(ctx);
    return monthlyChart(
      ctx.ledger,
      months,
      [
        line("Net worth", total, COLOR.up, { lineStyle: { width: 3, color: COLOR.up } }),
        line("Assets", assets, COLOR.assets),
        line("Liabilities", liabilities, COLOR.liabilities),
      ],
      { onClick: (month) => openLink(ctx.helpers, `../../balance_sheet/?time=${month}`) },
    );
  },

  async assetsComposition(ctx) {
    const { months, accounts } = await balances(ctx);
    const groups = groupSeries(accounts, "Assets:", CONFIG.assetGroups);
    const colorOf = (name) => (name === "Other" ? COLOR.muted : THEME.series[groups.findIndex((g) => g.name === name)]);
    return monthlyChart(ctx.ledger, months, stacked("line", groups, colorOf, "assets"), {
      onClick: (month) => openLink(ctx.helpers, `../../balance_sheet/?time=${month}`),
    });
  },

  async liabilitiesComposition(ctx) {
    const { months, accounts } = await balances(ctx);
    const groups = groupSeries(accounts, "Liabilities:", CONFIG.liabilityGroups, -1);
    // Offset by one so the mortgage is orange, like the liabilities line.
    const colorOf = (name) => (name === "Other" ? COLOR.muted : THEME.series[groups.findIndex((g) => g.name === name) + 1]);
    return monthlyChart(ctx.ledger, months, stacked("line", groups, colorOf, "liabilities"), {
      onClick: (month) => openLink(ctx.helpers, `../../balance_sheet/?time=${month}`),
    });
  },

  async netWorthChange(ctx) {
    const { months, netWorth: total } = await netWorth(ctx);
    const change = total.map((v, i) => (i ? v - total[i - 1] : 0));
    return monthlyChart(ctx.ledger, months, [signedBar("Change", change)], {
      legend: false,
      onClick: (month) => openLink(ctx.helpers, `../../balance_sheet/?time=${month}`),
    });
  },

  async assetAllocation(ctx) {
    const money = currencyFormatter(ctx.ledger.ccy);
    const { accounts } = await balances(ctx);
    const groups = groupSeries(accounts, "Assets:", CONFIG.assetGroups)
      .map(({ name, values }) => ({ name, value: values.at(-1) }))
      .filter((g) => g.value > 0.5)
      .sort((a, b) => b.value - a.value);
    const total = sum(groups.map((g) => g.value));
    const label = { show: true, position: "right", color: THEME.textMuted, formatter: (p) => `${money(p.value)} · ${formatPercent(p.value / total)}` };
    const chart = rankedBars(
      ctx.ledger,
      groups.map((g) => g.name),
      [bar("Value", groups.map((g) => g.value), COLOR.assets, { label })],
      { onClick: () => openLink(ctx.helpers, "../../balance_sheet/") },
    );
    chart.grid.right = 120; // room for the labels
    return chart;
  },

  async incomeVsExpenses(ctx) {
    const { months, income, expenses } = await flows(ctx);
    return monthlyChart(ctx.ledger, months, [bar("Income", income, COLOR.up), bar("Expenses", expenses, COLOR.down)], {
      onClick: (month) => openLink(ctx.helpers, `../../income_statement/?time=${month}`),
    });
  },

  async profitLoss(ctx) {
    const { months, net } = await flows(ctx);
    const average = rollingSum(net, 12).map((v) => (v === null ? null : v / 12));
    return monthlyChart(ctx.ledger, months, [signedBar("Net savings", net), dashedLine("12-month average", average)], {
      onClick: (month) => openLink(ctx.helpers, `../../income_statement/?time=${month}`),
    });
  },

  async savingsRate(ctx) {
    const { months, income, net } = await flows(ctx);
    const monthly = net.map((v, i) => (income[i] > 0 ? v / income[i] : null));
    const income12 = rollingSum(income, 12);
    const rolling = rollingSum(net, 12).map((v, i) => (v === null || !income12[i] ? null : v / income12[i]));
    const chart = monthlyChart(
      ctx.ledger,
      months,
      [bar("Monthly", monthly, COLOR.muted, { itemStyle: { color: COLOR.muted, opacity: 0.45 } }), line("12-month rolling", rolling, COLOR.up)],
      {
        yFormatter: formatPercent,
        // Months with almost no income give silly rates, so clamp to ±100%.
        yAxis: {
          min: (r) => Math.max(Math.floor(r.min * 4) / 4, -1),
          max: (r) => Math.min(Math.ceil(Math.max(r.max, 0) * 4) / 4, 1),
          interval: 0.25,
        },
      },
    );
    chart.tooltip.valueFormatter = formatPercent;
    return chart;
  },

  async yearlySummary(ctx) {
    const { months, income, expenses, net } = await flows(ctx);
    const years = [...new Set(months.map((m) => m.slice(0, 4)))];
    const perYear = (values) => years.map((y) => sum(values.filter((_, i) => months[i].startsWith(y))));
    const base = baseChart(ctx.ledger);
    return {
      ...base,
      tooltip: { ...base.tooltip, axisPointer: { type: "shadow" } },
      xAxis: categoryAxis(years),
      yAxis: valueAxis(compactFormatter(ctx.ledger.ccy)),
      series: [
        bar("Income", perYear(income), COLOR.up),
        bar("Expenses", perYear(expenses), COLOR.down),
        signedBar("Net savings", perYear(net), COLOR.assets),
      ],
      onClick: (event) => openLink(ctx.helpers, `../../income_statement/?time=${years[event.dataIndex]}`),
    };
  },

  async savingsHeatmap(ctx) {
    const money = currencyFormatter(ctx.ledger.ccy);
    const compact = compactFormatter(ctx.ledger.ccy);
    const { months, net } = await flows(ctx);
    const years = [...new Set(months.map((m) => m.slice(0, 4)))];
    const yearTotal = (y) => sum(net.filter((_, i) => months[i].startsWith(y)));
    // Color scale ends at the 90th percentile, otherwise one big month makes everything else grey.
    const sorted = net.map(Math.abs).sort((a, b) => a - b);
    const limit = Math.max(1, sorted[Math.floor(sorted.length * 0.9)] ?? 1);
    return {
      textStyle: { fontFamily: "inherit" },
      tooltip: { formatter: (p) => `${monthLabel(months[p.dataIndex], "long")}<br/><b>${money(p.value[2])}</b>` },
      grid: { left: 8, right: 8, top: 8, bottom: 56, containLabel: true },
      xAxis: categoryAxis(Array.from({ length: 12 }, (_, i) => shortMonthName(i + 1))),
      yAxis: categoryAxis(years.map((y) => `${y}  ${compact(yearTotal(y))}`), { inverse: true }),
      visualMap: {
        min: -limit,
        max: limit,
        calculable: true,
        orient: "horizontal",
        left: "center",
        bottom: 0,
        itemHeight: 240,
        formatter: compact,
        textStyle: { color: THEME.textMuted },
        inRange: { color: [COLOR.down, THEME.neutral, COLOR.up] },
      },
      series: [
        {
          type: "heatmap",
          data: months.map((m, i) => [parseMonth(m)[1] - 1, years.indexOf(m.slice(0, 4)), Math.round(net[i])]),
          itemStyle: { borderColor: SURFACE, borderWidth: 2, borderRadius: 4 },
          label: { show: true, formatter: (p) => compact(p.value[2]), fontSize: 10 },
        },
      ],
      onClick: (event) => openLink(ctx.helpers, `../../income_statement/?time=${months[event.dataIndex]}`),
    };
  },

  async expenseTrend(ctx) {
    const { months, rows } = await flows(ctx);
    const index = new Map(months.map((m, i) => [m, i]));
    const byCategory = {};
    for (const row of rows) {
      if (!row.account.startsWith("Expenses:")) continue;
      const category = row.account.split(":")[1];
      const name = CONFIG.pinnedCategories.includes(category) ? category : "Other";
      byCategory[name] ??= new Array(months.length).fill(0);
      byCategory[name][index.get(monthKey(row.year, row.month))] += amount(row.value, ctx.ledger.ccy);
    }
    const groups = [...CONFIG.pinnedCategories, "Other"].filter((n) => byCategory[n]).map((name) => ({ name, values: byCategory[name] }));
    return monthlyChart(ctx.ledger, months, stacked("bar", groups, categoryColor, "expenses"), {
      onClick: (month, event) =>
        openLink(ctx.helpers, event.seriesName === "Other" ? `../../income_statement/?time=${month}` : accountLink(`Expenses:${event.seriesName}`, month)),
    });
  },

  // Monthly

  async monthKpis(ctx) {
    const money = currencyFormatter(ctx.ledger.ccy);
    const m = await monthContext(ctx);
    const vsTypical = (key, higherIsBetter = true) => compare(m.totals[key], m.typical[key], money, "vs typical month", higherIsBetter);
    const rate = (t) => (t.income > 0 ? t.net / t.income : null);

    // Links that change the time filter and keep the rest of the URL.
    const linkTo = (month) => {
      const url = new URL(window.location.href);
      if (month) url.searchParams.set("time", month);
      else url.searchParams.delete("time");
      return url.toString();
    };
    const prev = addMonths(m.month, -1);
    const next = addMonths(m.month, 1);
    const nav = `
      <div style="display:flex; gap:16px; align-items:baseline; margin-bottom:12px;">
        <a href="${linkTo(prev)}">‹ ${monthLabel(prev)}</a>
        <strong style="font-size:16px;">${m.label}</strong>
        ${next <= toMonth(new Date().toISOString()) ? `<a href="${linkTo(next)}">${monthLabel(next)} ›</a>` : ""}
        <a href="${linkTo(null)}" style="margin-left:auto;">Latest month</a>
      </div>`;

    return kpiTiles(
      [
        { label: "Income", value: money(m.totals.income), ...vsTypical("income") },
        { label: "Expenses", value: money(m.totals.expenses), ...vsTypical("expenses", false) },
        { label: "Net savings", value: money(m.totals.net), ...vsTypical("net") },
        { label: "Savings rate", value: formatPercent(rate(m.totals)), note: m.typical.income ? `${formatPercent(rate(m.typical))} in a typical month` : "" },
      ],
      nav,
    );
  },

  async expenseTreemap(ctx) {
    const money = currencyFormatter(ctx.ledger.ccy);
    const m = await monthContext(ctx);
    const tree = accountTree(m.accounts, "Expenses:");
    const total = sum(tree.map((n) => n.value));
    tree.forEach((node) => (node.itemStyle = { color: categoryColor(node.name) }));
    return {
      textStyle: { fontFamily: "inherit" },
      tooltip: { formatter: (p) => `${escapeHtml(p.data.path ?? p.name)}<br/><b>${money(p.value)}</b> · ${formatPercent(p.value / total)}` },
      series: [
        {
          type: "treemap",
          name: "Expenses",
          data: tree,
          roam: false,
          top: 28,
          bottom: 4,
          left: 0,
          right: 0,
          breadcrumb: { top: 0, bottom: "auto", itemStyle: { color: THEME.neutral, textStyle: { color: THEME.text } } },
          label: { formatter: (p) => `${p.name}\n${money(p.value)}` },
          upperLabel: { show: true, height: 22, formatter: (p) => `${p.name}  ${money(p.value)}` },
          levels: [
            { itemStyle: { borderColor: SURFACE, borderWidth: 2, gapWidth: 2 } },
            { itemStyle: { borderColor: SURFACE, borderWidth: 1, gapWidth: 1 }, colorSaturation: [0.35, 0.6] },
          ],
        },
      ],
      // Single click zooms into a category, double click opens the account.
      onDblClick: (event) => event.data?.path && openLink(ctx.helpers, accountLink(event.data.path, m.month)),
    };
  },

  async categoryVsAverage(ctx) {
    const m = await monthContext(ctx);
    const current = rollUp(m.accounts, "Expenses:", 2);
    const average = rollUp(m.averageAccounts, "Expenses:", 2);
    const categories = [...new Set([...Object.keys(current), ...Object.keys(average)])]
      .filter((c) => (current[c] ?? 0) > 0.5 || (average[c] ?? 0) > 0.5)
      .sort((a, b) => (current[b] ?? 0) - (current[a] ?? 0) || (average[b] ?? 0) - (average[a] ?? 0))
      .slice(0, CONFIG.topN);
    const series = [bar(m.label, categories.map((c) => current[c] ?? 0), COLOR.up, { barGap: "10%" })];
    if (m.historyMonths) series.push(bar(`${m.historyMonths}-month average`, categories.map((c) => average[c] ?? 0), COLOR.muted));
    return rankedBars(ctx.ledger, categories.map(leafName), series, {
      onClick: (i) => openLink(ctx.helpers, accountLink(categories[i], m.month)),
    });
  },

  // Sankey: income sources -> Income -> categories -> subcategories, plus savings.
  async moneyFlow(ctx) {
    const m = await monthContext(ctx);
    const minValue = Math.max(m.totals.income, m.totals.expenses, 1) * CONFIG.sankeyMinShare;
    const nodes = [{ name: "Income" }];
    const links = [];
    const link = (source, target, value, label) => {
      nodes.push({ name: source === "Income" ? target : source, label });
      links.push({ source, target, value });
    };

    for (const [account, value] of Object.entries(rollUp(m.accounts, "Income:", 2))) {
      if (-value >= minValue) link(account, "Income", -value, leafName(account));
    }
    if (m.totals.net < 0) link("From savings", "Income", -m.totals.net);

    const addExpenses = (parent, children) => {
      for (const node of children) {
        if (node.value < minValue) continue;
        nodes.push({ name: node.path, label: node.name });
        links.push({ source: parent, target: node.path, value: node.value });
        if (node.children?.length > 1) addExpenses(node.path, node.children);
      }
    };
    addExpenses("Income", accountTree(m.accounts, "Expenses:"));
    if (m.totals.net > 0) link("Income", "Savings", m.totals.net);

    return {
      align: "left",
      valueFormatter: currencyFormatter(ctx.ledger.ccy),
      data: { nodes, links },
      onClick: (event, node) => node.name.includes(":") && openLink(ctx.helpers, accountLink(node.name, m.month)),
    };
  },

  // Cumulative spending through the month, next to the previous month.
  async spendingPace(ctx) {
    const { ext, ledger } = ctx;
    const m = await monthContext(ctx);
    const prev = addMonths(m.month, -1);
    const rows = await query(ext, BQL.dailyExpenses(ledger.ccy, monthStart(prev), m.to), { ignoreTime: true });
    const cumulative = (month, lastDay) => {
      const perDay = new Array(daysInMonth(month)).fill(0);
      for (const row of rows) if (toMonth(row.date) === month) perDay[Number(row.date.slice(8, 10)) - 1] += amount(row.value, ledger.ccy);
      let total = 0;
      return perDay.map((v, i) => (i < lastDay ? (total += v) : null));
    };
    // If the month isn't over yet, stop the line at the last day with data.
    const lastDay = toMonth(ledger.dateLast) === m.month ? Number(ledger.dateLast.slice(8, 10)) : 31;

    const current = line(m.label, cumulative(m.month, lastDay), COLOR.up, {
      areaStyle: { color: COLOR.up, opacity: 0.12 },
      lineStyle: { width: 3, color: COLOR.up },
    });
    if (m.typical.expenses) {
      current.markLine = {
        symbol: "none",
        silent: true,
        lineStyle: { color: COLOR.muted, type: "dotted" },
        label: { formatter: "Typical month", color: THEME.textMuted, position: "insideEndTop" },
        data: [{ yAxis: Math.round(m.typical.expenses) }],
      };
    }
    return {
      ...baseChart(ledger),
      xAxis: categoryAxis(Array.from({ length: 31 }, (_, i) => i + 1)),
      yAxis: valueAxis(compactFormatter(ledger.ccy)),
      series: [current, dashedLine(monthLabel(prev, "long"), cumulative(prev, 31))],
    };
  },

  async topPayees(ctx) {
    const { ext, ledger, helpers } = ctx;
    const money = currencyFormatter(ledger.ccy);
    const m = await monthContext(ctx);
    const rows = await query(ext, BQL.expenses(ledger.ccy, m.from, m.to), { ignoreTime: true });
    const totals = {};
    for (const row of rows) totals[cleanPayee(row)] = (totals[cleanPayee(row)] ?? 0) + (row.value ?? 0);
    const top = Object.entries(totals)
      .filter(([, v]) => v > 0)
      .sort(([, a], [, b]) => b - a)
      .slice(0, CONFIG.topN);
    const label = { show: true, position: "right", color: THEME.textMuted, formatter: (p) => money(p.value) };
    const chart = rankedBars(ledger, top.map(([name]) => name), [bar("Spent", top.map(([, v]) => v), COLOR.up, { label })], {
      onClick: (i) => openLink(helpers, `../../journal/?time=${m.month}&filter=${encodeURIComponent(`"${top[i][0]}"`)}`),
    });
    chart.grid.right = 72;
    return chart;
  },

  async largestExpenses(ctx) {
    const { ext, ledger, helpers } = ctx;
    const money = currencyFormatter(ledger.ccy, 2);
    const m = await monthContext(ctx);
    const rows = await query(ext, BQL.expenses(ledger.ccy, m.from, m.to), { ignoreTime: true });
    const top = [...rows].sort((a, b) => (b.value ?? 0) - (a.value ?? 0)).slice(0, CONFIG.topN);
    return htmlTable(
      [
        { label: "Date", html: (r) => r.date.slice(5) },
        { label: "Payee", html: (r) => `<span title="${escapeHtml(r.narration)}">${escapeHtml(cleanPayee(r))}</span>` },
        { label: "Category", html: (r) => `<a href="${helpers.urlFor(accountLink(r.account, m.month))}">${escapeHtml(leafName(r.account))}</a>` },
        { label: "Amount", align: "right", html: (r) => money(r.value) },
      ],
      top,
    );
  },
};

return {
  render(ctx) {
    const panel = PANELS[ctx.panel.chart];
    if (!panel) throw new Error(`Unknown chart "${ctx.panel.chart}"`);
    return panel(ctx);
  },
};
