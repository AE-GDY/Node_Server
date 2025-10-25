// server_fetch_multi_tickers_fixed.js
// npm i express @mathieuc/tradingview dotenv

const express = require("express");
const fs = require("fs");
const path = require("path");
require("dotenv").config();
const TradingView = require("@mathieuc/tradingview");

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 8080;

// ==== CONFIG ====
const MARKET_SYM = (t) => `EGX:${t}`;
const TIMEFRAMES = [
  { tf: "5", name: "one_day", bars: 54 },
  { tf: "15", name: "one_week", bars: 90 },
  { tf: "60", name: "one_month", bars: 105 },
  { tf: "1D", name: "six_months", bars: 120 },
  { tf: "1W", name: "one_year", bars: 52 },
  { tf: "1W", name: "five_years", bars: 260 },
].map((t, i) => ({ ...t, idx: i }));

const INTERVALS = [
  { key: "one_day",      tf: "1",   label: "1 Minute - 1 Day",     overfetch: 3000 },
  { key: "one_week",     tf: "15",  label: "15 Minutes - 1 Week",  overfetch: 24 * 4 * 14 },
  { key: "one_month",    tf: "60",  label: "1 Hour - 1 Month",     overfetch: 24 * 60 },
  { key: "six_months",   tf: "1D",  label: "Daily - 6 Months",     overfetch: 400 },
  { key: "one_year",     tf: "1W",  label: "Weekly - 1 Year",      overfetch: 200 },
  { key: "five_years",   tf: "1W",  label: "Weekly - 5 Years",     overfetch: 600 }
];

const TF_CONCURRENCY = Number(process.env.TF_CONCURRENCY || 6);
const TICKER_CONCURRENCY = Number(process.env.TICKER_CONCURRENCY || 33);
const TIMEOUT_MS = 20000;
const MAX_RETRIES = 3;
const INITIAL_BACKOFF_MS = 500;

const TZ = "Africa/Cairo";

// ==== HELPERS ====
const isIntraday = (tf) => tf === "1" || tf === "5" || tf === "15" || tf === "60";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = (min = 15, max = 60) => min + Math.floor(Math.random() * (max - min + 1));
const METRICS = { totalRetries: 0, totalTimeouts: 0, byTF: {} };

const fmtUTCDate = (d) =>
  `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,"0")}-${String(d.getUTCDate()).padStart(2,"0")}`;

function fmtLocalDateTime(date, tz = TZ) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: tz,
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit",
    hour12: true
  }).formatToParts(date);

  const m = Object.fromEntries(parts.map(p => [p.type, p.value]));
  // Some locales may include spaces before AM/PM — clean that up
  let period = (m.dayPeriod || "").toUpperCase().replace(/\./g, "").trim(); // remove dots
  return `${m.year}-${m.month}-${m.day} ${m.hour}:${m.minute} ${period}`;
}

const DH = {
  ymd(date, tz = TZ) {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit"
    }).formatToParts(date);
    const m = Object.fromEntries(parts.map(p => [p.type, p.value]));
    return `${m.year}-${m.month}-${m.day}`;
  },
  fromYMD(ymd) {
    const [y, m, d] = ymd.split("-").map(Number);
    return new Date(Date.UTC(y, m - 1, d, 0, 0, 0));
  },
  addDays(date, days) { const d = new Date(date); d.setUTCDate(d.getUTCDate() + days); return d; },
  addMonths(date, months) { const d = new Date(date); d.setUTCMonth(d.getUTCMonth() + months); return d; },
  addYears(date, years) { const d = new Date(date); d.setUTCFullYear(d.getUTCFullYear() + years); return d; },
  startOfWeek(date, tz = TZ) {
    const weekday = new Date(date.toLocaleString("en-US", { timeZone: tz })).getDay();
    const start = new Date(date);
    start.setUTCDate(start.getUTCDate() - weekday);
    return start;
  },
  nextWeek(d) { const n = new Date(d); n.setUTCDate(n.getUTCDate() + 7); return n; },
  ymdOfOffset(base, offset, tz = TZ) {
    const d = new Date(base);
    d.setUTCDate(d.getUTCDate() + offset);
    return new Intl.DateTimeFormat("en-CA", { timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit" }).format(d);
  }
};

// ==== POOL HELPER ====
async function mapPool(items, limit, worker) {
  const results = new Array(items.length);
  let idx = 0,
    active = 0;
  return new Promise((resolve) => {
    const launchNext = () => {
      if (idx >= items.length && active === 0) return resolve(results);
      while (active < limit && idx < items.length) {
        const current = idx++;
        active++;
        sleep(jitter()).then(() =>
          Promise.resolve(worker(items[current], current))
            .then((res) => {
              results[current] = res;
            })
            .catch((err) => {
              results[current] = err;
            })
            .finally(() => {
              active--;
              launchNext();
            })
        );
      }
    };
    launchNext();
  });
}

// ---- Fetch Bars ----
function waitForFirstUpdate(chart) {
  return new Promise((resolve, reject) => {
    let done = false;
    const finish = (p) => { if (!done) { done = true; resolve(p || []); } };
    chart.onUpdate(() => finish(chart.periods || []));
    chart.onError?.((e) => { if (!done) { done = true; reject(e); } });
  });
}

async function fetchRaw(ticker, tf, approxBars) {
  const client = new TradingView.Client({});
  let chart;
  const range = Math.min(approxBars + 50, 10000);

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      chart = new client.Session.Chart();
      chart.setTimezone("UTC");
      chart.setMarket(MARKET_SYM(ticker), { timeframe: tf, range });

      const periods = await Promise.race([
        waitForFirstUpdate(chart),
        new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), TIMEOUT_MS))
      ]);

      chart.delete(); client.end();
      if (!periods.length) throw new Error("no-data");

      return periods.map(p => {
        const dt = new Date(p.time * 1000);
        // Keep raw _ts for filtering & gap-fill; string formatting is decided later
        return { _ts: dt, Close: p.close };
      }).sort((a, b) => a._ts - b._ts);

    } catch (e) {
      console.warn(`⚠️ RETRY ${ticker} ${tf} (${attempt}/${MAX_RETRIES}) — ${e.message}`);
      try { chart?.delete(); client.end(); } catch {}
      await new Promise(r => setTimeout(r, INITIAL_BACKOFF_MS * Math.pow(2, attempt - 1)));
    }
  }
  return [];
}

// ---- Helpers ----
function pickOnOrAfterStartIndex(rows, cutoff) {
  for (let i = 0; i < rows.length; i++) if (DH.ymd(rows[i]._ts) >= cutoff) return i;
  return rows.length - 1;
}

function filterWindow(rows, key) {
  if (!rows.length) return rows;
  const last = rows.at(-1);
  const lastLocal = new Date(last._ts);

  const sliceFrom = (cutoff) => {
    const idx = pickOnOrAfterStartIndex(rows, cutoff);
    // Always include one bar before cutoff (if available)
    return idx > 0 ? rows.slice(idx - 1) : rows;
  };

  switch (key) {
    case "one_day": {
      const cutoff = DH.ymd(last._ts); // same day as last
      // Include all data from that day + one previous bar
      const filtered = rows.filter(r => DH.ymd(r._ts) === cutoff);
      const firstIdx = rows.findIndex(r => r === filtered[0]);
      return firstIdx > 0 ? rows.slice(firstIdx - 1) : filtered;
    }

    case "one_week": {
      const cutoff = DH.ymd(DH.addDays(lastLocal, -6));
      return sliceFrom(cutoff);
    }

    case "one_month": {
      const cutoff = DH.ymd(DH.addDays(lastLocal, -30));
      return sliceFrom(cutoff);
    }

    case "six_months": {
      const cutoff = DH.ymd(DH.addMonths(lastLocal, -6));
      return sliceFrom(cutoff);
    }

    default:
      return rows;
  }
}

// ---- GAP FILLING FOR INTRADAY ----
// Fill missing intraday bars by carrying the previous close, but ONLY within the same Cairo-local day.
// stepMinutes: 1 for one_day, 15 for one_week, 60 for one_month
function fillIntradayGapsWithinDay(rows, stepMinutes) {
  if (!rows.length) return rows;

  const stepMs = stepMinutes * 60 * 1000;
  const out = [];
  out.push(rows[0]);

  for (let i = 1; i < rows.length; i++) {
    const prev = out[out.length - 1];
    const curr = rows[i];

    // Only fill within the same Cairo-local day
    const prevDay = DH.ymd(prev._ts);
    const currDay = DH.ymd(curr._ts);

    if (prevDay === currDay) {
      let t = prev._ts.getTime() + stepMs;
      while (t < curr._ts.getTime()) {
        const synthetic = { _ts: new Date(t), Close: prev.Close };
        // Guard in case the step crosses to next local day (rare around DST jumps)
        if (DH.ymd(synthetic._ts) !== prevDay) break;
        out.push(synthetic);
        t += stepMs;
      }
    }
    out.push(curr);
  }

  return out;
}

// ---- Build Weekly (daily-close) Series ----
async function buildWeeklySeriesFromDaily(ticker, yearsBack) {
  const daily = await fetchRaw(ticker, "1D", Math.min(yearsBack * 370 + 120, 10000));
  if (!daily.length) return [];

  const last = daily.at(-1);
  const latestYMD = DH.ymd(last._ts);
  const cutoffYMD = DH.ymd(DH.addYears(last._ts, -yearsBack));
  const dailyMap = new Map(daily.map(r => [DH.ymd(r._ts), r.Close]));

  let week = DH.startOfWeek(DH.fromYMD(cutoffYMD));
  while (DH.ymd(week) < cutoffYMD) week = DH.nextWeek(week);
  const lastWeek = DH.startOfWeek(last._ts);

  const anchors = [];
  let firstWeek = true;

  const avail = (start, offs) => offs.map(o => DH.ymdOfOffset(start, o)).find(d => dailyMap.has(d));
  const nextWeekClosed = (start) => {
    const n = DH.nextWeek(start);
    return ![0,1,2,3,4].some(o => dailyMap.has(DH.ymdOfOffset(n, o)));
  };
  const lastInWeek = (start) => [4,3,2,1,0].map(o => DH.ymdOfOffset(start,o)).find(d => dailyMap.has(d));

  while (week <= lastWeek) {
    const sun = DH.ymd(week);
    let pick = null;
    if (dailyMap.has(sun)) pick = sun;
    else if (firstWeek) {
      const thu = DH.ymdOfOffset(week, -3);
      pick = (thu >= cutoffYMD && dailyMap.has(thu)) ? thu : avail(week, [1,2,3,4]);
    } else {
      const thu = DH.ymdOfOffset(week, -3);
      pick = dailyMap.has(thu) ? thu : avail(week, [1,2,3,4]);
    }

    if (pick) anchors.push(pick);
    if (nextWeekClosed(week)) {
      const liw = lastInWeek(week);
      if (liw && liw !== pick) anchors.push(liw);
    }

    firstWeek = false;
    week = DH.nextWeek(week);
  }

  if (anchors.at(-1) !== latestYMD) anchors.push(latestYMD);
  return [...new Set(anchors)].sort().map(d => ({ Date: d, Close: dailyMap.get(d) ?? null }));
}

// ---- Interval Worker ----
async function fetchInterval(ticker, def) {
  const { key, tf, label, overfetch } = def;
  console.log(`▶ ${ticker} — ${label} (${tf})`);

  // Weekly frames: weekly cadence derived from daily closes (Date stays YYYY-MM-DD)
  if (key === "one_year" || key === "five_years") {
    const yearsBack = key === "one_year" ? 1 : 5;
    const rows = await buildWeeklySeriesFromDaily(ticker, yearsBack);
    return { key, rows };
  }

  // Non-weekly frames: fetch base timeframe and trim by duration
  const raw = await fetchRaw(ticker, tf, overfetch);
  let trimmed = filterWindow(raw, key);

  // Gap fill within day for intraday frames
  if (key === "one_day") {
    trimmed = fillIntradayGapsWithinDay(trimmed, 1);    // 1-minute gaps
  } else if (key === "one_week") {
    trimmed = fillIntradayGapsWithinDay(trimmed, 15);   // 15-minute gaps
  } else if (key === "one_month") {
    trimmed = fillIntradayGapsWithinDay(trimmed, 60);   // 60-minute gaps
  }

  // Format dates: intraday -> Cairo local datetime; daily -> YYYY-MM-DD
  const rows = trimmed.map(r => ({
    Date: isIntraday(tf) ? fmtLocalDateTime(r._ts, TZ) : fmtUTCDate(r._ts),
    Close: r.Close
  }));

  return { key, rows };
}

// ---- Master Fetch ----
async function fetchAllIntervalsForTicker(tickerInput) {
  const ticker = tickerInput.replace(/^EGX:/i, "");
  const results = await mapPool(INTERVALS, TF_CONCURRENCY, def => fetchInterval(ticker, def));
  const combined = Object.fromEntries(results.map(r => [r.key, r.rows]));
  return { ticker, data: combined };
}

// ==== CORE FETCHERS ====
async function fetchTickerTF(client, ticker, tf, barsNeeded, tfName) {
  const pretty = `${ticker} ${tf}`;
  const range = Math.min(barsNeeded + 10, 10000);
  let attempt = 0,
    lastErr = null;
  if (!METRICS.byTF[tfName]) METRICS.byTF[tfName] = { retries: 0, timeouts: 0 };

  while (attempt < MAX_RETRIES) {
    attempt++;
    let chart;
    let timeoutHandle;

    try {
      chart = new client.Session.Chart();
      chart.setTimezone("UTC");
      chart.setMarket(MARKET_SYM(ticker), { timeframe: tf, range });

      const timeoutPromise = new Promise((_, reject) => {
        timeoutHandle = setTimeout(() => reject(new Error("timeout")), TIMEOUT_MS);
      });

      const updatePromise = new Promise((resolve, reject) => {
        let resolved = false;

        chart.onUpdate(() => {
          if (resolved) return;
          resolved = true;
          clearTimeout(timeoutHandle);
          resolve(chart.periods || []);
        });

        chart.onError((err) => {
          if (resolved) return;
          resolved = true;
          clearTimeout(timeoutHandle);
          reject(err || new Error("chart update error"));
        });
      });

      const periods = await Promise.race([updatePromise, timeoutPromise]);
      try {
        chart.delete();
      } catch {}

      if (!periods || periods.length === 0) throw new Error("no-data");

      // ✅ Sort by time ascending
      const sortedPeriods = periods.sort((a, b) => a.time - b.time);

      let recentBars;

      // ✅ Only apply "latest trading day" filter for intraday timeframes
      // const intradayTFs = ["1", "3", "5", "15", "30", "45", "60", "120", "240"];
      if (tf === "5") {
        const latestTimestamp = sortedPeriods[sortedPeriods.length - 1].time * 1000;
        const latestDate = new Date(latestTimestamp);
        const latestDateStr = latestDate.toISOString().split("T")[0]; // "YYYY-MM-DD"

        const sameDayPeriods = sortedPeriods.filter((p) => {
          const dateStr = new Date(p.time * 1000).toISOString().split("T")[0];
          return dateStr === latestDateStr;
        });

        recentBars = sameDayPeriods.slice(-barsNeeded);
      } else {
        // ✅ Daily / Weekly / etc.: Keep full history
        recentBars = sortedPeriods.slice(-barsNeeded);
      }

      // ✅ Format each time like "2025-10-16 9:15 AM"
      const dataPoints = recentBars.map((p) => {
        const date = new Date(p.time * 1000);
        const formattedTime = date
          .toLocaleString("en-US", {
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
            hour: "numeric",
            minute: "2-digit",
            hour12: true,
            timeZone: "UTC",
          })
          .replace(",", "")
          .replace(/(\d+)\/(\d+)\/(\d+)/, (_, m, d, y) => `${y}-${m}-${d}`);
        return { time: formattedTime, close: p.close };
      });

      console.log(`  ✓ ${pretty}: ${dataPoints.length}/${barsNeeded} bars`);
      return dataPoints;
    } catch (e) {
      lastErr = e;
      const msg = (e?.message || "").toLowerCase();
      METRICS.totalRetries++;
      METRICS.byTF[tfName].retries++;

      if (msg.includes("timeout")) {
        METRICS.totalTimeouts++;
        METRICS.byTF[tfName].timeouts++;
        console.warn(
          `  ⚠️ RETRY (timeout) — ${ticker} ${tf} attempt ${attempt}/${MAX_RETRIES}`
        );
      } else {
        console.warn(
          `  ⚠️ RETRY (${e.message}) — ${ticker} ${tf} attempt ${attempt}/${MAX_RETRIES}`
        );
      }

      try {
        if (chart) chart.delete();
      } catch {}
      clearTimeout(timeoutHandle);

      const backoff = INITIAL_BACKOFF_MS * Math.pow(2, attempt - 1);
      await sleep(backoff);
    }
  }

  console.warn(
    `  ❌ ${pretty} failed after ${MAX_RETRIES} attempts (${lastErr?.message || lastErr})`
  );
  return [];
}

async function processTimeframe(client, { tf, name, bars, idx }, tickers) {
  console.log(`\n▶ ${name} (${tf}) — last ${bars} bars [tickers x${tickers.length}]`);
  const data = {};
  await mapPool(tickers, TICKER_CONCURRENCY, async (ticker) => {
    const result = await fetchTickerTF(client, ticker, tf, bars, name);
    data[ticker] = result;
  });
  return { idx, name, data };
}

async function fetchEGXData(client, tickers) {
  const t0 = Date.now();
  const tfConcurrency = TF_CONCURRENCY;

  const tfResults = await mapPool(TIMEFRAMES, tfConcurrency, (tfInfo) =>
    processTimeframe(client, tfInfo, tickers)
  );

  const outputJSON = {};
  tfResults
    .filter(Boolean)
    .sort((a, b) => a.idx - b.idx)
    .forEach(({ name, data }) => {
      outputJSON[name] = data;
    });

  const dt = (Date.now() - t0) / 1000;
  console.log(`⏱️ Fetch done in ${dt.toFixed(2)}s`);
  return outputJSON;
}

// ==== POST ENDPOINT ====
app.post("/fetch_egx_data", async (req, res) => {
  const client = new TradingView.Client({}); // isolate per request

  try {
    const tickers = req.body.tickers;

    if (!Array.isArray(tickers) || tickers.length === 0) {
      return res.status(400).json({ error: "Invalid request. Expected { tickers: [ ... ] }" });
    }

    console.log(`📡 Fetching data for ${tickers.length} tickers: ${tickers.join(", ")}`);

    // Global timeout failsafe
    const json = await Promise.race([
      fetchEGXData(client, tickers),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("Global timeout exceeded (60s)")), 60000)
      ),
    ]);

    res.json(json);
  } catch (e) {
    console.error("❌ Error in fetch_egx_data:", e);
    res.status(500).json({ error: e.message });
  } finally {
    try {
      client.end();
    } catch {}
  }
});

app.post("/fetch_ticker", async (req, res) => {
  const { ticker } = req.body;
  if (!ticker) return res.status(400).json({ error: "Missing 'ticker'" });

  try {
    const { ticker: t, data } = await fetchAllIntervalsForTicker(ticker);
    res.json({ ticker: t, data });
  } catch (err) {
    console.error("❌ Error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);

});


