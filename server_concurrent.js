// fetch_multi_tickers_fixed_windows_timer.js
// npm i @mathieuc/tradingview dotenv xlsx

const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");
require("dotenv").config();
const TradingView = require("@mathieuc/tradingview");

// ==== CONFIG ====
// EGX tickers
const TICKERS = [
  "TMGH","EMFD","ORHD","PHDC","OCDI","HELI","MASR","ZMID","PRDC","ACAP",
  "ELKA","UNIT","ARAB","ELSH","EHDR","AMER","AIFI","ACAMD","MENA","DAPH",
  "IDRE","NARE","UEGC","NHPS","MAAL","RREI","TANM","OBRI","ICID","CCRS",
  "GIHD","BONY","AREH"
];
const MARKET_SYM = (t) => `EGX:${t}`;

// Timeframes & bar counts (back from latest)
const TIMEFRAMES = [
  { tf: "5",   name: "5 Minutes",   bars: 54  },
  { tf: "15",  name: "15 Minutes",  bars: 90  },
  { tf: "60",  name: "1 Hour",      bars: 21 * 5 },
  { tf: "1D",  name: "Daily",       bars: 120 },
  { tf: "1W",  name: "Weekly_52",   bars: 52  },
  { tf: "1W",  name: "Weekly_260",  bars: 260 },
];

// Reliability knobs
const TIMEOUT_MS = 20000;
const MAX_RETRIES = 3;
const INITIAL_BACKOFF_MS = 500;

// ==== HELPERS ====
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ymdhms = (d) => {
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const HH = String(d.getUTCHours()).padStart(2, "0");
  const MM = String(d.getUTCMinutes()).padStart(2, "0");
  const SS = String(d.getUTCSeconds()).padStart(2, "0");
  return ${yyyy}-${mm}-${dd} ${HH}:${MM}:${SS};
};
const ymd = (d) => {
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return ${yyyy}-${mm}-${dd};
};
const isIntraday = (tf) => tf === "5" || tf === "15" || tf === "60";

// Wait for hydrated history
function waitForFirstUpdate(chart) {
  return new Promise((resolve, reject) => {
    let resolved = false;
    const done = (periods) => {
      if (resolved) return;
      resolved = true;
      resolve(periods || []);
    };
    chart.onUpdate(() => done(chart.periods || []));
    if (typeof chart.onError === "function") {
      chart.onError((e) => {
        if (!resolved) {
          resolved = true;
          reject(e || new Error("chart update error"));
        }
      });
    }
  });
}

const client = new TradingView.Client({});

// Fetch one ticker/tf -> last N bars
async function fetchTickerTF(ticker, tf, barsNeeded) {
  const pretty = ${ticker} ${tf};
  const range = Math.min(barsNeeded + 10, 10000);
  let attempt = 0, lastErr = null;

  while (attempt < MAX_RETRIES) {
    attempt++;
    let chart;
    try {
      chart = new client.Session.Chart();
      chart.setTimezone("UTC");
      chart.setMarket(MARKET_SYM(ticker), { timeframe: tf, range });

      const periods = await Promise.race([
        waitForFirstUpdate(chart),
        new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), TIMEOUT_MS)),
      ]);

      try { chart.delete(); } catch {}

      if (!periods || periods.length === 0) throw new Error("no-data");

      const tail = periods.slice(-barsNeeded);
      const rows = tail.map((p) => {
        const dt = new Date(p.time * 1000);
        const dateStr = isIntraday(tf) ? ymdhms(dt) : ymd(dt);
        return { Date: dateStr, Close: p.close };
      });
      rows.sort((a, b) => (a.Date < b.Date ? -1 : a.Date > b.Date ? 1 : 0));
      console.log(`  ✓ ${pretty}: ${rows.length}/${barsNeeded} bars`);
      return rows;
    } catch (e) {
      lastErr = e;
      try { if (chart) chart.delete(); } catch {}
      const backoff = INITIAL_BACKOFF_MS * Math.pow(2, attempt - 1);
      console.warn(`  ⚠️ ${pretty} attempt ${attempt}/${MAX_RETRIES} failed: ${e?.message || e}. Retrying in ${backoff}ms...`);
      await sleep(backoff);
    }
  }

  console.warn(`  ❌ ${pretty} failed after ${MAX_RETRIES} attempts (${lastErr?.message || lastErr})`);
  return [];
}

// Merge by Date across tickers
function mergeByDate(tickerToRows) {
  const keySet = new Set();
  for (const rows of Object.values(tickerToRows)) {
    for (const r of rows) keySet.add(r.Date);
  }
  const allDates = Array.from(keySet).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  return allDates.map((d) => {
    const row = { Date: d };
    for (const t of TICKERS) {
      const found = (tickerToRows[t] || []).find((r) => r.Date === d);
      row[t] = found ? found.Close : undefined;
    }
    return row;
  });
}

async function main() {
  console.log(Fetching ${TICKERS.length} tickers for all intervals...);
  const startTime = Date.now(); // ← TIMER START

  const wb = XLSX.utils.book_new();

  for (const { tf, name, bars } of TIMEFRAMES) {
    console.log(\n▶ ${name} (${tf}) — last ${bars} bars);
    const tickerToRows = {};
    for (const t of TICKERS) {
      tickerToRows[t] = await fetchTickerTF(t, tf, bars);
    }
    const merged = mergeByDate(tickerToRows);
    const headers = ["Date", ...TICKERS];
    const ws = XLSX.utils.json_to_sheet(merged, { header: headers });
    ws["!cols"] = [{ wch: isIntraday(tf) ? 20 : 12 }, ...TICKERS.map(() => ({ wch: 12 }))];
    XLSX.utils.book_append_sheet(wb, ws, name);
  }

  const outDir = path.join(process.cwd(), "data");
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir);
  const outPath = path.join(outDir, EGX_RealEstate_5m_15m_1h_1d_weekly52_260_lastbars.xlsx);
  XLSX.writeFile(wb, outPath);

  const totalSec = (Date.now() - startTime) / 1000;
  const mins = Math.floor(totalSec / 60);
  const secs = (totalSec % 60).toFixed(2);

  console.log(\n✅ Wrote ${outPath});
  console.log(⏱️ Total fetch time: ${mins} min ${secs} sec);

  try { client.end(); } catch {}
}

main().catch((e) => {
  console.error("❌ Fatal error:", e);
  try { client.end(); } catch {}
  process.exit(1);
});