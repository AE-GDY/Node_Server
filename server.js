// ==========================
// server.js — EGX Data API
// ==========================

global.WebSocket = require("ws");

const express = require("express");
const TradingView = require("@mathieuc/tradingview");
require("dotenv").config();

const app = express();
const PORT = 3000;

app.use(express.json());

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function log(...args) {
  console.log("[TradingView-EGX]", ...args);
}

function formatDateTime(dt, timeframe) {
  const yyyy = dt.getUTCFullYear();
  const mm = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(dt.getUTCDate()).padStart(2, "0");

  // Include time for intraday frames
  if (["5", "15", "1H"].includes(timeframe)) {
    const hh = String(dt.getUTCHours()).padStart(2, "0");
    const min = String(dt.getUTCMinutes()).padStart(2, "0");
    const ss = String(dt.getUTCSeconds()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd} ${hh}:${min}:${ss}`;
  } else {
    return `${yyyy}-${mm}-${dd}`;
  }
}

function waitForFirstUpdate(chart) {
  return new Promise((resolve, reject) => {
    let done = false;
    const timeout = setTimeout(() => {
      if (!done) reject(new Error("Timeout waiting for data"));
    }, 15000);

    chart.onError((err) => {
      if (!done) {
        done = true;
        clearTimeout(timeout);
        reject(err);
      }
    });

    chart.onUpdate(() => {
      if (done) return;
      done = true;
      clearTimeout(timeout);
      resolve(chart.periods || []);
    });
  });
}

// --- Candle count mapping based on your definition ---
function getPeriodSettings(period) {
  const map = {
    "1D": { timeframe: "5", candles: 54 },
    "1W": { timeframe: "15", candles: 90 },
    "1M": { timeframe: "1H", candles: 105 },
    "6M": { timeframe: "1D", candles: 120 },
    "1Y": { timeframe: "1W", candles: 52 },
    "5Y": { timeframe: "1W", candles: 260 },
  };
  return map[period] || map["1M"]; // default to 1 month if not found
}

// --- Main fetch function ---
async function fetchEGXData(tickers, period) {
  const { timeframe, candles } = getPeriodSettings(period);
  const TO = new Date();
  const SLEEP_MS = 1000;
  const client = new TradingView.Client({});
  const jsonData = {};

  for (const t of tickers) {
    const tv = `EGX:${t}`;
    log(`\n▶ Fetching ${t} (${tv}) — ${candles} candles @ ${timeframe}`);

    try {
      const chart = new client.Session.Chart();
      chart.setTimezone("UTC");
      chart.setMarket(tv, { timeframe, range: candles });

      const periods = await waitForFirstUpdate(chart);
      chart.delete();

      if (!periods || periods.length === 0) {
        log(`⚠️ No data returned for ${t}`);
        await sleep(SLEEP_MS);
        continue;
      }

      const closes = [];
      for (const p of periods) {
        const dt = new Date(p.time * 1000);
        closes.push({
          date: formatDateTime(dt, timeframe),
          close: p.close,
        });
      }

      jsonData[t] = closes;
      log(`✅ Added ${closes.length} closes for ${t}`);
    } catch (e) {
      log(`❌ ${t} failed: ${e.message || e}`);
    }

    await sleep(SLEEP_MS);
  }

  client.end();
  return jsonData;
}

// --- POST Endpoint ---
app.post("/fetch", async (req, res) => {
  log("📡 Received POST /fetch");

  const { tickers, period } = req.body;

  if (!tickers || !Array.isArray(tickers) || tickers.length === 0) {
    return res
      .status(400)
      .json({ success: false, error: "Missing or invalid 'tickers' array." });
  }

  if (!period) {
    return res
      .status(400)
      .json({
        success: false,
        error: "Missing 'period' field (e.g. '1D', '1W', '1M', etc.).",
      });
  }

  try {
    const data = await fetchEGXData(tickers, period);
    res.json({ success: true, period, data });
  } catch (e) {
    console.error("Error fetching TradingView data:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// --- POST Endpoint for a single ticker with multiple periods ---
app.post("/fetchSingle", async (req, res) => {
  log("📡 Received POST /fetchSingle");

  const { ticker, periods } = req.body;

  if (!ticker || typeof ticker !== "string") {
    return res
      .status(400)
      .json({ success: false, error: "Missing or invalid 'ticker' field." });
  }

  if (!periods || !Array.isArray(periods) || periods.length === 0) {
    return res
      .status(400)
      .json({
        success: false,
        error: "Missing or invalid 'periods' array (e.g. ['1D', '1W', '1M']).",
      });
  }

  try {
    const data = {};

    // Loop through each requested period
    for (const period of periods) {
      log(`🔁 Fetching data for ${ticker} - ${period}`);
      const periodData = await fetchEGXData([ticker], period);
      data[period] = periodData[ticker] || [];
    }

    res.json({
      success: true,
      ticker,
      data,
    });
  } catch (e) {
    console.error("Error fetching TradingView data:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});


// --- Start Server ---
app.listen(PORT, () => {
  console.log(`🚀 Server running at http://localhost:${PORT}`);
});