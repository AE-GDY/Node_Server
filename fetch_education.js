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
  { tf: "5", name: "5 Minutes", bars: 54 },
  { tf: "15", name: "15 Minutes", bars: 90 },
  { tf: "60", name: "1 Hour", bars: 105 },
  { tf: "1D", name: "Daily", bars: 120 },
  { tf: "1W", name: "Weekly_52", bars: 52 },
  { tf: "1W", name: "Weekly_260", bars: 260 },
].map((t, i) => ({ ...t, idx: i }));

const TF_CONCURRENCY = Number(process.env.TF_CONCURRENCY || 6);
const TICKER_CONCURRENCY = Number(process.env.TICKER_CONCURRENCY || 33);
const TIMEOUT_MS = 20000;
const MAX_RETRIES = 3;
const INITIAL_BACKOFF_MS = 500;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = (min = 15, max = 60) => min + Math.floor(Math.random() * (max - min + 1));
const METRICS = { totalRetries: 0, totalTimeouts: 0, byTF: {} };

// ==== POOL HELPER ====
async function mapPool(items, limit, worker) {
  const results = new Array(items.length);
  let idx = 0, active = 0;
  return new Promise((resolve) => {
    const launchNext = () => {
      if (idx >= items.length && active === 0) return resolve(results);
      while (active < limit && idx < items.length) {
        const current = idx++;
        active++;
        sleep(jitter()).then(() =>
          Promise.resolve(worker(items[current], current))
            .then((res) => { results[current] = res; })
            .catch((err) => { results[current] = err; })
            .finally(() => { active--; launchNext(); })
        );
      }
    };
    launchNext();
  });
}

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

// ==== CORE FETCHERS ====
async function fetchTickerTF(client, ticker, tf, barsNeeded, tfName) {
  const pretty = `${ticker} ${tf}`;
  const range = Math.min(barsNeeded + 10, 10000);
  let attempt = 0, lastErr = null;
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
        timeoutHandle = setTimeout(() => {
          reject(new Error("timeout"));
        }, TIMEOUT_MS);
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

      try { chart.delete(); } catch {}

      if (!periods || periods.length === 0)
        throw new Error("no-data");

      const closes = periods.slice(-barsNeeded).map((p) => p.close);
      console.log(`  ✓ ${pretty}: ${closes.length}/${barsNeeded} bars`);
      return closes;

    } catch (e) {
      lastErr = e;
      const msg = (e?.message || "").toLowerCase();
      METRICS.totalRetries++;
      METRICS.byTF[tfName].retries++;

      if (msg.includes("timeout")) {
        METRICS.totalTimeouts++;
        METRICS.byTF[tfName].timeouts++;
        console.warn(`  ⚠️ RETRY (timeout) — ${ticker} ${tf} attempt ${attempt}/${MAX_RETRIES}`);
      } else {
        console.warn(`  ⚠️ RETRY (${e.message}) — ${ticker} ${tf} attempt ${attempt}/${MAX_RETRIES}`);
      }

      try { if (chart) chart.delete(); } catch {}
      clearTimeout(timeoutHandle);

      const backoff = INITIAL_BACKOFF_MS * Math.pow(2, attempt - 1);
      await sleep(backoff);
    }
  }

  console.warn(`  ❌ ${pretty} failed after ${MAX_RETRIES} attempts (${lastErr?.message || lastErr})`);
  return [];
}

async function processTimeframe(client, { tf, name, bars, idx }, tickers) {
  console.log(`\n▶ ${name} (${tf}) — last ${bars} bars [tickers x${tickers.length}]`);
  const data = {};
  await mapPool(tickers, TICKER_CONCURRENCY, async (ticker) => {
    const closes = await fetchTickerTF(client, ticker, tf, bars, name);
    data[ticker] = closes;
  });
  return { idx, name, data };
}

async function fetchEGXData(client, tickers) {
  const t0 = Date.now();
  const tfConcurrency = tickers.length === 1 ? 1 : TF_CONCURRENCY; // 🟢 dynamic concurrency

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
  const client = new TradingView.Client({}); // 🟢 isolate per request

  try {
    const tickers = req.body.tickers;

    if (!Array.isArray(tickers) || tickers.length === 0) {
      return res.status(400).json({ error: "Invalid request. Expected { tickers: [ ... ] }" });
    }

    console.log(`📡 Fetching data for ${tickers.length} tickers: ${tickers.join(", ")}`);

    // 🟢 Global timeout failsafe
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
    try { client.end(); } catch {}
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});