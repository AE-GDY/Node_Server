// ===============================
// EGX/ADX Stock Data Server (FULL FILE) — UPDATED + CORRECTED
// Fixes:
// ✅ Market-aware benchmark (EGX->EGX30, ADX->ADXGI by default)
// ✅ No more hardcoded EGX30 logic for ADX
// ✅ Renamed includeEGX30 -> includeBenchmark (clearer)
// ✅ Benchmark-only 5Y daily table is appended into five_years_daily for the benchmark ticker
// ✅ Filters benchmark out of "mainTickers" jobs when doing multi-ticker runs
//
// Notes:
// - Market prefix is still applied per-request (/fetch and /close)
// - REF ticker still EGX->COMI, ADX->FAB
// - TZ still EGX Africa/Cairo, ADX Asia/Dubai
// ===============================

const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");
const { DateTime } = require("luxon");
require("dotenv").config({ path: "./vars.env" });

console.log("CWD:", process.cwd());
console.log("SESSION length:", (process.env.SESSION || "").length);
console.log("SIGNATURE length:", (process.env.SIGNATURE || "").length);

const TradingView = require("@mathieuc/tradingview");
const speechsdk = require("microsoft-cognitiveservices-speech-sdk");

const express = require("express");
const app = express();
const cors = require("cors");

app.use(cors());
app.use(express.json());

// ===== CONFIG =====
let MARKET_PREFIX = "EGX"; // default, overridden per-request in /fetch and /close
const MARKET_TZ_MAP = {
  EGX: "Africa/Cairo",
  ADX: "Asia/Dubai",
  NASDAQ: "America/New_York",
};

// ✅ Market benchmark by market (edit ADXGI if your TradingView symbol differs)
const BENCHMARK_BY_MARKET = {
  EGX: "EGX30",
  ADX: "FADGI",
  // US benchmark: S&P 500. Pre-prefixed with the correct TradingView exchange
  // ("SP:") so MARKET_SYM passes it through unchanged.
  NASDAQ: "SP:SPX",
};
// Uses current MARKET_PREFIX unless symbol already has a prefix
const MARKET_SYM = (t) => (String(t).includes(":") ? String(t) : `${MARKET_PREFIX}:${t}`);

const TICKER_CONCURRENCY = Number(process.env.TICKER_CONCURRENCY || 20);
const TF_CONCURRENCY = Number(process.env.TF_CONCURRENCY || 6);

const TIMEOUT_DAILY_MS = Number(process.env.TIMEOUT_DAILY_MS || 12000);
const TIMEOUT_INTRADAY_MS = Number(process.env.TIMEOUT_INTRADAY_MS || 12000);

const MAX_RETRIES = Number(process.env.MAX_RETRIES || 3);
const INITIAL_BACKOFF_MS = Number(process.env.INITIAL_BACKOFF_MS || 500);

const FILL_LEADING_DEFAULT = /^true$/i.test(process.env.FILL_LEADING || "true");

// timezone set per request
let TZ = "Africa/Cairo";

// reference ticker set per request (EGX->COMI, ADX->FAB)
let REF_TICKER = "COMI";

// EGX local slots (kept as-is)
const DAY_15M_SLOTS = [
  "10:00:00", "10:15:00", "10:30:00", "10:45:00", "11:00:00", "11:15:00", "11:30:00", "11:45:00",
  "12:00:00", "12:15:00", "12:30:00", "12:45:00", "13:00:00", "13:15:00", "13:30:00", "13:45:00",
  "14:00:00", "14:15:00"
];
const DAY_60M_SLOTS = ["10:00:00", "11:00:00", "12:00:00", "13:00:00", "14:00:00"];

// US session slots (NYSE/NASDAQ: 09:30 → 16:00 ET)
const DAY_15M_SLOTS_US = [
  "09:30:00", "09:45:00",
  "10:00:00", "10:15:00", "10:30:00", "10:45:00",
  "11:00:00", "11:15:00", "11:30:00", "11:45:00",
  "12:00:00", "12:15:00", "12:30:00", "12:45:00",
  "13:00:00", "13:15:00", "13:30:00", "13:45:00",
  "14:00:00", "14:15:00", "14:30:00", "14:45:00",
  "15:00:00", "15:15:00", "15:30:00", "15:45:00"
];
const DAY_60M_SLOTS_US = [
  "09:30:00", "10:30:00", "11:30:00", "12:30:00",
  "13:30:00", "14:30:00", "15:30:00"
];

// Returns the appropriate slot list based on the current MARKET_PREFIX.
// EGX/ADX → existing Cairo-hour slots (no behavior change).
// NASDAQ  → US session slots.
function slotsForCurrentMarket(tfStr) {
  const isUS = MARKET_PREFIX === "NASDAQ";
  if (tfStr === "15") return isUS ? DAY_15M_SLOTS_US : DAY_15M_SLOTS;
  if (tfStr === "60") return isUS ? DAY_60M_SLOTS_US : DAY_60M_SLOTS;
  return null;
}
// ===== GLOBALS =====
// (kept names to preserve logic; now they track the current request’s REF_TICKER)
let GLOBAL_COMI_DAY = null; // "yyyy-MM-dd" of latest REF_TICKER 1D
let GLOBAL_COMI_TS = null;  // epoch seconds of latest REF_TICKER bar

// ===== UTILS =====
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function jitter(min = 200, max = 700) { return min + Math.floor(Math.random() * (max - min + 1)); }
function localNow() { return DateTime.now().setZone(TZ); }

function logEnvCheck() {
  const hasSession = !!process.env.SESSION && process.env.SESSION.trim() !== "";
  const hasSig = !!process.env.SIGNATURE && process.env.SIGNATURE.trim() !== "";
  console.log(
    `🔧 ENV: SESSION=${hasSession} SIGNATURE=${hasSig} TF_CONCURRENCY=${TF_CONCURRENCY} ` +
    `TICKER_CONCURRENCY=${TICKER_CONCURRENCY} TIMEOUT_DAILY_MS=${TIMEOUT_DAILY_MS} TIMEOUT_INTRADAY_MS=${TIMEOUT_INTRADAY_MS} ` +
    `MAX_RETRIES=${MAX_RETRIES}`
  );
}

function normalizeMarket(m) {
  const v = String(m || "EGX").trim().toUpperCase();
  if (v === "ADX") return "ADX";
  // Accept "america" (matches Python rag.py default), "us", or "nasdaq"
  // as aliases for the US market. Internally we use "NASDAQ" because
  // it doubles as the MARKET_PREFIX for TradingView symbols.
  if (v === "AMERICA" || v === "US" || v === "NASDAQ") return "NASDAQ";
  return "EGX";
}

function applyMarketContext(mkt) {
  MARKET_PREFIX = mkt; // "EGX", "ADX", or "NASDAQ"
  TZ = MARKET_TZ_MAP[mkt] || "Africa/Cairo";
  REF_TICKER = (mkt === "ADX") ? "FAB"
             : (mkt === "NASDAQ") ? "AAPL"   // liquid NASDAQ reference for latest-trading-day anchor
             : "COMI";

  // Reset per-request global anchors (since they depend on REF + TZ)
  GLOBAL_COMI_DAY = null;
  GLOBAL_COMI_TS = null;

  console.log(`🏛️ Market context: MARKET_PREFIX=${MARKET_PREFIX} REF_TICKER=${REF_TICKER} TZ=${TZ}`);
}
async function mapPool(items, limit, worker) {
  const results = new Array(items.length);
  let idx = 0, active = 0;
  return new Promise((resolve) => {
    const launchNext = () => {
      if (idx >= items.length && active === 0) return resolve(results);
      while (active < limit && idx < items.length) {
        const cur = idx++; active++;
        Promise.resolve()
          .then(() => worker(items[cur], cur))
          .then(res => { results[cur] = res; })
          .catch(err => { results[cur] = { error: String(err?.message || err) }; })
          .finally(() => { active--; launchNext(); });
      }
    };
    launchNext();
  });
}

function barsToDailyMapCairo(bars) {
  const map = new Map();
  for (const b of bars) {
    const d = DateTime.fromSeconds(b.time, { zone: TZ }).toISODate();
    map.set(d, b.close);
  }
  return map;
}

// ===== TV CLIENT LIFECYCLE =====
function assertTvEnv() {
  const hasSession = !!process.env.SESSION && process.env.SESSION.trim() !== "";
  const hasSig = !!process.env.SIGNATURE && process.env.SIGNATURE.trim() !== "";
  if (!hasSession || !hasSig) {
    console.warn("⚠️ TradingView auth missing: SESSION or SIGNATURE env var is not set on this instance.");
  }
  return { hasSession, hasSig };
}

function createTvClient() {
  assertTvEnv();
  return new TradingView.Client({
    token: process.env.SESSION,
    signature: process.env.SIGNATURE,
  });
}

function destroyTvClient(tv) {
  try { if (typeof tv?.end === "function") tv.end(); } catch { }
  try { if (typeof tv?.delete === "function") tv.delete(); } catch { }
}

function isRecoverableTvError(err) {
  const msg = String(err?.message || err || "").toLowerCase();
  return (
    msg.includes("timeout") ||
    msg.includes("socket") ||
    msg.includes("websocket") ||
    msg.includes("ws") ||
    msg.includes("econnreset") ||
    msg.includes("broken pipe") ||
    msg.includes("unexpected close") ||
    msg.includes("chart error")
  );
}

/**
 * Robust fetch:
 * - per-call timeout
 * - retries
 * - on recoverable errors: recreate TV client and retry
 *
 * clientRef = { tv: TradingViewClient }
 */
async function fetchBarsOnce(clientRef, symbol, { timeframe, range, toUnix, timeoutMs, tag }) {
  const timeout = Number(timeoutMs || TIMEOUT_INTRADAY_MS);

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const t0 = Date.now();
    let chart = null;
    let timeoutId = null;
    let settled = false;

    try {
      chart = new clientRef.tv.Session.Chart();
      chart.setTimezone("UTC");
      chart.setMarket(symbol, { timeframe, range, to: toUnix });

      const periods = await new Promise((resolve, reject) => {
        const done = (fn, val) => {
          if (settled) return;
          settled = true;
          if (timeoutId) clearTimeout(timeoutId);
          fn(val);
        };

        chart.onUpdate(() => done(resolve, chart.periods || []));
        if (typeof chart.onError === "function") {
          chart.onError((e) => done(reject, e || new Error("chart error")));
        }

        timeoutId = setTimeout(() => done(reject, new Error("timeout")), timeout);
      });

      if (!periods || !periods.length) throw new Error("no-data");

      console.log(`✅ Success ${symbol} [tf=${timeframe}] in ${Date.now() - t0} ms (attempt ${attempt}/${MAX_RETRIES})${tag ? ` (${tag})` : ""}`);
      return periods;

    } catch (e) {
      const msg = String(e?.message || e);
      console.log(`❌ Fetch error ${symbol} [tf=${timeframe}] attempt ${attempt}/${MAX_RETRIES}: ${msg}${tag ? ` (${tag})` : ""}`);



      if (attempt >= MAX_RETRIES) return [];

      const backoff = INITIAL_BACKOFF_MS * Math.pow(2, attempt - 1) + jitter();
      console.log(`🔁 Retry in ${backoff}ms`);
      await sleep(backoff);

    } finally {
      try { if (timeoutId) clearTimeout(timeoutId); } catch { }
      try { if (chart) chart.delete(); } catch { }
    }
  }

  return [];
}

// ===== RANGE HELPERS =====
function calc1DRangeToCoverYears(years) {
  const end = localNow().endOf("day");
  const start = end.minus({ years });
  const days = Math.max(1, Math.round(end.diff(start, "days").days)) + 15;
  const nowUnix = Math.floor(localNow().toSeconds());
  return { range: days, toUnix: Math.min(Math.floor(end.toSeconds()), nowUnix) };
}
function calc1DRangeToCoverMonths(months) {
  const end = localNow().endOf("day");
  const start = end.minus({ months });
  const days = Math.max(1, Math.round(end.diff(start, "days").days)) + 10;
  const nowUnix = Math.floor(localNow().toSeconds());
  return { range: days, toUnix: Math.min(Math.floor(end.toSeconds()), nowUnix) };
}

// ===== REF PREFETCH =====
async function prefetchGlobalComiLatest(clientRef) {
  const end = localNow().endOf("day");
  const toUnix = Math.floor(end.toSeconds());

  const CANDIDATE_RANGES = [16, 64, 200, 400];

  let latestTs = 0, usedRange = null, barsGot = 0;

  for (const R of CANDIDATE_RANGES) {
    console.log(`🧭 Prefetch ${REF_TICKER} 1D (probe) to=${toUnix} range=${R}`);
    const dBars = await fetchBarsOnce(clientRef, MARKET_SYM(REF_TICKER), {
      timeframe: "1D",
      range: R,
      toUnix,
      timeoutMs: TIMEOUT_DAILY_MS,
      tag: "prefetch-ref"
    });

    barsGot = dBars?.length || 0;
    if (!barsGot) continue;

    for (const b of dBars) {
      if (b && typeof b.time === "number" && b.time > latestTs) latestTs = b.time;
    }
    if (!latestTs) continue;

    const lastDT = DateTime.fromSeconds(latestTs, { zone: TZ });
    const daysGap = end.diff(lastDT.endOf("day"), "days").days;
    usedRange = R;

    if (daysGap <= 7) break;
  }

  if (!latestTs) {
    console.warn(`⚠️ ${REF_TICKER} 1D prefetch returned empty across all probes.`);
    GLOBAL_COMI_DAY = null;
    GLOBAL_COMI_TS = null;
    return;
  }

  GLOBAL_COMI_TS = latestTs;
  GLOBAL_COMI_DAY = DateTime.fromSeconds(latestTs, { zone: TZ }).toISODate();
  console.log(`🧭 ${REF_TICKER} latest (1D): ${GLOBAL_COMI_DAY} (ts=${latestTs}) via range=${usedRange}, bars=${barsGot}`);
}

async function getLatestTradingDateFromCOMI(clientRef) {
  if (!GLOBAL_COMI_DAY) await prefetchGlobalComiLatest(clientRef);
  return GLOBAL_COMI_DAY;
}

// ===== FILLERS =====
function fillDaySeries(keys, mm, {
  isLiveDay = false,
  nowKey = null,
  leadingSeed = null
} = {}) {
  const out = new Array(keys.length);
  let seenFirst = leadingSeed != null;
  let last = leadingSeed;

  for (let i = 0; i < keys.length; i++) {
    const k = keys[i];

    if (isLiveDay && nowKey && k > nowKey) {
      out[i] = null;
      continue;
    }

    const v = mm.get(k);
    if (v != null) {
      last = v;
      seenFirst = true;
      out[i] = v;
    } else {
      out[i] = (seenFirst ? last : null);
    }
  }
  return out;
}

// ===== 1D / 1m =====
async function buildOneDayOneMinute(clientRef, singleTickerRaw) {
  console.log(`📊 Build start: 1D_1min for ${singleTickerRaw}`);
  const tStart = Date.now();
  const tk = (String(singleTickerRaw).includes(":") ? String(singleTickerRaw).split(":")[1] : String(singleTickerRaw)).toUpperCase();

  if (!GLOBAL_COMI_DAY) await prefetchGlobalComiLatest(clientRef);
  if (!GLOBAL_COMI_DAY) {
    console.warn(`⚠️ Could not detect ${REF_TICKER} day; building empty 1D grid.`);
    return { ["1D_1min_Union"]: [["DateTime", tk]] };
  }

  const todayYMD = GLOBAL_COMI_DAY;
  console.log(`🧭 Latest ${REF_TICKER} day (anchor): ${todayYMD}`);

  // Session hours per market.
  //   EGX/ADX: 10:00 → 14:29 Cairo/Dubai
  //   NASDAQ : 09:30 → 15:59 ET
  const SESSION_START = (MARKET_PREFIX === "NASDAQ") ? "09:30:00" : "10:00:00";
  const SESSION_END   = (MARKET_PREFIX === "NASDAQ") ? "15:59:00" : "14:29:00";

  const dayStart = DateTime.fromISO(`${todayYMD}T${SESSION_START}`, { zone: TZ });
  const dayEnd = DateTime.fromISO(`${todayYMD}T${SESSION_END}`, { zone: TZ });

  const allKeys = [];
  for (let t = dayStart; t <= dayEnd; t = t.plus({ minutes: 1 })) {
    allKeys.push(t.toFormat("yyyy-LL-dd HH:mm:ss"));
  }
  const firstKeyOfDay = allKeys[0];

  const nowLocal = DateTime.now().setZone(TZ);
  const isLiveDay = (todayYMD === nowLocal.toISODate());
  // TradingView feed delay by market:
  //   EGX / ADX → 15-min delayed at source, mask the trailing 15 min.
  //   NASDAQ    → real-time feed, no masking.
  const delayMinutes = (MARKET_PREFIX === "NASDAQ") ? 0 : 15;
  const delayedNow = nowLocal.minus({ minutes: delayMinutes });
  const nowKey = (isLiveDay ? delayedNow.toFormat("yyyy-LL-dd HH:mm:ss") : null);

  const toUnix = Math.floor((isLiveDay ? nowLocal : dayEnd).toSeconds());
  const RANGE = 3000;

  console.log(`➡️  Fetching ${tk} 1m range=${RANGE} to=${toUnix}`);
  // Pass the ORIGINAL prefixed ticker to MARKET_SYM so already-prefixed
  // symbols (e.g. "NASDAQ:TSLA") pass through unchanged. Using bare `tk`
  // (after the prefix strip above) would cause MARKET_SYM to re-prefix
  // with MARKET_PREFIX, producing "EGX:TSLA" on US fetches.
  const bars = await fetchBarsOnce(clientRef, MARKET_SYM(singleTickerRaw), {
    timeframe: "1",
    range: RANGE,
    toUnix,
    timeoutMs: TIMEOUT_INTRADAY_MS,
    tag: "1D-1m"
  });

  const rows = (bars || [])
    .map(b => ({
      k: DateTime.fromSeconds(b.time, { zone: TZ }).toFormat("yyyy-LL-dd HH:mm:ss"),
      c: b.close
    }))
    .sort((a, b) => a.k.localeCompare(b.k));

  let leadingSeed = null;
  for (let i = rows.length - 1; i >= 0; i--) {
    if (rows[i].k < firstKeyOfDay) {
      leadingSeed = rows[i].c;
      break;
    }
  }

  const mm = new Map();
  const minKey = allKeys[0];
  const maxKey = allKeys[allKeys.length - 1];
  for (const r of rows) {
    if (r.k >= minKey && r.k <= maxKey) mm.set(r.k, r.c);
  }

  const filled = fillDaySeries(allKeys, mm, { isLiveDay, nowKey, leadingSeed });

  const header = ["DateTime", tk];
  const table = [header, ...allKeys.map((k, i) => [k, filled[i] ?? null])];

  console.log(`🏁 Build done: 1D_1min in ${Date.now() - tStart} ms (rows=${table.length - 1})`);
  return { ["1D_1min_Union"]: table };
}

// ===== 1W / 1M intraday unions =====
async function buildIntradayUnion(clientRef, tickersRaw, { tf, daysBack, label }) {
  const tfStr = String(tf);
  const t0 = Date.now();

  try {
    console.log(`📊 Build start: ${label} for ${tickersRaw.length} tickers (tf=${tfStr})`);

    const SLOT_LIST = slotsForCurrentMarket(tfStr);
    if (!SLOT_LIST) {
      throw new Error(`Unsupported tf=${tfStr} (expected "15" or "60")`);
    }

    // EOD_SLOT is the final slot of the session for the current market.
    //   EGX 15m → "14:15:00", EGX 60m → "14:00:00"   (matches previous hardcoded values)
    //   US 15m  → "15:45:00", US 60m  → "16:00:00"
    const EOD_SLOT = SLOT_LIST[SLOT_LIST.length - 1];
    const keyFor = (ymd, hhmmss) => `${ymd} ${hhmmss}`;

    const lastYMD = await getLatestTradingDateFromCOMI(clientRef);
    if (!lastYMD) {
      console.warn(`⚠️ ${label}: cannot detect ${REF_TICKER} latest day`);
      return { [label]: [["DateTime", ...tickersRaw.map(t => (String(t).includes(":") ? String(t).split(":")[1] : String(t)).toUpperCase())]] };
    }
    console.log(`🧭 ${label} latest ${REF_TICKER} day: ${lastYMD}`);

    const lastDay = DateTime.fromISO(lastYMD, { zone: TZ }).startOf("day");
    const anchorDay = lastDay.minus({ days: daysBack });
    const anchorKey = keyFor(anchorDay.toISODate(), EOD_SLOT);

    // active days from REF_TICKER 1D
    const { range: dRange, toUnix: dTo } = calc1DRangeToCoverMonths(2);
    const comi1d = await fetchBarsOnce(clientRef, MARKET_SYM(REF_TICKER), {
      timeframe: "1D",
      range: dRange,
      toUnix: dTo,
      timeoutMs: TIMEOUT_DAILY_MS,
      tag: `${label}-ref-1d`
    });
    const activeDays = new Set((comi1d || []).map(b => DateTime.fromSeconds(b.time, { zone: TZ }).toISODate()));

    const dayList = [];
    for (let d = anchorDay.plus({ days: 1 }); d <= lastDay; d = d.plus({ days: 1 })) {
      const ymd = d.toISODate();
      if (activeDays.has(ymd)) dayList.push(ymd);
    }
    if (!dayList.length) console.warn(`⚠️ ${label}: no active days between anchor and lastYMD`);

    const perDayBars = tfStr === "15" ? 24 : 5;
    const RANGE = (daysBack + 3) * perDayBars + 128;
    const toUnix = Math.floor(lastDay.endOf("day").toSeconds());

    console.log(`➡️  Fetching tf=${tfStr} range=${RANGE} to=${toUnix}`);

    const perTicker = await mapPool(tickersRaw, TICKER_CONCURRENCY, async (t) => {
      const sym = MARKET_SYM(t);
      const bars = await fetchBarsOnce(clientRef, sym, {
        timeframe: tfStr,
        range: RANGE,
        toUnix,
        timeoutMs: TIMEOUT_INTRADAY_MS,
        tag: label
      });

      const rows = (bars || [])
        .map(b => ({
          k: DateTime.fromSeconds(b.time, { zone: TZ }).toFormat("yyyy-LL-dd HH:mm:ss"),
          Close: b.close
        }))
        .sort((a, b) => a.k.localeCompare(b.k));

      const byKey = new Map(rows.map(r => [r.k, r.Close]));
      const byDay = new Map();
      for (const r of rows) {
        const ymd = r.k.slice(0, 10);
        if (!byDay.has(ymd)) byDay.set(ymd, []);
        byDay.get(ymd).push(r);
      }

      return {
        ticker: (String(t).includes(":") ? String(t).split(":")[1] : String(t)).toUpperCase(),
        rows, byKey, byDay
      };
    });

    const header = ["DateTime", ...perTicker.map(p => p.ticker)];
    const out = [header];

    // anchor row — value = latest <= anchorKey
    const anchorRow = [anchorKey];
    for (const p of perTicker) {
      let seed = null;
      const rows = p.rows;
      for (let i = rows.length - 1; i >= 0; i--) {
        if (rows[i].k <= anchorKey) { seed = rows[i].Close; break; }
      }
      anchorRow.push(seed);
    }
    out.push(anchorRow);

    // live cut
    // live cut
    const nowLocal = DateTime.now().setZone(TZ);
    const isLiveDay = nowLocal.toISODate() === lastYMD;
    // Same per-market split as buildOneDayOneMinute above.
    // EGX/ADX: 15-min delayed → mask trailing 15 min.
    // NASDAQ: real-time → no mask.
    const delayMinutes = (MARKET_PREFIX === "NASDAQ") ? 0 : 15;
    const delayedNow = nowLocal.minus({ minutes: delayMinutes });

    const liveMaxSlot = (() => {
      if (!isLiveDay) return EOD_SLOT;
      if (tfStr === "15") {
        const snap = delayedNow.set({
          minute: Math.floor(delayedNow.minute / 15) * 15,
          second: 0, millisecond: 0
        });
        return snap.toFormat("HH:mm:ss");
      } else {
        const snap = delayedNow.set({ minute: 0, second: 0, millisecond: 0 });
        return snap.toFormat("HH:mm:ss");
      }
    })();

    for (const ymd of dayList) {
      const slots = SLOT_LIST.slice();
      const firstKey = keyFor(ymd, slots[0]);

      const seeds = perTicker.map(p => {
        let seed = null;

        const dayRows = p.byDay.get(ymd) || [];
        for (let i = dayRows.length - 1; i >= 0; i--) {
          if (dayRows[i].k < firstKey) { seed = dayRows[i].Close; break; }
        }

        if (seed == null) {
          const rows = p.rows;
          for (let i = rows.length - 1; i >= 0; i--) {
            if (rows[i].k < firstKey) { seed = rows[i].Close; break; }
          }
        }

        return seed;
      });

      const running = seeds.slice();

      for (const s of slots) {
        const k = keyFor(ymd, s);
        const isFutureOnLive = (isLiveDay && ymd === lastYMD && s > liveMaxSlot);

        const row = [k];
        perTicker.forEach((p, idx) => {
          if (isFutureOnLive) { row.push(null); return; }
          const v = p.byKey.get(k);
          if (v != null) { running[idx] = v; row.push(v); }
          else { row.push(running[idx] ?? null); }
        });

        out.push(row);
      }
    }

    console.log(`🏁 Build done: ${label} in ${Date.now() - t0} ms (rows=${out.length - 1})`);
    return { [label]: out };

  } catch (e) {
    console.warn(`⚠️ ${label} failed: ${e.message || e}`);
    const hdr = ["DateTime", ...tickersRaw.map(t => (String(t).includes(":") ? String(t).split(":")[1] : String(t)).toUpperCase())];
    return { [label]: [hdr] };
  }
}

// ===== DAILY TABLES =====
function buildForwardFilledSeries(allKeys, keyToCloseMap, { fillLeading = FILL_LEADING_DEFAULT, leadingSeed = null } = {}) {
  const out = new Array(allKeys.length);
  let last = leadingSeed ?? null;
  let seenFirst = leadingSeed != null;

  if (leadingSeed == null && fillLeading) {
    for (let i = 0; i < allKeys.length; i++) {
      const v = keyToCloseMap.get(allKeys[i]);
      if (v != null) { last = v; seenFirst = true; break; }
    }
  }

  for (let i = 0; i < allKeys.length; i++) {
    const k = allKeys[i];
    const v = keyToCloseMap.get(k);
    if (v != null) { last = v; seenFirst = true; out[i] = v; }
    else { out[i] = (seenFirst && last != null) ? last : null; }
  }

  return out;
}

function chooseWeeklyAnchorDatesFromMap_old(map, yearsBack, TZ_IN) {
  if (!map || map.size === 0) return [];
  const dates = [...map.keys()].sort();
  const lastYMD = dates.at(-1);

  const _fromYMD = (ymd) => {
    const [y, m, d] = ymd.split("-").map(Number);
    return new Date(Date.UTC(y, m - 1, d, 0, 0, 0));
  };

  const ymdCairoJS = (date) => {
    const parts = new Intl.DateTimeFormat("en-CA", { timeZone: TZ_IN, year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(date);
    const m = Object.fromEntries(parts.map(p => [p.type, p.value]));
    return `${m.year}-${m.month}-${m.day}`;
  };

  const lastDate = _fromYMD(lastYMD);
  const cutoffYMD = ymdCairoJS(new Date(lastDate.setUTCFullYear(lastDate.getUTCFullYear() - yearsBack)));

  const has = (d) => map.has(d);

  const weekStart = (ymd) => {
    const d = _fromYMD(ymd);
    const weekday = new Date(d.toLocaleString("en-US", { timeZone: TZ_IN })).getDay();
    const start = new Date(d);
    start.setUTCDate(start.getUTCDate() - weekday);
    return start;
  };

  const nextWeek = (d) => { const n = new Date(d); n.setUTCDate(n.getUTCDate() + 7); return n; };

  let cur = weekStart(cutoffYMD);
  while (ymdCairoJS(cur) < cutoffYMD) cur = nextWeek(cur);

  const lastWeek = weekStart(lastYMD);
  const anchors = [];
  let first = true;

  while (cur <= lastWeek) {
    const sun = ymdCairoJS(cur);
    let pick = null;

    if (has(sun)) {
      pick = sun;
    } else if (first) {
      const thu = ymdCairoJS(new Date(cur.getTime() - 3 * 86400000));
      if (thu >= cutoffYMD && has(thu)) {
        pick = thu;
      } else {
        pick = [1, 2, 3, 4].map(o => ymdCairoJS(new Date(cur.getTime() + o * 86400000))).find(has);
      }
    } else {
      const thu = ymdCairoJS(new Date(cur.getTime() - 3 * 86400000));
      pick = has(thu) ? thu : [1, 2, 3, 4].map(o => ymdCairoJS(new Date(cur.getTime() + o * 86400000))).find(has);
    }

    if (pick) anchors.push(pick);
    first = false;
    cur = nextWeek(cur);
  }

  if (anchors.at(-1) !== lastYMD) anchors.push(lastYMD);
  return [...new Set(anchors)].sort();
}

async function buildDailyTables(clientRef, tickers, years, label) {
  console.log(`📊 Build start: ${label} for ${tickers.length} tickers`);
  const tStart = Date.now();

  const { range, toUnix } = calc1DRangeToCoverYears(years);

  console.log(`➡️  Fetching DAILY 1D for ${label} (years=${years}, range=${range}, to=${toUnix})`);
  const perTicker = await mapPool(tickers, TICKER_CONCURRENCY, async (t) => {
    const sym = MARKET_SYM(t);
    const bars = await fetchBarsOnce(clientRef, sym, {
      timeframe: "1D",
      range,
      toUnix,
      timeoutMs: TIMEOUT_DAILY_MS,
      tag: label
    });
    const map = barsToDailyMapCairo(bars);
    const tk = (String(t).includes(":") ? String(t).split(":")[1] : String(t)).toUpperCase();
    return { ticker: tk, map };
  });

  let refMap;
  {
    const hit = perTicker.find(x => x.ticker === String(REF_TICKER).toUpperCase());
    if (hit && hit.map && hit.map.size) {
      refMap = hit.map;
      console.log(`🧭 Using ${REF_TICKER} map from selected list`);
    } else {
      console.log(`🧭 Fetching ${REF_TICKER} reference separately`);
      const bars = await fetchBarsOnce(clientRef, MARKET_SYM(REF_TICKER), {
        timeframe: "1D",
        range,
        toUnix,
        timeoutMs: TIMEOUT_DAILY_MS,
        tag: `${label}-ref`
      });
      refMap = barsToDailyMapCairo(bars);
    }
  }

  function lastKeyOf(map) {
    if (!map || map.size === 0) return null;
    const ks = [...map.keys()].sort();
    return ks[ks.length - 1];
  }

  const lastDates = [
    ...perTicker.map(e => lastKeyOf(e.map)),
    lastKeyOf(refMap),
  ].filter(Boolean);

  if (!lastDates.length) {
    console.warn(`⚠️ No dates found in ${label}`);
    return {
      [`${label}_Daily_Union`]: [["Date"]],
      [`${label}_Weekly_Downsampled`]: [["Date"]],
    };
  }

  const globalLastYMD = lastDates.reduce((max, d) => (d > max ? d : max), lastDates[0]);
  const globalLast = DateTime.fromISO(globalLastYMD, { zone: TZ });
  const cutoffYMD = globalLast.minus({ years }).toISODate();
  console.log(`🪄 ${label} globalLast=${globalLastYMD} cutoff=${cutoffYMD}`);

  const prepared = perTicker.map(({ ticker, map }) => {
    const allKeys = [...map.keys()].sort();
    const firstEver = allKeys.length ? allKeys[0] : null;
    const isIPO = firstEver && firstEver > cutoffYMD;

    const mm = new Map();
    for (const [d, v] of map.entries()) if (d >= cutoffYMD) mm.set(d, v);

    let leadingSeed = null;
    if (!isIPO) {
      for (let i = allKeys.length - 1; i >= 0; i--) {
        if (allKeys[i] < cutoffYMD) {
          leadingSeed = map.get(allKeys[i]);
          break;
        }
      }
    }

    return { ticker, mm, leadingSeed, firstEver, isIPO };
  });

  const refTrim = [...refMap.keys()].filter(d => d >= cutoffYMD);
  const allDates = [...new Set([
    ...prepared.flatMap(p => [...p.mm.keys()]),
    ...refTrim,
  ])].sort();

  if (!allDates.length) {
    console.warn(`⚠️ ${label} produced empty union date set`);
    return {
      [`${label}_Daily_Union`]: [["Date", ...prepared.map(p => p.ticker)]],
      [`${label}_Weekly_Downsampled`]: [["Date", ...prepared.map(p => p.ticker)]],
    };
  }

  const header = ["Date", ...prepared.map(p => p.ticker)];
  const daily = [header];

  const ffs = prepared.map(p =>
    buildForwardFilledSeries(allDates, p.mm, {
      fillLeading: (!p.isIPO && (p.leadingSeed != null || FILL_LEADING_DEFAULT)),
      leadingSeed: (!p.isIPO ? (p.leadingSeed ?? null) : null)
    })
  );

  for (let i = 0; i < allDates.length; i++) {
    const row = [allDates[i]];
    for (const s of ffs) row.push(s[i] ?? null);
    daily.push(row);
  }

  const anchorMap = new Map(allDates.map(d => [d, true]));
  const keep = chooseWeeklyAnchorDatesFromMap_old(anchorMap, years, TZ);
  const weeklySet = new Set(keep);

  for (const r of prepared) {
    if (r.firstEver && r.firstEver > cutoffYMD) {
      weeklySet.add(r.firstEver);
      const idx = allDates.indexOf(r.firstEver);
      if (idx !== -1 && idx + 1 < allDates.length) weeklySet.add(allDates[idx + 1]);
    }
  }

  const weekly = [header];
  for (let i = 1; i < daily.length; i++) {
    const d = daily[i][0];
    if (weeklySet.has(d)) weekly.push(daily[i]);
  }

  console.log(`🏁 Build done: ${label} in ${Date.now() - tStart} ms (rows daily=${daily.length - 1}, weekly=${weekly.length - 1})`);
  return {
    [`${label}_Daily_Union`]: daily,
    [`${label}_Weekly_Downsampled`]: weekly,
  };
}

async function buildSixMonthDaily(clientRef, tickers, label) {
  console.log(`📊 Build start: ${label} for ${tickers.length} tickers`);
  const tStart = Date.now();

  const { range, toUnix } = calc1DRangeToCoverMonths(6);

  console.log(`➡️  Fetching DAILY 1D for ${label} (months=6, range=${range}, to=${toUnix})`);
  const perTicker = await mapPool(tickers, TICKER_CONCURRENCY, async (t) => {
    const sym = MARKET_SYM(t);
    const bars = await fetchBarsOnce(clientRef, sym, {
      timeframe: "1D",
      range,
      toUnix,
      timeoutMs: TIMEOUT_DAILY_MS,
      tag: label
    });
    const map = barsToDailyMapCairo(bars);
    const tk = (String(t).includes(":") ? String(t).split(":")[1] : String(t)).toUpperCase();
    return { ticker: tk, map };
  });

  let refMap;
  {
    const hit = perTicker.find(x => x.ticker === String(REF_TICKER).toUpperCase());
    if (hit && hit.map && hit.map.size) {
      refMap = hit.map;
      console.log(`🧭 Using ${REF_TICKER} map from selected list`);
    } else {
      console.log(`🧭 Fetching ${REF_TICKER} reference separately`);
      const bars = await fetchBarsOnce(clientRef, MARKET_SYM(REF_TICKER), {
        timeframe: "1D",
        range,
        toUnix,
        timeoutMs: TIMEOUT_DAILY_MS,
        tag: `${label}-ref`
      });
      refMap = barsToDailyMapCairo(bars);
    }
  }

  function lastKeyOf(map) {
    if (!map || map.size === 0) return null;
    const ks = [...map.keys()].sort();
    return ks[ks.length - 1];
  }

  const lastDates = [
    ...perTicker.map(e => lastKeyOf(e.map)),
    lastKeyOf(refMap)
  ].filter(Boolean);

  if (!lastDates.length) {
    console.warn(`⚠️ No dates found in ${label}`);
    return { [`${label}_Daily_Union`]: [["Date"]] };
  }

  const globalLastYMD = lastDates.reduce((max, d) => (d > max ? d : max), lastDates[0]);
  const globalLast = DateTime.fromISO(globalLastYMD, { zone: TZ });
  const cutoffYMD = globalLast.minus({ months: 6 }).toISODate();
  console.log(`🪄 ${label} globalLast=${globalLastYMD} cutoff=${cutoffYMD}`);

  const prepared = perTicker.map(({ ticker, map }) => {
    const allKeys = [...map.keys()].sort();
    const firstEver = allKeys.length ? allKeys[0] : null;
    const isIPO = firstEver && firstEver > cutoffYMD;

    const mm = new Map();
    for (const [d, v] of map.entries()) if (d >= cutoffYMD) mm.set(d, v);

    let leadingSeed = null;
    if (!isIPO) {
      for (let i = allKeys.length - 1; i >= 0; i--) {
        if (allKeys[i] < cutoffYMD) {
          leadingSeed = map.get(allKeys[i]);
          break;
        }
      }
    }

    return { ticker, mm, leadingSeed, firstEver, isIPO };
  });

  const refTrim = [...refMap.keys()].filter(d => d >= cutoffYMD);
  const allDates = [...new Set([
    ...prepared.flatMap(p => [...p.mm.keys()]),
    ...refTrim
  ])].sort();

  if (!allDates.length) {
    console.warn(`⚠️ ${label} produced empty union date set`);
    return { [`${label}_Daily_Union`]: [["Date", ...prepared.map(p => p.ticker)]] };
  }

  const header = ["Date", ...prepared.map(p => p.ticker)];
  const daily = [header];

  const ffs = prepared.map(p =>
    buildForwardFilledSeries(allDates, p.mm, {
      fillLeading: (!p.isIPO && (p.leadingSeed != null || FILL_LEADING_DEFAULT)),
      leadingSeed: (!p.isIPO ? (p.leadingSeed ?? null) : null)
    })
  );

  for (let i = 0; i < allDates.length; i++) {
    const row = [allDates[i]];
    for (const s of ffs) row.push(s[i] ?? null);
    daily.push(row);
  }

  console.log(`🏁 Build done: ${label} in ${Date.now() - tStart} ms (rows daily=${daily.length - 1})`);
  return { [`${label}_Daily_Union`]: daily };
}

// ===== JSON EXPORT =====
function buildJsonFromCombined(combined, tickers, { includeBenchmark, benchmarkTicker }) {
  const jsonPerTicker = tickers.map(tk => {
    const base = {
      one_week: [],
      one_month: [],
      six_months: [],
      one_year: [],
      five_years: [],
      five_years_daily: []
    };
    if (combined["1D_1min_Union"]) base.one_day = [];
    return { ticker: tk, data: base };
  });

  function pushSeries(tableName, jsonKey, includeNullsAsZero = false) {
    const table = combined[tableName];
    if (!table) {
      console.warn(`⏭️  Missing table ${tableName} for JSON key ${jsonKey}`);
      return;
    }
    const header = table[0];

    for (const entry of jsonPerTicker) {
      const col = header.indexOf(entry.ticker);
      if (col === -1) continue;

      for (let i = 1; i < table.length; i++) {
        const row = table[i];
        const ts = row[0];
        const val = row[col];

        if (ts == null) continue;

        if (val != null) {
          entry.data[jsonKey].push({ Date: ts, Close: val });
        } else if (includeNullsAsZero) {
          entry.data[jsonKey].push({ Date: ts, Close: 0 });
        }
      }
    }
  }

  console.log(`🧩 Building JSON export`);
  if (combined["1D_1min_Union"]) pushSeries("1D_1min_Union", "one_day", true);
  pushSeries("1W_15min_Union", "one_week", true);
  pushSeries("1M_Hourly_Union", "one_month", true);
  pushSeries("6M_Daily_Union", "six_months", false);
  pushSeries("1Y_Weekly_Downsampled", "one_year", false);
  pushSeries("5Y_Weekly_Downsampled", "five_years", false);

  if (combined["5Y_Daily_Union"]) pushSeries("5Y_Daily_Union", "five_years_daily", false);

  // ✅ Append benchmark 5Y daily into five_years_daily for benchmark ticker only
  if (includeBenchmark && benchmarkTicker && combined["5Y_BENCH_ONLY_Daily_Union"]) {
    pushSeries("5Y_BENCH_ONLY_Daily_Union", "five_years_daily", false);
  }

  return jsonPerTicker;
}

// ===== REQUEST MUTEX =====
let requestLock = Promise.resolve();
function withRequestLock(fn) {
  const start = requestLock;
  let release;
  requestLock = new Promise(r => (release = r));
  return start.then(async () => {
    try { return await fn(); }
    finally { release(); }
  });
}

function parseYMD(dateStr) {
  const dt = DateTime.fromISO(String(dateStr), { zone: TZ });
  if (!dt.isValid) return null;
  return dt.toISODate();
}

function parseYMDMany(datesInput) {
  const arr = Array.isArray(datesInput) ? datesInput : [datesInput];
  const out = [];
  for (const d of arr) {
    const ymd = parseYMD(d);
    if (!ymd) return null;
    out.push(ymd);
  }
  return [...new Set(out)].sort();
}

function daysBetweenInclusive(ymdMin, ymdMax) {
  const a = DateTime.fromISO(ymdMin, { zone: TZ }).startOf("day");
  const b = DateTime.fromISO(ymdMax, { zone: TZ }).startOf("day");
  const diff = Math.round(b.diff(a, "days").days);
  return Math.max(0, diff) + 1;
}

async function getClosesForDates(clientRef, tickerRaw, ymdList, { fallbackToPrevTradingDay = true } = {}) {
  const sym = MARKET_SYM(tickerRaw);

  const minYMD = ymdList[0];
  const maxYMD = ymdList[ymdList.length - 1];

  const maxEnd = DateTime.fromISO(maxYMD, { zone: TZ }).endOf("day");
  const nowUnix = Math.floor(localNow().toSeconds());
  const toUnix = Math.min(Math.floor(maxEnd.toSeconds()), nowUnix);

  const spanDays = daysBetweenInclusive(minYMD, maxYMD);
  const RANGE = spanDays + 40;

  const bars = await fetchBarsOnce(clientRef, sym, {
    timeframe: "1D",
    range: RANGE,
    toUnix,
    timeoutMs: TIMEOUT_DAILY_MS,
    tag: `close@batch:${minYMD}->${maxYMD}`
  });

  if (!bars || !bars.length) {
    const out = {};
    for (const ymd of ymdList) out[ymd] = { close: null };
    return out;
  }

  const map = barsToDailyMapCairo(bars);
  const available = [...map.keys()].sort();

  const resolveOne = (ymd) => {
    if (map.has(ymd)) return { close: map.get(ymd), resolvedDate: ymd };
    if (!fallbackToPrevTradingDay) return { close: null, resolvedDate: null };

    for (let i = available.length - 1; i >= 0; i--) {
      if (available[i] < ymd) {
        const pick = available[i];
        return { close: map.get(pick), resolvedDate: pick };
      }
    }
    return { close: null, resolvedDate: null };
  };

  const out = {};
  for (const ymd of ymdList) {
    const r = resolveOne(ymd);
    out[ymd] = { close: r.close };
  }
  return out;
}

// ===== /close =====
app.post("/close", async (req, res) => {
  return withRequestLock(async () => {
    const { market, ticker, tickers, date, dates, fallback } = req.body || {};

    const mkt = normalizeMarket(market);
    applyMarketContext(mkt);

    const tickersIn =
      Array.isArray(tickers) && tickers.length
        ? tickers
        : (ticker ? [ticker] : []);

    const datesIn =
      (Array.isArray(dates) && dates.length)
        ? dates
        : (date ? [date] : []);

    if (!tickersIn.length || !datesIn.length) {
      return res.status(400).json({
        error: "missing_params",
        detail:
          'Required JSON body: { "tickers": ["COMI","SWDY"], "dates": ["2025-08-10","2025-08-11"] } (or use "ticker"/"date")'
      });
    }

    const ymdList = parseYMDMany(datesIn);
    if (!ymdList) {
      return res.status(400).json({
        error: "bad_date",
        detail: 'All dates must be ISO yyyy-mm-dd (e.g., "2025-08-10")'
      });
    }

    const fallbackToPrevTradingDay =
      String(fallback ?? "true").toLowerCase() !== "false";

    const clientRef = { tv: createTvClient() };

    try {
      const CLOSE_CONCURRENCY = Number(process.env.CLOSE_CONCURRENCY || 6);

      const resultsArr = await mapPool(tickersIn, CLOSE_CONCURRENCY, async (t) => {
        const normalizedTicker = (String(t).includes(":")
          ? String(t).split(":")[1]
          : String(t)
        ).toUpperCase();

        const perDate = await getClosesForDates(
          clientRef,
          String(t),
          ymdList,
          { fallbackToPrevTradingDay }
        );

        return { ticker: normalizedTicker, perDate };
      });

      const results = {};
      for (const r of resultsArr) {
        if (!r || !r.ticker) continue;
        results[r.ticker] = r.perDate || {};
      }

      return res.json({
        market: mkt,
        fallback: fallbackToPrevTradingDay,
        results
      });

    } catch (err) {
      console.error("❌ /close failed:", err);
      return res.status(500).json({
        error: "internal_error",
        detail: String(err?.message || err)
      });

    } finally {
      destroyTvClient(clientRef.tv);
    }
  });
});

// ===== /live-tick =====
// Latest 1-minute bar close per ticker via TV WebSocket. Used by
// rag.py as the fallback for tickers TV's screener has dropped from
// its index but that still have chart data on the WebSocket (e.g.
// EGX:MMAT, EGX:SAIB). Reuses fetchBarsOnce so it inherits retry /
// timeout / client-recreation behavior.
app.post("/live-tick", async (req, res) => {
  return withRequestLock(async () => {
    const { market, tickers } = req.body || {};
    const mkt = normalizeMarket(market);
    applyMarketContext(mkt);

    const tickersIn = Array.isArray(tickers) ? tickers : [];
    if (!tickersIn.length) {
      return res.status(400).json({
        error: "missing_params",
        detail:
          'Required JSON body: { "tickers": ["MMAT","SAIB"], "market": "egypt" }',
      });
    }

    const clientRef = { tv: createTvClient() };

    try {
      const LIVE_CONCURRENCY = Number(process.env.LIVE_CONCURRENCY || 6);
      const nowUnix = Math.floor(localNow().toSeconds());

      const resultsArr = await mapPool(tickersIn, LIVE_CONCURRENCY, async (t) => {
        const normalizedTicker = (String(t).includes(":")
          ? String(t).split(":")[1]
          : String(t)
        ).toUpperCase();

        const sym = MARKET_SYM(String(t));
        const bars = await fetchBarsOnce(clientRef, sym, {
          timeframe: "1",
          range: 5,
          toUnix: nowUnix,
          timeoutMs: 10000,
          tag: `live-tick:${normalizedTicker}`,
        });

        if (!bars || !bars.length) {
          return { ticker: normalizedTicker, close: null };
        }
        const lastBar = bars[bars.length - 1];
        return {
          ticker: normalizedTicker,
          close: lastBar.close,
          time: lastBar.time,
        };
      });

      const results = {};
      for (const r of resultsArr) {
        if (!r || !r.ticker) continue;
        results[r.ticker] = { close: r.close, time: r.time };
      }

      return res.json({ market: mkt, results });
    } catch (err) {
      console.error("❌ /live-tick failed:", err);
      return res.status(500).json({
        error: "internal_error",
        detail: String(err?.message || err),
      });
    } finally {
      destroyTvClient(clientRef.tv);
    }
  });
});

// ===== MAIN ROUTE =====
app.post("/fetch", async (req, res) => {
  console.log("SESSION:", process.env.SESSION?.slice(0, 6), "...", process.env.SESSION?.length);
  console.log("SIGNATURE:", process.env.SIGNATURE?.slice(0, 6), "...", process.env.SIGNATURE?.length);
  console.log("PID:", process.pid);

  return withRequestLock(async () => {
    // ── Hybrid client pool ──
    // For ≤40 tickers: 1 shared client (current behavior — empirically
    //   faster setup at small scale, no cliff risk).
    // For >40 tickers: 1 client per timeframe job. TV's session limit
    //   is cumulative per Client (~200 sessions before
    //   'max sessions rate reached'). At 60 tickers × 5 timeframes =
    //   300 sessions, a single shared client trips that limit and we
    //   see ~50-60% subscription failures + cascading timeouts.
    // Empirical results (see _test_node/test_hybrid.js):
    //   40 tickers, 1 client : 4.7s, 100% success
    //   45 tickers, 1 client : 29.1s, 60% FAIL
    //   60 tickers, 1 client : 26.9s, 57% FAIL
    //   60 tickers, 5 clients: 5.0s, 100% success
    // The pool is created lazily after we know the job count.
    const POOL_THRESHOLD = 40;
    const clientPool = [{ tv: createTvClient() }];
    const clientRef = clientPool[0];

    try {
      console.log("Before building:", (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2), "MB");
      console.log("RSS Before building:", (process.memoryUsage().rss / 1024 / 1024).toFixed(2), "MB");

      const startAll = Date.now();
      logEnvCheck();

      const { tickersRaw, market } = req.body || {};
      const mkt = normalizeMarket(market);
      applyMarketContext(mkt);

      if (!Array.isArray(tickersRaw) || tickersRaw.length === 0) {
        return res.status(400).json({ error: "tickersRaw is required (non-empty array)" });
      }

      // ✅ Market-aware benchmark handling
      const benchmark = BENCHMARK_BY_MARKET[mkt] || null;

      let includeBenchmark = false;
      if (tickersRaw.length > 1 && benchmark) {
        includeBenchmark = true;

        // Compare against the BARE benchmark base so pre-prefixed
        // benchmarks like "SP:SPX" don't false-negative against
        // already-stripped ticker bases (e.g. "SPX").
        const benchUpperFull = String(benchmark).toUpperCase();
        const benchBase = benchUpperFull.includes(":")
          ? benchUpperFull.split(":")[1]
          : benchUpperFull;

        const hasBenchmarkAlready = tickersRaw.some(t => {
          const raw = String(t).toUpperCase();
          const base = raw.includes(":") ? raw.split(":")[1] : raw;
          return base === benchBase;
        });

        if (!hasBenchmarkAlready) tickersRaw.push(benchmark);

        console.log(`📈 Auto-included ${benchmark} for benchmarking (5Y daily only)`);
      } else {
        console.log("✅ Single ticker run — NOT adding benchmark");
      }

      console.log(`🎯 Market=${mkt} | Tickers: ${tickersRaw.join(", ")}`);

      // Prefetch REF_TICKER once (COMI for EGX, FAB for ADX)
      await prefetchGlobalComiLatest(clientRef);

      // Uppercase tickers (strip prefix)
      const tickers = tickersRaw.map(t => (String(t).includes(":") ? String(t).split(":")[1] : String(t)).toUpperCase());

      // Build jobs
      let jobs;

      if (includeBenchmark && benchmark) {
        // Same bare-base comparison as above, so "SP:SPX" filters out
        // ticker entries whose base is "SPX". For EGX where benchmark
        // is "EGX30" (no colon), benchBaseLocal === benchUpperLocal,
        // so behavior is identical to the previous hardcoded form.
        const benchUpperLocal = String(benchmark).toUpperCase();
        const benchBaseLocal = benchUpperLocal.includes(":")
          ? benchUpperLocal.split(":")[1]
          : benchUpperLocal;

        // mainTickers excludes benchmark (benchmark handled by a dedicated 5Y daily-only job)
        const mainTickers = tickersRaw.filter(t => {
          const raw = String(t).toUpperCase();
          const base = raw.includes(":") ? raw.split(":")[1] : raw;
          return base !== benchBaseLocal;
        });

        jobs = [
          { key: "5Y", run: async (cli) => await buildDailyTables(cli, mainTickers, 5, "5Y") },
          { key: "1Y", run: async (cli) => await buildDailyTables(cli, mainTickers, 1, "1Y") },
          { key: "6M", run: async (cli) => await buildSixMonthDaily(cli, mainTickers, "6M") },
          { key: "1M", run: async (cli) => await buildIntradayUnion(cli, mainTickers, { tf: "60", daysBack: 28, label: "1M_Hourly_Union" }) },
          { key: "1W", run: async (cli) => await buildIntradayUnion(cli, mainTickers, { tf: "15", daysBack: 7, label: "1W_15min_Union" }) },

          // ✅ Benchmark 5Y daily only
          { key: "5Y_BENCH_ONLY", run: async (cli) => await buildDailyTables(cli, [benchmark], 5, "5Y_BENCH_ONLY") },
        ];
      } else {
        jobs = [
          { key: "5Y", run: async (cli) => await buildDailyTables(cli, tickersRaw, 5, "5Y") },
          { key: "1Y", run: async (cli) => await buildDailyTables(cli, tickersRaw, 1, "1Y") },
          { key: "6M", run: async (cli) => await buildSixMonthDaily(cli, tickersRaw, "6M") },
          { key: "1M", run: async (cli) => await buildIntradayUnion(cli, tickersRaw, { tf: "60", daysBack: 28, label: "1M_Hourly_Union" }) },
          { key: "1W", run: async (cli) => await buildIntradayUnion(cli, tickersRaw, { tf: "15", daysBack: 7, label: "1W_15min_Union" }) },
        ];
      }

      // If exactly one ticker → add 1D/1m job
      if (tickersRaw.length === 1) {
        jobs.push({ key: "1D", run: async (cli) => await buildOneDayOneMinute(cli, tickersRaw[0]) });
      }

      // ── Expand client pool when ticker count exceeds threshold ──
      // Creates one additional client per remaining job, with a 100ms
      // throttle between creations to avoid TV's HTTP 429 rate limit
      // on WebSocket-upgrade handshakes. Without the throttle, ~half
      // the new clients fail to connect with 429.
      if (tickersRaw.length > POOL_THRESHOLD) {
        for (let i = 1; i < jobs.length; i++) {
          clientPool.push({ tv: createTvClient() });
          if (i < jobs.length - 1) await sleep(100);
        }
        console.log(`🏊 Pool expanded to ${clientPool.length} clients for ${tickersRaw.length} tickers`);
      } else {
        console.log(`🏊 Single shared client for ${tickersRaw.length} tickers`);
      }

      console.log(`🚀 Starting all windows with TF_CONCURRENCY=${TF_CONCURRENCY}`);
      const allTables = await mapPool(jobs, TF_CONCURRENCY, async (j, i) => {
        // Each job gets its own client when the pool is expanded;
        // otherwise all jobs share clientPool[0] (the legacy path).
        const cli = clientPool[i] || clientPool[0];
        const s = Date.now();
        console.log(`➡️  Window start: ${j.key}`);
        const out = await j.run(cli);
        console.log(`✅ Window done:  ${j.key} in ${Date.now() - s} ms`);
        return out;
      });

      const combined = Object.assign({}, ...allTables);

      const jsonPerTicker = buildJsonFromCombined(combined, tickers, {
        includeBenchmark,
        benchmarkTicker: benchmark ? String(benchmark).toUpperCase() : null
      });

      console.log(`✅ ALL DONE in ${((Date.now() - startAll) / 1000).toFixed(2)} s`);
      const used = process.memoryUsage();
      console.log({
        rss: (used.rss / 1024 / 1024).toFixed(2) + " MB",
        heapTotal: (used.heapTotal / 1024 / 1024).toFixed(2) + " MB",
        heapUsed: (used.heapUsed / 1024 / 1024).toFixed(2) + " MB",
        external: (used.external / 1024 / 1024).toFixed(2) + " MB",
        arrayBuffers: (used.arrayBuffers / 1024 / 1024).toFixed(2) + " MB"
      });

      return res.json(jsonPerTicker);

    } catch (err) {
      console.error("❌ /fetch crashed:", err);
      return res.status(500).json({ error: "internal_error", detail: String(err?.message || err) });

    } finally {
      // Destroy every client in the pool. When ≤40 tickers,
      // clientPool has only 1 entry (same as legacy behavior).
      for (const c of clientPool) {
        try { destroyTvClient(c.tv); } catch {}
      }
    }
  });
});

function ticksToMs(ticks) {
  // 1 tick = 100ns → 10,000 ticks = 1 ms
  return Math.round(Number(ticks) / 10000);
}

app.post("/tts", async (req, res) => {
  try {
    const text = String(req.body.text || "").trim();
    if (!text) {
      return res.status(400).json({ ok: false, error: "text is required" });
    }

    const voice = String(req.body.voice || "en-US-SaraNeural");
    const format = String(req.body.format || "wav"); // wav = best sync

    const key = process.env.AZURE_SPEECH_KEY;
    const region = process.env.AZURE_SPEECH_REGION;

    if (!key || !region) {
      return res.status(500).json({
        ok: false,
        error: "AZURE_SPEECH_KEY or AZURE_SPEECH_REGION missing",
      });
    }

    const speechConfig = speechsdk.SpeechConfig.fromSubscription(key, region);
    speechConfig.speechSynthesisVoiceName = voice;

    if (format === "wav") {
      speechConfig.speechSynthesisOutputFormat =
        speechsdk.SpeechSynthesisOutputFormat.Riff24Khz16BitMonoPcm;
    } else {
      speechConfig.speechSynthesisOutputFormat =
        speechsdk.SpeechSynthesisOutputFormat.Audio24Khz48KBitRateMonoMp3;
    }

    const synthesizer = new speechsdk.SpeechSynthesizer(speechConfig);

    const speechMarks = [];
    synthesizer.wordBoundary = (_, e) => {
      const start = typeof e.textOffset === "number" ? e.textOffset : -1;
      const len = typeof e.wordLength === "number" ? e.wordLength : 0;
      const end = start >= 0 ? start + len : -1;

      const value =
        typeof e.text === "string" && e.text.length
          ? e.text
          : start >= 0 && end > start
            ? text.substring(start, end)
            : "";

      speechMarks.push({
        time: ticksToMs(e.audioOffset),
        type: "word",
        start,
        end,
        value,
      });
    };

    synthesizer.speakTextAsync(
      text,
      (result) => {
        synthesizer.close();

        if (result.reason !== speechsdk.ResultReason.SynthesizingAudioCompleted) {
          return res.status(500).json({
            ok: false,
            error: "Synthesis failed",
            details: result.errorDetails || String(result.reason),
          });
        }

        const audioBuffer = Buffer.from(result.audioData);

        return res.json({
          ok: true,
          voice,
          format,
          audioBase64: audioBuffer.toString("base64"),
          speechMarks,
        });
      },
      (err) => {
        synthesizer.close();
        res.status(500).json({ ok: false, error: String(err) });
      }
    );
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// ===== START SERVER =====
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`✅ Server running on http://localhost:${PORT}`));
