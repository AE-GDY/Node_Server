// ===============================
// EGX/ADX Stock Data Server (FULL FILE)
// - Adds optional `market` param to /fetch (default "EGX")
// - If market === "ADX": uses "ADX:" prefix instead of "EGX:"
// - If market === "ADX": uses "FAB" as reference ticker instead of "COMI"
// - (Also switches TZ to Asia/Dubai for ADX; EGX stays Africa/Cairo)
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
let MARKET_PREFIX = "EGX"; // ✅ default, overridden per-request in /fetch and /close
const MARKET_TZ_MAP = {
  EGX: "Africa/Cairo",
  ADX: "Asia/Dubai",
};

// Uses current MARKET_PREFIX unless symbol already has a prefix
const MARKET_SYM = (t) => (String(t).includes(":") ? String(t) : `${MARKET_PREFIX}:${t}`);

const TICKER_CONCURRENCY = Number(process.env.TICKER_CONCURRENCY || 20);
const TF_CONCURRENCY     = Number(process.env.TF_CONCURRENCY     || 6);

const TIMEOUT_DAILY_MS     = Number(process.env.TIMEOUT_DAILY_MS || 12000);
const TIMEOUT_INTRADAY_MS  = Number(process.env.TIMEOUT_INTRADAY_MS || 12000);

const MAX_RETRIES        = Number(process.env.MAX_RETRIES        || 3);
const INITIAL_BACKOFF_MS = Number(process.env.INITIAL_BACKOFF_MS || 500);

const FILL_LEADING_DEFAULT = /^true$/i.test(process.env.FILL_LEADING || "true");

// ✅ timezone is set per request via MARKET_PREFIX
let TZ = "Africa/Cairo";

// ✅ reference ticker set per request (EGX->COMI, ADX->FAB)
let REF_TICKER = "COMI";

// EGX local slots (kept as-is)
const DAY_15M_SLOTS = [
  "10:00:00","10:15:00","10:30:00","10:45:00","11:00:00","11:15:00","11:30:00","11:45:00",
  "12:00:00","12:15:00","12:30:00","12:45:00","13:00:00","13:15:00","13:30:00","13:45:00",
  "14:00:00","14:15:00"
];
const DAY_60M_SLOTS = ["10:00:00","11:00:00","12:00:00","13:00:00","14:00:00"];

// ===== GLOBALS =====
// (kept names to preserve logic; now they track the current request’s REF_TICKER)
let GLOBAL_COMI_DAY = null;  // "yyyy-MM-dd" of latest REF_TICKER 1D
let GLOBAL_COMI_TS  = null;  // epoch seconds of latest REF_TICKER bar

// ===== UTILS =====
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));
function jitter(min=200,max=700){return min+Math.floor(Math.random()*(max-min+1));}
function localNow() { return DateTime.now().setZone(TZ); }

function logEnvCheck() {
  const hasSession = !!process.env.SESSION && process.env.SESSION.trim() !== "";
  const hasSig     = !!process.env.SIGNATURE && process.env.SIGNATURE.trim() !== "";
  console.log(
    `🔧 ENV: SESSION=${hasSession} SIGNATURE=${hasSig} TF_CONCURRENCY=${TF_CONCURRENCY} ` +
    `TICKER_CONCURRENCY=${TICKER_CONCURRENCY} TIMEOUT_DAILY_MS=${TIMEOUT_DAILY_MS} TIMEOUT_INTRADAY_MS=${TIMEOUT_INTRADAY_MS} ` +
    `MAX_RETRIES=${MAX_RETRIES}`
  );
}

function normalizeMarket(m) {
  const v = String(m || "EGX").trim().toUpperCase();
  return v === "ADX" ? "ADX" : "EGX";
}

function applyMarketContext(mkt) {
  MARKET_PREFIX = mkt; // "EGX" or "ADX"
  TZ = MARKET_TZ_MAP[mkt] || "Africa/Cairo";
  REF_TICKER = (mkt === "ADX") ? "FAB" : "COMI";

  // Reset per-request global anchors (since they depend on REF + TZ)
  GLOBAL_COMI_DAY = null;
  GLOBAL_COMI_TS  = null;

  console.log(`🏛️ Market context: MARKET_PREFIX=${MARKET_PREFIX} REF_TICKER=${REF_TICKER} TZ=${TZ}`);
}

async function mapPool(items, limit, worker){
  const results = new Array(items.length);
  let idx=0, active=0;
  return new Promise((resolve)=>{
    const launchNext=()=>{
      if(idx>=items.length && active===0) return resolve(results);
      while(active<limit && idx<items.length){
        const cur=idx++; active++;
        Promise.resolve()
          .then(()=>worker(items[cur],cur))
          .then(res=>{results[cur]=res;})
          .catch(err=>{results[cur]={error:String(err?.message||err)};})
          .finally(()=>{active--; launchNext();});
      }
    };
    launchNext();
  });
}

function barsToDailyMapCairo(bars){
  const map = new Map();
  for (const b of bars){
    const d = DateTime.fromSeconds(b.time, {zone: TZ}).toISODate();
    map.set(d, b.close);
  }
  return map;
}

// ===== TV CLIENT LIFECYCLE (restart-like behavior) =====
function assertTvEnv() {
  const hasSession = !!process.env.SESSION && process.env.SESSION.trim() !== "";
  const hasSig     = !!process.env.SIGNATURE && process.env.SIGNATURE.trim() !== "";
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
  try { if (typeof tv?.end === "function") tv.end(); } catch {}
  try { if (typeof tv?.delete === "function") tv.delete(); } catch {}
}

// Detect errors that usually recover with a fresh client (mimic restart)
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

      console.log(`✅ Success ${symbol} [tf=${timeframe}] in ${Date.now()-t0} ms (attempt ${attempt}/${MAX_RETRIES})${tag ? ` (${tag})` : ""}`);
      return periods;

    } catch (e) {
      const msg = String(e?.message || e);
      console.log(`❌ Fetch error ${symbol} [tf=${timeframe}] attempt ${attempt}/${MAX_RETRIES}: ${msg}${tag ? ` (${tag})` : ""}`);

      // Restart-like recovery
      if (isRecoverableTvError(e) && attempt < MAX_RETRIES) {
        console.log(`🔄 Recreating TradingView client to recover... (attempt ${attempt}/${MAX_RETRIES})`);
        try { destroyTvClient(clientRef.tv); } catch {}
        clientRef.tv = createTvClient();
      }

      if (attempt >= MAX_RETRIES) return [];

      const backoff = INITIAL_BACKOFF_MS * Math.pow(2, attempt - 1) + jitter();
      console.log(`🔁 Retry in ${backoff}ms`);
      await sleep(backoff);

    } finally {
      try { if (timeoutId) clearTimeout(timeoutId); } catch {}
      try { if (chart) chart.delete(); } catch {}
    }
  }

  return [];
}

// ===== RANGE HELPERS =====
function calc1DRangeToCoverYears(years){
  const end = localNow().endOf("day");
  const start = end.minus({ years });
  const days = Math.max(1, Math.round(end.diff(start,"days").days)) + 15;
  return { range: days, toUnix: Math.floor(end.toSeconds()) };
}
function calc1DRangeToCoverMonths(months){
  const end = localNow().endOf("day");
  const start = end.minus({ months });
  const days = Math.max(1, Math.round(end.diff(start,"days").days)) + 10;
  return { range: days, toUnix: Math.floor(end.toSeconds()) };
}

// ===== COMI PREFETCH (now: REF_TICKER prefetch) =====
async function prefetchGlobalComiLatest(clientRef) {
  const end = localNow().endOf("day");
  const toUnix = Math.floor(end.toSeconds());

  // Try small → larger ranges only if necessary
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
    GLOBAL_COMI_TS  = null;
    return;
  }

  GLOBAL_COMI_TS  = latestTs;
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

  const dayStart = DateTime.fromISO(`${todayYMD}T10:00:00`, { zone: TZ });
  const dayEnd   = DateTime.fromISO(`${todayYMD}T14:29:00`, { zone: TZ });

  const allKeys = [];
  for (let t = dayStart; t <= dayEnd; t = t.plus({ minutes: 1 })) {
    allKeys.push(t.toFormat("yyyy-LL-dd HH:mm:ss"));
  }
  const firstKeyOfDay = allKeys[0];

  const nowLocal = DateTime.now().setZone(TZ);
  const isLiveDay = (todayYMD === nowLocal.toISODate());
  const delayedNow = nowLocal.minus({ minutes: 15 });
  const nowKey = (isLiveDay ? delayedNow.toFormat("yyyy-LL-dd HH:mm:ss") : null);

  const toUnix = Math.floor((isLiveDay ? nowLocal : dayEnd).toSeconds());
  const RANGE  = 3000;

  console.log(`➡️  Fetching ${tk} 1m range=${RANGE} to=${toUnix}`);
  const bars = await fetchBarsOnce(clientRef, MARKET_SYM(tk), {
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
  const table  = [header, ...allKeys.map((k, i) => [k, filled[i] ?? null])];

  console.log(`🏁 Build done: 1D_1min in ${Date.now() - tStart} ms (rows=${table.length - 1})`);
  return { ["1D_1min_Union"]: table };
}

// ===== 1W / 1M intraday unions =====
async function buildIntradayUnion(clientRef, tickersRaw, { tf, daysBack, label }) {
  const tfStr = String(tf);
  const t0 = Date.now();

  try {
    console.log(`📊 Build start: ${label} for ${tickersRaw.length} tickers (tf=${tfStr})`);

    const SLOT_LIST = tfStr === "15"
      ? DAY_15M_SLOTS
      : tfStr === "60"
      ? DAY_60M_SLOTS
      : (() => { throw new Error(`Unsupported tf=${tfStr} (expected "15" or "60")`); })();

    const EOD_SLOT   = tfStr === "15" ? "14:15:00" : "14:00:00";
    const keyFor     = (ymd, hhmmss) => `${ymd} ${hhmmss}`;

    const lastYMD = await getLatestTradingDateFromCOMI(clientRef);
    if (!lastYMD) {
      console.warn(`⚠️ ${label}: cannot detect ${REF_TICKER} latest day`);
      return { [label]: [["DateTime", ...tickersRaw.map(t => (String(t).includes(":") ? String(t).split(":")[1] : String(t)).toUpperCase())]] };
    }
    console.log(`🧭 ${label} latest ${REF_TICKER} day: ${lastYMD}`);

    const lastDay   = DateTime.fromISO(lastYMD, { zone: TZ }).startOf("day");
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

    // fetch intraday once per ticker
    const perDayBars = tfStr === "15" ? 24 : 5;
    const RANGE = (daysBack + 3) * perDayBars + 128; // slightly smaller but safe buffer
    const toUnix = Math.floor(lastDay.endOf("day").toSeconds());

    console.log(`➡️  Fetching tf=${tfStr} range=${RANGE} to=${toUnix}`);

    const perTicker = await mapPool(tickersRaw, TICKER_CONCURRENCY, async (t) => {
      const sym  = MARKET_SYM(t);
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
        .sort((a,b)=>a.k.localeCompare(b.k));

      const byKey = new Map(rows.map(r => [r.k, r.Close]));
      const byDay = new Map();
      for (const r of rows) {
        const ymd = r.k.slice(0,10);
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
    const nowLocal = DateTime.now().setZone(TZ);
    const isLiveDay = nowLocal.toISODate() === lastYMD;
    const delayedNow = nowLocal.minus({ minutes: 15 });

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

    console.log(`🏁 Build done: ${label} in ${Date.now()-t0} ms (rows=${out.length-1})`);
    return { [label]: out };

  } catch (e) {
    console.warn(`⚠️ ${label} failed: ${e.message || e}`);
    const hdr = ["DateTime", ...tickersRaw.map(t => (String(t).includes(":") ? String(t).split(":")[1] : String(t)).toUpperCase())];
    return { [label]: [hdr] };
  }
}

// ===== DAILY TABLES (1Y/5Y) WITH WEEKLY DOWNSAMPLE & 6M daily =====
function buildForwardFilledSeries(allKeys, keyToCloseMap, { fillLeading=FILL_LEADING_DEFAULT, leadingSeed=null } = {}) {
  const out = new Array(allKeys.length);
  let last = leadingSeed ?? null;
  let seenFirst = leadingSeed != null;

  if (leadingSeed == null && fillLeading) {
    for (let i = 0; i < allKeys.length; i++) {
      const v = keyToCloseMap.get(allKeys[i]);
      if (v != null) { last = v; seenFirst = true; break; }
    }
  }

  for (let i=0;i<allKeys.length;i++){
    const k = allKeys[i];
    const v = keyToCloseMap.get(k);
    if (v != null) { last = v; seenFirst = true; out[i] = v; }
    else { out[i] = (seenFirst && last != null) ? last : null; }
  }

  return out;
}

// Weekly anchor chooser (kept as-is)
function chooseWeeklyAnchorDatesFromMap_old(map, yearsBack, TZ_IN) {
  if (!map || map.size === 0) return [];
  const dates = [...map.keys()].sort();
  const lastYMD = dates.at(-1);

  const _fromYMD = (ymd) => {
    const [y,m,d] = ymd.split("-").map(Number);
    return new Date(Date.UTC(y, m-1, d, 0, 0, 0));
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
        pick = [1,2,3,4].map(o => ymdCairoJS(new Date(cur.getTime() + o * 86400000))).find(has);
      }
    } else {
      const thu = ymdCairoJS(new Date(cur.getTime() - 3 * 86400000));
      pick = has(thu) ? thu : [1,2,3,4].map(o => ymdCairoJS(new Date(cur.getTime() + o * 86400000))).find(has);
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
    const sym  = MARKET_SYM(t);
    const bars = await fetchBarsOnce(clientRef, sym, {
      timeframe: "1D",
      range,
      toUnix,
      timeoutMs: TIMEOUT_DAILY_MS,
      tag: label
    });
    const map  = barsToDailyMapCairo(bars);
    const tk   = (String(t).includes(":") ? String(t).split(":")[1] : String(t)).toUpperCase();
    return { ticker: tk, map };
  });

  let comiMap;
  {
    const hit = perTicker.find(x => x.ticker === String(REF_TICKER).toUpperCase());
    if (hit && hit.map && hit.map.size) {
      comiMap = hit.map;
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
      comiMap = barsToDailyMapCairo(bars);
    }
  }

  function lastKeyOf(map) {
    if (!map || map.size === 0) return null;
    const ks = [...map.keys()].sort();
    return ks[ks.length - 1];
  }

  const lastDates = [
    ...perTicker.map(e => lastKeyOf(e.map)),
    lastKeyOf(comiMap),
  ].filter(Boolean);

  if (!lastDates.length) {
    console.warn(`⚠️ No dates found in ${label}`);
    return {
      [`${label}_Daily_Union`]:        [["Date"]],
      [`${label}_Weekly_Downsampled`]: [["Date"]],
    };
  }

  const globalLastYMD = lastDates.reduce((max, d) => (d > max ? d : max), lastDates[0]);
  const globalLast    = DateTime.fromISO(globalLastYMD, { zone: TZ });
  const cutoffYMD     = globalLast.minus({ years }).toISODate();
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

  const refTrim  = [...comiMap.keys()].filter(d => d >= cutoffYMD);
  const allDates = [...new Set([
    ...prepared.flatMap(p => [...p.mm.keys()]),
    ...refTrim,
  ])].sort();

  if (!allDates.length) {
    console.warn(`⚠️ ${label} produced empty union date set`);
    return {
      [`${label}_Daily_Union`]:        [["Date", ...prepared.map(p => p.ticker)]],
      [`${label}_Weekly_Downsampled`]: [["Date", ...prepared.map(p => p.ticker)]],
    };
  }

  const header = ["Date", ...prepared.map(p => p.ticker)];
  const daily  = [header];

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
  const keep      = chooseWeeklyAnchorDatesFromMap_old(anchorMap, years, TZ);
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

  console.log(`🏁 Build done: ${label} in ${Date.now()-tStart} ms (rows daily=${daily.length-1}, weekly=${weekly.length-1})`);
  return {
    [`${label}_Daily_Union`]:        daily,
    [`${label}_Weekly_Downsampled`]: weekly,
  };
}

async function buildSixMonthDaily(clientRef, tickers, label) {
  console.log(`📊 Build start: ${label} for ${tickers.length} tickers`);
  const tStart = Date.now();

  const { range, toUnix } = calc1DRangeToCoverMonths(6);

  console.log(`➡️  Fetching DAILY 1D for ${label} (months=6, range=${range}, to=${toUnix})`);
  const perTicker = await mapPool(tickers, TICKER_CONCURRENCY, async (t) => {
    const sym  = MARKET_SYM(t);
    const bars = await fetchBarsOnce(clientRef, sym, {
      timeframe: "1D",
      range,
      toUnix,
      timeoutMs: TIMEOUT_DAILY_MS,
      tag: label
    });
    const map  = barsToDailyMapCairo(bars);
    const tk   = (String(t).includes(":") ? String(t).split(":")[1] : String(t)).toUpperCase();
    return { ticker: tk, map };
  });

  let comiMap;
  {
    const hit = perTicker.find(x => x.ticker === String(REF_TICKER).toUpperCase());
    if (hit && hit.map && hit.map.size) {
      comiMap = hit.map;
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
      comiMap = barsToDailyMapCairo(bars);
    }
  }

  function lastKeyOf(map) {
    if (!map || map.size === 0) return null;
    const ks = [...map.keys()].sort();
    return ks[ks.length - 1];
  }

  const lastDates = [
    ...perTicker.map(e => lastKeyOf(e.map)),
    lastKeyOf(comiMap)
  ].filter(Boolean);

  if (!lastDates.length) {
    console.warn(`⚠️ No dates found in ${label}`);
    return { [`${label}_Daily_Union`]: [["Date"]] };
  }

  const globalLastYMD = lastDates.reduce((max, d) => (d > max ? d : max), lastDates[0]);
  const globalLast    = DateTime.fromISO(globalLastYMD, { zone: TZ });
  const cutoffYMD     = globalLast.minus({ months: 6 }).toISODate();
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

  const refTrim  = [...comiMap.keys()].filter(d => d >= cutoffYMD);
  const allDates = [...new Set([
    ...prepared.flatMap(p => [...p.mm.keys()]),
    ...refTrim
  ])].sort();

  if (!allDates.length) {
    console.warn(`⚠️ ${label} produced empty union date set`);
    return { [`${label}_Daily_Union`]: [["Date", ...prepared.map(p => p.ticker)]] };
  }

  const header = ["Date", ...prepared.map(p => p.ticker)];
  const daily  = [header];

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

  console.log(`🏁 Build done: ${label} in ${Date.now()-tStart} ms (rows daily=${daily.length-1})`);
  return { [`${label}_Daily_Union`]: daily };
}

// ===== JSON EXPORT (same behavior as your original) =====
function buildJsonFromCombined(combined, tickers, includeEGX30) {
  const jsonPerTicker = tickers.map(tk => {
    const base = {
      one_week:    [],
      one_month:   [],
      six_months:  [],
      one_year:    [],
      five_years:  [],
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
        const ts  = row[0];
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
  pushSeries("1W_15min_Union",        "one_week",          true);
  pushSeries("1M_Hourly_Union",       "one_month",         true);
  pushSeries("6M_Daily_Union",        "six_months",        false);
  pushSeries("1Y_Weekly_Downsampled", "one_year",          false);
  pushSeries("5Y_Weekly_Downsampled", "five_years",        false);
  if (combined["5Y_Daily_Union"])     pushSeries("5Y_Daily_Union", "five_years_daily", false);

  if (includeEGX30 && combined["5Y_EGX30_ONLY_Daily_Union"]) {
    pushSeries("5Y_EGX30_ONLY_Daily_Union", "five_years_daily", false);
  }

  return jsonPerTicker;
}

// ===== REQUEST MUTEX (no skipping data => prevent overlapping requests) =====
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
  return dt.toISODate(); // yyyy-MM-dd
}

// fetch a small daily window around the requested date, then pick exact match or nearest previous
async function getCloseOnOrBeforeDate(clientRef, tickerRaw, ymd, { fallbackToPrevTradingDay = true } = {}) {
  const sym = MARKET_SYM(tickerRaw);

  // Use end of requested day (local TZ) as "to"
  const targetEnd = DateTime.fromISO(ymd, { zone: TZ }).endOf("day");
  const toUnix = Math.floor(targetEnd.toSeconds());

  // Small range is usually enough (covers holidays/weekends)
  const RANGE = 30;

  const bars = await fetchBarsOnce(clientRef, sym, {
    timeframe: "1D",
    range: RANGE,
    toUnix,
    timeoutMs: TIMEOUT_DAILY_MS,
    tag: `close@${ymd}`
  });

  if (!bars || !bars.length) return { date: ymd, close: null, resolvedDate: null };

  // Map of yyyy-MM-dd -> close
  const map = barsToDailyMapCairo(bars);

  if (map.has(ymd)) {
    return { date: ymd, close: map.get(ymd), resolvedDate: ymd };
  }

  if (!fallbackToPrevTradingDay) {
    return { date: ymd, close: null, resolvedDate: null };
  }

  // Nearest previous date that exists in bars
  const available = [...map.keys()].sort(); // ascending
  let pick = null;
  for (let i = available.length - 1; i >= 0; i--) {
    if (available[i] < ymd) { pick = available[i]; break; }
  }

  return pick
    ? { date: ymd, close: map.get(pick), resolvedDate: pick }
    : { date: ymd, close: null, resolvedDate: null };
}

function parseYMDMany(datesInput) {
  const arr = Array.isArray(datesInput) ? datesInput : [datesInput];
  const out = [];
  for (const d of arr) {
    const ymd = parseYMD(d);
    if (!ymd) return null; // invalid
    out.push(ymd);
  }
  // dedupe + sort
  return [...new Set(out)].sort();
}

function daysBetweenInclusive(ymdMin, ymdMax) {
  const a = DateTime.fromISO(ymdMin, { zone: TZ }).startOf("day");
  const b = DateTime.fromISO(ymdMax, { zone: TZ }).startOf("day");
  const diff = Math.round(b.diff(a, "days").days);
  return Math.max(0, diff) + 1;
}

/**
 * Fetch once per ticker for a date window, then resolve each requested date
 * (exact match or nearest previous if fallback enabled).
 */
async function getClosesForDates(clientRef, tickerRaw, ymdList, { fallbackToPrevTradingDay = true } = {}) {
  const sym = MARKET_SYM(tickerRaw);

  const minYMD = ymdList[0];
  const maxYMD = ymdList[ymdList.length - 1];

  // Fetch up to end of max day (local TZ)
  const maxEnd = DateTime.fromISO(maxYMD, { zone: TZ }).endOf("day");
  const toUnix = Math.floor(maxEnd.toSeconds());

  // Range to cover [min..max] + buffer for weekends/holidays + fallback lookback
  const spanDays = daysBetweenInclusive(minYMD, maxYMD);
  const RANGE = spanDays + 40; // buffer (tweak if you want)

  const bars = await fetchBarsOnce(clientRef, sym, {
    timeframe: "1D",
    range: RANGE,
    toUnix,
    timeoutMs: TIMEOUT_DAILY_MS,
    tag: `close@batch:${minYMD}->${maxYMD}`
  });

  if (!bars || !bars.length) {
    // Return all nulls
    const out = {};
    for (const ymd of ymdList) out[ymd] = { close: null };
    return out;
  }

  const map = barsToDailyMapCairo(bars); // yyyy-MM-dd -> close
  const available = [...map.keys()].sort(); // ascending

  const resolveOne = (ymd) => {
    if (map.has(ymd)) return { close: map.get(ymd), resolvedDate: ymd };

    if (!fallbackToPrevTradingDay) return { close: null, resolvedDate: null };

    // nearest previous available date < ymd
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
    // EXACT format you requested: only close.
    out[ymd] = { close: r.close };
  }

  return out;
}

// ===== /close (kept same; added optional market for consistency with new global prefix) =====
app.post("/close", async (req, res) => {
  return withRequestLock(async () => {
    const { market, ticker, tickers, date, dates, fallback } = req.body || {};

    // ✅ Default EGX; if provided, apply market context so prefix/ref/tz match
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
          'Required JSON body: { "tickers": ["COMI","SWDY"], "dates": ["2025-08-10","2025-08-11"] } ' +
          '(or use "ticker"/"date")'
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

      // Build nested map: results[ticker][ymd] = { close }
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

// ===== MAIN ROUTE =====
app.post("/fetch", async (req, res) => {

  console.log("SESSION:", process.env.SESSION?.slice(0, 6), "...", process.env.SESSION?.length);
  console.log("SIGNATURE:", process.env.SIGNATURE?.slice(0, 6), "...", process.env.SIGNATURE?.length);
  console.log('PID:', process.pid);

  return withRequestLock(async () => {
    const clientRef = { tv: createTvClient() };

    try {
      console.log("Before building:", (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2), "MB");
      console.log("RSS Before building:", (process.memoryUsage().rss / 1024 / 1024).toFixed(2), "MB");

      const startAll = Date.now();
      logEnvCheck();

      // ✅ NEW: optional market param (default EGX)
      const { tickersRaw, market } = req.body;
      const mkt = normalizeMarket(market);
      applyMarketContext(mkt);

      if (!Array.isArray(tickersRaw) || tickersRaw.length === 0) {
        return res.status(400).json({ error: "tickersRaw is required (non-empty array)" });
      }

      // ---- AUTO-ADD EGX30 IF MULTI-TICKER RUN ----
      let includeEGX30 = false;

      if (tickersRaw.length > 1) {
        includeEGX30 = true;
        if (!tickersRaw.some(t => String(t).toUpperCase().includes("EGX30"))) {
          tickersRaw.push("EGX30");
        }
        console.log("📈 Auto-included EGX30 for benchmarking (5Y daily only)");
      } else {
        console.log("✅ Single ticker run — NOT adding EGX30");
      }

      console.log(`🎯 Tickers: ${tickersRaw.join(", ")}`);

      // Prefetch REF_TICKER once (COMI for EGX, FAB for ADX)
      await prefetchGlobalComiLatest(clientRef);

      const tickers = tickersRaw.map(t => (String(t).includes(":") ? String(t).split(":")[1] : String(t)).toUpperCase());

      // Build jobs
      let jobs;
      if (includeEGX30) {
        const mainTickers = tickersRaw.filter(t => String(t).toUpperCase() !== "EGX30");

        jobs = [
          { key: "5Y", run: async () => await buildDailyTables(clientRef, mainTickers, 5, "5Y") },
          { key: "1Y", run: async () => await buildDailyTables(clientRef, mainTickers, 1, "1Y") },
          { key: "6M", run: async () => await buildSixMonthDaily(clientRef, mainTickers, "6M") },
          { key: "1M", run: async () => await buildIntradayUnion(clientRef, mainTickers, { tf: "60", daysBack: 28, label: "1M_Hourly_Union" }) },
          { key: "1W", run: async () => await buildIntradayUnion(clientRef, mainTickers, { tf: "15", daysBack: 7,  label: "1W_15min_Union" }) },
          { key: "5Y_EGX30_ONLY", run: async () => await buildDailyTables(clientRef, ["EGX30"], 5, "5Y_EGX30_ONLY") },
        ];
      } else {
        jobs = [
          { key: "5Y", run: async () => await buildDailyTables(clientRef, tickersRaw, 5, "5Y") },
          { key: "1Y", run: async () => await buildDailyTables(clientRef, tickersRaw, 1, "1Y") },
          { key: "6M", run: async () => await buildSixMonthDaily(clientRef, tickersRaw, "6M") },
          { key: "1M", run: async () => await buildIntradayUnion(clientRef, tickersRaw, { tf: "60", daysBack: 28, label: "1M_Hourly_Union" }) },
          { key: "1W", run: async () => await buildIntradayUnion(clientRef, tickersRaw, { tf: "15", daysBack: 7,  label: "1W_15min_Union" }) },
        ];
      }

      // If exactly one ticker → add 1D/1m job
      if (tickersRaw.length === 1) {
        jobs.push({ key: "1D", run: async () => await buildOneDayOneMinute(clientRef, tickersRaw[0]) });
      }

      console.log(`🚀 Starting all windows with TF_CONCURRENCY=${TF_CONCURRENCY}`);
      const allTables = await mapPool(jobs, TF_CONCURRENCY, async (j) => {
        const s = Date.now();
        console.log(`➡️  Window start: ${j.key}`);
        const out = await j.run();
        console.log(`✅ Window done:  ${j.key} in ${Date.now()-s} ms`);
        return out;
      });

      const combined = Object.assign({}, ...allTables);

      const jsonPerTicker = buildJsonFromCombined(combined, tickers, includeEGX30);

      console.log(`✅ ALL DONE in ${((Date.now()-startAll)/1000).toFixed(2)} s`);
      const used = process.memoryUsage();
      console.log({
        rss: (used.rss / 1024 / 1024).toFixed(2) + " MB",
        heapTotal: (used.heapTotal / 1024 / 1024).toFixed(2) + " MB",
        heapUsed: (used.heapUsed / 1024 / 1024).toFixed(2) + " MB",
        external: (used.external / 1024 / 1024).toFixed(2) + " MB",
        arrayBuffers: (used.arrayBuffers / 1024 / 1024).toFixed(2) + " MB"
      });

      // include market in response? (doesn't change your structure; keeping it identical)
      return res.json(jsonPerTicker);

    } catch (err) {
      console.error("❌ /fetch crashed:", err);
      return res.status(500).json({ error: "internal_error", detail: String(err?.message || err) });

    } finally {
      // Mimic "restart cleanup" without restarting the whole service
      destroyTvClient(clientRef.tv);
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

    const voice  = String(req.body.voice || "en-US-SaraNeural");
    const format = String(req.body.format || "wav"); // wav = best sync

    const key    = process.env.AZURE_SPEECH_KEY;
    const region = process.env.AZURE_SPEECH_REGION;

    if (!key || !region) {
      return res.status(500).json({
        ok: false,
        error: "AZURE_SPEECH_KEY or AZURE_SPEECH_REGION missing",
      });
    }

    const speechConfig = speechsdk.SpeechConfig.fromSubscription(key, region);
    speechConfig.speechSynthesisVoiceName = voice;

    // WAV avoids decoder latency → minimal drift with timestamps
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
      const len   = typeof e.wordLength === "number" ? e.wordLength : 0;
      const end   = start >= 0 ? start + len : -1;

      const value =
        typeof e.text === "string" && e.text.length
          ? e.text
          : start >= 0 && end > start
          ? text.substring(start, end)
          : "";

      speechMarks.push({
        time: ticksToMs(e.audioOffset), // ms from start
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
