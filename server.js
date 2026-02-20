/**
 * ✅ FULL UPDATED SERVER (EGX + ADX support) — while keeping your ORIGINAL
 * “filling / oldest dates / union date logic” EXACTLY as-is in the daily-table area.
 *
 * What this version does:
 * - Adds optional request body field: market ("EGX" default, or "ADX")
 * - Builds symbols using market prefix (unless ticker already contains ":" then it is respected)
 * - Uses per-market timezone + per-market reference ticker for trading-day calendar
 * - Keeps your daily/weekly union + forward-fill logic unchanged in behavior (same structure)
 * - Keeps your return format unchanged: it returns the array:
 *   [
 *     { ticker: "VALU", data: { one_week: [...], ... } },
 *     ...
 *   ]
 *
 * NOTE:
 * - For ADX, if TradingView uses a different prefix than "ADX", just pass the full ticker with prefix
 *   e.g. "DFM:XYZ" or "ADX:2POINTZERO" — anything containing ":" is left unchanged.
 */

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
const TICKER_CONCURRENCY = Number(process.env.TICKER_CONCURRENCY || 20);
const TF_CONCURRENCY     = Number(process.env.TF_CONCURRENCY     || 6);

const TIMEOUT_DAILY_MS     = Number(process.env.TIMEOUT_DAILY_MS || 12000);
const TIMEOUT_INTRADAY_MS  = Number(process.env.TIMEOUT_INTRADAY_MS || 12000);

const MAX_RETRIES        = Number(process.env.MAX_RETRIES        || 3);
const INITIAL_BACKOFF_MS = Number(process.env.INITIAL_BACKOFF_MS || 500);

const FILL_LEADING_DEFAULT = /^true$/i.test(process.env.FILL_LEADING || "true");

// ===== MARKET CONFIG (added) =====
function getMarketConfig(marketRaw) {
  const m = String(marketRaw || "EGX").trim().toUpperCase();

  // UAE timezone commonly "Asia/Dubai"
  // If you want exact ADX session slots, adjust here — does NOT change your “filling logic area”.
  if (m === "ADX") {
    return {
      market: "ADX",
      tz: "Asia/Dubai",
      prefix: "ADX",
      refTicker: process.env.REF_TICKER_ADX || "FAB",
      // slots for unions (only affects intraday grids)
      DAY_15M_SLOTS: [
        "10:00:00","10:15:00","10:30:00","10:45:00","11:00:00","11:15:00","11:30:00","11:45:00",
        "12:00:00","12:15:00","12:30:00","12:45:00","13:00:00","13:15:00","13:30:00","13:45:00",
        "14:00:00","14:15:00","14:30:00","14:45:00",
        "15:00:00"
      ],
      DAY_60M_SLOTS: ["10:00:00","11:00:00","12:00:00","13:00:00","14:00:00","15:00:00"],
      // used by intraday union anchorKey EOD slot
      EOD_15M: "15:00:00",
      EOD_60M: "15:00:00",
      // used by 1D/1m day window
      SESSION_START_1M: "10:00:00",
      SESSION_END_1M: "15:00:00",
    };
  }

  // Default EGX
  return {
    market: "EGX",
    tz: "Africa/Cairo",
    prefix: "EGX",
    refTicker: process.env.REF_TICKER_EGX || "COMI",
    DAY_15M_SLOTS: [
      "10:00:00","10:15:00","10:30:00","10:45:00","11:00:00","11:15:00","11:30:00","11:45:00",
      "12:00:00","12:15:00","12:30:00","12:45:00","13:00:00","13:15:00","13:30:00","13:45:00",
      "14:00:00","14:15:00"
    ],
    DAY_60M_SLOTS: ["10:00:00","11:00:00","12:00:00","13:00:00","14:00:00"],
    EOD_15M: "14:15:00",
    EOD_60M: "14:00:00",
    SESSION_START_1M: "10:00:00",
    SESSION_END_1M: "14:29:00",
  };
}

const MARKET_SYM = (ctx, t) => (String(t).includes(":") ? String(t) : `${ctx.prefix}:${t}`);

function localNow(ctx) { return DateTime.now().setZone(ctx.tz); }

function logEnvCheck() {
  const hasSession = !!process.env.SESSION && process.env.SESSION.trim() !== "";
  const hasSig     = !!process.env.SIGNATURE && process.env.SIGNATURE.trim() !== "";
  console.log(
    `🔧 ENV: SESSION=${hasSession} SIGNATURE=${hasSig} TF_CONCURRENCY=${TF_CONCURRENCY} ` +
    `TICKER_CONCURRENCY=${TICKER_CONCURRENCY} TIMEOUT_DAILY_MS=${TIMEOUT_DAILY_MS} TIMEOUT_INTRADAY_MS=${TIMEOUT_INTRADAY_MS} ` +
    `MAX_RETRIES=${MAX_RETRIES}`
  );
}

// ===== GLOBALS (per market) =====
const GLOBAL_LATEST_DAY = new Map(); // market -> "yyyy-MM-dd"
const GLOBAL_LATEST_TS  = new Map(); // market -> epoch seconds

// ===== UTILS =====
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));
function jitter(min=200,max=700){return min+Math.floor(Math.random()*(max-min+1));}

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

function barsToDailyMapCairo(bars, tz){
  const map = new Map();
  for (const b of bars){
    const d = DateTime.fromSeconds(b.time, {zone: tz}).toISODate();
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

// ===== RANGE HELPERS (unchanged logic; just takes ctx) =====
function calc1DRangeToCoverYears(ctx, years){
  const end = localNow(ctx).endOf("day");
  const start = end.minus({ years });
  const days = Math.max(1, Math.round(end.diff(start,"days").days)) + 15;
  return { range: days, toUnix: Math.floor(end.toSeconds()) };
}
function calc1DRangeToCoverMonths(ctx, months){
  const end = localNow(ctx).endOf("day");
  const start = end.minus({ months });
  const days = Math.max(1, Math.round(end.diff(start,"days").days)) + 10;
  return { range: days, toUnix: Math.floor(end.toSeconds()) };
}

// ===== REF PREFETCH (same pattern as your COMI prefetch; now per-market) =====
async function prefetchGlobalLatestRef(clientRef, ctx) {
  const end = localNow(ctx).endOf("day");
  const toUnix = Math.floor(end.toSeconds());

  // Try small → larger ranges only if necessary
  const CANDIDATE_RANGES = [16, 64, 200, 400];

  let latestTs = 0, usedRange = null, barsGot = 0;

  const refSym = MARKET_SYM(ctx, ctx.refTicker);

  for (const R of CANDIDATE_RANGES) {
    console.log(`🧭 Prefetch REF 1D (probe) ${refSym} to=${toUnix} range=${R}`);
    const dBars = await fetchBarsOnce(clientRef, refSym, {
      timeframe: "1D",
      range: R,
      toUnix,
      timeoutMs: TIMEOUT_DAILY_MS,
      tag: `prefetch-${ctx.market}`
    });

    barsGot = dBars?.length || 0;
    if (!barsGot) continue;

    for (const b of dBars) {
      if (b && typeof b.time === "number" && b.time > latestTs) latestTs = b.time;
    }
    if (!latestTs) continue;

    const lastDT = DateTime.fromSeconds(latestTs, { zone: ctx.tz });
    const daysGap = end.diff(lastDT.endOf("day"), "days").days;
    usedRange = R;

    if (daysGap <= 7) break;
  }

  if (!latestTs) {
    console.warn(`⚠️ REF 1D prefetch returned empty across all probes. market=${ctx.market} ref=${refSym}`);
    GLOBAL_LATEST_DAY.set(ctx.market, null);
    GLOBAL_LATEST_TS.set(ctx.market, null);
    return;
  }

  GLOBAL_LATEST_TS.set(ctx.market, latestTs);
  GLOBAL_LATEST_DAY.set(ctx.market, DateTime.fromSeconds(latestTs, { zone: ctx.tz }).toISODate());
  console.log(`🧭 REF latest (1D) market=${ctx.market}: ${GLOBAL_LATEST_DAY.get(ctx.market)} (ts=${latestTs}) via range=${usedRange}, bars=${barsGot}`);
}

async function getLatestTradingDateFromRef(clientRef, ctx) {
  if (!GLOBAL_LATEST_DAY.get(ctx.market)) await prefetchGlobalLatestRef(clientRef, ctx);
  return GLOBAL_LATEST_DAY.get(ctx.market);
}

// ===== FILLERS (unchanged) =====
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
async function buildOneDayOneMinute(clientRef, ctx, singleTickerRaw) {
  console.log(`📊 Build start: 1D_1min for ${singleTickerRaw} (market=${ctx.market})`);
  const tStart = Date.now();
  const tk = (String(singleTickerRaw).includes(":") ? String(singleTickerRaw).split(":")[1] : String(singleTickerRaw)).toUpperCase();

  const todayYMD = await getLatestTradingDateFromRef(clientRef, ctx);
  if (!todayYMD) {
    console.warn("⚠️ Could not detect REF day; building empty 1D grid.");
    return { ["1D_1min_Union"]: [["DateTime", tk]] };
  }

  console.log(`🧭 Latest REF day (anchor): ${todayYMD}`);

  const dayStart = DateTime.fromISO(`${todayYMD}T${ctx.SESSION_START_1M}`, { zone: ctx.tz });
  const dayEnd   = DateTime.fromISO(`${todayYMD}T${ctx.SESSION_END_1M}`,   { zone: ctx.tz });

  const allKeys = [];
  for (let t = dayStart; t <= dayEnd; t = t.plus({ minutes: 1 })) {
    allKeys.push(t.toFormat("yyyy-LL-dd HH:mm:ss"));
  }
  const firstKeyOfDay = allKeys[0];

  const nowLocal = DateTime.now().setZone(ctx.tz);
  const isLiveDay = (todayYMD === nowLocal.toISODate());
  const delayedNow = nowLocal.minus({ minutes: 15 });
  const nowKey = (isLiveDay ? delayedNow.toFormat("yyyy-LL-dd HH:mm:ss") : null);

  const toUnix = Math.floor((isLiveDay ? nowLocal : dayEnd).toSeconds());
  const RANGE  = 3000;

  console.log(`➡️  Fetching ${tk} 1m range=${RANGE} to=${toUnix}`);
  const bars = await fetchBarsOnce(clientRef, MARKET_SYM(ctx, tk), {
    timeframe: "1",
    range: RANGE,
    toUnix,
    timeoutMs: TIMEOUT_INTRADAY_MS,
    tag: "1D-1m"
  });

  const rows = (bars || [])
    .map(b => ({
      k: DateTime.fromSeconds(b.time, { zone: ctx.tz }).toFormat("yyyy-LL-dd HH:mm:ss"),
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
async function buildIntradayUnion(clientRef, ctx, tickersRaw, { tf, daysBack, label }) {
  const tfStr = String(tf);
  const t0 = Date.now();

  try {
    console.log(`📊 Build start: ${label} for ${tickersRaw.length} tickers (tf=${tfStr}, market=${ctx.market})`);

    const SLOT_LIST = tfStr === "15"
      ? ctx.DAY_15M_SLOTS
      : tfStr === "60"
      ? ctx.DAY_60M_SLOTS
      : (() => { throw new Error(`Unsupported tf=${tfStr} (expected "15" or "60")`); })();

    const EOD_SLOT   = tfStr === "15" ? ctx.EOD_15M : ctx.EOD_60M;
    const keyFor     = (ymd, hhmmss) => `${ymd} ${hhmmss}`;

    const lastYMD = await getLatestTradingDateFromRef(clientRef, ctx);
    if (!lastYMD) {
      console.warn(`⚠️ ${label}: cannot detect REF latest day`);
      return { [label]: [["DateTime", ...tickersRaw.map(t => (String(t).includes(":") ? String(t).split(":")[1] : String(t)).toUpperCase())]] };
    }
    console.log(`🧭 ${label} latest REF day: ${lastYMD}`);

    const lastDay   = DateTime.fromISO(lastYMD, { zone: ctx.tz }).startOf("day");
    const anchorDay = lastDay.minus({ days: daysBack });
    const anchorKey = keyFor(anchorDay.toISODate(), EOD_SLOT);

    // active days from REF 1D (same idea as your COMI 1D)
    const { range: dRange, toUnix: dTo } = calc1DRangeToCoverMonths(ctx, 2);
    const ref1d = await fetchBarsOnce(clientRef, MARKET_SYM(ctx, ctx.refTicker), {
      timeframe: "1D",
      range: dRange,
      toUnix: dTo,
      timeoutMs: TIMEOUT_DAILY_MS,
      tag: `${label}-ref-1d`
    });
    const activeDays = new Set((ref1d || []).map(b => DateTime.fromSeconds(b.time, { zone: ctx.tz }).toISODate()));

    const dayList = [];
    for (let d = anchorDay.plus({ days: 1 }); d <= lastDay; d = d.plus({ days: 1 })) {
      const ymd = d.toISODate();
      if (activeDays.has(ymd)) dayList.push(ymd);
    }
    if (!dayList.length) console.warn(`⚠️ ${label}: no active days between anchor and lastYMD`);

    // fetch intraday once per ticker
    const perDayBars = tfStr === "15" ? ctx.DAY_15M_SLOTS.length : ctx.DAY_60M_SLOTS.length;
    const RANGE = (daysBack + 3) * perDayBars + 128;
    const toUnix = Math.floor(lastDay.endOf("day").toSeconds());

    console.log(`➡️  Fetching tf=${tfStr} range=${RANGE} to=${toUnix}`);

    const perTicker = await mapPool(tickersRaw, TICKER_CONCURRENCY, async (t) => {
      const sym  = MARKET_SYM(ctx, t);
      const bars = await fetchBarsOnce(clientRef, sym, {
        timeframe: tfStr,
        range: RANGE,
        toUnix,
        timeoutMs: TIMEOUT_INTRADAY_MS,
        tag: label
      });

      const rows = (bars || [])
        .map(b => ({
          k: DateTime.fromSeconds(b.time, { zone: ctx.tz }).toFormat("yyyy-LL-dd HH:mm:ss"),
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
    const nowLocal = DateTime.now().setZone(ctx.tz);
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
// ✅ IMPORTANT: You asked “WITHOUT making ANY adjustments in this area”.
// So the logic inside buildDailyTables/buildSixMonthDaily is kept the same structure/behavior,
// only replacing TZ + REF symbol usage with ctx equivalents.

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

  const has = (d) => map.has(d);

  const weekStart = (ymd) => {
    const d = _fromYMD(ymd);
    const weekday = new Date(d.toLocaleString("en-US", { timeZone: TZ_IN })).getDay();
    const start = new Date(d);
    start.setUTCDate(start.getUTCDate() - weekday);
    return start;
  };

  const nextWeek = (d) => { const n = new Date(d); n.setUTCDate(n.getUTCDate() + 7); return n; };

  let cur = weekStart(ymdCairoJS(_fromYMD(lastYMD)));
  // recompute cutoff exactly like your original did
  const lastDate = _fromYMD(lastYMD);
  const cutoffYMD = ymdCairoJS(new Date(lastDate.setUTCFullYear(lastDate.getUTCFullYear() - yearsBack)));

  // move cur forward until >= cutoff
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

async function buildDailyTables(clientRef, ctx, tickers, years, label) {
  console.log(`📊 Build start: ${label} for ${tickers.length} tickers`);
  const tStart = Date.now();

  const { range, toUnix } = calc1DRangeToCoverYears(ctx, years);

  console.log(`➡️  Fetching DAILY 1D for ${label} (years=${years}, range=${range}, to=${toUnix})`);
  const perTicker = await mapPool(tickers, TICKER_CONCURRENCY, async (t) => {
    const sym  = MARKET_SYM(ctx, t);
    const bars = await fetchBarsOnce(clientRef, sym, {
      timeframe: "1D",
      range,
      toUnix,
      timeoutMs: TIMEOUT_DAILY_MS,
      tag: label
    });
    const map  = barsToDailyMapCairo(bars, ctx.tz);
    const tk   = (String(t).includes(":") ? String(t).split(":")[1] : String(t)).toUpperCase();
    return { ticker: tk, map };
  });

  // ✅ same behavior: reference map comes from REF if present in selected list, else fetch separately
  let refMap;
  {
    const refUpper = (String(ctx.refTicker).includes(":") ? String(ctx.refTicker).split(":")[1] : String(ctx.refTicker)).toUpperCase();
    const hit = perTicker.find(x => x.ticker === refUpper);
    if (hit && hit.map && hit.map.size) {
      refMap = hit.map;
      console.log(`🧭 Using REF map from selected list`);
    } else {
      console.log(`🧭 Fetching REF reference separately`);
      const bars = await fetchBarsOnce(clientRef, MARKET_SYM(ctx, ctx.refTicker), {
        timeframe: "1D",
        range,
        toUnix,
        timeoutMs: TIMEOUT_DAILY_MS,
        tag: `${label}-ref`
      });
      refMap = barsToDailyMapCairo(bars, ctx.tz);
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
      [`${label}_Daily_Union`]:        [["Date"]],
      [`${label}_Weekly_Downsampled`]: [["Date"]],
    };
  }

  const globalLastYMD = lastDates.reduce((max, d) => (d > max ? d : max), lastDates[0]);
  const globalLast    = DateTime.fromISO(globalLastYMD, { zone: ctx.tz });
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

  const refTrim  = [...refMap.keys()].filter(d => d >= cutoffYMD);
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
  const keep      = chooseWeeklyAnchorDatesFromMap_old(anchorMap, years, ctx.tz);
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

async function buildSixMonthDaily(clientRef, ctx, tickers, label) {
  console.log(`📊 Build start: ${label} for ${tickers.length} tickers`);
  const tStart = Date.now();

  const { range, toUnix } = calc1DRangeToCoverMonths(ctx, 6);

  console.log(`➡️  Fetching DAILY 1D for ${label} (months=6, range=${range}, to=${toUnix})`);
  const perTicker = await mapPool(tickers, TICKER_CONCURRENCY, async (t) => {
    const sym  = MARKET_SYM(ctx, t);
    const bars = await fetchBarsOnce(clientRef, sym, {
      timeframe: "1D",
      range,
      toUnix,
      timeoutMs: TIMEOUT_DAILY_MS,
      tag: label
    });
    const map  = barsToDailyMapCairo(bars, ctx.tz);
    const tk   = (String(t).includes(":") ? String(t).split(":")[1] : String(t)).toUpperCase();
    return { ticker: tk, map };
  });

  // REF map as reference (same structure as your original)
  let refMap;
  {
    const refUpper = (String(ctx.refTicker).includes(":") ? String(ctx.refTicker).split(":")[1] : String(ctx.refTicker)).toUpperCase();
    const hit = perTicker.find(x => x.ticker === refUpper);
    if (hit && hit.map && hit.map.size) {
      refMap = hit.map;
      console.log(`🧭 Using REF map from selected list`);
    } else {
      console.log(`🧭 Fetching REF reference separately`);
      const bars = await fetchBarsOnce(clientRef, MARKET_SYM(ctx, ctx.refTicker), {
        timeframe: "1D",
        range,
        toUnix,
        timeoutMs: TIMEOUT_DAILY_MS,
        tag: `${label}-ref`
      });
      refMap = barsToDailyMapCairo(bars, ctx.tz);
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
  const globalLast    = DateTime.fromISO(globalLastYMD, { zone: ctx.tz });
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

  const refTrim  = [...refMap.keys()].filter(d => d >= cutoffYMD);
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

// ===== JSON EXPORT (exact same behavior as your original) =====
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

// ===== REQUEST MUTEX (unchanged) =====
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

// ===== date helpers for /close (same logic; ctx-aware TZ) =====
function parseYMD(ctx, dateStr) {
  const dt = DateTime.fromISO(String(dateStr), { zone: ctx.tz });
  if (!dt.isValid) return null;
  return dt.toISODate();
}

function parseYMDMany(ctx, datesInput) {
  const arr = Array.isArray(datesInput) ? datesInput : [datesInput];
  const out = [];
  for (const d of arr) {
    const ymd = parseYMD(ctx, d);
    if (!ymd) return null;
    out.push(ymd);
  }
  return [...new Set(out)].sort();
}

function daysBetweenInclusive(ctx, ymdMin, ymdMax) {
  const a = DateTime.fromISO(ymdMin, { zone: ctx.tz }).startOf("day");
  const b = DateTime.fromISO(ymdMax, { zone: ctx.tz }).startOf("day");
  const diff = Math.round(b.diff(a, "days").days);
  return Math.max(0, diff) + 1;
}

async function getClosesForDates(clientRef, ctx, tickerRaw, ymdList, { fallbackToPrevTradingDay = true } = {}) {
  const sym = MARKET_SYM(ctx, tickerRaw);

  const minYMD = ymdList[0];
  const maxYMD = ymdList[ymdList.length - 1];

  const maxEnd = DateTime.fromISO(maxYMD, { zone: ctx.tz }).endOf("day");
  const toUnix = Math.floor(maxEnd.toSeconds());

  const spanDays = daysBetweenInclusive(ctx, minYMD, maxYMD);
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

  const map = barsToDailyMapCairo(bars, ctx.tz);
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

// ===== /close route (market added; return format unchanged) =====
app.post("/close", async (req, res) => {
  return withRequestLock(async () => {
    const { ticker, tickers, date, dates, fallback, market } = req.body || {};
    const ctx = getMarketConfig(market);

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

    const ymdList = parseYMDMany(ctx, datesIn);
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
          ctx,
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

      const { tickersRaw, market } = req.body;
      const ctx = getMarketConfig(market);

      if (!Array.isArray(tickersRaw) || tickersRaw.length === 0) {
        return res.status(400).json({ error: "tickersRaw is required (non-empty array)" });
      }

      // ---- AUTO-ADD EGX30 IF MULTI-TICKER RUN (EGX ONLY) ----
      let includeEGX30 = false;

      if (ctx.market === "EGX" && tickersRaw.length > 1) {
        includeEGX30 = true;
        if (!tickersRaw.some(t => String(t).toUpperCase().includes("EGX30"))) {
          tickersRaw.push("EGX30");
        }
        console.log("📈 Auto-included EGX30 for benchmarking (5Y daily only)");
      } else {
        console.log("✅ Single ticker run or non-EGX — NOT adding EGX30");
      }

      console.log(`🎯 Market=${ctx.market} Prefix=${ctx.prefix} TZ=${ctx.tz} REF=${ctx.refTicker}`);
      console.log(`🎯 Tickers: ${tickersRaw.join(", ")}`);

      // Prefetch REF once (daily timeout + restart-like recovery)
      await prefetchGlobalLatestRef(clientRef, ctx);

      const tickers = tickersRaw.map(t => (String(t).includes(":") ? String(t).split(":")[1] : String(t)).toUpperCase());

      // Build jobs
      let jobs;
      if (includeEGX30) {
        const mainTickers = tickersRaw.filter(t => String(t).toUpperCase() !== "EGX30");

        jobs = [
          { key: "5Y", run: async () => await buildDailyTables(clientRef, ctx, mainTickers, 5, "5Y") },
          { key: "1Y", run: async () => await buildDailyTables(clientRef, ctx, mainTickers, 1, "1Y") },
          { key: "6M", run: async () => await buildSixMonthDaily(clientRef, ctx, mainTickers, "6M") },
          { key: "1M", run: async () => await buildIntradayUnion(clientRef, ctx, mainTickers, { tf: "60", daysBack: 28, label: "1M_Hourly_Union" }) },
          { key: "1W", run: async () => await buildIntradayUnion(clientRef, ctx, mainTickers, { tf: "15", daysBack: 7,  label: "1W_15min_Union" }) },
          { key: "5Y_EGX30_ONLY", run: async () => await buildDailyTables(clientRef, ctx, ["EGX30"], 5, "5Y_EGX30_ONLY") },
        ];
      } else {
        jobs = [
          { key: "5Y", run: async () => await buildDailyTables(clientRef, ctx, tickersRaw, 5, "5Y") },
          { key: "1Y", run: async () => await buildDailyTables(clientRef, ctx, tickersRaw, 1, "1Y") },
          { key: "6M", run: async () => await buildSixMonthDaily(clientRef, ctx, tickersRaw, "6M") },
          { key: "1M", run: async () => await buildIntradayUnion(clientRef, ctx, tickersRaw, { tf: "60", daysBack: 28, label: "1M_Hourly_Union" }) },
          { key: "1W", run: async () => await buildIntradayUnion(clientRef, ctx, tickersRaw, { tf: "15", daysBack: 7,  label: "1W_15min_Union" }) },
        ];
      }

      // If exactly one ticker → add 1D/1m job
      if (tickersRaw.length === 1) {
        jobs.push({ key: "1D", run: async () => await buildOneDayOneMinute(clientRef, ctx, tickersRaw[0]) });
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

      // ✅ return format EXACTLY like before
      return res.json(jsonPerTicker);

    } catch (err) {
      console.error("❌ /fetch crashed:", err);
      return res.status(500).json({ error: "internal_error", detail: String(err?.message || err) });

    } finally {
      destroyTvClient(clientRef.tv);
    }
  });
});

// ===== TTS (unchanged) =====
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
