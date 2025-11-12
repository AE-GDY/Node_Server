const fs   = require("fs");
const path = require("path");
const XLSX = require("xlsx");
const { DateTime } = require("luxon");
require("dotenv").config();
const TradingView = require("@mathieuc/tradingview");

// ===== CONFIG =====
const MARKET_SYM = (t) => (t.includes(":") ? t : `EGX:${t}`);
const TICKER_CONCURRENCY = Number(process.env.TICKER_CONCURRENCY || 33);
const TF_CONCURRENCY     = Number(process.env.TF_CONCURRENCY     || 6);
const TIMEOUT_MS         = Number(process.env.TIMEOUT_MS         || 40000);
const MAX_RETRIES        = Number(process.env.MAX_RETRIES        || 2);
const INITIAL_BACKOFF_MS = Number(process.env.INITIAL_BACKOFF_MS || 500);
const FILL_LEADING_DEFAULT = /^true$/i.test(process.env.FILL_LEADING || "true");
const OUT_XLSX = path.join(process.cwd(), "yarab3.xlsx");
const OUT_JSON = path.join(process.cwd(), "yarab3.json");
const TZ = "Africa/Cairo";

// EGX reference ticker for trading-day calendar
const REF_TICKER = "COMI";

// EGX local slots
const DAY_15M_SLOTS = [
  "10:00:00","10:15:00","10:30:00","10:45:00","11:00:00","11:15:00","11:30:00","11:45:00",
  "12:00:00","12:15:00","12:30:00","12:45:00","13:00:00","13:15:00","13:30:00","13:45:00",
  "14:00:00","14:15:00"
];
const DAY_60M_SLOTS = ["10:00:00","11:00:00","12:00:00","13:00:00","14:00:00"];

// ===== GLOBALS =====
let GLOBAL_COMI_DAY = null;  // "yyyy-MM-dd" of latest COMI 1D
let GLOBAL_COMI_TS  = null;  // epoch seconds of latest COMI bar

// ===== UTILS =====
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));
function jitter(min=3,max=10){return min+Math.floor(Math.random()*(max-min+1));}
function ymdCairo(date) { return DateTime.fromJSDate(date, {zone: TZ}).toISODate(); }
function fmtLocalDateTime(date){ return DateTime.fromJSDate(date,{zone:TZ}).toFormat("yyyy-LL-dd HH:mm:ss"); }
function toLocalKey(dt){ return dt.setZone(TZ).toFormat("yyyy-LL-dd HH:mm:ss"); }
function localNow() { return DateTime.now().setZone(TZ); }
function endOfCairoDay(ts){ return DateTime.fromSeconds(ts, {zone:TZ}).endOf("day"); }
function dateToYMD(dt){ return dt.setZone(TZ).toISODate(); }

function logEnvCheck() {
  console.log(`🔧 ENV: SESSION=${!!process.env.SESSION} SIGNATURE=${!!process.env.SIGNATURE}  TF_CONCURRENCY=${TF_CONCURRENCY}  TICKER_CONCURRENCY=${TICKER_CONCURRENCY}  TIMEOUT_MS=${TIMEOUT_MS}`);
}

async function mapPool(items, limit, worker){
  const results = new Array(items.length);
  let idx=0, active=0;
  return new Promise((resolve)=>{
    const launchNext=()=>{
      if(idx>=items.length && active===0) return resolve(results);
      while(active<limit && idx<items.length){
        const cur=idx++; active++;
        sleep(jitter()).then(()=>Promise.resolve(worker(items[cur],cur))
          .then(res=>{results[cur]=res;})
          .catch(err=>{results[cur]={error:String(err?.message||err)};})
          .finally(()=>{active--; launchNext();}));
      }
    };
    launchNext();
  });
}

// ===== TRADINGVIEW CLIENT =====
const client = new TradingView.Client({
  token: process.env.SESSION,
  signature: process.env.SIGNATURE,
});

// Robust fetch with timeout + limited retries
async function fetchBarsOnce(symbol, { timeframe, range, toUnix }) {
  let attempt = 0;
  while (attempt < MAX_RETRIES) {
    attempt++;
    let chart, timeoutId, settled = false;
    const t0 = Date.now();
    try {
      chart = new client.Session.Chart();
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
        timeoutId = setTimeout(() => done(reject, new Error("timeout")), TIMEOUT_MS);
      });

      try { chart.delete(); } catch {}
      if (!periods || !periods.length) throw new Error("no-data");

      console.log(`✅ Success ${symbol} [tf=${timeframe}] in ${Date.now()-t0} ms (attempt ${attempt}/${MAX_RETRIES})`);
      return periods;
    } catch (e) {
      console.log(`error catched: ${e}`)
      try { if (chart) chart.delete(); } catch {}
      if (attempt >= MAX_RETRIES) {
        console.log(`❌ FAILED ${symbol} [tf=${timeframe}] after ${attempt} attempts (last error: ${e.message || e})`);
        return [];
      }
      const backoff = INITIAL_BACKOFF_MS * Math.pow(2, attempt - 1);
      console.log(`🔁 Retry ${symbol} [tf=${timeframe}] attempt ${attempt}/${MAX_RETRIES} — ${e.message || e} → backoff ${backoff}ms`);
      await sleep(backoff);
    }
  }
  return [];
}

function barsToDailyMapCairo(bars){
  const map = new Map();
  for (const b of bars){
    const d = DateTime.fromSeconds(b.time, {zone: TZ}).toISODate();
    map.set(d, b.close);
  }
  return map;
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
function intradayBarsNeeded(days, perDay){
  return days * perDay + 64;
}

async function prefetchGlobalComiLatest() {
  const end = localNow().endOf("day");
  const toUnix = Math.floor(end.toSeconds());

  // Try small → larger ranges only if necessary
  const CANDIDATE_RANGES = [16, 64, 200, 400];

  let latestTs = 0, usedRange = null, barsGot = 0, lastYMD = null;

  for (const R of CANDIDATE_RANGES) {
    console.log(`🧭 Prefetch COMI 1D (probe) to=${toUnix} range=${R}`);
    const dBars = await fetchBarsOnce(MARKET_SYM(REF_TICKER), {
      timeframe: "1D",
      range: R,
      toUnix
    });

    barsGot = dBars?.length || 0;
    if (!barsGot) continue;

    // Take the maximum timestamp explicitly (arrays may be unsorted)
    for (const b of dBars) {
      if (b && typeof b.time === "number" && b.time > latestTs) latestTs = b.time;
    }
    if (!latestTs) continue;

    // Heuristic: if the latest bar is within ~7 calendar days of 'end',
    // we assume we captured the true latest and stop widening.
    const lastDT = DateTime.fromSeconds(latestTs, { zone: TZ });
    const daysGap = end.diff(lastDT.endOf("day"), "days").days; // negative if lastDT is 'today'
    usedRange = R;

    if (daysGap <= 7) break; // good enough; don't overfetch more
    // else loop to widen range and retry
  }

  if (!latestTs) {
    console.warn("⚠️ COMI 1D prefetch returned empty across all probes.");
    GLOBAL_COMI_DAY = null;
    GLOBAL_COMI_TS  = null;
    return;
  }

  GLOBAL_COMI_TS  = latestTs;
  GLOBAL_COMI_DAY = DateTime.fromSeconds(latestTs, { zone: TZ }).toISODate();
  console.log(`🧭 COMI latest (1D): ${GLOBAL_COMI_DAY} (ts=${latestTs}) via range=${usedRange}, bars=${barsGot}`);
}


// ✨ helper used by builders
async function getLatestTradingDateFromCOMI() {
  if (!GLOBAL_COMI_DAY) await prefetchGlobalComiLatest();
  return GLOBAL_COMI_DAY;
}

// ===== FILLERS =====

// Null-until-first-trade, then FF (per day).
// If isLiveDay=true: slots after nowKey are forced null.
// NEW: leadingSeed seeds the day if no trade happened yet (e.g., no trades today).
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

    // future slots on live day => null (keep the row but no value)
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


async function buildOneDayOneMinute(singleTickerRaw) {
  console.log(`📊 Build start: 1D_1min for ${singleTickerRaw}`);
  const tStart = Date.now();
  const tk = (singleTickerRaw.includes(":") ? singleTickerRaw.split(":")[1] : singleTickerRaw).toUpperCase();

  // Ensure COMI latest trading day is known
  if (!GLOBAL_COMI_DAY) await prefetchGlobalComiLatest();
  if (!GLOBAL_COMI_DAY) {
    console.warn("⚠️ Could not detect COMI day; building empty 1D grid.");
    return { ["1D_1min_Union"]: [["DateTime", tk]] };
  }

  const todayYMD = GLOBAL_COMI_DAY;
  console.log(`🧭 Latest COMI day (anchor): ${todayYMD}`);

  // Grid 10:00..14:29 (inclusive)
  const dayStart = DateTime.fromISO(`${todayYMD}T10:00:00`, { zone: TZ });
  const dayEnd   = DateTime.fromISO(`${todayYMD}T14:29:00`, { zone: TZ });
  const allKeys = [];
  for (let t = dayStart; t <= dayEnd; t = t.plus({ minutes: 1 })) {
    allKeys.push(t.toFormat("yyyy-LL-dd HH:mm:ss"));
  }
  const firstKeyOfDay = allKeys[0];

  const nowLocal = DateTime.now().setZone(TZ);
  const isLiveDay = (todayYMD === nowLocal.toISODate());
  // TradingView is ~15m delayed → the latest *available* minute is now-15m
  const delayedNow = nowLocal.minus({ minutes: 15 });
  const nowKey = (isLiveDay ? delayedNow.toFormat("yyyy-LL-dd HH:mm:ss") : null);

  // Fetch enough 1m history to find a seed BEFORE the day starts
  // (bigger range to cover weekends/holidays)
  const toUnix = Math.floor((isLiveDay ? nowLocal : dayEnd).toSeconds());
  const RANGE  = 3000; // ~2+ days of minutes; safe across gaps
  console.log(`➡️  Fetching ${tk} 1m range=${RANGE} to=${toUnix}`);
  const bars = await fetchBarsOnce(MARKET_SYM(tk), { timeframe: "1", range: RANGE, toUnix });

  // Build a sorted list of all retrieved minute bars (localized keys)
  const rows = (bars || [])
    .map(b => ({
      k: DateTime.fromSeconds(b.time, { zone: TZ }).toFormat("yyyy-LL-dd HH:mm:ss"),
      c: b.close
    }))
    .sort((a, b) => a.k.localeCompare(b.k));

  // Find seed = latest trade strictly BEFORE the day's first slot
  let leadingSeed = null;
  for (let i = rows.length - 1; i >= 0; i--) {
    if (rows[i].k < firstKeyOfDay) {
      leadingSeed = rows[i].c;
      break;
    }
  }

  // Map only bars that fall inside today’s grid
  const mm = new Map();
  const minKey = allKeys[0];
  const maxKey = allKeys[allKeys.length - 1];
  for (const r of rows) {
    if (r.k >= minKey && r.k <= maxKey) mm.set(r.k, r.c);
  }

  // Fill using the seed (so a no-trade day gets yesterday’s last close)
  const filled = fillDaySeries(allKeys, mm, { isLiveDay, nowKey, leadingSeed });

  const header = ["DateTime", tk];
  const table  = [header, ...allKeys.map((k, i) => [k, filled[i] ?? null])];

  console.log(`🏁 Build done: 1D_1min in ${Date.now() - tStart} ms (rows=${table.length - 1})`);
  return { ["1D_1min_Union"]: table };
}


// ===== 1W / 1M intraday unions (COMI calendar, EOD anchor, FF past days, live future nulls) =====
async function buildIntradayUnion(tickersRaw, { tf, daysBack, label }) {
  const tfStr = String(tf); // <- normalize once
  const t0 = Date.now();

  try {
    console.log(`📊 Build start: ${label} for ${tickersRaw.length} tickers (tf=${tfStr})`);

    // slots & EOD
    const SLOT_LIST = tfStr === "15"
      ? DAY_15M_SLOTS
      : tfStr === "60"
      ? DAY_60M_SLOTS
      : (() => { throw new Error(`Unsupported tf=${tfStr} (expected "15" or "60")`); })();

    const EOD_SLOT   = tfStr === "15" ? "14:15:00" : "14:00:00";
    const keyFor     = (ymd, hhmmss) => `${ymd} ${hhmmss}`;

    // latest COMI trading day
    const lastYMD = await getLatestTradingDateFromCOMI();
    if (!lastYMD) {
      console.warn(`⚠️ ${label}: cannot detect COMI latest day`);
      return { [label]: [["DateTime", ...tickersRaw.map(t => (t.includes(":") ? t.split(":")[1] : t).toUpperCase())]] };
    }
    console.log(`🧭 ${label} latest COMI day: ${lastYMD}`);

    // anchor = (lastYMD - daysBack) @ EOD slot
    const lastDay   = DateTime.fromISO(lastYMD, { zone: TZ }).startOf("day");
    const anchorDay = lastDay.minus({ days: daysBack });
    const anchorKey = keyFor(anchorDay.toISODate(), EOD_SLOT);

    // active days using COMI 1D
    const { range: dRange, toUnix: dTo } = calc1DRangeToCoverMonths(2);
    const comi1d = await fetchBarsOnce(MARKET_SYM(REF_TICKER), { timeframe: "1D", range: dRange, toUnix: dTo });
    const activeDays = new Set((comi1d || []).map(b => DateTime.fromSeconds(b.time, { zone: TZ }).toISODate()));

    const dayList = [];
    for (let d = anchorDay.plus({ days: 1 }); d <= lastDay; d = d.plus({ days: 1 })) {
      const ymd = d.toISODate();
      if (activeDays.has(ymd)) dayList.push(ymd);
    }
    if (!dayList.length) console.warn(`⚠️ ${label}: no active days between anchor and lastYMD`);

    // fetch intraday once per ticker
    const perDayBars = tfStr === "15" ? 24 : 5;
    const RANGE = (daysBack + 1) * perDayBars + 256; // buffer
    const toUnix = Math.floor(lastDay.endOf("day").toSeconds());

    console.log(`➡️  Fetching tf=${tfStr} range=${RANGE} to=${toUnix}`);
    const perTicker = await mapPool(tickersRaw, TICKER_CONCURRENCY, async (t) => {
      const sym  = MARKET_SYM(t);
      const bars = await fetchBarsOnce(sym, { timeframe: tfStr, range: RANGE, toUnix });
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
        ticker: (t.includes(":") ? t.split(":")[1] : t).toUpperCase(),
        rows, byKey, byDay
      };
    });

    const header = ["DateTime", ...perTicker.map(p => p.ticker)];
    const out = [header];

    // anchor row — EOD slot only; value = latest <= anchorKey
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

    // live cut: include all slots but set future = null
    const nowLocal = DateTime.now().setZone(TZ);
    const isLiveDay = nowLocal.toISODate() === lastYMD;

    // TradingView delay: treat (now - 15m) as the latest available time
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

    // emit rows per active day, per slot
    for (const ymd of dayList) {
      const slots = SLOT_LIST.slice(); // include all grid slots

      // per-ticker seeds = latest < first slot of the day (fallback: last before this day)
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

      const running = seeds.slice(); // FF state within this day

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
    const hdr = ["DateTime", ...tickersRaw.map(t => (t.includes(":") ? t.split(":")[1] : t).toUpperCase())];
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

// Weekly anchor chooser (your legacy logic kept intact)
function chooseWeeklyAnchorDatesFromMap_old(map, yearsBack, TZ_IN) {
  if (!map || map.size === 0) return [];
  const dates = [...map.keys()].sort();
  const lastYMD = dates.at(-1);
  const _fromYMD = (ymd) => { const [y,m,d] = ymd.split("-").map(Number); return new Date(Date.UTC(y, m-1, d, 0, 0, 0)); };
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
    const weekday = new Date(d.toLocaleString("en-US", { timeZone: TZ_IN })).getDay(); // 0=Sun
    const start = new Date(d);
    start.setUTCDate(start.getUTCDate() - weekday); // back to Sunday
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

async function buildDailyTables(tickers, years, label) {
  console.log(`📊 Build start: ${label} for ${tickers.length} tickers`);
  const tStart = Date.now();

  const { range, toUnix } = calc1DRangeToCoverYears(years);

  // Fetch tickers' 1D
  console.log(`➡️  Fetching DAILY 1D for ${label} (years=${years}, range=${range}, to=${toUnix})`);
  const perTicker = await mapPool(tickers, TICKER_CONCURRENCY, async (t) => {
    const sym  = MARKET_SYM(t);
    const bars = await fetchBarsOnce(sym, { timeframe: "1D", range, toUnix });
    const map  = barsToDailyMapCairo(bars);
    const tk   = (t.includes(":") ? t.split(":")[1] : t).toUpperCase();
    return { ticker: tk, map };
  });

  // COMI reference (reuse if present)
  let comiMap;
  {
    const hit = perTicker.find(x => x.ticker === "COMI");
    if (hit && hit.map && hit.map.size) {
      comiMap = hit.map;
      console.log(`🧭 Using COMI map from selected list`);
    } else {
      console.log(`🧭 Fetching COMI reference separately`);
      const bars = await fetchBarsOnce(MARKET_SYM(REF_TICKER), { timeframe: "1D", range, toUnix });
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
  const ffs    = prepared.map(p =>
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

async function buildSixMonthDaily(tickers, label) {
  console.log(`📊 Build start: ${label} for ${tickers.length} tickers`);
  const tStart = Date.now();

  const { range, toUnix } = calc1DRangeToCoverMonths(6);

  console.log(`➡️  Fetching DAILY 1D for ${label} (months=6, range=${range}, to=${toUnix})`);
  const perTicker = await mapPool(tickers, TICKER_CONCURRENCY, async (t) => {
    const sym  = MARKET_SYM(t);
    const bars = await fetchBarsOnce(sym, { timeframe: "1D", range, toUnix });
    const map  = barsToDailyMapCairo(bars);
    const tk   = (t.includes(":") ? t.split(":")[1] : t).toUpperCase();
    return { ticker: tk, map };
  });

  let comiMap;
  {
    const hit = perTicker.find(x => x.ticker === "COMI");
    if (hit && hit.map && hit.map.size) {
      comiMap = hit.map;
      console.log(`🧭 Using COMI map from selected list`);
    } else {
      console.log(`🧭 Fetching COMI reference separately`);
      const bars = await fetchBarsOnce(MARKET_SYM(REF_TICKER), { timeframe: "1D", range, toUnix });
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


const express = require("express");  // or: 
const app = express();
app.use(express.json());

// ===== MAIN =====
app.post('/fetch',async (req, res)=>{

  console.log("STARTED HERE");

  const startAll = Date.now();
  logEnvCheck();

  const {tickersRaw} = req.body;

  

  console.log(tickersRaw);
  if (!tickersRaw.length){
    console.error("Usage: node yarab3.js <TICKERS>");
    process.exit(1);
  }

  // ---- AUTO-ADD EGX30 IF MULTI-TICKER RUN ----
let includeEGX30 = false;

if (tickersRaw.length > 1) {
  includeEGX30 = true;

  // Avoid duplicates just in case
  if (!tickersRaw.some(t => t.toUpperCase().includes("EGX30"))) {
    tickersRaw.push("EGX30");
  }

  console.log("📈 Auto-included EGX30 for benchmarking (5Y daily only)");
} else {
  console.log("✅ Single ticker run — NOT adding EGX30");
}

  console.log(`🎯 Tickers: ${tickersRaw.join(", ")}`);

  // Prefetch COMI once
  await prefetchGlobalComiLatest();

  const tickers = tickersRaw.map(t => (t.includes(":") ? t.split(":")[1] : t).toUpperCase());

  // Build jobs
let jobs;
if (includeEGX30) {
  const mainTickers = tickersRaw.filter(t => t.toUpperCase() !== "EGX30");

  jobs = [
    // main tickers full set
    { key: "5Y", run: async () => await buildDailyTables(mainTickers, 5, "5Y") },
    { key: "1Y", run: async () => await buildDailyTables(mainTickers, 1, "1Y") },
    { key: "6M", run: async () => await buildSixMonthDaily(mainTickers, "6M") },

    // 1M hourly grid from COMI active dates
    { key: "1M", run: async () =>
        await buildIntradayUnion(mainTickers, { tf: "60", daysBack: 28, label: "1M_Hourly_Union" })
    },

    // 1W 15m grid from COMI active dates
    { key: "1W", run: async () =>
        await buildIntradayUnion(mainTickers, { tf: "15", daysBack: 7, label: "1W_15min_Union" })
    },

    // EGX30 only → fetch 5Y daily *only*
    { key: "5Y_EGX30_ONLY", run: async () => await buildDailyTables(["EGX30"], 5, "5Y_EGX30_ONLY") },
  ];
} else {
  jobs = [
    { key: "5Y", run: async ()=> await buildDailyTables(tickersRaw, 5, "5Y") },
    { key: "1Y", run: async ()=> await buildDailyTables(tickersRaw, 1, "1Y") },
    { key: "6M", run: async ()=> await buildSixMonthDaily(tickersRaw, "6M") },

    // 1M hourly grid from COMI active dates
    { key: "1M", run: async ()=> await buildIntradayUnion(
        tickersRaw,
        { tf:"60", daysBack: 28, label:"1M_Hourly_Union" }
      )
    },

    // 1W 15m grid from COMI active dates
    { key: "1W", run: async ()=> await buildIntradayUnion(
        tickersRaw,
        { tf:"15", daysBack: 7, label:"1W_15min_Union" }
      )
    },
  ];
}


  // If exactly one ticker → add 1D/1m job (grid 10:00..14:29)
  if (tickersRaw.length === 1) {
    jobs.push({ key: "1D", run: async ()=> await buildOneDayOneMinute(tickersRaw[0]) });
  }

  console.log(`🚀 Starting all windows with TF_CONCURRENCY=${TF_CONCURRENCY}`);
  const allTables = await mapPool(jobs, TF_CONCURRENCY, async (j)=>{
    const s = Date.now();
    console.log(`➡️  Window start: ${j.key}`);
    const res = await j.run();
    console.log(`✅ Window done:  ${j.key} in ${Date.now()-s} ms`);
    return res;
  });
  let combined = Object.assign({}, ...allTables);

  // Save XLSX
  const wb = XLSX.utils.book_new();
  for (const [name, table] of Object.entries(combined)) {
  if (!Array.isArray(table) || !Array.isArray(table[0])) {
    console.warn(`⏭️  Skipping sheet "${name}" — not a 2D array (job may have failed).`);
    continue;
  }

  // ✅ shorten long sheet names
  let sheetName = name;
  if (sheetName.length > 31) {
    // custom mapping
    if (sheetName === "5Y_EGX30_ONLY_Daily_Union") {
      sheetName = "5Y_EGX30_ONLY_DU"; // <= 31 chars
    } else {
      sheetName = sheetName.substring(0, 31);
    }
  }

  console.log(`📝 Writing sheet: ${sheetName}  (rows=${table.length-1}, cols=${table[0].length})`);
  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(table), sheetName);
}

  //XLSX.writeFile(wb, OUT_XLSX);
  //console.log(`💾 Saved Excel → ${OUT_XLSX}`);

  // ===== JSON export (keep future points for 1D/1W/1M as 0) =====
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

/**
 * Push a table into JSON.
 * If includeNullsAsZero=true, rows with null are kept with Close=0 (used for 1D/1W/1M future points).
 */
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
        // keep future points with Close=0
        entry.data[jsonKey].push({ Date: ts, Close: 0 });
      }
      // else skip nulls for historical daily/weekly series
    }
  }
}

console.log(`🧩 Building JSON export`);
if (combined["1D_1min_Union"]) pushSeries("1D_1min_Union", "one_day", true);          // keep future minutes as 0
pushSeries("1W_15min_Union",        "one_week",          true);                        // keep future 15m as 0
pushSeries("1M_Hourly_Union",       "one_month",         true);                        // keep future hourly as 0
pushSeries("6M_Daily_Union",        "six_months",        false);
pushSeries("1Y_Weekly_Downsampled", "one_year",          false);
pushSeries("5Y_Weekly_Downsampled", "five_years",        false);
if (combined["5Y_Daily_Union"])     pushSeries("5Y_Daily_Union", "five_years_daily", false);
// If EGX30 was added only for 5Y daily, push its daily data too
if (includeEGX30 && combined["5Y_EGX30_ONLY_Daily_Union"]) {
  pushSeries("5Y_EGX30_ONLY_Daily_Union", "five_years_daily", false);
}


//fs.writeFileSync(OUT_JSON, JSON.stringify(jsonPerTicker, null, 2), "utf8");
//console.log(`💾 Saved JSON  → ${OUT_JSON}`);
res.json(jsonPerTicker);


  console.log(`✅ ALL DONE in ${((Date.now()-startAll)/1000).toFixed(2)} s`);
  //try{ client.end(); }catch{}
});


const PORT = process.env.PORT || 8080;
app.listen(PORT, () =>
  console.log(`✅ Server running on http://localhost:${PORT}`)
);
