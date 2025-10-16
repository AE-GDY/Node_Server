// =====================
//  EGX Data Fetcher + JSON Export
// =====================

global.WebSocket = require("ws");

const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");
require("dotenv").config();

const TradingView = require("@mathieuc/tradingview");

// --- Custom Logger ---
function log(...args) {
  console.log("[TradingView-EGX]", ...args);
}

// ---- YOUR TICKERS ----
const RAW_TICKERS = `
COMI SWDY EGCH ETEL AMOC
`.trim().split(/\s+/);

// ---- Settings ----
const TICKERS = RAW_TICKERS.map(t => ({ tv: `EGX:${t}`, col: t }));
const FROM = new Date("2020-01-01T00:00:00Z"); // go back 5 years
const TO = new Date();
const TIMEFRAME = "1D";
const SLEEP_MS = 1000;

const client = new TradingView.Client({});
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function waitForFirstUpdate(chart) {
  return new Promise((resolve, reject) => {
    let done = false;
    const timeout = setTimeout(() => {
      if (!done) reject(new Error("Timeout waiting for data"));
    }, 15000);

    chart.onError(err => {
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

function ymd(d) {
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

(async () => {
  try {
    log(`📊 Fetching closes for ${TICKERS.length} tickers...`);

    const table = new Map();
    const jsonData = {}; // <-- new structure for JSON output
    const days = Math.ceil((TO - FROM) / (24 * 3600 * 1000)) + 5;

    for (const { tv, col } of TICKERS) {
      log(`\n▶ Fetching ${col} (${tv})`);
      try {
        const chart = new client.Session.Chart();
        chart.setTimezone("UTC");
        chart.setMarket(tv, { timeframe: TIMEFRAME, range: days });

        const periods = await waitForFirstUpdate(chart);
        chart.delete();

        if (!periods || periods.length === 0) {
          log(`⚠️ No data returned for ${col}`);
          await sleep(SLEEP_MS);
          continue;
        }

        let added = 0;
        const closes = []; // collect close prices for JSON
        for (const p of periods) {
          const dt = new Date(p.time * 1000);
          if (dt < FROM || dt > TO) continue;
          const key = ymd(dt);
          const row = table.get(key) || { Date: key };
          row[col] = p.close;
          table.set(key, row);
          closes.push(p.close);
          added++;
        }

        jsonData[col] = closes; // store in JSON
        log(`✅ Added ${added} closes for ${col}`);
      } catch (e) {
        log(`❌ ${col} failed: ${e.message || e}`);
      }
      await sleep(SLEEP_MS);
    }

    // Assemble rows
    const rows = Array.from(table.values()).sort((a, b) =>
      a.Date < b.Date ? -1 : a.Date > b.Date ? 1 : 0
    );

    // --- Write Excel ---
    const headers = ["Date", ...RAW_TICKERS];
    const wb = XLSX.utils.book_new();
    const normalized = rows.map(r => {
      const obj = { Date: r.Date };
      for (const c of RAW_TICKERS) obj[c] = r[c] ?? "";
      return obj;
    });
    const ws = XLSX.utils.json_to_sheet(normalized, { header: headers });
    XLSX.utils.book_append_sheet(wb, ws, "Closes");

    const outDir = path.join(process.cwd(), "data");
    if (!fs.existsSync(outDir)) fs.mkdirSync(outDir);
    const excelPath = path.join(outDir, "EGX_closes_ALL.xlsx");
    XLSX.writeFile(wb, excelPath);

    // --- Write JSON ---
    const jsonPath = path.join(outDir, "EGX_closes.json");
    fs.writeFileSync(jsonPath, JSON.stringify(jsonData, null, 2));

    log(`\n✅ Wrote ${rows.length} rows -> ${excelPath}`);
    log(`✅ Wrote JSON -> ${jsonPath}`);

    // Print a preview of the JSON (first few closes per ticker)
    log("\n📈 JSON Data Preview:");
    for (const [ticker, closes] of Object.entries(jsonData)) {
      log(`${ticker}: [${closes.slice(0, 5).join(", ")} ... ${closes.slice(-3).join(", ")}]`);
    }

    client.end();
  } catch (e) {
    console.error("❌ Fatal Error:", e);
    try { client.end(); } catch {}
  }
})();