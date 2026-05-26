#!/usr/bin/env node
/**
 * Polymarket 5m BTC Up/Down algo
 * --------------------------------
 * Market: btc-updown-5m only (300s windows)
 * Signal: first two 1m Chainlink closes BOTH > PTB → BUY UP
 * Entry:  limit GTC @ ALGO_LIMIT_PRICE (default 71¢)
 *
 * State JSON → ../data/algo_state.json (Flask /algo dashboard)
 */

import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import WebSocket from "ws";
import { ClobClient, OrderType, Side } from "@polymarket/clob-client-v2";
import { createWalletClient, http } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { polygon } from "viem/chains";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "..");
const STATE_PATH = path.join(REPO_ROOT, "data", "algo_state.json");
const TRADED_SLUGS_PATH = path.join(REPO_ROOT, "data", "algo_traded_slugs.json");

const RTDS_WS = "wss://ws-live-data.polymarket.com";
const GAMMA = "https://gamma-api.polymarket.com";
const CLOB_HOST = "https://clob.polymarket.com";
const CHAIN_ID = 137;

const LIVE = String(process.env.ALGO_LIVE || "0") === "1";
const LIMIT_PRICE = Number(process.env.ALGO_LIMIT_PRICE || "0.71");
const ORDER_SIZE = Number(process.env.ALGO_ORDER_SIZE || "1");
const SPAN_SEC = Number(process.env.ALGO_MARKET_SPAN_SEC || "300");
const SLUG_PREFIX = process.env.ALGO_SLUG_PREFIX || "btc-updown-5m";
const TICK_SIZE = "0.01";

/** @type {Array<{tsMs: number, tsSec: number, price: number}>} */
const ticks = [];
const MAX_TICKS = 200_000;

/** @type {Set<string>} one order attempt max per 5m slug — persisted across restarts */
const tradedSlugs = loadTradedSlugs();

function loadTradedSlugs() {
  try {
    if (fs.existsSync(TRADED_SLUGS_PATH)) {
      const arr = JSON.parse(fs.readFileSync(TRADED_SLUGS_PATH, "utf8"));
      if (Array.isArray(arr)) return new Set(arr);
    }
  } catch {
    /* ignore */
  }
  return new Set();
}

function persistTradedSlugs() {
  fs.mkdirSync(path.dirname(TRADED_SLUGS_PATH), { recursive: true });
  fs.writeFileSync(TRADED_SLUGS_PATH, JSON.stringify([...tradedSlugs], null, 2));
}

function markSlugTraded(slug, reason) {
  if (tradedSlugs.has(slug)) return false;
  tradedSlugs.add(slug);
  persistTradedSlugs();
  state.traded_slugs = [...tradedSlugs];
  state.trade_taken_for_current_window = true;
  logEvent("LOCK", `One trade only for ${slug} — ${reason}`);
  return true;
}

/** @type {Record<string, unknown>} */
let state = {
  running: true,
  dry_run: !LIVE,
  live_trading: LIVE,
  strategy: {
    market: "BTC Up/Down 5m only",
    rule: "m1 & m2 Chainlink close > PTB → BUY UP",
    limit_price: LIMIT_PRICE,
    order_size_shares: ORDER_SIZE,
    max_trades_per_window: 1,
  },
  traded_slugs: [...tradedSlugs],
  trade_taken_for_current_window: false,
  chainlink_price: null,
  chainlink_ts_iso: null,
  current_market: null,
  candle1_close: null,
  candle2_close: null,
  ptb_usd: null,
  signal: "PENDING",
  signal_ready: false,
  seconds_into_window: null,
  orders: [],
  events: [],
  last_error: null,
  updated_at: new Date().toISOString(),
};

function logEvent(type, msg, extra = {}) {
  const row = { ts: new Date().toISOString(), type, msg, ...extra };
  state.events.unshift(row);
  state.events = state.events.slice(0, 200);
  console.log(`[${type}] ${msg}`, extra.error || "");
  flushState();
}

function flushState() {
  state.updated_at = new Date().toISOString();
  fs.mkdirSync(path.dirname(STATE_PATH), { recursive: true });
  fs.writeFileSync(STATE_PATH, JSON.stringify(state, null, 2));
}

function recordTick(tsMs, price) {
  const tsSec = Math.floor(tsMs / 1000);
  ticks.push({ tsMs, tsSec, price });
  if (ticks.length > MAX_TICKS) ticks.splice(0, ticks.length - MAX_TICKS);
  state.chainlink_price = price;
  state.chainlink_ts_iso = new Date(tsMs).toISOString();
}

function lastBtcInRange(t0Sec, t1Sec) {
  let best = null;
  for (let i = ticks.length - 1; i >= 0; i--) {
    const t = ticks[i];
    if (t.tsSec < t0Sec) break;
    if (t.tsSec >= t0Sec && t.tsSec < t1Sec) {
      best = t.price;
      break;
    }
  }
  return best;
}

async function fetchJson(url) {
  const r = await fetch(url, { headers: { "User-Agent": "poly-5m-algo/1.0" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}

async function fetchPtb(slug) {
  try {
    const d = await fetchJson(`https://polymarket.com/api/crypto/price-to-beat/${slug}`);
    const p = Number(d?.price);
    return Number.isFinite(p) && p > 0 ? p : null;
  } catch {
    return null;
  }
}

async function discoverActive5mMarket() {
  const now = Math.floor(Date.now() / 1000);
  const base = Math.floor(now / SPAN_SEC) * SPAN_SEC;
  const candidates = [base - SPAN_SEC, base, base + SPAN_SEC, base + 2 * SPAN_SEC];

  for (const ws of candidates) {
    const slug = `${SLUG_PREFIX}-${ws}`;
    let data;
    try {
      data = await fetchJson(`${GAMMA}/events?slug=${slug}`);
    } catch {
      continue;
    }
    if (!Array.isArray(data) || !data.length) continue;
    const ev = data[0];
    if (!ev?.active) continue;
    const mkts = ev.markets || [];
    if (!mkts.length) continue;
    const m = mkts[0];
    let tokens = m.clobTokenIds;
    if (typeof tokens === "string") {
      try {
        tokens = JSON.parse(tokens);
      } catch {
        continue;
      }
    }
    if (!Array.isArray(tokens) || tokens.length < 2) continue;

    const windowEnd = ws + SPAN_SEC;
    if (windowEnd <= now) continue;

    let ptb = m.startingPrice ? Number(m.startingPrice) : null;
    const ptbApi = await fetchPtb(slug);
    if (ptbApi != null) ptb = ptbApi;

    return {
      slug,
      title: ev.title || slug,
      window_start: ws,
      window_end: windowEnd,
      up_token: tokens[0],
      down_token: tokens[1],
      ptb_usd: ptb,
      seconds_left: windowEnd - now,
    };
  }
  return null;
}

function evaluateSignal(market) {
  const ws = market.window_start;
  const ptb = market.ptb_usd;
  const c1 = lastBtcInRange(ws, ws + 60);
  const c2 = lastBtcInRange(ws + 60, ws + 120);
  state.candle1_close = c1;
  state.candle2_close = c2;
  state.ptb_usd = ptb;

  const now = Math.floor(Date.now() / 1000);
  state.seconds_into_window = now - ws;
  state.signal_ready = now >= ws + 120;

  if (c1 == null || c2 == null || ptb == null) {
    state.signal = state.signal_ready ? "NO_DATA" : "PENDING";
    return null;
  }
  if (c1 > ptb && c2 > ptb) {
    state.signal = "UP";
    return "UP";
  }
  if (c1 < ptb && c2 < ptb) {
    state.signal = "DOWN";
    return null; // we only trade UP at 71c per spec
  }
  state.signal = "NONE";
  return null;
}

const PLACEHOLDER_CREDS = new Set([
  "550e8400-e29b-41d4-a716-446655440000",
  "base64EncodedSecretString",
  "randomPassphraseString",
]);

async function buildClobClient() {
  const pk = process.env.PRIVATE_KEY;
  if (!pk || pk === "0x..." || pk.length < 66) {
    throw new Error("PRIVATE_KEY missing or invalid in algo/.env");
  }

  const account = privateKeyToAccount(/** @type {`0x${string}`} */ (pk));
  const walletClient = createWalletClient({
    account,
    chain: polygon,
    transport: http(process.env.POLYGON_RPC || "https://polygon-rpc.com"),
  });

  const creds =
    process.env.CLOB_API_KEY && process.env.CLOB_SECRET && process.env.CLOB_PASS_PHRASE
      ? (() => {
          if (
            PLACEHOLDER_CREDS.has(process.env.CLOB_API_KEY) ||
            PLACEHOLDER_CREDS.has(process.env.CLOB_SECRET) ||
            PLACEHOLDER_CREDS.has(process.env.CLOB_PASS_PHRASE)
          ) {
            throw new Error(
              "CLOB creds look like Polymarket doc examples — run npm run setup-creds with your PRIVATE_KEY",
            );
          }
          return {
            key: process.env.CLOB_API_KEY,
            secret: process.env.CLOB_SECRET,
            passphrase: process.env.CLOB_PASS_PHRASE,
          };
        })()
      : await new ClobClient({
          host: CLOB_HOST,
          chain: CHAIN_ID,
          signer: walletClient,
        }).createOrDeriveApiKey();

  const opts = {
    host: CLOB_HOST,
    chain: CHAIN_ID,
    signer: walletClient,
    creds,
    throwOnError: true,
  };
  if (process.env.FUNDER_ADDRESS) {
    opts.funder = process.env.FUNDER_ADDRESS;
    opts.signatureType = Number(process.env.SIGNATURE_TYPE || "1");
  }

  return new ClobClient(opts);
}

async function placeUpOrder(market) {
  const slug = market.slug;
  if (tradedSlugs.has(slug)) {
    state.trade_taken_for_current_window = true;
    return;
  }

  const orderMeta = {
    slug,
    side: "UP",
    token_id: market.up_token,
    price: LIMIT_PRICE,
    size: ORDER_SIZE,
    ts: new Date().toISOString(),
    dry_run: !LIVE,
    candle1_close: state.candle1_close,
    candle2_close: state.candle2_close,
    ptb_usd: state.ptb_usd,
  };

  // Lock immediately — never more than one attempt per 5m window (even on error/restart).
  if (!markSlugTraded(slug, "single UP @ 71¢ allowed per window")) return;

  if (!LIVE) {
    orderMeta.status = "DRY_RUN";
    orderMeta.message = "Paper order — set ALGO_LIVE=1 to submit";
    state.orders.unshift(orderMeta);
    logEvent("DRY_ORDER", `Would BUY UP @ ${LIMIT_PRICE} on ${slug}`, orderMeta);
    return;
  }

  try {
    const client = await buildClobClient();
    const resp = await client.createAndPostOrder(
      {
        tokenID: market.up_token,
        price: LIMIT_PRICE,
        side: Side.BUY,
        size: ORDER_SIZE,
      },
      { tickSize: TICK_SIZE },
      OrderType.GTC,
    );
    orderMeta.status = "SUBMITTED";
    orderMeta.response = resp;
    orderMeta.order_id = resp?.orderID || resp?.id || null;
    state.orders.unshift(orderMeta);
    logEvent("ORDER", `BUY UP @ ${LIMIT_PRICE} submitted`, { order_id: orderMeta.order_id });
  } catch (err) {
    orderMeta.status = "ERROR";
    orderMeta.error = String(err?.message || err);
    state.orders.unshift(orderMeta);
    state.last_error = orderMeta.error;
    logEvent("ERROR", "Order failed", { error: orderMeta.error });
  }
}

function startChainlinkWs() {
  const ws = new WebSocket(RTDS_WS, {
    headers: { Origin: "https://polymarket.com" },
  });

  ws.on("open", () => {
    ws.send(
      JSON.stringify({
        action: "subscribe",
        subscriptions: [
          {
            topic: "crypto_prices_chainlink",
            type: "*",
            filters: '{"symbol":"btc/usd"}',
          },
        ],
      }),
    );
    logEvent("WS", "Chainlink RTDS connected");
  });

  ws.on("message", (raw) => {
    if (raw.toString() === "PONG") return;
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (msg?.topic !== "crypto_prices_chainlink") return;
    const payload = msg.payload || {};
    const ingest = (item) => {
      if (!item || typeof item !== "object") return;
      const price = Number(item.value ?? item.price);
      const ts = Number(item.timestamp ?? item.ts ?? Date.now());
      if (Number.isFinite(price) && price > 0) recordTick(ts, price);
    };
    if (Array.isArray(payload.data)) payload.data.forEach(ingest);
    else ingest(payload);
  });

  ws.on("close", () => {
    logEvent("WS", "Chainlink disconnected — reconnecting in 2s");
    setTimeout(startChainlinkWs, 2000);
  });

  ws.on("error", (e) => {
    state.last_error = String(e?.message || e);
  });

  setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) ws.send("PING");
  }, 5000);
}

async function mainLoop() {
  try {
    const market = await discoverActive5mMarket();
    state.current_market = market;
    if (!market) {
      state.signal = "NO_MARKET";
      state.trade_taken_for_current_window = false;
      flushState();
      return;
    }

    // New 5m window → reset per-window UI flag (slug lock stays in tradedSlugs forever for that slug)
    state.trade_taken_for_current_window = tradedSlugs.has(market.slug);

    const sig = evaluateSignal(market);
    flushState();

    if (
      sig === "UP" &&
      state.signal_ready &&
      !tradedSlugs.has(market.slug)
    ) {
      await placeUpOrder(market);
    }
  } catch (err) {
    state.last_error = String(err?.message || err);
    logEvent("ERROR", "mainLoop", { error: state.last_error });
  }
}

async function main() {
  const hasKey = Boolean(process.env.PRIVATE_KEY && String(process.env.PRIVATE_KEY).startsWith("0x"));
  state.config_note = LIVE
    ? hasKey
      ? "Live trading enabled (ALGO_LIVE=1)"
      : "ALGO_LIVE=1 but PRIVATE_KEY missing — orders will fail"
    : hasKey
      ? "Dry-run: set ALGO_LIVE=1 in algo/.env for real orders"
      : "Dry-run: add PRIVATE_KEY to algo/.env then ALGO_LIVE=1 for live";

  console.log("=".repeat(60));
  console.log("  Polymarket 5m algo — BTC Up/Down");
  console.log(`  LIVE trading: ${LIVE ? "YES ⚠" : "NO (dry-run)"}`);
  console.log(`  One trade max per 5m slug (persisted)`);
  console.log(`  Rule: 2×1m closes > PTB → BUY UP @ ${LIMIT_PRICE}`);
  console.log(`  State → ${STATE_PATH}`);
  if (!hasKey) console.log("  ⚠ PRIVATE_KEY not set — live orders disabled");
  console.log("=".repeat(60));

  logEvent("START", state.config_note, { live: LIVE, has_key: hasKey });
  startChainlinkWs();
  await mainLoop();
  setInterval(mainLoop, 1000);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
