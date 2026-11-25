// Early-runner rider for the pump.fun replay (agent-fun/replay/v2).
//
// What the dev window taught me (1,222 coins, 24h of pump.fun chain data):
// most coins bleed out, 28 of the 35 that complete do so inside their create
// slot (bundled launches no bot can catch), and only 7 complete organically.
// The coins that DO pay are the ones showing genuine traction in their first
// ~100 slots: real SOL >= 15, >= 30 trades, >= 15 distinct buyers, price
// >= 2x their create price. A few of those run +100-300%; the rest must be
// cut fast, because a coin can dump -85% inside the 2-slot fill latency and
// no stop-loss can catch that. So this bot (1) refuses to chase spikes — a
// buy whose price jumped more than 3% in the latency is rejected outright —
// (2) holds at most ~500 slots, and (3) banks gains with a tight trailing
// stop plus a partial take-profit. One entry per coin, no pyramid, no
// re-entry, no dip-buying.
//
// Entry, decided once per coin, at age 100-200 slots since its CreateEvent:
// trades >= 30, distinct buyers >= 15, real SOL >= 15, price >= 2.0x the
// create price. Buy min(0.50 SOL, 1% of the real reserve), at least 0.02 SOL,
// while a position slot (max 5 open) and cash are free.
//
// Exits, checked every batch for every held position (whole position unless
// noted): hard stop when the net sell value falls to 85% of cost; trailing
// stop once the peak net value reaches 115% of cost, sell at 92% of peak;
// take half off at +25% over cost (the rest keeps the same rules, its peak
// scaled down by the sold half); stale exit after 500 slots held. New
// entries stop if equity falls below 80% of starting capital.
//
// Deterministic: a pure function of the batch stream. No clock, no random,
// no network, no environment. Iteration only over insertion-ordered maps,
// so the two graded executions produce byte-identical action logs.

import { LAMPORTS_PER_SOL, runBot, type Batch, type BotState, type OrderInput } from './vendor/makefun-bot.ts';

const SOL = LAMPORTS_PER_SOL;
const STAKE = (SOL * 50n) / 100n; // 0.50 SOL per coin (the 1% reserve cap binds first on small curves)
const MIN_BUY = (SOL * 2n) / 100n; // 0.02 SOL
const AGE_MIN = 100; // slots since CreateEvent before an entry is allowed
const AGE_MAX = 200; // ...and after this the coin is too old to chase
const MIN_TRADES = 30;
const MIN_BUYERS = 15;
const MIN_REAL_SOL = 15n * SOL;
const MIN_RUNUP_NUM = 200n; // price >= 2.0x create price (rational, denominator 100)
const STOP_NUM = 85n; // sell when net value <= 85% of cost
const TRAIL_ARM_NUM = 115n; // trailing arms once peak value >= 115% of cost
const TRAIL_NUM = 92n; // ...then sell when value <= 92% of peak
const TP1_NUM = 125n; // take half off at +25% over cost
const TP1_PART_NUM = 50n; // ...half of the tokens
const MAX_HOLD = 500; // slots
const BUY_SLIP_BPS = 300; // refuse a buy whose fill price jumped >3% in the latency
const SELL_SLIP_BPS = 10000;
const FEE_BPS = 125n; // protocol 95 + creator 30, charged on every fill
const DD_GUARD_NUM = 80n; // stop entering below 80% of starting equity

interface Coin {
  mint: string;
  createSlot: number;
  vq0: bigint;
  vt0: bigint;
  trades: number;
  buyers: Set<string>;
  realSol: bigint;
  lastVq: bigint;
  lastVt: bigint;
  decided: boolean;
}

interface Held {
  openSlot: number;
  peakValue: bigint;
  tp1Done: boolean;
}

// What selling `tokens` against (vq, vt) pays after curve fees.
function sellValue(tokens: bigint, vq: bigint, vt: bigint): bigint {
  if (tokens <= 0n || vt <= 0n || vq <= 0n) return 0n;
  const q = (tokens * vq) / (vt + tokens);
  if (q <= 0n) return 0n;
  const fee = (q * FEE_BPS + 9999n) / 10000n;
  return q > fee ? q - fee : 0n;
}

// One run per process: module-level state is the bot's memory.
const coins = new Map<string, Coin>();
const held = new Map<string, Held>();
const selling = new Set<string>();
let startEquity = 0n;

function onBatch(state: BotState, batch: Batch): OrderInput[] {
  const orders: OrderInput[] = [];
  const nowSlot = batch.slot;

  // Fold this batch's events into per-coin tapes.
  for (const e of batch.events) {
    if (e.event === 'CreateEvent') {
      if (!coins.has(e.mint)) {
        coins.set(e.mint, {
          mint: e.mint,
          createSlot: e.slot,
          vq0: BigInt(e.virtual_sol_reserves),
          vt0: BigInt(e.virtual_token_reserves),
          trades: 0,
          buyers: new Set<string>(),
          realSol: 0n,
          lastVq: BigInt(e.virtual_sol_reserves),
          lastVt: BigInt(e.virtual_token_reserves),
          decided: false,
        });
      }
    } else if (e.event === 'TradeEvent') {
      let c = coins.get(e.mint);
      if (c === undefined) {
        // First seen through a trade: treat this trade's reserves as the base.
        c = {
          mint: e.mint,
          createSlot: e.slot,
          vq0: BigInt(e.virtual_sol_reserves),
          vt0: BigInt(e.virtual_token_reserves),
          trades: 0,
          buyers: new Set<string>(),
          realSol: 0n,
          lastVq: BigInt(e.virtual_sol_reserves),
          lastVt: BigInt(e.virtual_token_reserves),
          decided: false,
        };
        coins.set(e.mint, c);
      }
      c.trades += 1;
      if (e.is_buy) c.buyers.add(e.user);
      c.realSol = BigInt(e.real_sol_reserves);
      c.lastVq = BigInt(e.virtual_sol_reserves);
      c.lastVt = BigInt(e.virtual_token_reserves);
    } else if (e.event === 'CompleteEvent') {
      // The replay closes the position itself; never buy this coin again.
      const c = coins.get(e.mint);
      if (c !== undefined) c.decided = true;
      held.delete(e.mint);
    }
  }

  // A sell already in flight stays in flight: do not re-sell the same
  // tokens while the first sell is still pending (it lands 2 slots later).
  for (const f of batch.fills) {
    if (f.side === 'sell' && f.status !== 'rejected') selling.delete(f.mint);
  }

  // Refresh peak values for held positions from the latest marks.
  for (const [mint, h] of held) {
    const curve = state.curves.get(mint);
    const pos = state.positions.get(mint);
    if (curve === undefined || pos === undefined) continue;
    const v = sellValue(pos.token_amount, curve.virtual_quote_reserves, curve.virtual_token_reserves);
    if (v > h.peakValue) h.peakValue = v;
  }

  // Exits first: free slots and cap losses before considering entries.
  const sellingNow = new Set<string>(selling);
  for (const [mint, pos] of state.positions) {
    if (sellingNow.has(mint)) continue;
    const curve = state.curves.get(mint);
    if (curve === undefined) continue;
    let h = held.get(mint);
    if (h === undefined) {
      h = { openSlot: nowSlot, peakValue: 0n, tp1Done: false };
      held.set(mint, h);
    }
    const value = sellValue(pos.token_amount, curve.virtual_quote_reserves, curve.virtual_token_reserves);
    if (value > h.peakValue) h.peakValue = value;
    const cost = pos.cost_lamports;
    const stopHit = value * 100n <= cost * STOP_NUM;
    const trailHit =
      h.peakValue * 100n >= cost * TRAIL_ARM_NUM && value * 100n <= h.peakValue * TRAIL_NUM;
    const stale = nowSlot - h.openSlot > MAX_HOLD;
    if (stopHit || trailHit || stale) {
      orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: SELL_SLIP_BPS });
      sellingNow.add(mint);
      selling.add(mint);
    } else if (!h.tp1Done && value * 100n >= cost * TP1_NUM) {
      // Bank half of a winner early. The rest keeps the same rules, with
      // its peak scaled down by the sold half so the trail does not fire
      // on the smaller remaining position.
      const sellTokens = (pos.token_amount * TP1_PART_NUM) / 100n;
      if (sellTokens > 0n && sellTokens < pos.token_amount) {
        orders.push({ side: 'sell', mint, token_amount: sellTokens, max_slippage_bps: SELL_SLIP_BPS });
        h.tp1Done = true;
        h.peakValue = (h.peakValue * (100n - TP1_PART_NUM)) / 100n;
      }
    }
  }

  // Equity guard: cash plus the marked value of positions not being sold.
  let equity = state.cash;
  for (const [mint, pos] of state.positions) {
    if (sellingNow.has(mint)) continue;
    const curve = state.curves.get(mint);
    if (curve === undefined) continue;
    equity += sellValue(pos.token_amount, curve.virtual_quote_reserves, curve.virtual_token_reserves);
  }
  if (startEquity === 0n) startEquity = equity;
  const enteringAllowed = equity * 100n >= startEquity * DD_GUARD_NUM;

  // Entries: one clean buy per qualifying coin, decided once for good.
  if (enteringAllowed) {
    const caps = state.hello.caps;
    let free = state.freeCashLamports();
    let slots = caps.max_open_positions - state.committedPositionCount();
    const tx = state.txFeeLamports();
    for (const c of coins.values()) {
      if (orders.length >= caps.max_orders_per_batch || slots <= 0) break;
      if (c.decided) continue;
      const age = nowSlot - c.createSlot;
      if (age < AGE_MIN || age > AGE_MAX) continue;
      if (c.trades < MIN_TRADES || c.buyers.size < MIN_BUYERS) continue;
      if (c.realSol < MIN_REAL_SOL) continue;
      if (c.vt0 <= 0n || c.lastVt <= 0n) continue;
      // price / createPrice >= 2.0, in integers:
      // lastVq * vt0 * 100 >= 200 * lastVt * vq0.
      if (c.lastVq * c.vt0 * 100n < MIN_RUNUP_NUM * c.lastVt * c.vq0) continue;
      const curve = state.curves.get(c.mint);
      if (curve === undefined || curve.complete) {
        c.decided = true;
        continue;
      }
      const cap = state.maxBuyLamports(c.mint);
      const amount = cap < STAKE ? cap : STAKE;
      c.decided = true;
      if (amount < MIN_BUY || free < amount + tx) continue;
      orders.push({ side: 'buy', mint: c.mint, quote_lamports: amount, max_slippage_bps: BUY_SLIP_BPS });
      free -= amount + tx;
      slots -= 1;
    }
  }

  // Record opens for exit tracking (fills land in later batches; openSlot is
  // set at decision time, peak is seeded from the position once it appears).
  for (const o of orders) {
    if (o.side === 'buy' && !held.has(o.mint)) {
      held.set(o.mint, { openSlot: nowSlot, peakValue: 0n, tp1Done: false });
    }
  }
  return orders;
}

await runBot({ onBatch });
