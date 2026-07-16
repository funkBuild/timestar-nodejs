// Shared helpers for the API correctness suites (test/correctness/*).
//
// These suites target a dedicated TimeStar instance (default port 58086)
// started from a scratch data directory with a small WAL size threshold
// (TIMESTAR_WAL_THRESHOLD, default 2 MiB) so that memstore->TSM flushes can
// be forced cheaply by volume. They honor TIMESTAR_HOST / TIMESTAR_PORT.
//
// When TIMESTAR_DATA_DIR points at the server's data directory (as in the
// canonical local setup), flushes are VERIFIED by watching for new TSM file
// sequence numbers in every shard_*/tsm directory. Without it, the flush
// helper still writes enough filler volume to exceed the WAL threshold on
// every shard and settles on a timer, but cannot verify.

import * as fs from "fs";
import * as path from "path";
import { TimestarClient } from "../../src/client";
import type { QueryResponse, FieldData, WritePoint } from "../../src/types";

export const HOST = process.env.TIMESTAR_HOST || "localhost";
export const PORT = parseInt(process.env.TIMESTAR_PORT || "58086", 10);
export const DATA_DIR =
  process.env.TIMESTAR_DATA_DIR ||
  "/tmp/claude-1000/-home-matt-Desktop-source-tsdb/f37ff3c3-2d8c-4ec8-8a83-fdc7237839f4/scratchpad/correctness_data";
export const WAL_THRESHOLD = parseInt(process.env.TIMESTAR_WAL_THRESHOLD || String(2 * 1024 * 1024), 10);

// Fixed, exactly-double-representable base timestamp: 1.7e18 ns
// (17 * 5^17 * 2^18 -> 53-bit mantissa fits). Multiple of 1s/5s/10s/1m/1h? It
// is a multiple of 10s but NOT of 1h/1d — bucket starts are computed with
// bucketStart() below rather than assumed.
export const BASE = 1_700_000_000_000_000_000;
export const S = 1_000_000_000; // 1s in ns

export function makeClient(opts: { precise?: boolean; requestTimeoutMs?: number } = {}): TimestarClient {
  return new TimestarClient({
    host: HOST,
    port: PORT,
    requestTimeoutMs: opts.requestTimeoutMs ?? 25_000,
    precise: opts.precise,
  });
}

// Epoch-aligned bucket start (floor), computed in BigInt to stay exact for
// ns-scale timestamps. Returns a Number (all test timestamps are chosen to be
// exactly double-representable).
export function bucketStart(tsNs: number, intervalNs: number): number {
  const t = BigInt(tsNs);
  const i = BigInt(intervalNs);
  return Number((t / i) * i);
}

// ---------------------------------------------------------------------------
// Expected-value math (population semantics, pinned by probing the server)
// ---------------------------------------------------------------------------

export const sum = (xs: number[]) => xs.reduce((a, b) => a + b, 0);
export const avg = (xs: number[]) => sum(xs) / xs.length;
export const min = (xs: number[]) => Math.min(...xs);
export const max = (xs: number[]) => Math.max(...xs);
export const spread = (xs: number[]) => max(xs) - min(xs);
// Median: average of the two middle elements for even counts (probed).
export function median(xs: number[]): number {
  const s = [...xs].sort((a, b) => a - b);
  const n = s.length;
  return n % 2 === 1 ? s[(n - 1) / 2] : (s[n / 2 - 1] + s[n / 2]) / 2;
}
// Population variance/stddev (probed: stdvar([1,2,3,4]) = 1.25).
export const stdvar = (xs: number[]) => {
  const m = avg(xs);
  return sum(xs.map((x) => (x - m) * (x - m))) / xs.length;
};
export const stddev = (xs: number[]) => Math.sqrt(stdvar(xs));

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

// Find a named field across all series of a response (fields of one
// measurement+tags may arrive consolidated or split depending on path).
export function findField(resp: QueryResponse, name: string): FieldData {
  for (const s of resp.series) {
    if (s.fields[name]) return s.fields[name];
  }
  throw new Error(
    `Field '${name}' not found; series=${JSON.stringify(resp.series.map((s) => Object.keys(s.fields)))}`,
  );
}

export function fieldOrNull(resp: QueryResponse, name: string): FieldData | null {
  for (const s of resp.series) {
    if (s.fields[name]) return s.fields[name];
  }
  return null;
}

// ---------------------------------------------------------------------------
// Flush-to-TSM forcing
// ---------------------------------------------------------------------------

function shardTsmDirs(): string[] {
  try {
    return fs
      .readdirSync(DATA_DIR)
      .filter((d) => /^shard_\d+$/.test(d) && fs.statSync(path.join(DATA_DIR, d)).isDirectory())
      .map((d) => path.join(DATA_DIR, d, "tsm"));
  } catch {
    return [];
  }
}

// Per-shard max TSM sequence number. Rollover creates `0_<seq+1>.tsm`, and
// seq grows monotonically even when compaction later merges files (compaction
// outputs reuse lower sequence numbers at a higher tier), so "max seq
// increased in every shard" is a reliable every-shard-flushed signal.
function shardMaxSeqs(): number[] {
  return shardTsmDirs().map((dir) => {
    try {
      let maxSeq = -1;
      for (const f of fs.readdirSync(dir)) {
        const m = /^(\d+)_(\d+)\.tsm$/.exec(f);
        if (m) maxSeq = Math.max(maxSeq, parseInt(m[2], 10));
      }
      return maxSeq;
    } catch {
      return -1;
    }
  });
}

// Fixed per-call filler series set: reusing the same 40 series keeps the
// index cold path off the flush critical path (a run that creates thousands
// of fresh series makes WAL->TSM conversion lag far behind the writes).
let fillerCallCounter = 0;
const FILLER_RUN_ID = `f${process.pid}_${Date.now() % 1e7}`;

// Per-shard list of WAL file numbers (files named `<seq>.wal` in each shard
// directory). A rollover creates a new higher-numbered WAL; when the
// background WAL->TSM conversion of an old WAL completes, that WAL file is
// deleted. "Every shard has exactly one WAL, numbered above its pre-flush
// max" therefore means every pre-existing memstore has been fully converted
// to TSM and dropped — the strongest available "data is now in TSM" signal.
function shardWalNumbers(): number[][] {
  try {
    return fs
      .readdirSync(DATA_DIR)
      .filter((d) => /^shard_\d+$/.test(d) && fs.statSync(path.join(DATA_DIR, d)).isDirectory())
      .map((d) =>
        fs
          .readdirSync(path.join(DATA_DIR, d))
          .map((f) => /^(\d+)\.wal$/.exec(f)?.[1])
          .filter((x): x is string => x !== undefined)
          .map((x) => parseInt(x, 10)),
      );
  } catch {
    return [];
  }
}

// Force a WAL rollover + memstore->TSM conversion on EVERY shard by writing
// incompressible filler until each shard's WAL exceeds its size threshold.
// Filler lives in its own measurement and a far-away time window (~1998) so
// it never pollutes test queries. Returns true when the flush was verified
// via the data directory, false when it was volume+timer based only.
export async function flushToTsm(client: TimestarClient): Promise<boolean> {
  const before = shardMaxSeqs();
  const canVerify = before.length > 0;
  const FILLER_BASE = 900_000_000_000_000_000;
  const fillerMeasurement = `zz_corr_filler_${FILLER_RUN_ID}_${fillerCallCounter++}`;

  // Each chunk: 40 series x 400 random doubles ~= 230 KB of WAL across all
  // shards. Enough chunks to push every shard past WAL_THRESHOLD twice over.
  const chunkBytes = 40 * 400 * 14; // ~14 WAL bytes/point measured
  const shards = Math.max(before.length, 2);
  const maxChunks = Math.max(8, Math.ceil((2.5 * WAL_THRESHOLD * shards) / chunkBytes));

  const rolledOver = () => {
    const after = shardMaxSeqs();
    return after.length === before.length && after.every((seq, i) => seq > before[i]);
  };

  let chunksWritten = 0;
  const writeChunk = async () => {
    const chunkNo = chunksWritten++;
    const writes: WritePoint[] = [];
    for (let s = 0; s < 40; s++) {
      writes.push({
        measurement: fillerMeasurement,
        tags: { s: `s${s}` },
        fields: { v: Array.from({ length: 400 }, () => Math.random() * 1e9) },
        timestamps: Array.from({ length: 400 }, (_, i) => FILLER_BASE + (chunkNo * 400 + i) * S),
      });
    }
    const w = await client.write(writes);
    if (w.status !== "success") throw new Error(`filler write failed: ${w.status} ${w.errors.join("; ")}`);
  };

  const walsBefore = shardWalNumbers();

  // Settle in two stages:
  //  1. Conversion completion: every shard is down to a single WAL file whose
  //     number is above the shard's pre-flush max — all pre-flush memstores
  //     have been converted to TSM and their WAL files deleted (rollover
  //     alone is not enough: queries keep hitting the immutable memstore
  //     until its conversion finishes, so placement-sensitive expectations
  //     would silently test the wrong tier).
  //  2. Visibility: a count over THIS call's filler measurement reports every
  //     written point. This guards against a transient server-side window
  //     right after conversion where a just-converted series can briefly be
  //     invisible to queries (observed intermittently).
  const settle = async () => {
    for (let i = 0; i < 200; i++) {
      const wals = shardWalNumbers();
      const converted =
        wals.length === walsBefore.length &&
        wals.every((nums, s) => {
          const beforeMax = Math.max(-1, ...walsBefore[s]);
          return nums.length === 1 && nums[0] > beforeMax;
        });
      if (converted) break;
      if (i === 199) throw new Error("flushToTsm: WAL->TSM conversions did not complete");
      await sleep(100);
    }

    const expected = chunksWritten * 40 * 400;
    const end = FILLER_BASE + chunksWritten * 400 * S; // exact: both are multiples of 1e9 well below 2^63
    for (let i = 0; i < 100; i++) {
      try {
        const r = await client.query(`count:${fillerMeasurement}(v)`, {
          startTime: FILLER_BASE, endTime: end, aggregationInterval: "30d",
        });
        const f = r.series[0]?.fields?.v;
        const total = f ? (f.values as number[]).reduce((a, b) => a + b, 0) : 0;
        if (total === expected) return;
      } catch {
        // retry
      }
      await sleep(100);
    }
    throw new Error(`flushToTsm: filler not fully visible after flush (${fillerMeasurement})`);
  };

  for (let chunk = 0; chunk < maxChunks; chunk++) {
    await writeChunk();
    if (canVerify && rolledOver()) {
      await settle();
      return true;
    }
  }

  if (canVerify) {
    // All volume written; wait for lagging rollover/conversion.
    for (let i = 0; i < 60; i++) {
      await sleep(250);
      if (rolledOver()) {
        await settle();
        return true;
      }
    }
    throw new Error(
      `flushToTsm: no TSM sequence advance in ${DATA_DIR} after writing ${maxChunks} filler chunks`,
    );
  }
  await sleep(1500);
  return false;
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// SERVER BUG (transient, masked here so the suite stays deterministic):
// immediately after a WAL->TSM conversion there is a short window (tens to
// hundreds of ms, intermittent) where a freshly-converted series returns an
// EMPTY result even though the data is durable and reappears moments later.
// This helper retries a query while it returns zero series, bounded by
// timeoutMs, and returns the last response either way. Use it for reads that
// run right after flushToTsm() and expect data to exist.
export async function queryUntilSeries(
  client: TimestarClient,
  query: string,
  opts: { startTime: number | bigint; endTime: number | bigint; aggregationInterval?: string; precise?: boolean },
  timeoutMs = 5000,
): Promise<QueryResponse> {
  const t0 = Date.now();
  let last = await client.query(query, opts);
  while (last.series.length === 0 && Date.now() - t0 < timeoutMs) {
    await sleep(100);
    last = await client.query(query, opts);
  }
  return last;
}

// Unique measurement prefix per suite run.
export function uniquePrefix(area: string): string {
  return `corr_${area}_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
}
