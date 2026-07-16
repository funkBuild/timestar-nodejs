// Aggregation correctness: every aggregation method verified arithmetically
// over a known dataset, for each of three data placements:
//   (a) fresh memory-store only
//   (b) after TSM flush (WAL rollover forced by volume, verified on disk)
//   (c) spanning both (half flushed to TSM, half in the memory store)
// plus interval/bucketing semantics.
//
// Placement-dependent divergence is exactly the class of bug fixed in the
// server today (derived sub-queries never received aggregationInterval), so
// each aggregation is additionally cross-checked for equality across
// placements.

import { describe, it, expect, beforeAll } from "vitest";
import {
  makeClient, flushToTsm, uniquePrefix, findField, bucketStart, queryUntilSeries,
  BASE, S, sum, avg, min, max, spread, median, stddev, stdvar,
} from "./helpers";
import type { QueryResponse } from "../../src/types";

const client = makeClient();
const P = uniquePrefix("agg");

// Known dataset: negatives, duplicates, a fractional value, non-sorted.
const D = [3, -7, 12.5, 0.25, 42, -1.5, 8, 8, 27.75, -19, 5.5, 33];
const N = D.length;
const TS = Array.from({ length: N }, (_, i) => BASE + i * S);
const HALF = N / 2;

// Whole-dataset expectations, computed arithmetically from D.
const EXPECT: Record<string, { value: number; exact: boolean; ts?: number }> = {
  avg: { value: avg(D), exact: false },
  min: { value: min(D), exact: true },
  max: { value: max(D), exact: true },
  sum: { value: sum(D), exact: false },
  count: { value: N, exact: true },
  // latest/first report the actual point timestamp, not the bucket start (probed).
  latest: { value: D[N - 1], exact: true, ts: TS[N - 1] },
  first: { value: D[0], exact: true, ts: TS[0] },
  median: { value: median(D), exact: false },
  stddev: { value: stddev(D), exact: false },
  stdvar: { value: stdvar(D), exact: false },
  spread: { value: spread(D), exact: true },
};
const METHODS = Object.keys(EXPECT);

// One query per method per placement, captured in beforeAll so that the
// memory-store placement is queried BEFORE any flush happens.
type Placement = "memory" | "tsm" | "spanning";
const results = new Map<Placement, Map<string, QueryResponse>>();
let flushVerified = false;

async function queryAll(measurement: string): Promise<Map<string, QueryResponse>> {
  const out = new Map<string, QueryResponse>();
  for (const m of METHODS) {
    // One 1h bucket covers the whole 12s dataset (verified: bucket boundaries
    // are epoch-aligned multiples of the interval; BASE..BASE+11s crosses no
    // 3600s multiple because BASE mod 3600e9 = 800e9 and 800+11 < 3600).
    // queryUntilSeries: masks the transient post-conversion invisibility
    // window (see helpers.ts) for the tsm/spanning placements.
    out.set(m, await queryUntilSeries(client, `${m}:${measurement}(v)`, {
      startTime: BASE,
      endTime: BASE + N * S,
      aggregationInterval: "1h",
    }));
  }
  return out;
}

beforeAll(async () => {
  expect(await client.isHealthy(), `TimeStar server not reachable at :${process.env.TIMESTAR_PORT ?? 58086}`).toBe(true);

  // (a) memory-store only: write then query immediately, before any flush.
  await client.write({ measurement: `${P}.mem`, tags: { t: "a" }, fields: { v: D }, timestamps: TS });
  results.set("memory", await queryAll(`${P}.mem`));

  // (b) TSM: write, force rollover+conversion, query.
  await client.write({ measurement: `${P}.tsm`, tags: { t: "a" }, fields: { v: D }, timestamps: TS });
  flushVerified = await flushToTsm(client);
  results.set("tsm", await queryAll(`${P}.tsm`));

  // (c) spanning: first half, flush, second half, query.
  await client.write({
    measurement: `${P}.span`, tags: { t: "a" },
    fields: { v: D.slice(0, HALF) }, timestamps: TS.slice(0, HALF),
  });
  await flushToTsm(client);
  await client.write({
    measurement: `${P}.span`, tags: { t: "a" },
    fields: { v: D.slice(HALF) }, timestamps: TS.slice(HALF),
  });
  results.set("spanning", await queryAll(`${P}.span`));
});

describe("aggregation methods x data placement", () => {
  it("TSM flush was actually verified on disk", () => {
    // If this fails, placements (b)/(c) silently degrade to memory-store
    // tests. Set TIMESTAR_DATA_DIR to the server's data directory.
    expect(flushVerified).toBe(true);
  });

  for (const method of METHODS) {
    for (const placement of ["memory", "tsm", "spanning"] as Placement[]) {
      it(`${method} over known dataset [${placement}]`, () => {
        const resp = results.get(placement)!.get(method)!;
        expect(resp.status).toBe("success");
        const f = findField(resp, "v");
        expect(f.values.length).toBe(1);
        const got = f.values[0] as number;
        const exp = EXPECT[method];
        if (exp.exact) expect(got).toBe(exp.value);
        else expect(got).toBeCloseTo(exp.value, 9);
        if (exp.ts !== undefined) {
          expect(f.timestamps[0]).toBe(exp.ts);
        } else {
          // Aggregate buckets are stamped with the epoch-aligned bucket start.
          expect(f.timestamps[0]).toBe(bucketStart(BASE, 3600 * S));
        }
      });
    }

    it(`${method}: all three placements agree exactly`, () => {
      const vals = (["memory", "tsm", "spanning"] as Placement[]).map(
        (p) => findField(results.get(p)!.get(method)!, "v").values[0] as number,
      );
      expect(vals[1]).toBeCloseTo(vals[0], 9);
      expect(vals[2]).toBeCloseTo(vals[0], 9);
    });
  }
});

describe("interval bucketing", () => {
  // NOTE (pinned by probing): the server treats endTime as INCLUSIVE — a
  // point exactly at endTime is part of the result. All expectations below
  // account for that.
  const M = `${P}.buckets`;
  // 13 points every 5s over 60s; values 0..12. BASE is a multiple of 10s.
  const bTs = Array.from({ length: 13 }, (_, i) => BASE + i * 5 * S);
  const bV = Array.from({ length: 13 }, (_, i) => i);

  // Group written points into epoch-aligned buckets of width ivNs, restricted
  // to [startTime, endTime] (endTime inclusive).
  function expectedBuckets(ivNs: number, st: number, en: number) {
    const groups = new Map<number, number[]>();
    bTs.forEach((t, i) => {
      if (t < st || t > en) return;
      const b = bucketStart(t, ivNs);
      groups.set(b, [...(groups.get(b) ?? []), bV[i]]);
    });
    const ts = [...groups.keys()].sort((a, b) => a - b);
    return { ts, values: ts.map((t) => avg(groups.get(t)!)) };
  }

  beforeAll(async () => {
    await client.write({ measurement: M, tags: { t: "a" }, fields: { v: bV }, timestamps: bTs });
  });

  it("10s buckets align to epoch multiples; boundary points land in the bucket they open", async () => {
    const r = await client.query(`avg:${M}(v)`, { startTime: BASE, endTime: BASE + 60 * S, aggregationInterval: "10s" });
    const f = findField(r, "v");
    // Buckets [BASE+10k, BASE+10k+10): pts {10k, 10k+5} -> avg; last bucket
    // holds only the +60s point because endTime is inclusive.
    const exp = expectedBuckets(10 * S, BASE, BASE + 60 * S);
    expect(f.timestamps).toEqual(exp.ts);
    expect(f.values).toEqual(exp.values);
  });

  it("endTime is inclusive: point exactly at endTime is aggregated", async () => {
    const r = await client.query(`avg:${M}(v)`, { startTime: BASE, endTime: BASE + 57 * S, aggregationInterval: "10s" });
    const f = findField(r, "v");
    // +55s point (v=11) is <= endTime, +60s point is excluded.
    const exp = expectedBuckets(10 * S, BASE, BASE + 57 * S);
    expect(f.timestamps).toEqual(exp.ts);
    expect(f.values).toEqual(exp.values);
    expect(exp.ts[exp.ts.length - 1]).toBe(BASE + 50 * S);
    expect(exp.values[exp.values.length - 1]).toBe((10 + 11) / 2);
  });

  it("misaligned startTime keeps epoch-aligned buckets but excludes points before startTime", async () => {
    // A dense series is used because misaligned starts over sparse ranges
    // (< ~20 in-range points) hit the single-bucket collapse bug pinned
    // below. 60 points at 1s spacing, v = i.
    const m = `${P}.misaligned`;
    const dTs = Array.from({ length: 60 }, (_, i) => BASE + i * S);
    const dV = Array.from({ length: 60 }, (_, i) => i);
    await client.write({ measurement: m, tags: { t: "a" }, fields: { v: dV }, timestamps: dTs });
    const r = await client.query(`avg:${m}(v)`, { startTime: BASE + 3 * S, endTime: BASE + 59 * S, aggregationInterval: "10s" });
    const f = findField(r, "v");
    // First bucket is still stamped BASE (epoch-aligned) but only contains
    // points +3s..+9s (avg = 6); the +0..+2s points are before startTime.
    const expTs = [0, 10, 20, 30, 40, 50].map((s) => BASE + s * S);
    const expV = [avg([3, 4, 5, 6, 7, 8, 9]), 14.5, 24.5, 34.5, 44.5, 54.5];
    expect(f.timestamps).toEqual(expTs);
    expect(f.values).toEqual(expV);
  });

  it("interval larger than the query range yields one bucket with all points", async () => {
    const r = await client.query(`avg:${M}(v)`, { startTime: BASE, endTime: BASE + 60 * S, aggregationInterval: "1d" });
    const f = findField(r, "v");
    expect(f.timestamps).toEqual([bucketStart(BASE, 86400 * S)]);
    expect(f.values).toEqual([avg(bV)]); // 6
  });

  it("interval smaller than point spacing returns one bucket per point, no gap fill", async () => {
    const r = await client.query(`avg:${M}(v)`, { startTime: BASE, endTime: BASE + 60 * S, aggregationInterval: "1s" });
    const f = findField(r, "v");
    // Every point sits exactly on a 1s boundary -> its own bucket; empty
    // buckets are omitted entirely (no gap filling).
    expect(f.timestamps).toEqual(bTs);
    expect(f.values).toEqual(bV);
  });

  it("decimal interval 1.5s buckets correctly", async () => {
    const r = await client.query(`avg:${M}(v)`, { startTime: BASE, endTime: BASE + 60 * S, aggregationInterval: "1.5s" });
    const f = findField(r, "v");
    // 5s spacing vs 1.5s buckets: every point in its own (floor-aligned) bucket.
    const exp = expectedBuckets(1.5e9, BASE, BASE + 60 * S);
    expect(exp.ts.length).toBe(13);
    expect(f.timestamps).toEqual(exp.ts);
    expect(f.values).toEqual(exp.values);
  });

  it("decimal interval 0.5m buckets correctly", async () => {
    const r = await client.query(`avg:${M}(v)`, { startTime: BASE, endTime: BASE + 60 * S, aggregationInterval: "0.5m" });
    const f = findField(r, "v");
    // BASE mod 30s = 20s, so buckets start at BASE-20s, BASE+10s, BASE+40s.
    const exp = expectedBuckets(30e9, BASE, BASE + 60 * S);
    expect(exp.ts).toEqual([bucketStart(BASE, 30e9), BASE + 10 * S, BASE + 40 * S]);
    expect(f.timestamps).toEqual(exp.ts);
    expect(f.values).toEqual(exp.values);
  });

  it("5m and 1h intervals produce a single bucket for a 60s dataset", async () => {
    for (const iv of ["5m", "1h"] as const) {
      const ns = iv === "5m" ? 300e9 : 3600e9;
      const r = await client.query(`sum:${M}(v)`, { startTime: BASE, endTime: BASE + 60 * S, aggregationInterval: iv });
      const f = findField(r, "v");
      expect(f.timestamps, iv).toEqual([bucketStart(BASE, ns)]);
      expect(f.values, iv).toEqual([sum(bV)]);
    }
  });

  // SERVER BUG (warmed server): interval queries collapse ALL points into a
  // SINGLE bucket stamped with the first bucket start instead of returning
  // epoch-aligned per-bucket results, when either
  //   - startTime is not bucket-aligned and fewer than ~20 points are in
  //     range (13 in-range points collapse, 20+ do not), or
  //   - endTime - startTime <= 1 interval (even with aligned start).
  // Repro A: 5 pts at +0/5/10/15/20s (v=10..50); avg 10s buckets over
  //   [first+3s, first+21s] -> expect buckets [BASE,+10s,+20s]=[20,35,50],
  //   actual: single bucket [BASE]=[35].
  // Repro B (aligned start, 60-pt series): avg 10s buckets over
  //   [first+3s, first+13s] -> expect [BASE]=6, [BASE+10s]=11.5(incl. end),
  //   actual: single bucket [BASE]=8.
  // Does NOT reproduce on a freshly-started server (first seconds after
  // boot) — the collapse appears once the server has warmed up, which is
  // itself a determinism problem.
  it.fails("SERVER BUG: small-window interval queries collapse into a single bucket (misaligned start)", async () => {
    const m = `${P}.collapse1`;
    const ts5 = [BASE, BASE + 5 * S, BASE + 10 * S, BASE + 15 * S, BASE + 20 * S];
    await client.write({ measurement: m, tags: { t: "a" }, fields: { v: [10, 20, 30, 40, 50] }, timestamps: ts5 });
    const r = await client.query(`avg:${m}(v)`, { startTime: BASE + 3 * S, endTime: BASE + 21 * S, aggregationInterval: "10s" });
    const f = findField(r, "v");
    expect(f.timestamps).toEqual([BASE, BASE + 10 * S, BASE + 20 * S]);
    expect(f.values).toEqual([20, 35, 50]);
  });

  it.fails("SERVER BUG: small-window interval queries collapse into a single bucket (aligned start, range == interval)", async () => {
    const m = `${P}.collapse2`;
    await client.write({
      measurement: m, tags: { t: "a" },
      fields: { v: [0, 1, 2, 3] }, timestamps: [BASE, BASE + S, BASE + 2 * S, BASE + 3 * S],
    });
    // endTime inclusive: [BASE, BASE+1s] holds two 1s buckets with one point each.
    const r = await client.query(`avg:${m}(v)`, { startTime: BASE, endTime: BASE + S, aggregationInterval: "1s" });
    const f = findField(r, "v");
    expect(f.timestamps).toEqual([BASE, BASE + S]);
    expect(f.values).toEqual([0, 1]); // actual: single bucket [0.5]
  });

  it("numeric nanosecond interval strings are rejected with a descriptive error (unit suffix required)", async () => {
    // The JSON API documents numeric-ns aggregationInterval, but the protobuf
    // API carries it as a string and the parser demands a unit suffix. Pinned.
    await expect(
      client.query(`avg:${M}(v)`, { startTime: BASE, endTime: BASE + 21 * S, aggregationInterval: "10000000000" }),
    ).rejects.toMatchObject({ statusCode: 400, code: "INVALID_QUERY" });
  });
});

describe("no-interval query semantics", () => {
  // Without aggregationInterval and with startTime exactly at the first data
  // point, a single-series query passes raw points through (pinned).
  it("raw passthrough when startTime == first point [memory]", async () => {
    const M = `${P}.raw`;
    // Pre-flush: brings the shard WALs back to ~empty so this small write is
    // guaranteed to still be in the memory store when queried (a near-full
    // WAL from earlier suites would otherwise flush it immediately).
    await flushToTsm(client);
    await client.write({ measurement: M, tags: { t: "a" }, fields: { v: D }, timestamps: TS });
    const r = await client.query(`avg:${M}(v)`, { startTime: TS[0], endTime: TS[N - 1] + S });
    const f = findField(r, "v");
    expect(f.timestamps).toEqual(TS);
    expect(f.values).toEqual(D);
  });

  // SERVER BUG: the shape of a no-interval query result depends on where the
  // data lives and on range/data-extent alignment, instead of being a stable
  // function of the query:
  //   - memory store + startTime == first point  -> N raw points
  //   - memory store + startTime <  first point  -> 1 collapsed aggregate
  //   - TSM          + any startTime             -> 1 collapsed aggregate
  // Repro: write [3,-7,...] (12 pts); avg:m(v) startTime=first-1s -> 1 value
  // 9.375; same query with startTime=first -> 12 raw values. After flushing
  // to TSM even startTime=first returns 1 collapsed value.
  it.fails("SERVER BUG: no-interval result shape is independent of startTime alignment", async () => {
    const M = `${P}.raw2`;
    await flushToTsm(client); // normalize WAL fill: keep this write in the memory store
    await client.write({ measurement: M, tags: { t: "a" }, fields: { v: D }, timestamps: TS });
    const exact = await client.query(`avg:${M}(v)`, { startTime: TS[0], endTime: TS[N - 1] + S });
    const before = await client.query(`avg:${M}(v)`, { startTime: TS[0] - S, endTime: TS[N - 1] + S });
    // Same data, same semantics — the extra empty second before the first
    // point must not change the result shape.
    expect(findField(before, "v").values.length).toBe(findField(exact, "v").values.length);
  });

  it.fails("SERVER BUG: no-interval result shape is independent of data placement (memstore vs TSM)", async () => {
    const M = `${P}.raw3`;
    await flushToTsm(client); // normalize WAL fill: keep this write in the memory store
    await client.write({ measurement: M, tags: { t: "a" }, fields: { v: D }, timestamps: TS });
    const mem = await client.query(`avg:${M}(v)`, { startTime: TS[0], endTime: TS[N - 1] + S });
    await flushToTsm(client);
    const tsm = await queryUntilSeries(client, `avg:${M}(v)`, { startTime: TS[0], endTime: TS[N - 1] + S });
    // memstore: 12 raw points; TSM: 1 collapsed aggregate. Must be identical.
    expect(findField(tsm, "v").values.length).toBe(findField(mem, "v").values.length);
  });
});

describe("NaN handling x placement", () => {
  // SERVER BUG: count/avg over a series containing NaN diverge between the
  // memory-store fold (NaN excluded: count=2, avg=(1+3)/2=2) and the TSM
  // block-stats fold (NaN counted: count=3, avg=4/3). sum/min/max agree.
  // Repro: write v=[1,NaN,3]; count 1h-bucket -> memstore 2, after flush 3.
  it.fails("SERVER BUG: count over [1,NaN,3] is placement-independent", async () => {
    const M = `${P}.nan1`;
    await flushToTsm(client); // normalize WAL fill: keep this write in the memory store
    await client.write({ measurement: M, tags: { t: "a" }, fields: { v: [1, NaN, 3] }, timestamps: [BASE, BASE + S, BASE + 2 * S] });
    const q = () => queryUntilSeries(client, `count:${M}(v)`, { startTime: BASE, endTime: BASE + 3 * S, aggregationInterval: "1h" });
    const mem = findField(await q(), "v").values[0];
    await flushToTsm(client);
    const tsm = findField(await q(), "v").values[0];
    expect(tsm).toBe(mem);
  });

  it.fails("SERVER BUG: avg over [1,NaN,3] is placement-independent", async () => {
    const M = `${P}.nan2`;
    await flushToTsm(client); // normalize WAL fill: keep this write in the memory store
    await client.write({ measurement: M, tags: { t: "a" }, fields: { v: [1, NaN, 3] }, timestamps: [BASE, BASE + S, BASE + 2 * S] });
    const q = () => queryUntilSeries(client, `avg:${M}(v)`, { startTime: BASE, endTime: BASE + 3 * S, aggregationInterval: "1h" });
    const mem = findField(await q(), "v").values[0];
    await flushToTsm(client);
    const tsm = findField(await q(), "v").values[0];
    expect(tsm).toBe(mem); // memstore: 2 (NaN skipped), TSM: 1.333... (NaN counted)
  });

  it("sum/min/max over [1,NaN,3] agree across placements (NaN skipped)", async () => {
    const M = `${P}.nan3`;
    await flushToTsm(client); // normalize WAL fill: keep this write in the memory store
    await client.write({ measurement: M, tags: { t: "a" }, fields: { v: [1, NaN, 3] }, timestamps: [BASE, BASE + S, BASE + 2 * S] });
    const q = async (agg: string) =>
      findField(await queryUntilSeries(client, `${agg}:${M}(v)`, { startTime: BASE, endTime: BASE + 3 * S, aggregationInterval: "1h" }), "v").values[0];
    const memVals = [await q("sum"), await q("min"), await q("max")];
    expect(memVals).toEqual([4, 1, 3]);
    await flushToTsm(client);
    expect([await q("sum"), await q("min"), await q("max")]).toEqual(memVals);
  });
});

describe("multi-series aggregation (no group-by)", () => {
  // Two series in the same measurement: per-bucket aggregation across series.
  const M = `${P}.multi`;
  const A = [10, 20, 30, 40];
  const B = [1, 2, 3, 4];
  const mTs = Array.from({ length: 4 }, (_, i) => BASE + i * S);

  beforeAll(async () => {
    await client.write([
      { measurement: M, tags: { host: "a" }, fields: { v: A }, timestamps: mTs },
      { measurement: M, tags: { host: "b" }, fields: { v: B }, timestamps: mTs },
    ]);
  });

  it("avg across series per 1s bucket", async () => {
    const r = await client.query(`avg:${M}(v)`, { startTime: BASE, endTime: BASE + 4 * S, aggregationInterval: "1s" });
    const f = findField(r, "v");
    expect(f.timestamps).toEqual(mTs);
    expect(f.values).toEqual(A.map((a, i) => (a + B[i]) / 2));
  });

  it("sum/min/max/count across series in one whole-range bucket", async () => {
    const whole = { startTime: BASE, endTime: BASE + 4 * S, aggregationInterval: "1h" };
    const all = [...A, ...B];
    expect(findField(await client.query(`sum:${M}(v)`, whole), "v").values).toEqual([sum(all)]);
    expect(findField(await client.query(`min:${M}(v)`, whole), "v").values).toEqual([min(all)]);
    expect(findField(await client.query(`max:${M}(v)`, whole), "v").values).toEqual([max(all)]);
    expect(findField(await client.query(`count:${M}(v)`, whole), "v").values).toEqual([all.length]);
  });
});
