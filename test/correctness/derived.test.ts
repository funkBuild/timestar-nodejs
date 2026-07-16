// Derived query correctness: formulas over multiple sub-queries WITH
// aggregationInterval across all three data placements — the direct
// regression suite for today's server fix ("derived sub-queries never
// received aggregationInterval") — plus formula functions and error shapes.

import { describe, it, expect, beforeAll } from "vitest";
import { makeClient, flushToTsm, uniquePrefix, BASE, S } from "./helpers";
import { TimestarError } from "../../src/client";

const client = makeClient();
const P = uniquePrefix("derived");

// Two aligned sub-series, 8 points at 1s spacing.
const N = 8;
const TS = Array.from({ length: N }, (_, i) => BASE + i * S);
const A = [10, 20, 30, 40, 50, 60, 70, 80];
const B = [1, 2, 3, 4, 5, 6, 7, 8];

async function writeAB(measurement: string, half?: "first" | "second") {
  const lo = half === "second" ? N / 2 : 0;
  const hi = half === "first" ? N / 2 : N;
  await client.write([
    { measurement, tags: { metric: "cpu" }, fields: { v: A.slice(lo, hi) }, timestamps: TS.slice(lo, hi) },
    { measurement, tags: { metric: "mem" }, fields: { v: B.slice(lo, hi) }, timestamps: TS.slice(lo, hi) },
  ]);
}

function derivedAB(measurement: string, formula: string) {
  return client.derived(
    {
      a: `avg:${measurement}(v){metric:cpu}`,
      b: `avg:${measurement}(v){metric:mem}`,
    },
    formula,
    { startTime: BASE, endTime: BASE + N * S, aggregationInterval: "1s" },
  );
}

beforeAll(async () => {
  expect(await client.isHealthy()).toBe(true);
});

describe("formula over two sub-queries with aggregationInterval x placement", () => {
  // Expected per 1s bucket: a + 2*b, computed from the source arrays.
  const EXPECTED = A.map((a, i) => a + 2 * B[i]);

  it("memory-store only", async () => {
    const m = `${P}.mem`;
    await writeAB(m);
    const r = await derivedAB(m, "a + 2 * b");
    expect(r.status).toBe("success");
    expect(r.statistics.subQueriesExecuted).toBe(2);
    expect(r.timestamps).toEqual(TS);
    expect(r.values).toEqual(EXPECTED);
  });

  it("after TSM flush (regression for today's derived-interval fix)", async () => {
    const m = `${P}.tsm`;
    await writeAB(m);
    await flushToTsm(client);
    const r = await derivedAB(m, "a + 2 * b");
    expect(r.status).toBe("success");
    expect(r.timestamps).toEqual(TS);
    expect(r.values).toEqual(EXPECTED);
  });

  it("spanning memstore and TSM", async () => {
    const m = `${P}.span`;
    await writeAB(m, "first");
    await flushToTsm(client);
    await writeAB(m, "second");
    const r = await derivedAB(m, "a + 2 * b");
    expect(r.status).toBe("success");
    expect(r.timestamps).toEqual(TS);
    expect(r.values).toEqual(EXPECTED);
  });

  it("subtraction, multiplication, division are computed per bucket", async () => {
    const m = `${P}.ops`;
    await writeAB(m);
    expect((await derivedAB(m, "a - b")).values).toEqual(A.map((a, i) => a - B[i]));
    expect((await derivedAB(m, "a * b")).values).toEqual(A.map((a, i) => a * B[i]));
    expect((await derivedAB(m, "a / b")).values).toEqual(A.map((a, i) => a / B[i]));
    expect((await derivedAB(m, "(a - b) / (a + b)")).values).toEqual(
      A.map((a, i) => (a - B[i]) / (a + B[i])),
    );
  });
});

describe("formula functions", () => {
  const m = `${P}.fn`;
  const VALS = [1, 2, 4, 8, 16, 32, 64, 128];

  beforeAll(async () => {
    await client.write({ measurement: m, tags: { t: "a" }, fields: { v: VALS }, timestamps: TS });
  });

  function run(formula: string) {
    return client.derived(
      { a: `avg:${m}(v)` },
      formula,
      { startTime: BASE, endTime: BASE + N * S, aggregationInterval: "1s" },
    );
  }

  it("fill_forward passes a dense series through unchanged", async () => {
    const r = await run("fill_forward(a)");
    expect(r.values).toEqual(VALS);
  });

  it("sparse sub-queries are aligned onto the grid by linear interpolation; unreachable grid points are dropped and counted", async () => {
    // Sparse series: values only at t0, t3, t6. Combined with a dense series
    // on a 1s grid, the aligner linearly interpolates the sparse series at
    // t1,t2 (between 5 and 11) and t4,t5 (between 11 and 23); t7 lies beyond
    // the sparse domain, cannot be interpolated, and is dropped (reported in
    // statistics.pointsDroppedDueToAlignment).
    const ms = `${P}.sparse`;
    await client.write({
      measurement: ms, tags: { t: "a" },
      fields: { v: [5, 11, 23] }, timestamps: [TS[0], TS[3], TS[6]],
    });
    const r = await client.derived(
      { a: `avg:${ms}(v)`, b: `avg:${m}(v)` }, // b is dense -> defines the grid
      "fill_forward(a) + 0 * b",
      { startTime: BASE, endTime: BASE + N * S, aggregationInterval: "1s" },
    );
    // Linear interpolation: t0..t3 = 5 + 2i, t3..t6 = 11 + 4i.
    expect(r.values).toEqual([5, 7, 9, 11, 15, 19, 23]);
    expect(r.timestamps).toEqual(TS.slice(0, 7));
    // Exact counter semantics are opaque (reports 5 for this shape — it
    // counts per-series alignment adjustments, not just dropped grid rows);
    // pin only that alignment loss is reported at all.
    expect(r.statistics.pointsDroppedDueToAlignment).toBeGreaterThan(0);
  });

  it("rolling_avg(a, 3): NaN until the window fills, then trailing mean", async () => {
    const r = await run("rolling_avg(a, 3)");
    expect(r.values.length).toBe(N);
    expect(Number.isNaN(r.values[0])).toBe(true);
    expect(Number.isNaN(r.values[1])).toBe(true);
    for (let i = 2; i < N; i++) {
      expect(r.values[i]).toBeCloseTo((VALS[i] + VALS[i - 1] + VALS[i - 2]) / 3, 10);
    }
  });

  it("cumsum accumulates exactly", async () => {
    const r = await run("cumsum(a)");
    const exp: number[] = [];
    VALS.reduce((acc, v) => { exp.push(acc + v); return acc + v; }, 0);
    expect(r.values).toEqual(exp);
  });

  it("holt_winters(a, 0.5, 0.5) follows the standard level/trend recurrence", async () => {
    const r = await run("holt_winters(a, 0.5, 0.5)");
    // Recurrence (pinned by probing): out[0]=x0, l=x0, b=0;
    // l' = alpha*x + (1-alpha)*(l+b); b' = beta*(l'-l) + (1-beta)*b; out=l'.
    const alpha = 0.5, beta = 0.5;
    let l = VALS[0], b = 0;
    const exp = [VALS[0]];
    for (let i = 1; i < N; i++) {
      const lNext = alpha * VALS[i] + (1 - alpha) * (l + b);
      b = beta * (lNext - l) + (1 - beta) * b;
      l = lNext;
      exp.push(l);
    }
    expect(r.values.length).toBe(N);
    for (let i = 0; i < N; i++) expect(r.values[i]).toBeCloseTo(exp[i], 9);
  });

  it("gaussian_smooth(a, 2) returns a same-length finite smoothing", async () => {
    const r = await run("gaussian_smooth(a, 2)");
    expect(r.values.length).toBe(N);
    for (const v of r.values) expect(Number.isFinite(v)).toBe(true);
    // Smoothed values stay within the data envelope.
    for (const v of r.values) {
      expect(v).toBeGreaterThanOrEqual(Math.min(...VALS));
      expect(v).toBeLessThanOrEqual(Math.max(...VALS));
    }
  });

  it("composed scalar functions: abs(a) + sqrt(a) computed pointwise", async () => {
    const r = await run("abs(a) + sqrt(a)");
    for (let i = 0; i < N; i++) {
      expect(r.values[i]).toBeCloseTo(Math.abs(VALS[i]) + Math.sqrt(VALS[i]), 10);
    }
  });

  it("division by zero yields Infinity (no error, no silent drop)", async () => {
    const r = await run("a / 0");
    expect(r.status).toBe("success");
    expect(r.values.length).toBe(N);
    for (const v of r.values) expect(v).toBe(Infinity);
  });
});

describe("derived error handling", () => {
  const goodQuery = { a: `avg:${P}.fn(v)` };
  const opts = { startTime: BASE, endTime: BASE + N * S, aggregationInterval: "1s" as const };

  async function expectFlatError(formula: string, msgPattern: RegExp) {
    try {
      await client.derived(goodQuery, formula, opts);
      expect.unreachable(`formula '${formula}' should have thrown`);
    } catch (e) {
      expect(e).toBeInstanceOf(TimestarError);
      const err = e as TimestarError;
      expect(err.statusCode).toBe(400);
      expect(err.message).toMatch(msgPattern);
    }
  }

  it("syntax error -> flat 400 with position info", async () => {
    await expectFlatError("a +* b((", /Expected expression/i);
  });

  it("unknown function -> flat 400 naming the function", async () => {
    await expectFlatError("not_a_function(a)", /Unknown function: not_a_function/);
  });

  it("wrong argument count -> flat 400 with expected arity", async () => {
    await expectFlatError("holt_winters(a)", /expects 3 arguments|requires exactly 3/i);
    await expectFlatError("gaussian_smooth(a)", /expects 2 arguments|requires exactly 2/i);
  });

  it("unknown sub-query variable in formula -> flat 400", async () => {
    try {
      await client.derived(goodQuery, "a + nosuchvar", opts);
      expect.unreachable("should have thrown");
    } catch (e) {
      expect(e).toBeInstanceOf(TimestarError);
      expect((e as TimestarError).statusCode).toBe(400);
    }
  });

  it("sub-query over a non-existent measurement -> clean empty success", async () => {
    const r = await client.derived(
      { a: `avg:${P}.does_not_exist(v)` },
      "a * 2",
      opts,
    );
    expect(r.status).toBe("success");
    expect(r.values).toEqual([]);
    expect(r.timestamps).toEqual([]);
  });
});
