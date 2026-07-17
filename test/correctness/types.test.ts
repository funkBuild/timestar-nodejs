// Value-type correctness: floats (including edge values), int64 (bigint),
// booleans, strings — plus client-side compressed-vs-raw write equivalence
// and precise:true bigint round-trips.

import { describe, it, expect, beforeAll } from "vitest";
import { makeClient, flushToTsm, uniquePrefix, findField, BASE, S, avg } from "./helpers";
import type { WritePoint } from "../../src/types";

const client = makeClient();
const P = uniquePrefix("types");

beforeAll(async () => {
  expect(await client.isHealthy()).toBe(true);
});

// Raw passthrough read: no interval, startTime exactly on the first point
// (pinned passthrough shape for memory-store data).
async function readRaw(measurement: string, firstTs: number | bigint, lastTs: number | bigint, opts: { precise?: boolean } = {}) {
  const start = typeof firstTs === "bigint" ? firstTs : BigInt(firstTs);
  const end = typeof lastTs === "bigint" ? lastTs : BigInt(lastTs);
  const r = await client.query(`avg:${measurement}(v)`, { startTime: start, endTime: end + BigInt(S), ...opts });
  expect(r.status).toBe("success");
  return findField(r, "v");
}

describe("float values", () => {
  it("plain doubles round-trip exactly (compressed ALP write + read)", async () => {
    const m = `${P}.f1`;
    const vals = [3.141592653589793, -2.718281828459045, 0.1, 1 / 3, 123456.789];
    const ts = vals.map((_, i) => BASE + i * S);
    const w = await client.write({ measurement: m, tags: { t: "a" }, fields: { v: vals }, timestamps: ts });
    expect(w.status).toBe("success");
    expect(w.pointsWritten).toBe(vals.length);
    const f = await readRaw(m, ts[0], ts[ts.length - 1]);
    expect(f.timestamps).toEqual(ts);
    expect(f.values).toEqual(vals);
  });

  it("denormal min (5e-324), +-1e308 and exact integers round-trip exactly", async () => {
    const m = `${P}.f2`;
    const vals = [5e-324, -5e-324, 1e308, -1e308, 2 ** 52, -(2 ** 52)];
    const ts = vals.map((_, i) => BASE + i * S);
    await client.write({ measurement: m, tags: { t: "a" }, fields: { v: vals }, timestamps: ts });
    const f = await readRaw(m, ts[0], ts[ts.length - 1]);
    expect(f.values).toEqual(vals);
  });

  it("-0 is accepted and reads back as a zero (sign of zero may or may not survive — pinned loosely)", async () => {
    const m = `${P}.f3`;
    const ts = [BASE, BASE + S];
    await client.write({ measurement: m, tags: { t: "a" }, fields: { v: [-0, 1] }, timestamps: ts });
    const f = await readRaw(m, ts[0], ts[1]);
    // Raw reads round-trip -0 bit-exactly (ALP raw-bit exceptions), but a
    // query served through an aggregation-shaped path may normalize -0 to +0
    // (IEEE addition) — the server documents both. Accept either zero.
    // NOTE: .toBe uses Object.is, where Object.is(-0, 0) === false — the ===
    // comparison below is what "either zero" actually requires.
    expect(f.values[0] === 0).toBe(true);
    expect(f.values[1]).toBe(1);
  });

  it("NaN is accepted on write and reads back as NaN", async () => {
    const m = `${P}.f4`;
    const ts = [BASE, BASE + S];
    const w = await client.write({ measurement: m, tags: { t: "a" }, fields: { v: [NaN, 7] }, timestamps: ts });
    expect(w.status).toBe("success");
    const f = await readRaw(m, ts[0], ts[1]);
    expect(Number.isNaN(f.values[0] as number)).toBe(true);
    expect(f.values[1]).toBe(7);
  });

  it("+-Infinity is accepted on write; reads back as Infinity or NaN (SERVER: unstable across read paths)", async () => {
    // SERVER BUG (documented, deliberately pinned loosely): Infinity written
    // to a float field reads back as Infinity from some read paths (fresh
    // memory store on a cold server) but as NaN from others (bucket folds /
    // warmed server). The value is never silently turned into a finite
    // number, so this test pins the containment set {Infinity, NaN} rather
    // than a single unstable value.
    const m = `${P}.f5`;
    const ts = [BASE, BASE + S];
    const w = await client.write({ measurement: m, tags: { t: "a" }, fields: { v: [Infinity, -Infinity] }, timestamps: ts });
    expect(w.status).toBe("success");
    const f = await readRaw(m, ts[0], ts[1]);
    const v0 = f.values[0] as number;
    const v1 = f.values[1] as number;
    expect(v0 === Infinity || Number.isNaN(v0)).toBe(true);
    expect(v1 === -Infinity || Number.isNaN(v1)).toBe(true);
  });
});

describe("int64 values", () => {
  it("0, negatives, and +-(2^53-1) round-trip exactly through aggregation", async () => {
    const m = `${P}.i1`;
    const vals = [0n, -1n, 42n, -9007199254740991n, 9007199254740991n];
    const ts = vals.map((_, i) => BASE + i * S);
    const w = await client.write({ measurement: m, tags: { t: "a" }, fields: { v: { int64Values: vals } }, timestamps: ts });
    expect(w.status).toBe("success");
    const f = await readRaw(m, ts[0], ts[ts.length - 1]);
    // Server query paths fold int64 numerically; all these are <= 2^53-1 so
    // the doubles are exact.
    expect(f.values).toEqual(vals.map(Number));
  });

  it("sum of int64 values is exact within double range", async () => {
    const m = `${P}.i2`;
    const vals = [2n ** 52n, 2n ** 52n, 100n, -50n];
    const ts = vals.map((_, i) => BASE + i * S);
    await client.write({ measurement: m, tags: { t: "a" }, fields: { v: { int64Values: vals } }, timestamps: ts });
    const r = await client.query(`sum:${m}(v)`, { startTime: BASE, endTime: BASE + 4 * S, aggregationInterval: "1h" });
    // 2*2^52 + 50 = 2^53 + 50, exactly representable.
    expect(findField(r, "v").values).toEqual([2 ** 53 + 50]);
  });

  it("+-(2^53+1) and +-(2^63-1) survive the write wire exactly; query returns nearest double (server aggregates int64 as float64 — pinned)", async () => {
    const m = `${P}.i3`;
    const vals = [9007199254740993n, -9007199254740993n, 9223372036854775807n, -9223372036854775807n];
    const ts = vals.map((_, i) => BASE + i * S);
    const w = await client.write({ measurement: m, tags: { t: "a" }, fields: { v: { int64Values: vals } }, timestamps: ts });
    expect(w.status).toBe("success");
    expect(w.failedWrites).toBe(0);
    const f = await readRaw(m, ts[0], ts[ts.length - 1], { precise: true });
    // The client writes full 64-bit precision (codec-verified in
    // compression.test.ts); the SERVER's query path renders int64 as the
    // nearest double. Number(bigint) is exactly that nearest double.
    expect(f.values.length).toBe(4);
    for (let i = 0; i < 4; i++) {
      expect(f.values[i]).toBe(Number(vals[i]));
    }
  });
});

describe("boolean values", () => {
  it("bool arrays read back as booleans, never numeric 0/1", async () => {
    const m = `${P}.b1`;
    const vals = [true, false, true, true, false];
    const ts = vals.map((_, i) => BASE + i * S);
    await client.write({ measurement: m, tags: { t: "a" }, fields: { v: vals }, timestamps: ts });
    const f = await readRaw(m, ts[0], ts[ts.length - 1]);
    expect(f.values).toEqual(vals);
    for (const v of f.values) expect(typeof v).toBe("boolean");
  });

  it("bool aggregations are ignored: every method reduces to LATEST-per-bucket", async () => {
    // Booleans are non-numeric — the aggregation method named in the query is
    // ignored, exactly as it is for strings. One 1h bucket spans the whole
    // range, so every method returns that bucket's latest value.
    const m = `${P}.b2`;
    const vals = [true, false, true, true];
    const ts = vals.map((_, i) => BASE + i * S);
    await client.write({ measurement: m, tags: { t: "a" }, fields: { v: vals }, timestamps: ts });
    const whole = { startTime: BASE, endTime: BASE + 4 * S, aggregationInterval: "1h" as const };
    for (const method of ["avg", "sum", "count", "min", "max", "latest"]) {
      const f = findField(await client.query(`${method}:${m}(v)`, whole), "v");
      expect(f.values, `${method} of a boolean field`).toEqual([true]);
    }
  });
});

describe("string values", () => {
  it("strings round-trip exactly: empty, newlines/tabs, control chars, JSON metacharacters, UTF-8 multibyte", async () => {
    const m = `${P}.s1`;
    const vals = [
      "",                                     // empty string
      "line1\nline2\r\ntab\there",            // newlines (LF + CRLF) and tabs
      "\x01\x02\x1f bel\x07",                 // C0 control characters
      `{"json":"with \\"quotes\\", commas, }{ braces"}`, // JSON metacharacters
      "héllo wörld 🌍 日本語 中文 한국어 עברית", // UTF-8 multibyte incl. RTL
      "back\\slash 'single' \"double\"",
    ];
    const ts = vals.map((_, i) => BASE + i * S);
    const w = await client.write({ measurement: m, tags: { t: "a" }, fields: { v: vals }, timestamps: ts });
    expect(w.status).toBe("success");
    const f = await readRaw(m, ts[0], ts[ts.length - 1]);
    expect(f.timestamps).toEqual(ts);
    expect(f.values).toEqual(vals);
  });

  it("a ~1MB string round-trips byte-for-byte", async () => {
    const m = `${P}.s2`;
    // Mildly incompressible content so zstd cannot trivialize the test.
    let big = "";
    let seed = 12345;
    while (big.length < 1_000_000) {
      seed = (seed * 1103515245 + 12345) % 2147483648;
      big += seed.toString(36);
    }
    const ts = [BASE];
    const w = await client.write({ measurement: m, tags: { t: "a" }, fields: { v: big }, timestamps: ts });
    expect(w.status).toBe("success");
    const f = await readRaw(m, BASE, BASE);
    expect((f.values[0] as string).length).toBe(big.length);
    expect(f.values[0]).toBe(big);
  });

  it("string fields pass through aggregation queries untouched, with tags preserved", async () => {
    const m = `${P}.s3`;
    const vals = ["alpha", "beta", "gamma"];
    const ts = vals.map((_, i) => BASE + i * S);
    await client.write({ measurement: m, tags: { sensor: "cam-1" }, fields: { v: vals }, timestamps: ts });
    // Aggregation method is ignored for string fields — they are returned raw.
    const r = await client.query(`avg:${m}(v)`, { startTime: BASE, endTime: BASE + 3 * S });
    const s = r.series.find((x) => x.fields.v);
    expect(s).toBeDefined();
    expect(s!.tags).toEqual({ sensor: "cam-1" });
    expect(s!.fields.v.values).toEqual(vals);
  });
});

describe("mixed-type points and per-type placement stability", () => {
  it("one point with float+int+bool+string fields writes and reads back", async () => {
    const m = `${P}.mixed`;
    const w = await client.write({
      measurement: m,
      tags: { dev: "d1" },
      fields: { f: 21.5, i: 42n, b: true, s: "note" },
      timestamps: [BASE],
    });
    expect(w.status).toBe("success");
    expect(w.pointsWritten).toBe(4); // field-points: 4 fields x 1 timestamp
    const r = await client.query(`latest:${m}()`, { startTime: BASE, endTime: BASE + S });
    expect(findField(r, "f").values[0]).toBe(21.5);
    expect(findField(r, "i").values[0]).toBe(42);
    expect(findField(r, "b").values[0]).toBe(true); // non-numeric: written type kept
    expect(findField(r, "s").values[0]).toBe("note");
  });

  it("int64, bool, and string data survive a TSM flush unchanged", async () => {
    const m = `${P}.place`;
    const ints = [-3n, 0n, 999999n, 2n ** 40n];
    const bools = [true, true, false, true];
    const strs = ["a", "", "sensor_🌡", "end"];
    const ts = ints.map((_, i) => BASE + i * S);
    await client.write({
      measurement: m, tags: { t: "a" },
      fields: { i: { int64Values: ints }, b: bools, s: strs },
      timestamps: ts,
    });

    const read = async () => {
      // Numeric fields: per-point via 1s buckets (stable across placements).
      // Boolean fields: non-numeric, so the 1s buckets reduce them to
      // LATEST-per-bucket — one boolean per point here, in written type.
      // String fields: raw no-interval passthrough — strings bypass
      // aggregation on no-interval queries and pass through verbatim.
      const r = await client.query(`latest:${m}(i,b)`, { startTime: BASE, endTime: BASE + 4 * S, aggregationInterval: "1s" });
      const rs = await client.query(`latest:${m}(s)`, { startTime: BASE, endTime: BASE + 4 * S });
      return {
        i: findField(r, "i").values,
        b: findField(r, "b").values,
        s: findField(rs, "s").values,
      };
    };

    const mem = await read();
    expect(mem.i).toEqual(ints.map(Number));
    expect(mem.b).toEqual(bools);
    expect(mem.s).toEqual(strs);

    await flushToTsm(client);
    const tsm = await read();
    expect(tsm).toEqual(mem);
  });

  // String fields participate in interval queries as latest-per-bucket:
  // each bucket reports the value with the greatest timestamp in the bucket,
  // stamped with the epoch-aligned bucket start. (Strings were formerly
  // silently omitted from any query with an aggregationInterval; fixed
  // server-side.)
  it("string fields in interval queries are latest-per-bucket at bucket-start timestamps", async () => {
    const m = `${P}.strdrop`;
    await client.write({
      measurement: m, tags: { t: "a" },
      fields: { s: ["a", "b", "c", "d"], f: [1, 2, 3, 4] },
      timestamps: [BASE, BASE + S, BASE + 2 * S, BASE + 3 * S],
    });
    // 2s buckets (BASE is a multiple of 2s): [BASE,+2s) holds "a"@+0s,"b"@+1s;
    // [+2s,+4s) holds "c"@+2s,"d"@+3s. Latest-per-bucket keeps the greatest-ts
    // value of each bucket, stamped with the bucket start.
    const r = await client.query(`latest:${m}()`, { startTime: BASE, endTime: BASE + 4 * S, aggregationInterval: "2s" });
    const s = findField(r, "s");
    expect(s.timestamps).toEqual([BASE, BASE + 2 * S]);
    expect(s.values).toEqual(["b", "d"]);
    // The numeric sibling field folds identically alongside.
    const f = findField(r, "f");
    expect(f.timestamps).toEqual([BASE, BASE + 2 * S]);
    expect(f.values).toEqual([2, 4]);
  });
});

describe("client compressed vs uncompressed write paths", () => {
  // The client compresses (FFOR/ALP/RLE/zstd) whenever a field has >= 2
  // values matching the timestamp count, and sends raw protobuf arrays for
  // single-timestamp points. Writing the same dataset through both paths
  // must produce identical query results.
  it("array write (compressed) and per-point writes (raw) yield identical query results", async () => {
    const vals = [1.5, -2.25, 3.125, 42.0, 0.0625, -17.75, 8.5, 9.875];
    const bools = [true, false, false, true, true, false, true, false];
    const ints = [10n, -20n, 30n, -40n, 50n, -60n, 70n, -80n];
    const strs = ["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7"];
    const ts = vals.map((_, i) => BASE + i * S);

    // Path A: one batched write -> compressed wire encodings for all types.
    const mA = `${P}.cmp_a`;
    const wA = await client.write({
      measurement: mA, tags: { t: "a" },
      fields: { f: vals, b: bools, i: { int64Values: ints }, s: strs },
      timestamps: ts,
    });
    expect(wA.status).toBe("success");

    // Path B: eight single-timestamp writes -> raw (uncompressed) wire arrays.
    const mB = `${P}.cmp_b`;
    const points: WritePoint[] = ts.map((t, i) => ({
      measurement: mB, tags: { t: "a" },
      fields: { f: vals[i], b: bools[i], i: ints[i], s: strs[i] },
      timestamps: [t],
    }));
    for (const p of points) {
      const w = await client.write(p);
      expect(w.status).toBe("success");
    }

    const read = async (m: string) => {
      const r = await client.query(`avg:${m}()`, { startTime: ts[0], endTime: ts[ts.length - 1] + S });
      return {
        f: findField(r, "f"), b: findField(r, "b"),
        i: findField(r, "i"), s: findField(r, "s"),
      };
    };
    const a = await read(mA);
    const b = await read(mB);
    for (const k of ["f", "b", "i", "s"] as const) {
      expect(b[k].timestamps, `field ${k} timestamps`).toEqual(a[k].timestamps);
      expect(b[k].values, `field ${k} values`).toEqual(a[k].values);
    }
    expect(a.f.values).toEqual(vals);
    expect(a.b.values).toEqual(bools);
    expect(a.i.values).toEqual(ints.map(Number));
    expect(a.s.values).toEqual(strs);
  });
});

describe("large writes (client 1024-timestamp chunking)", () => {
  // The client splits large points into <= 1024-timestamp chunks (see [S1]
  // in src/client.ts) — kept for compatibility with servers older than
  // commit 8425b17, whose compressed-timestamp decode cap silently truncated
  // larger points (e.g. 300k written -> 3380 stored, status "success").
  // Harmless on fixed servers; these tests prove the chunked full round-trip.
  it("5000-point single-series write stores every point with exact values", async () => {
    const m = `${P}.large`;
    const n = 5000;
    const vals = Array.from({ length: n }, (_, i) => 20 + (i % 977) * 0.25);
    const ts = Array.from({ length: n }, (_, i) => BASE + i * S);
    const w = await client.write({ measurement: m, tags: { t: "a" }, fields: { v: vals }, timestamps: ts });
    expect(w.status).toBe("success");
    expect(w.pointsWritten).toBe(n);

    const whole = { startTime: BASE, endTime: BASE + n * S, aggregationInterval: "1d" as const };
    const count = findField(await client.query(`count:${m}(v)`, whole), "v");
    expect((count.values as number[]).reduce((a, b) => a + b, 0)).toBe(n);
    const sumR = findField(await client.query(`sum:${m}(v)`, whole), "v");
    const expSum = vals.reduce((a, b) => a + b, 0);
    expect((sumR.values as number[]).reduce((a, b) => a + b, 0)).toBeCloseTo(expSum, 6);

    // Exact value spot-check via raw passthrough over a sub-range.
    const r = await client.query(`avg:${m}(v)`, { startTime: ts[1020], endTime: ts[1030] });
    const f = findField(r, "v");
    expect(f.timestamps).toEqual(ts.slice(1020, 1031)); // endTime inclusive
    expect(f.values).toEqual(vals.slice(1020, 1031));
  });

  it("multi-field 3000-point write keeps all columns aligned", async () => {
    const m = `${P}.large2`;
    const n = 3000;
    const f1 = Array.from({ length: n }, (_, i) => i * 0.5);
    const i1 = Array.from({ length: n }, (_, i) => BigInt(i) - 1500n);
    const ts = Array.from({ length: n }, (_, i) => BASE + i * S);
    const w = await client.write({
      measurement: m, tags: { t: "a" },
      fields: { f: f1, i: { int64Values: i1 } }, timestamps: ts,
    });
    expect(w.pointsWritten).toBe(2 * n);

    const whole = { startTime: BASE, endTime: BASE + n * S, aggregationInterval: "1d" as const };
    const cf = findField(await client.query(`count:${m}(f)`, whole), "f");
    expect((cf.values as number[]).reduce((a, b) => a + b, 0)).toBe(n);
    const ci = findField(await client.query(`count:${m}(i)`, whole), "i");
    expect((ci.values as number[]).reduce((a, b) => a + b, 0)).toBe(n);
    // avg(i) = mean of (i - 1500) for i in 0..2999 = (2999/2) - 1500 = -0.5
    const ai = findField(await client.query(`avg:${m}(i)`, whole), "i");
    expect(ai.values[0]).toBeCloseTo((n - 1) / 2 - 1500, 9);
  });
});

describe("precise:true bigint round-trips", () => {
  it("odd nanosecond timestamps beyond 2^53 round-trip exactly as bigint", async () => {
    const m = `${P}.precise`;
    // +1ns offsets that a double cannot represent.
    const ts = Array.from({ length: 20 }, (_, i) => 1_700_000_000_000_000_001n + BigInt(i) * 1_000_000_003n);
    const vals = Array.from({ length: 20 }, (_, i) => i * 1.5);
    const w = await client.write({ measurement: m, tags: { t: "a" }, fields: { v: vals }, timestamps: ts });
    expect(w.status).toBe("success");

    const r = await client.query(`avg:${m}(v)`, { startTime: ts[0], endTime: ts[19] + 10n, precise: true });
    const f = findField(r, "v");
    expect(f.timestamps).toEqual(ts);
    expect(typeof f.timestamps[0]).toBe("bigint");
    expect(f.values).toEqual(vals);
  });

  it("default (non-precise) mode returns the same timestamps rounded to doubles", async () => {
    const m = `${P}.precise2`;
    const ts = [1_700_000_000_000_000_001n, 1_700_000_001_000_000_003n];
    await client.write({ measurement: m, tags: { t: "a" }, fields: { v: [1, 2] }, timestamps: ts });
    const r = await client.query(`avg:${m}(v)`, { startTime: ts[0], endTime: ts[1] + 10n });
    const f = findField(r, "v");
    expect(typeof f.timestamps[0]).toBe("number");
    expect(f.timestamps).toEqual(ts.map(Number)); // nearest-double rounding
  });
});
