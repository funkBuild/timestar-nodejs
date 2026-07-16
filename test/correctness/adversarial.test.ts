// Adversarial inputs: the server must never 500 or hang; errors must be flat
// and descriptive; valid-but-extreme inputs must behave sanely.

import { describe, it, expect, beforeAll } from "vitest";
import { makeClient, uniquePrefix, findField, BASE, S, WAL_THRESHOLD, sleep } from "./helpers";
import { TimestarClient, TimestarError } from "../../src/client";
import { codecs, init as protoInit } from "../../src/proto";
import { compressTimestamps } from "../../src/compression";

const client = makeClient();
const P = uniquePrefix("adv");
const HOST = process.env.TIMESTAR_HOST || "localhost";
const PORT = parseInt(process.env.TIMESTAR_PORT || "58086", 10);

beforeAll(async () => {
  expect(await client.isHealthy()).toBe(true);
});

async function expectQueryError(
  query: string,
  opts: { startTime: number | bigint; endTime: number | bigint; aggregationInterval?: string },
  match: { statusCode: number; code?: string; msg?: RegExp },
) {
  try {
    await client.query(query, opts);
    expect.unreachable(`query '${query}' should have thrown`);
  } catch (e) {
    expect(e).toBeInstanceOf(TimestarError);
    const err = e as TimestarError;
    expect(err.statusCode).toBe(match.statusCode);
    if (match.code) expect(err.code).toBe(match.code);
    if (match.msg) expect(err.message).toMatch(match.msg);
  }
}

describe("time-range edge cases", () => {
  const M = `${P}.range`;

  beforeAll(async () => {
    await client.write({ measurement: M, tags: { t: "a" }, fields: { v: [1, 2] }, timestamps: [BASE, BASE + S] });
  });

  it("inverted time range -> flat 400 INVALID_QUERY", async () => {
    await expectQueryError(`avg:${M}(v)`, { startTime: BASE + 10 * S, endTime: BASE },
      { statusCode: 400, code: "INVALID_QUERY", msg: /startTime must be less than endTime/ });
  });

  it("zero-width range (startTime == endTime) -> flat 400", async () => {
    await expectQueryError(`avg:${M}(v)`, { startTime: BASE, endTime: BASE },
      { statusCode: 400, code: "INVALID_QUERY", msg: /startTime must be less than endTime/ });
  });

  it("negative query times -> clean 400, no hang", async () => {
    // start/end are uint64 on the wire; negative JS numbers wrap to huge
    // uint64 values, which the server rejects via its range check.
    await expectQueryError(`avg:${M}(v)`, { startTime: -2_000_000_000, endTime: 1 },
      { statusCode: 400, code: "INVALID_QUERY" });
  });

  it("negative timestamps on write are accepted but land at wrapped uint64 positions (documented hazard)", async () => {
    // The wire type is uint64: -5e9 wraps to 2^64-5e9. The write succeeds
    // and the data is invisible to all sane query ranges. Pinned as-is.
    const m = `${P}.negts`;
    const w = await client.write({ measurement: m, tags: { t: "a" }, fields: { v: 1.5 }, timestamps: [-5_000_000_000] });
    expect(w.status).toBe("success");
    const r = await client.query(`avg:${m}(v)`, { startTime: 0, endTime: BASE });
    expect(r.series).toEqual([]);
  });

  it("timestamps near int64 max round-trip", async () => {
    const m = `${P}.i64max`;
    const ts = 9_223_372_036_854_775_806n; // 2^63 - 2
    await client.write({ measurement: m, tags: { t: "a" }, fields: { v: 2.5 }, timestamps: [ts] });
    const r = await client.query(`avg:${m}(v)`, { startTime: ts - 10n, endTime: ts + 1n, precise: true });
    const f = findField(r, "v");
    expect(f.timestamps).toEqual([ts]);
    expect(f.values).toEqual([2.5]);
  });

  it("timestamps beyond int64 max (uint64 range) round-trip", async () => {
    const m = `${P}.u64`;
    const ts = 2n ** 63n + 1000n;
    await client.write({ measurement: m, tags: { t: "a" }, fields: { v: 1.25 }, timestamps: [ts] });
    const r = await client.query(`avg:${m}(v)`, { startTime: ts - 10n, endTime: ts + 10n, precise: true });
    const f = findField(r, "v");
    expect(f.timestamps).toEqual([ts]);
    expect(f.values).toEqual([1.25]);
  });

  it("year-9999 timestamps cannot exist (beyond uint64 ns); far-future year-2500 works", async () => {
    // Year 9999 in ns (~2.5e20) exceeds uint64 max (~1.8e19) — unrepresentable
    // on the wire by construction. Pin the furthest sane future instead.
    const m = `${P}.future`;
    const ts = 16_725_225_600_000_000_000n; // ~year 2500 in ns
    await client.write({ measurement: m, tags: { t: "a" }, fields: { v: 9.9 }, timestamps: [ts] });
    const r = await client.query(`avg:${m}(v)`, { startTime: ts - 10n, endTime: ts + 10n, precise: true });
    expect(findField(r, "v").timestamps).toEqual([ts]);
  });
});

describe("query string edge cases", () => {
  it("empty query string -> flat 400 with descriptive message", async () => {
    await expectQueryError("", { startTime: BASE, endTime: BASE + S },
      { statusCode: 400, code: "INVALID_QUERY", msg: /cannot be empty/i });
  });

  it("unknown aggregation method -> flat 400 listing valid methods", async () => {
    await expectQueryError(`p99:${P}.x(v)`, { startTime: BASE, endTime: BASE + S },
      { statusCode: 400, code: "INVALID_QUERY", msg: /avg.*min.*max/s });
  });

  it("missing aggregation method / malformed syntax -> flat 400", async () => {
    for (const q of [`${P}.x(v)`, `avg:`, `avg:m{unclosed`, `avg:m(v) by deviceId}`]) {
      await expectQueryError(q, { startTime: BASE, endTime: BASE + S }, { statusCode: 400, code: "INVALID_QUERY" });
    }
  });

  it("unknown measurement -> clean empty success, not an error", async () => {
    const r = await client.query(`avg:${P}.definitely_not_here(v)`, { startTime: BASE, endTime: BASE + S });
    expect(r.status).toBe("success");
    expect(r.series).toEqual([]);
    expect(r.statistics.seriesCount).toBe(0);
    expect(r.statistics.pointCount).toBe(0);
  });
});

describe("measurement and tag name edge cases", () => {
  it("10000-char measurement name (server's exact limit) writes and queries", async () => {
    const name = `${P}_` + "m".repeat(10_000 - P.length - 1);
    expect(name.length).toBe(10_000);
    const w = await client.write({ measurement: name, tags: { t: "a" }, fields: { v: 7 }, timestamps: [BASE] });
    expect(w.status).toBe("success");
    const r = await client.query(`avg:${name}(v)`, { startTime: BASE, endTime: BASE + S });
    expect(findField(r, "v").values).toEqual([7]);
  });

  // SERVER BUG: the FIRST write of a measurement name longer than 10000
  // chars fails with a raw 500 "Internal server error" (server log:
  // "SeriesMetadata has suspiciously long strings") instead of a flat 4xx
  // validation error; a RETRY of the same write then reports success, and
  // the measurement never appears in /measurements. Repro: POST /write with
  // a fresh 10001+-char measurement -> 500.
  it.fails("SERVER BUG: measurement name over 10000 chars is rejected with a flat 4xx (not 500)", async () => {
    const name = `${P}_long_` + "m".repeat(10_100);
    try {
      await client.write({ measurement: name, tags: { t: "a" }, fields: { v: 1 }, timestamps: [BASE] });
      // Either a clean partial/error response...
      expect.unreachable("expected a validation error");
    } catch (e) {
      const err = e as TimestarError;
      // ...or a thrown TimestarError with a 4xx status. The actual behavior
      // is a 500, which fails this assertion.
      expect(err.statusCode).toBeGreaterThanOrEqual(400);
      expect(err.statusCode).toBeLessThan(500);
    }
  });

  it("empty tag key -> partial with per-point error, nothing written", async () => {
    const w = await client.write({ measurement: `${P}.etk`, tags: { "": "x" }, fields: { v: 1 }, timestamps: [BASE] });
    expect(w.status).toBe("partial");
    expect(w.pointsWritten).toBe(0);
    expect(w.failedWrites).toBe(1);
    expect(w.errors.join(" ")).toMatch(/Invalid tag key/);
  });

  it("empty tag value -> partial with per-point error, nothing written", async () => {
    const w = await client.write({ measurement: `${P}.etv`, tags: { x: "" }, fields: { v: 1 }, timestamps: [BASE] });
    expect(w.status).toBe("partial");
    expect(w.pointsWritten).toBe(0);
    expect(w.errors.join(" ")).toMatch(/Invalid tag value/);
  });

  it("empty measurement name -> partial with per-point error", async () => {
    const w = await client.write({ measurement: "", tags: { t: "a" }, fields: { v: 1 }, timestamps: [BASE] });
    expect(w.status).toBe("partial");
    expect(w.pointsWritten).toBe(0);
    expect(w.errors.join(" ")).toMatch(/Invalid measurement name/);
  });

  it("write with zero fields succeeds vacuously and creates no measurement", async () => {
    const m = `${P}.nofields`;
    const w = await client.write({ measurement: m, tags: { t: "a" }, fields: {}, timestamps: [BASE] });
    expect(w.status).toBe("success");
    expect(w.pointsWritten).toBe(0);
    expect(w.failedWrites).toBe(0);
    const list = await client.measurements({ prefix: m });
    expect(list.measurements).toEqual([]);
  });
});

describe("duplicate and concurrent writes", () => {
  it("1000 duplicate points at the same timestamp append (count == 1000)", async () => {
    const m = `${P}.dup`;
    const w = await client.write({
      measurement: m, tags: { t: "a" },
      fields: { v: Array.from({ length: 1000 }, () => 5) },
      timestamps: Array.from({ length: 1000 }, () => BASE),
    });
    expect(w.status).toBe("success");
    expect(w.pointsWritten).toBe(1000);
    const r = await client.query(`count:${m}(v)`, { startTime: BASE, endTime: BASE + S, aggregationInterval: "1h" });
    expect(findField(r, "v").values).toEqual([1000]);
    // And the aggregate over duplicates is exact: sum = 5000, avg = 5.
    const rs = await client.query(`sum:${m}(v)`, { startTime: BASE, endTime: BASE + S, aggregationInterval: "1h" });
    expect(findField(rs, "v").values).toEqual([5000]);
  });

  it("queries during heavy concurrent writes never fail and counts grow monotonically to the total", async () => {
    const m = `${P}.conc`;
    const BATCHES = 20;
    const PER_BATCH = 500;
    const writeOne = (b: number) =>
      client.write({
        measurement: m, tags: { w: `w${b % 4}` },
        fields: { v: Array.from({ length: PER_BATCH }, (_, i) => b * PER_BATCH + i) },
        timestamps: Array.from({ length: PER_BATCH }, (_, i) => BASE + (b * PER_BATCH + i) * S),
      });
    const countNow = async () => {
      const r = await client.query(`count:${m}(v)`, {
        startTime: BASE, endTime: BASE + BATCHES * PER_BATCH * S, aggregationInterval: "30d",
      });
      expect(r.status).toBe("success");
      const f = r.series[0]?.fields?.v;
      return f ? (f.values as number[]).reduce((a, b) => a + b, 0) : 0;
    };

    const writes: Promise<unknown>[] = [];
    const observed: number[] = [];
    for (let b = 0; b < BATCHES; b++) {
      writes.push(writeOne(b).then((w) => expect(w.status).toBe("success")));
      if (b % 4 === 3) observed.push(await countNow());
    }
    await Promise.all(writes);
    observed.push(await countNow());

    // Monotone non-decreasing, never exceeding the final total.
    for (let i = 1; i < observed.length; i++) {
      expect(observed[i]).toBeGreaterThanOrEqual(observed[i - 1]);
    }
    expect(observed[observed.length - 1]).toBe(BATCHES * PER_BATCH);
    for (const c of observed) expect(c).toBeLessThanOrEqual(BATCHES * PER_BATCH);
  });

  it("rapid create/delete/create of the same measurement leaves only the final write", async () => {
    const m = `${P}.rapid`;
    for (let i = 0; i < 3; i++) {
      await client.write({ measurement: m, tags: { t: "a" }, fields: { v: i + 1 }, timestamps: [BASE + i * S] });
      const d = await client.delete({ measurement: m, tags: { t: "a" }, fields: ["v"] });
      expect(d.status).toBe("success");
    }
    await client.write({ measurement: m, tags: { t: "a" }, fields: { v: 99 }, timestamps: [BASE + 10 * S] });
    const r = await client.query(`avg:${m}(v)`, { startTime: BASE, endTime: BASE + 20 * S });
    const f = findField(r, "v");
    expect(f.timestamps).toEqual([BASE + 10 * S]);
    expect(f.values).toEqual([99]);
  });
});

describe("protocol-level pins (raw codecs)", () => {
  // SERVER BUG (CRITICAL, worked around in this client — see [S1] in
  // src/client.ts): compressed_timestamps are decoded with an upper bound of
  // bytes/2 + 1024 values (lib/http/proto_converters.cpp: "compressed data
  // can't encode more values than bytes/2"). FFOR delta-of-delta encodes
  // regular timestamps at ~0.03 bytes/value, so a single point with 2000
  // 1s-spaced timestamps decodes to exactly 1052 points — and the write
  // reports SUCCESS with no error. Silent data loss.
  it.fails("SERVER BUG: a single point with 2000 compressed timestamps stores all 2000", async () => {
    await protoInit();
    const m = `${P}.trunc`;
    const n = 2000;
    const ts = Array.from({ length: n }, (_, i) => BASE + i * S);
    const vals = Array.from({ length: n }, (_, i) => i * 0.5);
    const body = await codecs.WriteRequest.encode({
      writes: [{
        measurement: m, tags: { t: "a" },
        fields: { v: { doubleValues: { values: vals } } } as any,
        compressedTimestamps: compressTimestamps(ts),
      }],
    });
    const res = await fetch(`http://${HOST}:${PORT}/write`, {
      method: "POST",
      headers: { "Content-Type": "application/protobuf", Accept: "application/protobuf" },
      body,
    });
    const wr = await codecs.WriteResponse.decode(new Uint8Array(await res.arrayBuffer()));
    // Actual: status "partial" with "Field 'v' has 2000 values but 1052
    // timestamps" (raw values path) — or silent truncation to 1052 when the
    // values are compressed too.
    expect(wr.status).toBe("success");
    expect(wr.pointsWritten).toBe(n);
  });

  // SERVER BUG: a bare protobuf DeleteRequest that uses the STRUCTURED form
  // (measurement/tags/fields, no series key) parses as a VALID-BUT-EMPTY
  // BatchDeleteRequest (its fields are unknown-field-skipped), so the server
  // executes zero deletes and reports success with totalRequests=0. The
  // client works around this by always sending BatchDeleteRequest.
  it.fails("SERVER BUG: bare structured protobuf DeleteRequest deletes the series", async () => {
    await protoInit();
    const m = `${P}.baredelete`;
    await client.write({ measurement: m, tags: { t: "a" }, fields: { v: [1, 2] }, timestamps: [BASE, BASE + S] });
    const body = await codecs.DeleteRequest.encode({ measurement: m, tags: { t: "a" }, fields: ["v"] });
    const res = await fetch(`http://${HOST}:${PORT}/delete`, {
      method: "POST",
      headers: { "Content-Type": "application/protobuf", Accept: "application/protobuf" },
      body,
    });
    expect(res.status).toBe(200);
    const dr = await codecs.DeleteResponse.decode(new Uint8Array(await res.arrayBuffer()));
    expect(dr.totalRequests).toBe(1); // actual: 0 — request silently ignored
    expect(dr.deletedCount).toBe(1);
  });

  // SERVER BUG: a write whose WAL entry exceeds the configured WAL size
  // threshold is rejected with HTTP 500 (message: "Insert batch too large
  // ... exceeds WAL limit") instead of a 4xx with a flat error body. Only
  // meaningful to pin when the threshold is small enough to reach in a test
  // (the dedicated correctness server runs with a 2 MiB threshold; against
  // a default 16 MiB server this test is skipped).
  it.fails.skipIf(WAL_THRESHOLD > 8 * 1024 * 1024)(
    "SERVER BUG: oversized write batch is rejected with 4xx, not 500",
    async () => {
      const m = `${P}.oversize`;
      // Random doubles ~8.2 WAL bytes/pt; 1.5x the threshold guarantees the
      // single-series (single-shard) entry exceeds the limit.
      const n = Math.ceil((WAL_THRESHOLD * 1.5) / 8);
      const w = client.write({
        measurement: m, tags: { t: "a" },
        fields: { v: Array.from({ length: n }, () => Math.random() * 1e9) },
        timestamps: Array.from({ length: n }, (_, i) => BASE + i * S),
      });
      try {
        await w;
        // Success would also be acceptable server behavior (auto-split).
      } catch (e) {
        const err = e as TimestarError;
        expect(err.statusCode).toBeGreaterThanOrEqual(400);
        expect(err.statusCode).toBeLessThan(500); // actual: 500
      }
    },
  );

  it("corrupt compressed payloads are rejected per-point with partial status (never 500)", async () => {
    await protoInit();
    const garbage = new Uint8Array(64).fill(0xab);
    const body = await codecs.WriteRequest.encode({
      writes: [{
        measurement: `${P}.corrupt`, tags: { t: "a" },
        fields: { bad: { doubleValues: { compressedAlp: garbage } } } as any,
        compressedTimestamps: compressTimestamps([BASE, BASE + S]),
      }],
    });
    const res = await fetch(`http://${HOST}:${PORT}/write`, {
      method: "POST",
      headers: { "Content-Type": "application/protobuf", Accept: "application/protobuf" },
      body,
    });
    expect(res.status).toBe(200);
    const wr = await codecs.WriteResponse.decode(new Uint8Array(await res.arrayBuffer()));
    expect(wr.status).toBe("partial");
    expect(wr.pointsWritten).toBe(0);
    expect(wr.failedWrites).toBe(1);
    expect(wr.errors.join(" ")).toMatch(/decode|corrupt/i);
  });

  it("server stays healthy after the adversarial barrage", async () => {
    await sleep(100);
    expect(await client.isHealthy()).toBe(true);
    // And a normal write/query still works end to end.
    const m = `${P}.sanity`;
    await client.write({ measurement: m, tags: { t: "a" }, fields: { v: 1.5 }, timestamps: [BASE] });
    const r = await client.query(`avg:${m}(v)`, { startTime: BASE, endTime: BASE + S });
    expect(findField(r, "v").values).toEqual([1.5]);
  });
});
