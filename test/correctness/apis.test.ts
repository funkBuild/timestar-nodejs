// Metadata / cardinality / retention / delete API correctness.

import { describe, it, expect, beforeAll } from "vitest";
import { makeClient, flushToTsm, uniquePrefix, findField, BASE, S } from "./helpers";
import { TimestarError } from "../../src/client";

const client = makeClient();
const P = uniquePrefix("apis");

beforeAll(async () => {
  expect(await client.isHealthy()).toBe(true);
});

describe("measurements listing", () => {
  const NAMES = ["alpha", "beta", "gamma", "delta"].map((n) => `${P}.m_${n}`);

  beforeAll(async () => {
    await client.write(NAMES.map((m) => ({
      measurement: m, tags: { t: "a" }, fields: { v: 1 }, timestamps: [BASE],
    })));
  });

  it("prefix filter returns exactly the written measurements", async () => {
    const r = await client.measurements({ prefix: `${P}.m_` });
    expect(r.status).toBe("success");
    expect([...r.measurements].sort()).toEqual([...NAMES].sort());
    expect(r.total).toBe(NAMES.length);
  });

  it("limit and offset paginate consistently with the full listing", async () => {
    const all = await client.measurements({ prefix: `${P}.m_` });
    const page1 = await client.measurements({ prefix: `${P}.m_`, limit: 2, offset: 0 });
    const page2 = await client.measurements({ prefix: `${P}.m_`, limit: 2, offset: 2 });
    expect(page1.measurements.length).toBe(2);
    expect(page2.measurements.length).toBe(2);
    expect([...page1.measurements, ...page2.measurements]).toEqual(all.measurements);
    expect(page1.total).toBe(4); // total reflects the full filtered count
  });

  it("unknown prefix returns an empty listing", async () => {
    const r = await client.measurements({ prefix: `${P}.zz_nothing_` });
    expect(r.status).toBe("success");
    expect(r.measurements).toEqual([]);
    expect(r.total).toBe(0);
  });
});

describe("tags endpoint", () => {
  const M = `${P}.tagged`;

  beforeAll(async () => {
    await client.write([
      { measurement: M, tags: { host: "h1", dc: "east", env: "prod" }, fields: { v: 1 }, timestamps: [BASE] },
      { measurement: M, tags: { host: "h2", dc: "west", env: "prod" }, fields: { v: 2 }, timestamps: [BASE] },
      { measurement: M, tags: { host: "h3", dc: "east", env: "stage" }, fields: { v: 3 }, timestamps: [BASE] },
    ]);
  });

  it("returns every tag key with its full value set", async () => {
    const r = await client.tags(M);
    expect(r.status).toBe("success");
    expect(r.measurement).toBe(M);
    expect(Object.keys(r.tags).sort()).toEqual(["dc", "env", "host"]);
    expect([...r.tags.host].sort()).toEqual(["h1", "h2", "h3"]);
    expect([...r.tags.dc].sort()).toEqual(["east", "west"]);
    expect([...r.tags.env].sort()).toEqual(["prod", "stage"]);
  });

  it("tag parameter narrows to a single key", async () => {
    const r = await client.tags(M, { tag: "dc" });
    expect([...(r.tags.dc ?? [])].sort()).toEqual(["east", "west"]);
    expect(Object.keys(r.tags)).toEqual(["dc"]);
  });

  it("unknown measurement returns empty tags, not an error", async () => {
    const r = await client.tags(`${P}.no_such_measurement`);
    expect(r.status).toBe("success");
    expect(r.tags).toEqual({});
  });
});

describe("fields endpoint", () => {
  it("reports every field with its correct type", async () => {
    const M = `${P}.typed`;
    await client.write({
      measurement: M, tags: { t: "a" },
      fields: { fl: 1.5, in: 42n, bo: true, st: "text" },
      timestamps: [BASE],
    });
    const r = await client.fields(M);
    expect(r.status).toBe("success");
    const byName = Object.fromEntries(r.fields.map((f) => [f.name, f.type]));
    expect(byName).toEqual({ fl: "float", in: "integer", bo: "boolean", st: "string" });
  });

  it("fields from different series of one measurement are merged", async () => {
    const M = `${P}.fmerge`;
    await client.write([
      { measurement: M, tags: { host: "a" }, fields: { cpu: 1.0 }, timestamps: [BASE] },
      { measurement: M, tags: { host: "b" }, fields: { mem: 2.0, disk: 3.0 }, timestamps: [BASE] },
    ]);
    const r = await client.fields(M);
    expect(r.fields.map((f) => f.name).sort()).toEqual(["cpu", "disk", "mem"]);
  });

  it("unknown measurement returns an empty field list", async () => {
    const r = await client.fields(`${P}.no_such_measurement`);
    expect(r.status).toBe("success");
    expect(r.fields).toEqual([]);
  });
});

describe("cardinality endpoint", () => {
  it("estimates series and per-tag cardinalities for known data", async () => {
    const M = `${P}.card`;
    // 12 series: 4 hosts x 3 racks (all combinations).
    const writes = [];
    for (let h = 0; h < 4; h++) {
      for (const r of ["r1", "r2", "r3"]) {
        writes.push({ measurement: M, tags: { host: `h${h}`, rack: r }, fields: { v: 1 }, timestamps: [BASE] });
      }
    }
    await client.write(writes);
    const r = await client.cardinality(M);
    expect(r.status).toBe("success");
    expect(r.measurement).toBe(M);
    // HLL estimate: allow 5% error on 12 series.
    expect(r.estimatedSeriesCount).toBeGreaterThan(12 * 0.95);
    expect(r.estimatedSeriesCount).toBeLessThan(12 * 1.05);
    const byKey = Object.fromEntries(r.tagCardinalities.map((t) => [t.tagKey, t.estimatedCount]));
    expect(byKey.host).toBe(4);
    expect(byKey.rack).toBe(3);
  });
});

describe("retention API", () => {
  const M = `${P}.retention`;

  it("PUT/GET round-trips a plain TTL with exact nanosecond conversion", async () => {
    await client.setRetention(M, "30d");
    const r = await client.getRetention(M);
    expect(r.status).toBe("success");
    expect(r.policy.measurement).toBe(M);
    expect(r.policy.ttl).toBe("30d");
    // 30 days = 30 * 86400e9 ns, derived arithmetically.
    expect(r.policy.ttlNanos).toBe(30 * 86400 * 1e9);
  });

  it("PUT with downsample stores after/interval/method with exact nanos", async () => {
    await client.setRetention(M, "90d", { after: "7d", interval: "1h", method: "avg" });
    const r = await client.getRetention(M);
    expect(r.policy.ttl).toBe("90d");
    expect(r.policy.ttlNanos).toBe(90 * 86400 * 1e9);
    expect(r.policy.downsample).toBeDefined();
    expect(r.policy.downsample!.after).toBe("7d");
    expect(r.policy.downsample!.interval).toBe("1h");
    expect(r.policy.downsample!.method).toBe("avg");
    expect((r.policy.downsample as any).afterNanos).toBe(7 * 86400 * 1e9);
    expect((r.policy.downsample as any).intervalNanos).toBe(3600 * 1e9);
  });

  it("updating an existing policy overwrites it", async () => {
    await client.setRetention(M, "14d");
    const r = await client.getRetention(M);
    expect(r.policy.ttl).toBe("14d");
    expect(r.policy.downsample == null || r.policy.downsample.after === "").toBe(true);
  });

  it("DELETE removes the policy; GET then 404s with a descriptive message", async () => {
    await client.deleteRetention(M);
    try {
      await client.getRetention(M);
      expect.unreachable("getRetention should have thrown after delete");
    } catch (e) {
      expect(e).toBeInstanceOf(TimestarError);
      const err = e as TimestarError;
      expect(err.statusCode).toBe(404);
      expect(err.message).toContain("No retention policy");
    }
  });

  it("GET for a measurement that never had a policy 404s cleanly", async () => {
    await expect(client.getRetention(`${P}.never_had_policy`)).rejects.toMatchObject({ statusCode: 404 });
  });
});

describe("delete API", () => {
  it("structured delete (measurement+tags+fields) removes the series", async () => {
    const M = `${P}.del1`;
    const ts = [BASE, BASE + S, BASE + 2 * S];
    await client.write({ measurement: M, tags: { host: "a" }, fields: { v: [1, 2, 3] }, timestamps: ts });
    const d = await client.delete({ measurement: M, tags: { host: "a" }, fields: ["v"] });
    expect(d.status).toBe("success");
    expect(d.totalRequests).toBe(1);
    expect(d.deletedCount).toBe(1);
    const r = await client.query(`avg:${M}(v)`, { startTime: BASE, endTime: BASE + 3 * S });
    expect(r.series).toEqual([]);
  });

  it("delete by series-key string removes exactly that series", async () => {
    const M = `${P}.del2`;
    await client.write([
      { measurement: M, tags: { host: "a" }, fields: { v: [1] }, timestamps: [BASE] },
      { measurement: M, tags: { host: "b" }, fields: { v: [2] }, timestamps: [BASE] },
    ]);
    // Canonical series key format: "measurement,tag=value field"
    const d = await client.delete({ series: `${M},host=a v` });
    expect(d.status).toBe("success");
    expect(d.deletedCount).toBe(1);
    const r = await client.query(`avg:${M}(v) by {host}`, { startTime: BASE, endTime: BASE + S, aggregationInterval: "1h" });
    expect(r.series.length).toBe(1);
    expect(r.series[0].tags.host).toBe("b");
    expect(r.series[0].fields.v.values).toEqual([2]);
  });

  it("time-range delete removes only the middle segment (range is INCLUSIVE at both ends)", async () => {
    const M = `${P}.del3`;
    const n = 12;
    const ts = Array.from({ length: n }, (_, i) => BASE + i * S);
    const vals = Array.from({ length: n }, (_, i) => i * 10);
    await client.write({ measurement: M, tags: { host: "a" }, fields: { v: vals }, timestamps: ts });

    // Delete [t4, t7] inclusive -> 4 points removed.
    const d = await client.delete({
      measurement: M, tags: { host: "a" }, fields: ["v"],
      startTime: ts[4], endTime: ts[7],
    });
    expect(d.status).toBe("success");

    const r = await client.query(`avg:${M}(v)`, { startTime: ts[0], endTime: ts[n - 1] + S });
    const f = findField(r, "v");
    const keep = [0, 1, 2, 3, 8, 9, 10, 11];
    expect(f.timestamps).toEqual(keep.map((i) => ts[i]));
    expect(f.values).toEqual(keep.map((i) => vals[i]));
  });

  it("re-insert after delete works and returns the new values", async () => {
    const M = `${P}.del4`;
    const ts = [BASE, BASE + S, BASE + 2 * S];
    await client.write({ measurement: M, tags: { host: "a" }, fields: { v: [1, 2, 3] }, timestamps: ts });
    await client.delete({ measurement: M, tags: { host: "a" }, fields: ["v"], startTime: ts[1], endTime: ts[1] });

    const afterDelete = await client.query(`avg:${M}(v)`, { startTime: ts[0], endTime: ts[2] + S });
    expect(findField(afterDelete, "v").timestamps).toEqual([ts[0], ts[2]]);

    // Re-insert the deleted timestamp with a different value.
    await client.write({ measurement: M, tags: { host: "a" }, fields: { v: 22 }, timestamps: [ts[1]] });
    const after = await client.query(`avg:${M}(v)`, { startTime: ts[0], endTime: ts[2] + S });
    const f = findField(after, "v");
    expect(f.timestamps).toEqual(ts);
    expect(f.values).toEqual([1, 22, 3]);
  });

  it("delete of one field leaves sibling fields intact", async () => {
    const M = `${P}.del5`;
    await client.write({
      measurement: M, tags: { dev: "s1" },
      fields: { temperature: 22.5, humidity: 55.0, pressure: 1013.25 },
      timestamps: [BASE],
    });
    await client.delete({ measurement: M, tags: { dev: "s1" }, fields: ["temperature"] });
    const r = await client.query(`avg:${M}()`, { startTime: BASE, endTime: BASE + S });
    const names = r.series.flatMap((s) => Object.keys(s.fields)).sort();
    expect(names).toEqual(["humidity", "pressure"]);
    expect(findField(r, "humidity").values).toEqual([55.0]);
    expect(findField(r, "pressure").values).toEqual([1013.25]);
  });

  it("batch delete removes multiple series in one call", async () => {
    const M = `${P}.del6`;
    await client.write([
      { measurement: M, tags: { host: "a" }, fields: { v: [1] }, timestamps: [BASE] },
      { measurement: M, tags: { host: "b" }, fields: { v: [2] }, timestamps: [BASE] },
      { measurement: M, tags: { host: "c" }, fields: { v: [3] }, timestamps: [BASE] },
    ]);
    const d = await client.delete([
      { measurement: M, tags: { host: "a" }, fields: ["v"] },
      { measurement: M, tags: { host: "c" }, fields: ["v"] },
    ]);
    expect(d.status).toBe("success");
    expect(d.totalRequests).toBe(2);
    expect(d.deletedCount).toBe(2);
    const r = await client.query(`avg:${M}(v) by {host}`, { startTime: BASE, endTime: BASE + S, aggregationInterval: "1h" });
    expect(r.series.map((s) => s.tags.host)).toEqual(["b"]);
  });

  it("delete of non-existent data succeeds idempotently with deletedCount 0", async () => {
    const d = await client.delete({
      measurement: `${P}.never_existed`, tags: { x: "y" }, fields: ["v"],
      startTime: BASE, endTime: BASE + S,
    });
    expect(d.status).toBe("success");
    expect(d.deletedCount).toBe(0);
  });

  it("delete works on data already flushed to TSM (tombstones honored by queries)", async () => {
    const M = `${P}.del7`;
    const ts = Array.from({ length: 6 }, (_, i) => BASE + i * S);
    const vals = [10, 20, 30, 40, 50, 60];
    await client.write({ measurement: M, tags: { host: "a" }, fields: { v: vals }, timestamps: ts });
    await flushToTsm(client);

    await client.delete({
      measurement: M, tags: { host: "a" }, fields: ["v"],
      startTime: ts[2], endTime: ts[3],
    });
    const r = await client.query(`sum:${M}(v)`, { startTime: ts[0], endTime: ts[5] + S, aggregationInterval: "1h" });
    // 10+20+50+60 (30 and 40 tombstoned in TSM)
    expect(findField(r, "v").values).toEqual([140]);
  });
});
