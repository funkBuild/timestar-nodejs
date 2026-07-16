// Group-by and scope-filter correctness.

import { describe, it, expect, beforeAll } from "vitest";
import { makeClient, uniquePrefix, BASE, S, avg } from "./helpers";
import type { QueryResponse, SeriesResult } from "../../src/types";

const client = makeClient();
const P = uniquePrefix("query");

// Fixture: measurement with hosts x dcs, one field. v = deterministic per series.
//   hosts: web-1, web-2, api-1, db-x   dcs: east, west
const HOSTS = ["web-1", "web-2", "api-1", "db-x"] as const;
const DCS: Record<(typeof HOSTS)[number], string> = {
  "web-1": "east", "web-2": "west", "api-1": "east", "db-x": "west",
};
// 4 points per series at BASE..BASE+3s; value = base value per host + i.
const HOST_BASE: Record<string, number> = { "web-1": 10, "web-2": 20, "api-1": 40, "db-x": 80 };
const NPTS = 4;
const TS = Array.from({ length: NPTS }, (_, i) => BASE + i * S);
const M = `${P}.metrics`;

function seriesValues(host: string): number[] {
  return Array.from({ length: NPTS }, (_, i) => HOST_BASE[host] + i);
}

// Whole-range single bucket per group.
const WHOLE = { startTime: BASE, endTime: BASE + NPTS * S, aggregationInterval: "1h" } as const;

function groupOf(resp: QueryResponse, wantTags: Record<string, string>): SeriesResult {
  const found = resp.series.filter((s) =>
    Object.entries(wantTags).every(([k, v]) => s.tags[k] === v),
  );
  expect(found.length, `series with tags ${JSON.stringify(wantTags)} (got ${JSON.stringify(resp.series.map((s) => s.tags))})`).toBe(1);
  return found[0];
}

beforeAll(async () => {
  expect(await client.isHealthy()).toBe(true);
  await client.write(
    HOSTS.map((h) => ({
      measurement: M,
      tags: { host: h, dc: DCS[h] },
      fields: { v: seriesValues(h) },
      timestamps: TS,
    })),
  );
});

describe("group-by", () => {
  it("single tag: one series per host with the per-group aggregate", async () => {
    const r = await client.query(`avg:${M}(v) by {host}`, WHOLE);
    expect(r.status).toBe("success");
    expect(r.series.length).toBe(HOSTS.length);
    for (const h of HOSTS) {
      const g = groupOf(r, { host: h });
      expect(g.fields.v.values).toEqual([avg(seriesValues(h))]);
    }
  });

  it("two tags: one series per (dc, host) combination present in the data", async () => {
    const r = await client.query(`avg:${M}(v) by {dc,host}`, WHOLE);
    // Only combinations that exist are returned (4, not 4x2).
    expect(r.series.length).toBe(HOSTS.length);
    for (const h of HOSTS) {
      const g = groupOf(r, { host: h, dc: DCS[h] });
      expect(g.fields.v.values).toEqual([avg(seriesValues(h))]);
    }
  });

  it("group-by one tag aggregates across the other: by {dc} averages the dc's hosts", async () => {
    const r = await client.query(`avg:${M}(v) by {dc}`, WHOLE);
    expect(r.series.length).toBe(2);
    for (const dc of ["east", "west"]) {
      const hosts = HOSTS.filter((h) => DCS[h] === dc);
      const all = hosts.flatMap((h) => seriesValues(h));
      const g = groupOf(r, { dc });
      expect(g.fields.v.values[0]).toBeCloseTo(avg(all), 10);
    }
  });

  it("group-by with scope filter: only matching groups are returned", async () => {
    const r = await client.query(`avg:${M}(v){dc:east} by {host}`, WHOLE);
    const eastHosts = HOSTS.filter((h) => DCS[h] === "east");
    expect(r.series.length).toBe(eastHosts.length);
    for (const h of eastHosts) {
      const g = groupOf(r, { host: h });
      expect(g.fields.v.values).toEqual([avg(seriesValues(h))]);
    }
  });

  it("sum group-by with per-second interval buckets", async () => {
    const r = await client.query(`sum:${M}(v) by {dc}`, {
      startTime: BASE, endTime: BASE + NPTS * S, aggregationInterval: "1s",
    });
    expect(r.series.length).toBe(2);
    for (const dc of ["east", "west"]) {
      const hosts = HOSTS.filter((h) => DCS[h] === dc);
      const g = groupOf(r, { dc });
      expect(g.fields.v.timestamps).toEqual(TS);
      // Per 1s bucket: sum of that timestamp's value across the dc's hosts.
      const expected = TS.map((_, i) => hosts.reduce((acc, h) => acc + HOST_BASE[h] + i, 0));
      expect(g.fields.v.values).toEqual(expected);
    }
  });

  it("group-by tag with unicode values", async () => {
    const MU = `${P}.uni`;
    const cities: Array<[string, number]> = [["東京", 1], ["Zürich", 2], ["São Paulo", 3], ["🚀-base", 4]];
    await client.write(
      cities.map(([city, v]) => ({
        measurement: MU, tags: { city }, fields: { v }, timestamps: [BASE],
      })),
    );
    const r = await client.query(`avg:${MU}(v) by {city}`, { startTime: BASE, endTime: BASE + S, aggregationInterval: "1h" });
    expect(r.series.length).toBe(cities.length);
    for (const [city, v] of cities) {
      const g = groupOf(r, { city });
      expect(g.fields.v.values).toEqual([v]);
    }
  });

  it("high-cardinality group-by: 1000 tag values complete with correct per-group values", async () => {
    const MH = `${P}.hc`;
    // 1000 series, one point each: id{i} -> v = i. Written in chunks to stay
    // well under the WAL batch limit.
    const CHUNK = 100;
    for (let c = 0; c < 10; c++) {
      await client.write(
        Array.from({ length: CHUNK }, (_, k) => {
          const id = c * CHUNK + k;
          return { measurement: MH, tags: { id: `id${id}` }, fields: { v: id }, timestamps: [BASE] };
        }),
      );
    }
    const r = await client.query(`avg:${MH}(v) by {id}`, { startTime: BASE, endTime: BASE + S, aggregationInterval: "1h" });
    expect(r.status).toBe("success");
    expect(r.series.length).toBe(1000);
    // Every group's value equals its id; sum over groups = 0+1+...+999.
    let total = 0;
    for (const s of r.series) {
      const id = parseInt(s.tags.id.slice(2), 10);
      expect(s.fields.v.values).toEqual([id]);
      total += s.fields.v.values[0] as number;
    }
    expect(total).toBe((999 * 1000) / 2);
  });
});

describe("scopes", () => {
  const whole = WHOLE;

  it("exact match selects one series", async () => {
    const r = await client.query(`avg:${M}(v){host:web-1}`, whole);
    expect(r.series.length).toBe(1);
    expect(r.series[0].fields.v.values).toEqual([avg(seriesValues("web-1"))]);
  });

  it("two ANDed scopes must both match", async () => {
    const both = await client.query(`avg:${M}(v){host:web-1,dc:east}`, whole);
    expect(both.series.length).toBe(1);
    expect(both.series[0].fields.v.values).toEqual([avg(seriesValues("web-1"))]);

    // Same host, wrong dc -> nothing.
    const none = await client.query(`avg:${M}(v){host:web-1,dc:west}`, whole);
    expect(none.status).toBe("success");
    expect(none.series.length).toBe(0);
  });

  it("wildcard * suffix matches prefixed values", async () => {
    const r = await client.query(`avg:${M}(v){host:web-*} by {host}`, whole);
    expect(r.series.map((s) => s.tags.host).sort()).toEqual(["web-1", "web-2"]);
  });

  it("bare * matches all values", async () => {
    const r = await client.query(`avg:${M}(v){host:*} by {host}`, whole);
    expect(r.series.map((s) => s.tags.host).sort()).toEqual([...HOSTS].sort());
  });

  it("? matches exactly one character", async () => {
    const r = await client.query(`avg:${M}(v){host:web-?} by {host}`, whole);
    expect(r.series.map((s) => s.tags.host).sort()).toEqual(["web-1", "web-2"]);
    // db-x is 4 chars; 'db-?' matches it, '?b-x' also — check single-char position.
    const r2 = await client.query(`avg:${M}(v){host:?b-x} by {host}`, whole);
    expect(r2.series.map((s) => s.tags.host)).toEqual(["db-x"]);
  });

  it("~regex scope filters by regular expression", async () => {
    const r = await client.query(`avg:${M}(v){host:~(web|api)-[0-9]+} by {host}`, whole);
    expect(r.series.map((s) => s.tags.host).sort()).toEqual(["api-1", "web-1", "web-2"]);
  });

  it("/re/ scope filters by regular expression", async () => {
    const r = await client.query(`avg:${M}(v){host:/^web-[12]$/} by {host}`, whole);
    expect(r.series.map((s) => s.tags.host).sort()).toEqual(["web-1", "web-2"]);
  });

  it("scope matching zero series returns a clean empty success, not an error", async () => {
    for (const scope of ["{host:nope}", "{host:zzz-*}", "{host:~xyz.*}", "{nosuchtag:web-1}"]) {
      const r = await client.query(`avg:${M}(v)${scope}`, whole);
      expect(r.status, scope).toBe("success");
      expect(r.series, scope).toEqual([]);
      expect(r.statistics.seriesCount, scope).toBe(0);
    }
  });

  it("unicode exact scope matches", async () => {
    const MU = `${P}.uniscope`;
    await client.write([
      { measurement: MU, tags: { city: "東京" }, fields: { v: 5 }, timestamps: [BASE] },
      { measurement: MU, tags: { city: "Osaka" }, fields: { v: 6 }, timestamps: [BASE] },
    ]);
    const r = await client.query(`avg:${MU}(v){city:東京}`, { startTime: BASE, endTime: BASE + S, aggregationInterval: "1h" });
    expect(r.series.length).toBe(1);
    expect(r.series[0].fields.v.values).toEqual([5]);
  });
});
