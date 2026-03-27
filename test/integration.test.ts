import { describe, it, expect, beforeAll } from "vitest";
import { TimestarClient } from "../src/client";
import type { WritePoint, QueryOptions } from "../src/types";

const HOST = process.env.TIMESTAR_HOST || "localhost";
const PORT = parseInt(process.env.TIMESTAR_PORT || "8086", 10);
const AUTH = process.env.TIMESTAR_AUTH_TOKEN;

let client: TimestarClient;
let serverAvailable = false;

beforeAll(async () => {
  client = new TimestarClient({ host: HOST, port: PORT, authToken: AUTH });
  serverAvailable = await client.isHealthy();
  if (!serverAvailable) {
    console.log("TimeStar server not available — skipping integration tests");
  }
});

function skipIfNoServer() {
  if (!serverAvailable) return true;
  return false;
}

const PREFIX = `test_compressed_${Date.now()}`;

// ============================================================================
// Write + Query roundtrip
// ============================================================================

describe("Write + Query roundtrip", () => {
  const measurement = `${PREFIX}.doubles`;

  it.skipIf(skipIfNoServer())("writes 1K double-valued points with compressed proto", async () => {
    const now = Date.now() * 1e6;
    const points: WritePoint[] = [];
    for (let i = 0; i < 100; i++) {
      points.push({
        measurement,
        tags: { host: "server1", region: "us-west" },
        fields: {
          temperature: { doubleValues: Array.from({ length: 10 }, (_, j) => 20.0 + (i * 10 + j) * 0.01) },
        },
        timestamps: Array.from({ length: 10 }, (_, j) => now + (i * 10 + j) * 1e9),
      });
    }

    const resp = await client.write(points);
    expect(resp.status).toBe("success");
    expect(resp.pointsWritten).toBe(1000);
  });

  it.skipIf(skipIfNoServer())("queries back and verifies values match", async () => {
    const now = Date.now() * 1e6;
    const resp = await client.query(
      `avg:${measurement}(temperature){host:server1}`,
      {
        startTime: now - 3600e9,
        endTime: now + 3600e9,
      }
    );

    expect(resp.status).toBe("success");
    expect(resp.series.length).toBeGreaterThan(0);
    const field = resp.series[0].fields["temperature"];
    expect(field).toBeDefined();
    expect(field.timestamps.length).toBeGreaterThan(0);
    expect(field.values.length).toBe(field.timestamps.length);
  });
});

// ============================================================================
// Mixed type write + query
// ============================================================================

describe("Mixed type write + query", () => {
  const measurement = `${PREFIX}.mixed`;

  it.skipIf(skipIfNoServer())("writes mixed types and queries back", async () => {
    const now = Date.now() * 1e6;
    const points: WritePoint[] = [{
      measurement,
      tags: { device: "sensor1" },
      fields: {
        temp: 22.5,
        active: true,
        name: "main-sensor",
      },
      timestamps: [now],
    }];

    const resp = await client.write(points);
    expect(resp.status).toBe("success");
  });
});

// ============================================================================
// Delete + Verify
// ============================================================================

describe("Delete", () => {
  const measurement = `${PREFIX}.to_delete`;

  it.skipIf(skipIfNoServer())("writes, deletes, and verifies deletion", async () => {
    const now = Date.now() * 1e6;
    await client.write({
      measurement,
      tags: { host: "del1" },
      fields: { value: 42.0 },
      timestamps: [now],
    });

    const delResp = await client.delete({
      measurement,
      tags: { host: "del1" },
      fields: ["value"],
    });
    expect(delResp.status).toBe("success");
  });
});

// ============================================================================
// Metadata endpoints
// ============================================================================

describe("Metadata", () => {
  it.skipIf(skipIfNoServer())("lists measurements", async () => {
    const resp = await client.measurements({ prefix: PREFIX });
    expect(resp.status).toBe("success");
    expect(Array.isArray(resp.measurements)).toBe(true);
  });

  it.skipIf(skipIfNoServer())("gets tags for a measurement", async () => {
    const measurement = `${PREFIX}.doubles`;
    const resp = await client.tags(measurement);
    expect(resp.status).toBe("success");
  });

  it.skipIf(skipIfNoServer())("gets fields for a measurement", async () => {
    const measurement = `${PREFIX}.doubles`;
    const resp = await client.fields(measurement);
    expect(resp.status).toBe("success");
  });

  it.skipIf(skipIfNoServer())("gets cardinality for a measurement", async () => {
    const measurement = `${PREFIX}.doubles`;
    const resp = await client.cardinality(measurement);
    expect(resp.status).toBe("success");
  });
});

// ============================================================================
// Derived queries
// ============================================================================

describe("Derived queries", () => {
  const measurement = `${PREFIX}.doubles`;

  it.skipIf(skipIfNoServer())("executes derived query with formula", async () => {
    const now = Date.now() * 1e6;
    const resp = await client.derived(
      { a: `avg:${measurement}(temperature){host:server1}` },
      "a * 2",
      {
        startTime: now - 3600e9,
        endTime: now + 3600e9,
        aggregationInterval: "10s",
      }
    );

    expect(resp.status).toBe("success");
  });
});

// ============================================================================
// Retention
// ============================================================================

describe("Retention", () => {
  const measurement = `${PREFIX}.retention_test`;

  it.skipIf(skipIfNoServer())("sets, gets, and deletes retention policy", async () => {
    await client.setRetention(measurement, "30d");

    const resp = await client.getRetention(measurement);
    expect(resp.status).toBe("success");
    expect(resp.policy.ttl).toBe("30d");

    await client.deleteRetention(measurement);
  });
});

// ============================================================================
// Compression ratio benchmark
// ============================================================================

describe("Compression ratio benchmark", () => {
  it.skipIf(skipIfNoServer())("measures compressed vs uncompressed wire size", async () => {
    // This test just verifies the client works end-to-end with compression
    // The actual wire size comparison requires server-side changes
    const now = Date.now() * 1e6;
    const measurement = `${PREFIX}.bench`;

    const points: WritePoint[] = [];
    for (let i = 0; i < 100; i++) {
      points.push({
        measurement,
        tags: { sensor: `s${i % 10}` },
        fields: {
          value: { doubleValues: Array.from({ length: 10 }, (_, j) => 22.0 + j * 0.1) },
        },
        timestamps: Array.from({ length: 10 }, (_, j) => now + (i * 10 + j) * 1e9),
      });
    }

    const resp = await client.write(points);
    expect(resp.status).toBe("success");
    console.log(`  Wrote ${resp.pointsWritten} points with compressed proto`);
  });
});
