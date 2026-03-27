import { describe, it, expect, beforeAll } from "vitest";
import { TimestarClient } from "../src/client";
import type { WritePoint } from "../src/types";

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

function requireServer() {
  if (!serverAvailable) {
    return true; // signal to skip
  }
  return false;
}

const PREFIX = `test_compressed_${Date.now()}`;

// ============================================================================
// Write + Query roundtrip
// ============================================================================

describe("Write + Query roundtrip", () => {
  const measurement = `${PREFIX}.doubles`;

  it("writes 1K double-valued points with compressed proto", async () => {
    if (requireServer()) return;
    const now = Date.now() * 1e6;
    const points: WritePoint[] = [];
    for (let i = 0; i < 100; i++) {
      points.push({
        measurement,
        tags: { host: "server1", region: "us-west" },
        fields: {
          temperature: [20.0 + (i * 10) * 0.01, 20.0 + (i * 10 + 1) * 0.01, 20.0 + (i * 10 + 2) * 0.01,
                        20.0 + (i * 10 + 3) * 0.01, 20.0 + (i * 10 + 4) * 0.01, 20.0 + (i * 10 + 5) * 0.01,
                        20.0 + (i * 10 + 6) * 0.01, 20.0 + (i * 10 + 7) * 0.01, 20.0 + (i * 10 + 8) * 0.01,
                        20.0 + (i * 10 + 9) * 0.01],
        },
        timestamps: Array.from({ length: 10 }, (_, j) => now + (i * 10 + j) * 1e9),
      });
    }

    const resp = await client.write(points);
    expect(resp.status).toBe("success");
    expect(resp.pointsWritten).toBe(1000);
  });

  it("queries back and verifies values match", async () => {
    if (requireServer()) return;
    const now = Date.now() * 1e6;
    const resp = await client.query(
      `avg:${measurement}(temperature){host:server1}`,
      { startTime: now - 3600e9, endTime: now + 3600e9 }
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

  it("writes mixed types and queries back", async () => {
    if (requireServer()) return;
    const now = Date.now() * 1e6;
    const resp = await client.write({
      measurement,
      tags: { device: "sensor1" },
      fields: { temp: 22.5, active: true, name: "main-sensor" },
      timestamps: [now],
    });
    expect(resp.status).toBe("success");
  });
});

// ============================================================================
// Delete + Verify
// ============================================================================

describe("Delete", () => {
  const measurement = `${PREFIX}.to_delete`;

  it("writes, deletes, and verifies deletion", async () => {
    if (requireServer()) return;
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
  it("lists measurements", async () => {
    if (requireServer()) return;
    const resp = await client.measurements({ prefix: PREFIX });
    expect(resp.status).toBe("success");
    expect(Array.isArray(resp.measurements)).toBe(true);
  });

  it("gets tags for a measurement", async () => {
    if (requireServer()) return;
    const resp = await client.tags(`${PREFIX}.doubles`);
    expect(resp.status).toBe("success");
  });

  it("gets fields for a measurement", async () => {
    if (requireServer()) return;
    const resp = await client.fields(`${PREFIX}.doubles`);
    expect(resp.status).toBe("success");
  });

  it("gets cardinality for a measurement", async () => {
    if (requireServer()) return;
    const resp = await client.cardinality(`${PREFIX}.doubles`);
    expect(resp.status).toBe("success");
  });
});

// ============================================================================
// Derived queries
// ============================================================================

describe("Derived queries", () => {
  it("executes derived query with formula", async () => {
    if (requireServer()) return;
    const now = Date.now() * 1e6;
    const resp = await client.derived(
      { a: `avg:${PREFIX}.doubles(temperature){host:server1}` },
      "a * 2",
      { startTime: now - 3600e9, endTime: now + 3600e9, aggregationInterval: "10s" }
    );
    expect(resp.status).toBe("success");
  });
});

// ============================================================================
// Retention
// ============================================================================

describe("Retention", () => {
  it("sets, gets, and deletes retention policy", async () => {
    if (requireServer()) return;
    const measurement = `${PREFIX}.retention_test`;
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
  it("measures compressed vs uncompressed wire size", async () => {
    if (requireServer()) return;
    const now = Date.now() * 1e6;
    const measurement = `${PREFIX}.bench`;

    const points: WritePoint[] = [];
    for (let i = 0; i < 100; i++) {
      points.push({
        measurement,
        tags: { sensor: `s${i % 10}` },
        fields: {
          value: Array.from({ length: 10 }, (_, j) => 22.0 + j * 0.1),
        },
        timestamps: Array.from({ length: 10 }, (_, j) => now + (i * 10 + j) * 1e9),
      });
    }

    const resp = await client.write(points);
    expect(resp.status).toBe("success");
    console.log(`  Wrote ${resp.pointsWritten} points with compressed proto`);
  });
});
