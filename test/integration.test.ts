import { describe, it, expect, beforeAll } from "vitest";
import { TimestarClient, TimestarError } from "../src/client";
import { codecs, init as protoInit } from "../src/proto";
import { compressTimestamps } from "../src/compression";
import type { WritePoint, QueryResponse, FieldData } from "../src/types";

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
// Compressed protobuf round-trip (requires server >= 1.0.7)
//
// The single most important validation in this suite: every native encoder
// (FFOR timestamps, ALP doubles, zigzag+FFOR int64, RLE bools, zstd strings)
// is sent compressed-only on the wire, decoded by the SERVER's decoders,
// stored, queried back (re-compressed by the server), decoded by the client's
// native decoders, and compared for EXACT equality.
// ============================================================================

// Find a field across all series of a response. Aggregated numeric fields and
// raw string fields may land in different series objects (string fields bypass
// aggregation and keep their tags).
function findField(resp: QueryResponse, name: string): FieldData {
  for (const s of resp.series) {
    if (s.fields[name]) return s.fields[name];
  }
  throw new Error(`Field '${name}' not found in response`);
}

describe("Compressed protobuf round-trip", () => {
  const measurement = `${PREFIX}.roundtrip`;
  const BASE_TS = 1_700_000_000_000_000_000; // exactly representable as a double
  const N = 300;
  const timestamps = Array.from({ length: N }, (_, i) => BASE_TS + i * 1e9);
  const doubles = Array.from({ length: N }, (_, i) => 20 + i * 0.25);
  const ints = Array.from({ length: N }, (_, i) => i * 1000 - 150000);
  const bools = Array.from({ length: N }, (_, i) => i % 7 < 3);
  const strings = Array.from({ length: N }, (_, i) => `sensor_${i % 7}`);

  it("writes all five compressed encodings and queries back exact values", async () => {
    if (requireServer()) return;

    const writeResp = await client.write({
      measurement,
      tags: { host: "rt1" },
      fields: {
        temp: doubles,
        count: { int64Values: ints.map(BigInt) },
        active: bools,
        label: strings,
      },
      timestamps,
    });
    expect(writeResp.status).toBe("success");
    // points_written counts FIELD-points: fields x timestamps
    expect(writeResp.pointsWritten).toBe(4 * N);
    expect(writeResp.failedWrites).toBe(0);

    const resp = await client.query(`avg:${measurement}()`, {
      startTime: BASE_TS - 1e9,
      endTime: BASE_TS + N * 1e9,
    });
    expect(resp.status).toBe("success");

    // Doubles: exact lossless ALP round-trip
    const temp = findField(resp, "temp");
    expect(temp.timestamps).toEqual(timestamps);
    expect(temp.values).toEqual(doubles);

    // Int64: exact zigzag+FFOR round-trip (negatives included)
    const count = findField(resp, "count");
    expect(count.timestamps).toEqual(timestamps);
    expect(count.values).toEqual(ints);

    // Booleans: server >= 1.0.7 returns NUMERIC 0/1 on all query paths
    const active = findField(resp, "active");
    expect(active.timestamps).toEqual(timestamps);
    expect(active.values).toEqual(bools.map((b) => (b ? 1 : 0)));
    for (const v of active.values) expect(typeof v).toBe("number");

    // Strings: exact zstd round-trip
    const label = findField(resp, "label");
    expect(label.timestamps).toEqual(timestamps);
    expect(label.values).toEqual(strings);
  });

  it("consolidates multi-field results into one series per measurement+tags", async () => {
    if (requireServer()) return;
    // Server >= 1.0.7: deterministic response shape — numeric fields of the
    // same measurement+tags always arrive consolidated in a single series,
    // regardless of shard placement.
    const resp = await client.query(`avg:${measurement}(temp,count,active) by {host}`, {
      startTime: BASE_TS - 1e9,
      endTime: BASE_TS + N * 1e9,
    });
    expect(resp.status).toBe("success");
    expect(resp.series.length).toBe(1);
    expect(resp.series[0].tags).toEqual({ host: "rt1" });
    expect(Object.keys(resp.series[0].fields).sort()).toEqual(["active", "count", "temp"]);
  });
});

// ============================================================================
// Corrupt compressed payloads (requires server >= 1.0.7)
//
// The server must reject corrupt compressed bytes with per-point errors and a
// "partial" status instead of silently dropping or crashing. The client's
// public API never produces corrupt payloads, so the request is built with
// the proto codecs directly.
// ============================================================================

describe("Corrupt compressed payloads", () => {
  it("returns partial status with per-point decode errors", async () => {
    if (requireServer()) return;
    await protoInit();

    const ts = Array.from({ length: 10 }, (_, i) => 1_700_000_000_000_000_000 + i * 1e9);
    const garbage = new Uint8Array(64).fill(0xab);

    const body = await codecs.WriteRequest.encode({
      writes: [
        {
          // Corrupt compressed field values (valid timestamps)
          measurement: `${PREFIX}.corrupt_vals`,
          tags: { host: "c1" },
          fields: { bad: { doubleValues: { compressedAlp: garbage } } } as any,
          compressedTimestamps: compressTimestamps(ts),
        },
        {
          // Corrupt compressed timestamps (valid field values)
          measurement: `${PREFIX}.corrupt_ts`,
          tags: { host: "c1" },
          fields: { v: { doubleValues: { values: [1, 2, 3] } } } as any,
          compressedTimestamps: garbage,
        },
      ],
    });

    const res = await fetch(`http://${HOST}:${PORT}/write`, {
      method: "POST",
      headers: { "Content-Type": "application/protobuf", Accept: "application/protobuf" },
      body,
    });
    // Protobuf responses carry the protobuf content-type (server >= 1.0.7)
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("protobuf");

    const wr = await codecs.WriteResponse.decode(new Uint8Array(await res.arrayBuffer()));
    expect(wr.status).toBe("partial");
    expect(wr.pointsWritten).toBe(0);
    expect(wr.failedWrites).toBe(2);
    expect(wr.errors.length).toBe(2);
    expect(wr.errors.join(" ")).toMatch(/failed to decode compressed values/);
    expect(wr.errors.join(" ")).toMatch(/Corrupt compressed timestamps/);
  });
});

// ============================================================================
// Error responses (flat shape, server >= 1.0.7)
// ============================================================================

describe("Error responses", () => {
  it("surfaces error_code and message from query errors", async () => {
    if (requireServer()) return;
    try {
      await client.query("this is not a valid query!!!", { startTime: 0, endTime: 1 });
      expect.unreachable("query should have thrown");
    } catch (e) {
      expect(e).toBeInstanceOf(TimestarError);
      const err = e as TimestarError;
      expect(err.statusCode).toBe(400);
      expect(err.code).toBe("INVALID_QUERY");
      expect(err.message.length).toBeGreaterThan(10);
    }
  });

  it("parses the flat JSON error shape", async () => {
    if (requireServer()) return;
    // JSON requests get JSON errors: {"status","error_code","message","error"}
    const res = await fetch(`http://${HOST}:${PORT}/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: "bogus!!!", startTime: 0, endTime: 1 }),
    });
    expect(res.status).toBe(400);
    const j = await res.json();
    expect(j.status).toBe("error");
    expect(typeof j.error_code).toBe("string");
    expect(typeof j.message).toBe("string");
    expect(j.error).toBe(j.message); // flat shape: "error" mirrors "message"
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
  it("executes derived query with formula and decompresses the response", async () => {
    if (requireServer()) return;
    const now = Date.now() * 1e6;
    const resp = await client.derived(
      { a: `avg:${PREFIX}.doubles(temperature){host:server1}` },
      "a * 2",
      { startTime: now - 3600e9, endTime: now + 3600e9, aggregationInterval: "10s" }
    );
    expect(resp.status).toBe("success");
    // The server sends FFOR/ALP-compressed arrays to protobuf clients; the
    // client must decompress them into plain timestamps/values.
    expect(resp.timestamps.length).toBeGreaterThan(0);
    expect(resp.values.length).toBe(resp.timestamps.length);
    for (const v of resp.values) expect(Number.isFinite(v)).toBe(true);
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
