#!/usr/bin/env npx tsx
// 3-way comparison: JSON vs Raw Protobuf vs Compressed Protobuf
// Runs identical workloads in each mode, measuring wire bytes, write
// throughput, and query latency.

import * as http from "http";
import { codecs, init as protoInit } from "../src/proto";
import {
  compressTimestamps,
  compressDoubles,
} from "../src/compression";

// ============================================================================
// Configuration
// ============================================================================

const args = process.argv.slice(2);
function getArg(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}

const HOST = getArg("--host", "localhost");
const PORT = parseInt(getArg("--port", "8086"), 10);
const NUM_BATCHES = parseInt(getArg("--batches", "50"), 10);
const BATCH_SIZE = parseInt(getArg("--batch-size", "500"), 10);
const QUERY_ITERS = parseInt(getArg("--query-iters", "50"), 10);
const CONCURRENCY = parseInt(getArg("--concurrency", "8"), 10);

const MEASUREMENT_JSON = "bench_json";
const MEASUREMENT_PROTO = "bench_proto";
const MEASUREMENT_COMP = "bench_compressed";
const FIELD_NAMES = [
  "cpu_usage", "memory_usage", "disk_io_read", "disk_io_write",
  "network_in", "network_out", "load_avg_1m", "load_avg_5m",
  "load_avg_15m", "temperature",
];
const HOST_NAMES = Array.from({ length: 10 }, (_, i) => `host-${String(i + 1).padStart(2, "0")}`);
const RACKS = ["rack-1", "rack-2"];
const BASE_TS = 1_000_000_000_000_000_000;
const MINUTE_NS = 60_000_000_000;

// ============================================================================
// HTTP helper
// ============================================================================

function httpPost(
  path: string, body: Uint8Array | Buffer, contentType: string, accept: string,
): Promise<{ status: number; body: Buffer; responseBytes: number }> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname: HOST, port: PORT, path, method: "POST",
        headers: { "Content-Type": contentType, "Accept": accept, "Content-Length": body.length },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c: Buffer) => chunks.push(c));
        res.on("end", () => {
          const respBody = Buffer.concat(chunks);
          resolve({ status: res.statusCode ?? 0, body: respBody, responseBytes: respBody.length });
        });
        res.on("error", reject);
      },
    );
    req.on("error", reject);
    req.write(body);
    req.end();
  });
}

// ============================================================================
// PRNG
// ============================================================================

class PRNG {
  private s: Uint32Array;
  constructor(seed: number) {
    this.s = new Uint32Array(4);
    this.s[0] = seed ^ 0x12345678; this.s[1] = seed ^ 0x9abcdef0;
    this.s[2] = seed ^ 0xdeadbeef; this.s[3] = seed ^ 0xcafebabe;
    for (let i = 0; i < 20; i++) this.next();
  }
  next(): number {
    const s = this.s;
    const result = Math.imul(s[1] * 5, 0) >>> 0;
    const t = s[1] << 9;
    s[2] ^= s[0]; s[3] ^= s[1]; s[1] ^= s[2]; s[0] ^= s[3];
    s[2] ^= t; s[3] = (s[3] << 11) | (s[3] >>> 21);
    return (result >>> 0) / 0x100000000;
  }
  float(min: number, max: number): number { return min + this.next() * (max - min); }
}

// ============================================================================
// Generate raw data for a batch (shared across all three modes)
// ============================================================================

interface RawBatchData {
  hosts: Array<{
    host: string;
    rack: string;
    timestamps: number[];
    fields: Record<string, number[]>;
  }>;
}

function generateBatchData(batchIdx: number): RawBatchData {
  const startTs = BASE_TS + batchIdx * BATCH_SIZE * MINUTE_NS;
  const hosts: RawBatchData["hosts"] = [];

  for (let h = 0; h < HOST_NAMES.length; h++) {
    const seed = 42 ^ (h << 16) ^ batchIdx;
    const rng = new PRNG(seed);

    const timestamps: number[] = [];
    const fields: Record<string, number[]> = {};
    for (const f of FIELD_NAMES) fields[f] = [];

    for (let t = 0; t < BATCH_SIZE; t++) {
      timestamps.push(startTs + t * MINUTE_NS);
      for (const f of FIELD_NAMES) fields[f].push(rng.float(0, 100));
    }

    hosts.push({ host: HOST_NAMES[h], rack: RACKS[h % 2], timestamps, fields });
  }
  return { hosts };
}

// ============================================================================
// Build payloads for each mode
// ============================================================================

function buildJsonPayload(batch: RawBatchData, measurement: string): Buffer {
  // TimeStar JSON write format: { writes: [{ measurement, tags, fields, timestamps }] }
  // Each write carries columnar arrays.
  const writes: any[] = [];
  for (const h of batch.hosts) {
    const fields: Record<string, number[]> = {};
    for (const f of FIELD_NAMES) fields[f] = h.fields[f];
    writes.push({
      measurement,
      tags: { host: h.host, rack: h.rack },
      fields,
      timestamps: h.timestamps,
    });
  }
  return Buffer.from(JSON.stringify({ writes }));
}

async function buildProtoPayload(batch: RawBatchData, measurement: string): Promise<Uint8Array> {
  const writes: any[] = [];
  for (const h of batch.hosts) {
    const fields: Record<string, any> = {};
    for (const f of FIELD_NAMES) {
      fields[f] = { doubleValues: { values: h.fields[f] } };
    }
    writes.push({ measurement, tags: { host: h.host, rack: h.rack }, fields, timestamps: h.timestamps });
  }
  return codecs.WriteRequest.encode({ writes });
}

async function buildCompressedPayload(batch: RawBatchData, measurement: string): Promise<Uint8Array> {
  const writes: any[] = [];
  for (const h of batch.hosts) {
    const fields: Record<string, any> = {};
    for (const f of FIELD_NAMES) {
      fields[f] = { doubleValues: { compressedAlp: compressDoubles(h.fields[f]) } };
    }
    writes.push({
      measurement, tags: { host: h.host, rack: h.rack }, fields,
      compressedTimestamps: compressTimestamps(h.timestamps),
    });
  }
  return codecs.WriteRequest.encode({ writes });
}

// ============================================================================
// Stats
// ============================================================================

function percentile(sorted: number[], p: number): number {
  return sorted[Math.min(Math.floor(sorted.length * p), sorted.length - 1)];
}
function calcStats(values: number[]) {
  const sorted = [...values].sort((a, b) => a - b);
  const sum = values.reduce((a, b) => a + b, 0);
  return {
    mean: sum / values.length,
    p50: percentile(sorted, 0.5),
    p95: percentile(sorted, 0.95),
    p99: percentile(sorted, 0.99),
  };
}

// ============================================================================
// Write benchmark
// ============================================================================

interface WriteResult {
  totalBytes: number;
  wallSec: number;
  ptsPerSec: number;
  batchLatencies: number[];
}

async function benchWrites(
  payloads: Array<Uint8Array | Buffer>,
  contentType: string,
  accept: string,
): Promise<WriteResult> {
  const pointsPerBatch = BATCH_SIZE * HOST_NAMES.length * FIELD_NAMES.length;
  const totalPoints = pointsPerBatch * payloads.length;
  let totalBytes = 0;
  for (const p of payloads) totalBytes += p.length;

  const batchLatencies: number[] = [];
  let batchIdx = 0;

  const writeStart = performance.now();
  while (batchIdx < payloads.length) {
    const chunk: Promise<number>[] = [];
    const end = Math.min(batchIdx + CONCURRENCY, payloads.length);
    for (let i = batchIdx; i < end; i++) {
      const payload = payloads[i];
      chunk.push((async () => {
        const t0 = performance.now();
        const res = await httpPost("/write", payload as Uint8Array, contentType, accept);
        if (res.status >= 400) {
          throw new Error(`Write failed (${res.status}): ${res.body.toString().slice(0, 200)}`);
        }
        return performance.now() - t0;
      })());
    }
    const lats = await Promise.all(chunk);
    batchLatencies.push(...lats);
    batchIdx = end;
  }

  const wallSec = (performance.now() - writeStart) / 1000;
  return { totalBytes, wallSec, ptsPerSec: totalPoints / wallSec, batchLatencies };
}

// ============================================================================
// Query benchmark
// ============================================================================

interface QueryDef {
  name: string;
  query: string;
  startTime: number;
  endTime: number;
  aggregationInterval?: string;
}

function buildQueries(measurement: string): QueryDef[] {
  const endTs = BASE_TS + NUM_BATCHES * BATCH_SIZE * MINUTE_NS;
  return [
    { name: "latest: single field", query: `latest:${measurement}(cpu_usage){host:host-01}`, startTime: BASE_TS, endTime: endTs },
    { name: "avg: full range, 1 field", query: `avg:${measurement}(cpu_usage){}`, startTime: BASE_TS, endTime: endTs, aggregationInterval: "5m" },
    { name: "avg: full range, 10 fields", query: `avg:${measurement}(${FIELD_NAMES.join(",")}){}`, startTime: BASE_TS, endTime: endTs, aggregationInterval: "5m" },
    { name: "avg: tag filter (host)", query: `avg:${measurement}(cpu_usage){host:host-01}`, startTime: BASE_TS, endTime: endTs, aggregationInterval: "5m" },
    { name: "avg: group by host", query: `avg:${measurement}(cpu_usage){} by {host}`, startTime: BASE_TS, endTime: endTs, aggregationInterval: "5m" },
    { name: "avg: 1h buckets, by rack", query: `avg:${measurement}(cpu_usage){} by {rack}`, startTime: BASE_TS, endTime: endTs, aggregationInterval: "1h" },
  ];
}

async function benchQueries(
  measurement: string,
  contentType: string,
  accept: string,
): Promise<{ statsMap: Map<string, ReturnType<typeof calcStats>>; totalResponseBytes: number }> {
  const queries = buildQueries(measurement);
  const latencyMap = new Map<string, number[]>();
  for (const q of queries) latencyMap.set(q.name, []);

  // Warmup
  for (const q of queries) {
    for (let i = 0; i < 5; i++) {
      let body: Uint8Array | Buffer;
      if (contentType === "application/json") {
        body = Buffer.from(JSON.stringify({
          query: q.query, startTime: q.startTime, endTime: q.endTime,
          aggregationInterval: q.aggregationInterval,
        }));
      } else {
        body = await codecs.QueryRequest.encode({
          query: q.query, startTime: q.startTime, endTime: q.endTime,
          aggregationInterval: q.aggregationInterval ?? "",
        });
      }
      await httpPost("/query", body as Uint8Array, contentType, accept);
    }
  }

  // Timed (interleaved)
  let totalResponseBytes = 0;
  for (let iter = 0; iter < QUERY_ITERS; iter++) {
    for (const q of queries) {
      let body: Uint8Array | Buffer;
      if (contentType === "application/json") {
        body = Buffer.from(JSON.stringify({
          query: q.query, startTime: q.startTime, endTime: q.endTime,
          aggregationInterval: q.aggregationInterval,
        }));
      } else {
        body = await codecs.QueryRequest.encode({
          query: q.query, startTime: q.startTime, endTime: q.endTime,
          aggregationInterval: q.aggregationInterval ?? "",
        });
      }
      const t0 = performance.now();
      const res = await httpPost("/query", body as Uint8Array, contentType, accept);
      latencyMap.get(q.name)!.push(performance.now() - t0);
      totalResponseBytes += res.responseBytes;
    }
  }

  const statsMap = new Map<string, ReturnType<typeof calcStats>>();
  for (const q of queries) statsMap.set(q.name, calcStats(latencyMap.get(q.name)!));
  return { statsMap, totalResponseBytes };
}

// ============================================================================
// Formatting helpers
// ============================================================================

function fmtKB(bytes: number): string { return (bytes / 1024).toFixed(0); }
function fmtMB(bytes: number): string { return (bytes / (1024 * 1024)).toFixed(1); }
function fmtRate(pts: number): string { return (pts / 1e6).toFixed(2); }
function pad(s: string, w: number): string { return s.padStart(w); }

// ============================================================================
// Main
// ============================================================================

async function main() {
  console.log("TimeStar 3-Way Protocol Comparison Benchmark");
  console.log(`Target: ${HOST}:${PORT}`);
  const pointsPerBatch = BATCH_SIZE * HOST_NAMES.length * FIELD_NAMES.length;
  const totalPoints = pointsPerBatch * NUM_BATCHES;
  console.log(`Config: ${NUM_BATCHES} batches x ${BATCH_SIZE} timestamps x ${HOST_NAMES.length} hosts x ${FIELD_NAMES.length} fields`);
  console.log(`Total:  ${totalPoints.toLocaleString()} points per mode\n`);

  // Health check
  try {
    const res = await new Promise<number>((resolve, reject) => {
      http.get(`http://${HOST}:${PORT}/health`, (r) => resolve(r.statusCode ?? 0)).on("error", reject);
    });
    if (res !== 200) throw new Error("unhealthy");
  } catch {
    console.error("ERROR: TimeStar server not reachable at " + HOST + ":" + PORT);
    process.exit(1);
  }

  await protoInit();

  // ── Generate all batch data ──
  console.log("Generating batch data...");
  const batchData: RawBatchData[] = [];
  for (let b = 0; b < NUM_BATCHES; b++) batchData.push(generateBatchData(b));

  // ── Build payloads for all three modes ──
  console.log("Building payloads...");
  const jsonPayloads: Buffer[] = [];
  const protoPayloads: Uint8Array[] = [];
  const compPayloads: Uint8Array[] = [];

  for (let b = 0; b < NUM_BATCHES; b++) {
    jsonPayloads.push(buildJsonPayload(batchData[b], MEASUREMENT_JSON));
    protoPayloads.push(await buildProtoPayload(batchData[b], MEASUREMENT_PROTO));
    compPayloads.push(await buildCompressedPayload(batchData[b], MEASUREMENT_COMP));
  }

  const jsonTotalBytes = jsonPayloads.reduce((s, p) => s + p.length, 0);
  const protoTotalBytes = protoPayloads.reduce((s, p) => s + p.length, 0);
  const compTotalBytes = compPayloads.reduce((s, p) => s + p.length, 0);

  console.log(`  JSON payloads:       ${fmtKB(jsonTotalBytes)} KB (${fmtKB(jsonTotalBytes / NUM_BATCHES)} KB/batch)`);
  console.log(`  Proto payloads:      ${fmtKB(protoTotalBytes)} KB (${fmtKB(protoTotalBytes / NUM_BATCHES)} KB/batch)`);
  console.log(`  Compressed payloads: ${fmtKB(compTotalBytes)} KB (${fmtKB(compTotalBytes / NUM_BATCHES)} KB/batch)`);
  console.log();

  // ── Write benchmarks ──
  console.log("=== WRITE BENCHMARKS ===\n");

  process.stdout.write("  JSON...");
  const jsonWrite = await benchWrites(jsonPayloads, "application/json", "application/json");
  console.log(` ${fmtRate(jsonWrite.ptsPerSec)}M pts/sec (${jsonWrite.wallSec.toFixed(3)}s)`);

  process.stdout.write("  Proto...");
  const protoWrite = await benchWrites(protoPayloads, "application/protobuf", "application/protobuf");
  console.log(` ${fmtRate(protoWrite.ptsPerSec)}M pts/sec (${protoWrite.wallSec.toFixed(3)}s)`);

  process.stdout.write("  Compressed...");
  const compWrite = await benchWrites(compPayloads, "application/protobuf", "application/protobuf");
  console.log(` ${fmtRate(compWrite.ptsPerSec)}M pts/sec (${compWrite.wallSec.toFixed(3)}s)`);

  // ── Wait for WAL flush ──
  console.log("\nWaiting 5s for WAL flush...\n");
  await new Promise(r => setTimeout(r, 5000));

  // ── Query benchmarks ──
  console.log("=== QUERY BENCHMARKS ===\n");

  process.stdout.write("  JSON...");
  const jsonQuery = await benchQueries(MEASUREMENT_JSON, "application/json", "application/json");
  console.log(" done");

  process.stdout.write("  Proto...");
  const protoQuery = await benchQueries(MEASUREMENT_PROTO, "application/protobuf", "application/protobuf");
  console.log(" done");

  process.stdout.write("  Compressed...");
  const compQuery = await benchQueries(MEASUREMENT_COMP, "application/protobuf", "application/protobuf");
  console.log(" done");

  // ══════════════════════════════════════════════════════════════════════════
  // REPORT
  // ══════════════════════════════════════════════════════════════════════════

  const W = 14; // column width
  const LBL = 32; // label width

  console.log("\n" + "=".repeat(90));
  console.log("   TIMESTAR NODE.JS CLIENT — JSON vs PROTOBUF vs COMPRESSED PROTOBUF");
  console.log("=".repeat(90));
  console.log(`\n${totalPoints.toLocaleString()} points per mode | ${NUM_BATCHES} batches | batch_size=${BATCH_SIZE} | concurrency=${CONCURRENCY}\n`);

  // ── Wire Size ──
  console.log("--- WRITE PAYLOAD SIZE ---");
  console.log(`${"".padEnd(LBL)} ${pad("JSON", W)} ${pad("Proto", W)} ${pad("Compressed", W)}`);
  console.log("-".repeat(LBL + W * 3));
  console.log(`${"Total (KB)".padEnd(LBL)} ${pad(fmtKB(jsonTotalBytes), W)} ${pad(fmtKB(protoTotalBytes), W)} ${pad(fmtKB(compTotalBytes), W)}`);
  console.log(`${"Per batch (KB)".padEnd(LBL)} ${pad(fmtKB(jsonTotalBytes / NUM_BATCHES), W)} ${pad(fmtKB(protoTotalBytes / NUM_BATCHES), W)} ${pad(fmtKB(compTotalBytes / NUM_BATCHES), W)}`);
  console.log(`${"vs JSON".padEnd(LBL)} ${pad("1.0x", W)} ${pad((jsonTotalBytes / protoTotalBytes).toFixed(1) + "x", W)} ${pad((jsonTotalBytes / compTotalBytes).toFixed(1) + "x", W)}`);

  // ── Write Throughput ──
  console.log("\n--- WRITE THROUGHPUT ---");
  console.log(`${"".padEnd(LBL)} ${pad("JSON", W)} ${pad("Proto", W)} ${pad("Compressed", W)}`);
  console.log("-".repeat(LBL + W * 3));
  console.log(`${"Throughput (M pts/sec)".padEnd(LBL)} ${pad(fmtRate(jsonWrite.ptsPerSec), W)} ${pad(fmtRate(protoWrite.ptsPerSec), W)} ${pad(fmtRate(compWrite.ptsPerSec), W)}`);
  console.log(`${"Wall time (sec)".padEnd(LBL)} ${pad(jsonWrite.wallSec.toFixed(3), W)} ${pad(protoWrite.wallSec.toFixed(3), W)} ${pad(compWrite.wallSec.toFixed(3), W)}`);

  const jbs = calcStats(jsonWrite.batchLatencies);
  const pbs = calcStats(protoWrite.batchLatencies);
  const cbs = calcStats(compWrite.batchLatencies);
  console.log(`${"Batch latency p50 (ms)".padEnd(LBL)} ${pad(jbs.p50.toFixed(1), W)} ${pad(pbs.p50.toFixed(1), W)} ${pad(cbs.p50.toFixed(1), W)}`);
  console.log(`${"Batch latency p95 (ms)".padEnd(LBL)} ${pad(jbs.p95.toFixed(1), W)} ${pad(pbs.p95.toFixed(1), W)} ${pad(cbs.p95.toFixed(1), W)}`);
  console.log(`${"Batch latency p99 (ms)".padEnd(LBL)} ${pad(jbs.p99.toFixed(1), W)} ${pad(pbs.p99.toFixed(1), W)} ${pad(cbs.p99.toFixed(1), W)}`);
  console.log(`${"vs JSON".padEnd(LBL)} ${pad("1.0x", W)} ${pad((protoWrite.ptsPerSec / jsonWrite.ptsPerSec).toFixed(1) + "x", W)} ${pad((compWrite.ptsPerSec / jsonWrite.ptsPerSec).toFixed(1) + "x", W)}`);

  // ── Query Response Size ──
  console.log("\n--- QUERY RESPONSE BYTES (total across all queries x iters) ---");
  console.log(`${"".padEnd(LBL)} ${pad("JSON", W)} ${pad("Proto", W)} ${pad("Compressed", W)}`);
  console.log("-".repeat(LBL + W * 3));
  console.log(`${"Total response (KB)".padEnd(LBL)} ${pad(fmtKB(jsonQuery.totalResponseBytes), W)} ${pad(fmtKB(protoQuery.totalResponseBytes), W)} ${pad(fmtKB(compQuery.totalResponseBytes), W)}`);
  if (jsonQuery.totalResponseBytes > 0) {
    console.log(`${"vs JSON".padEnd(LBL)} ${pad("1.0x", W)} ${pad((jsonQuery.totalResponseBytes / Math.max(protoQuery.totalResponseBytes, 1)).toFixed(1) + "x", W)} ${pad((jsonQuery.totalResponseBytes / Math.max(compQuery.totalResponseBytes, 1)).toFixed(1) + "x", W)}`);
  }

  // ── Query Latency ──
  console.log("\n--- QUERY LATENCY (p50 ms) ---");
  const queryNames = [...jsonQuery.statsMap.keys()];
  console.log(`${"Query".padEnd(LBL)} ${pad("JSON", W)} ${pad("Proto", W)} ${pad("Compressed", W)}`);
  console.log("-".repeat(LBL + W * 3));
  let jTotal = 0, pTotal = 0, cTotal = 0;
  for (const name of queryNames) {
    const j = jsonQuery.statsMap.get(name)!;
    const p = protoQuery.statsMap.get(name)!;
    const c = compQuery.statsMap.get(name)!;
    jTotal += j.p50; pTotal += p.p50; cTotal += c.p50;
    console.log(`${name.padEnd(LBL)} ${pad(j.p50.toFixed(2), W)} ${pad(p.p50.toFixed(2), W)} ${pad(c.p50.toFixed(2), W)}`);
  }
  console.log("-".repeat(LBL + W * 3));
  console.log(`${"TOTAL (sum of p50)".padEnd(LBL)} ${pad(jTotal.toFixed(2), W)} ${pad(pTotal.toFixed(2), W)} ${pad(cTotal.toFixed(2), W)}`);
  console.log(`${"vs JSON".padEnd(LBL)} ${pad("1.0x", W)} ${pad((jTotal / pTotal).toFixed(1) + "x", W)} ${pad((jTotal / cTotal).toFixed(1) + "x", W)}`);
  console.log();
}

main().catch(err => { console.error("Failed:", err); process.exit(1); });
