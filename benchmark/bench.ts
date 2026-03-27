#!/usr/bin/env npx tsx
// TimeStar Node.js Client Performance Benchmark
//
// Modeled on the Python influxdb_comparison tool from ../tsdb/benchmark/.
// Same data shape: server.metrics with 10 float fields, 10 hosts, 2 racks.
// Measures insert throughput (pts/sec) and query latencies (ms).
//
// Usage:
//   npx tsx benchmark/bench.ts [options]
//
// Options:
//   --host HOST          TimeStar host (default: localhost)
//   --port PORT          TimeStar port (default: 8086)
//   --batches N          Number of batches (default: 100)
//   --batch-size N       Timestamps per batch (default: 1000)
//   --query-iters N      Query iterations (default: 50)
//   --concurrency N      Concurrent write batches (default: 8)
//   --no-compress        Use uncompressed protobuf (for comparison)

import { TimestarClient } from "../src/client";
import {
  compressTimestamps,
  compressDoubles,
} from "../src/compression";
import type { WritePoint } from "../src/types";

// ============================================================================
// Configuration
// ============================================================================

const args = process.argv.slice(2);
function getArg(name: string, defaultVal: string): string {
  const idx = args.indexOf(name);
  return idx >= 0 && idx + 1 < args.length ? args[idx + 1] : defaultVal;
}
function hasFlag(name: string): boolean {
  return args.includes(name);
}

const HOST = getArg("--host", "localhost");
const PORT = parseInt(getArg("--port", "8086"), 10);
const NUM_BATCHES = parseInt(getArg("--batches", "100"), 10);
const BATCH_SIZE = parseInt(getArg("--batch-size", "1000"), 10);
const QUERY_ITERS = parseInt(getArg("--query-iters", "50"), 10);
const CONCURRENCY = parseInt(getArg("--concurrency", "8"), 10);
const USE_COMPRESS = !hasFlag("--no-compress");

const MEASUREMENT = "server.metrics";
const FIELD_NAMES = [
  "cpu_usage", "memory_usage", "disk_io_read", "disk_io_write",
  "network_in", "network_out", "load_avg_1m", "load_avg_5m",
  "load_avg_15m", "temperature",
];
const HOSTS = Array.from({ length: 10 }, (_, i) => `host-${String(i + 1).padStart(2, "0")}`);
const RACKS = ["rack-1", "rack-2"];
const BASE_TS = 1_000_000_000_000_000_000; // 1e18 ns
const MINUTE_NS = 60_000_000_000;

// ============================================================================
// Deterministic PRNG (xoshiro128** seeded like the Python benchmark)
// ============================================================================

class PRNG {
  private s: Uint32Array;
  constructor(seed: number) {
    this.s = new Uint32Array(4);
    this.s[0] = seed ^ 0x12345678;
    this.s[1] = seed ^ 0x9abcdef0;
    this.s[2] = seed ^ 0xdeadbeef;
    this.s[3] = seed ^ 0xcafebabe;
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
  float(min: number, max: number): number {
    return min + this.next() * (max - min);
  }
}

// ============================================================================
// Data generation (matches influxdb_comparison.py)
// ============================================================================

function generateBatch(batchIdx: number, batchSize: number): WritePoint[] {
  const startTs = BASE_TS + batchIdx * batchSize * MINUTE_NS;
  const points: WritePoint[] = [];

  for (let h = 0; h < HOSTS.length; h++) {
    const seed = 42 ^ (h << 16) ^ batchIdx;
    const rng = new PRNG(seed);
    const host = HOSTS[h];
    const rack = RACKS[h % 2];

    const timestamps: number[] = [];
    const fieldArrays: Record<string, number[]> = {};
    for (const f of FIELD_NAMES) fieldArrays[f] = [];

    for (let t = 0; t < batchSize; t++) {
      timestamps.push(startTs + t * MINUTE_NS);
      for (const f of FIELD_NAMES) {
        fieldArrays[f].push(rng.float(0, 100));
      }
    }

    const fields: Record<string, { doubleValues: number[] }> = {};
    for (const f of FIELD_NAMES) {
      fields[f] = { doubleValues: fieldArrays[f] };
    }

    points.push({
      measurement: MEASUREMENT,
      tags: { host, rack },
      fields,
      timestamps,
    });
  }
  return points;
}

// ============================================================================
// Statistics helpers
// ============================================================================

function percentile(sorted: number[], p: number): number {
  const idx = Math.floor(sorted.length * p);
  return sorted[Math.min(idx, sorted.length - 1)];
}

function stats(values: number[]) {
  const sorted = [...values].sort((a, b) => a - b);
  const sum = values.reduce((a, b) => a + b, 0);
  return {
    mean: sum / values.length,
    median: percentile(sorted, 0.5),
    p95: percentile(sorted, 0.95),
    p99: percentile(sorted, 0.99),
    min: sorted[0],
    max: sorted[sorted.length - 1],
  };
}

// ============================================================================
// Write benchmark
// ============================================================================

async function benchmarkWrites(client: TimestarClient): Promise<{
  totalPoints: number;
  wallSec: number;
  ptsPerSec: number;
  batchLatencies: number[];
}> {
  const pointsPerBatch = BATCH_SIZE * HOSTS.length * FIELD_NAMES.length;
  const totalPoints = pointsPerBatch * NUM_BATCHES;

  console.log(`\nGenerating ${NUM_BATCHES} batches (${BATCH_SIZE} timestamps x ${HOSTS.length} hosts x ${FIELD_NAMES.length} fields = ${pointsPerBatch.toLocaleString()} pts/batch)...`);
  console.log(`Total: ${totalPoints.toLocaleString()} points`);

  // Pre-generate all batches
  const batches: WritePoint[][] = [];
  const genStart = performance.now();
  for (let b = 0; b < NUM_BATCHES; b++) {
    batches.push(generateBatch(b, BATCH_SIZE));
  }
  const genMs = performance.now() - genStart;
  console.log(`Data generation: ${genMs.toFixed(0)}ms`);

  // Write with concurrency
  const batchLatencies: number[] = [];
  let batchIdx = 0;

  console.log(`\nWriting with concurrency=${CONCURRENCY}...`);
  const writeStart = performance.now();

  while (batchIdx < NUM_BATCHES) {
    const chunk: Promise<number>[] = [];
    const end = Math.min(batchIdx + CONCURRENCY, NUM_BATCHES);

    for (let i = batchIdx; i < end; i++) {
      const b = i;
      chunk.push((async () => {
        const t0 = performance.now();
        await client.write(batches[b]);
        const latency = performance.now() - t0;
        return latency;
      })());
    }

    const latencies = await Promise.all(chunk);
    batchLatencies.push(...latencies);
    batchIdx = end;

    // Progress
    if (batchIdx % 20 === 0 || batchIdx === NUM_BATCHES) {
      const elapsed = (performance.now() - writeStart) / 1000;
      const pointsSoFar = batchIdx * pointsPerBatch;
      const rate = pointsSoFar / elapsed;
      process.stdout.write(`\r  ${batchIdx}/${NUM_BATCHES} batches (${(rate / 1e6).toFixed(2)}M pts/sec)`);
    }
  }

  const wallMs = performance.now() - writeStart;
  const wallSec = wallMs / 1000;
  const ptsPerSec = totalPoints / wallSec;
  console.log();

  return { totalPoints, wallSec, ptsPerSec, batchLatencies };
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

function buildQueries(): QueryDef[] {
  const endTs = BASE_TS + NUM_BATCHES * BATCH_SIZE * MINUTE_NS;
  const midTs = BASE_TS + Math.floor(NUM_BATCHES * BATCH_SIZE / 2) * MINUTE_NS;
  const narrowStart = midTs - 60 * MINUTE_NS;
  const narrowEnd = midTs + 60 * MINUTE_NS;

  return [
    {
      name: "latest: single field",
      query: `latest:${MEASUREMENT}(cpu_usage){host:host-01}`,
      startTime: BASE_TS, endTime: endTs,
    },
    {
      name: "avg: full range, 1 field",
      query: `avg:${MEASUREMENT}(cpu_usage){}`,
      startTime: BASE_TS, endTime: endTs,
      aggregationInterval: "5m",
    },
    {
      name: "avg: full range, all fields",
      query: `avg:${MEASUREMENT}(${FIELD_NAMES.join(",")}){}`,
      startTime: BASE_TS, endTime: endTs,
      aggregationInterval: "5m",
    },
    {
      name: "avg: narrow range, 1 field",
      query: `avg:${MEASUREMENT}(cpu_usage){}`,
      startTime: narrowStart, endTime: narrowEnd,
      aggregationInterval: "1m",
    },
    {
      name: "max: full range, 1 field",
      query: `max:${MEASUREMENT}(temperature){}`,
      startTime: BASE_TS, endTime: endTs,
      aggregationInterval: "1h",
    },
    {
      name: "sum: full range, 1 field",
      query: `sum:${MEASUREMENT}(network_in){}`,
      startTime: BASE_TS, endTime: endTs,
      aggregationInterval: "1h",
    },
    {
      name: "avg: tag filter (1 host)",
      query: `avg:${MEASUREMENT}(cpu_usage){host:host-01}`,
      startTime: BASE_TS, endTime: endTs,
      aggregationInterval: "5m",
    },
    {
      name: "avg: tag filter (1 rack)",
      query: `avg:${MEASUREMENT}(cpu_usage){rack:rack-1}`,
      startTime: BASE_TS, endTime: endTs,
      aggregationInterval: "5m",
    },
    {
      name: "avg: group by host",
      query: `avg:${MEASUREMENT}(cpu_usage){} by {host}`,
      startTime: BASE_TS, endTime: endTs,
      aggregationInterval: "5m",
    },
    {
      name: "avg: group by host,rack",
      query: `avg:${MEASUREMENT}(cpu_usage){} by {host,rack}`,
      startTime: BASE_TS, endTime: endTs,
      aggregationInterval: "5m",
    },
    {
      name: "avg: 5m buckets, group by host",
      query: `avg:${MEASUREMENT}(cpu_usage){} by {host}`,
      startTime: BASE_TS, endTime: endTs,
      aggregationInterval: "5m",
    },
    {
      name: "avg: 1h buckets, group by rack",
      query: `avg:${MEASUREMENT}(cpu_usage){} by {rack}`,
      startTime: BASE_TS, endTime: endTs,
      aggregationInterval: "1h",
    },
  ];
}

async function benchmarkQueries(client: TimestarClient): Promise<{
  results: Array<{ name: string; latencies: number[]; stats: ReturnType<typeof stats> }>;
}> {
  const queries = buildQueries();
  const results: Array<{ name: string; latencies: number[]; stats: ReturnType<typeof stats> }> = [];

  console.log(`\nRunning ${queries.length} queries x ${QUERY_ITERS} iterations each...`);

  // Warmup: 5 iterations per query
  process.stdout.write("  Warmup...");
  for (const q of queries) {
    for (let i = 0; i < 5; i++) {
      await client.query(q.query, {
        startTime: q.startTime,
        endTime: q.endTime,
        aggregationInterval: q.aggregationInterval,
      });
    }
  }
  console.log(" done");

  // Timed: interleaved execution (round-robin across queries)
  const latencyMap = new Map<string, number[]>();
  for (const q of queries) latencyMap.set(q.name, []);

  for (let iter = 0; iter < QUERY_ITERS; iter++) {
    for (const q of queries) {
      const t0 = performance.now();
      await client.query(q.query, {
        startTime: q.startTime,
        endTime: q.endTime,
        aggregationInterval: q.aggregationInterval,
      });
      const latency = performance.now() - t0;
      latencyMap.get(q.name)!.push(latency);
    }
    if ((iter + 1) % 10 === 0) {
      process.stdout.write(`\r  ${iter + 1}/${QUERY_ITERS} iterations`);
    }
  }
  console.log();

  for (const q of queries) {
    const lats = latencyMap.get(q.name)!;
    results.push({ name: q.name, latencies: lats, stats: stats(lats) });
  }

  return { results };
}

// ============================================================================
// Derived query benchmark
// ============================================================================

async function benchmarkDerived(client: TimestarClient): Promise<{
  results: Array<{ name: string; latencies: number[]; stats: ReturnType<typeof stats> }>;
}> {
  const endTs = BASE_TS + NUM_BATCHES * BATCH_SIZE * MINUTE_NS;
  const results: Array<{ name: string; latencies: number[]; stats: ReturnType<typeof stats> }> = [];

  const derivedQueries = [
    {
      name: "derived: (a + b) / 2",
      queries: {
        a: `avg:${MEASUREMENT}(cpu_usage){}`,
        b: `avg:${MEASUREMENT}(memory_usage){}`,
      },
      formula: "(a + b) / 2",
    },
    {
      name: "derived: ratio a / (a + b) * 100",
      queries: {
        a: `avg:${MEASUREMENT}(network_in){}`,
        b: `avg:${MEASUREMENT}(network_out){}`,
      },
      formula: "a / (a + b) * 100",
    },
  ];

  console.log(`\nRunning ${derivedQueries.length} derived queries x ${QUERY_ITERS} iterations...`);

  for (const dq of derivedQueries) {
    // Warmup
    for (let i = 0; i < 5; i++) {
      try {
        await client.derived(dq.queries, dq.formula, {
          startTime: BASE_TS, endTime: endTs, aggregationInterval: "5m",
        });
      } catch { /* ignore warmup errors */ }
    }

    const lats: number[] = [];
    for (let i = 0; i < QUERY_ITERS; i++) {
      const t0 = performance.now();
      try {
        await client.derived(dq.queries, dq.formula, {
          startTime: BASE_TS, endTime: endTs, aggregationInterval: "5m",
        });
      } catch { /* skip errors */ }
      lats.push(performance.now() - t0);
    }
    results.push({ name: dq.name, latencies: lats, stats: stats(lats) });
  }

  return { results };
}

// ============================================================================
// Report
// ============================================================================

function printReport(
  writeResult: Awaited<ReturnType<typeof benchmarkWrites>>,
  queryResult: Awaited<ReturnType<typeof benchmarkQueries>>,
  derivedResult: Awaited<ReturnType<typeof benchmarkDerived>>,
) {
  const batchStats = stats(writeResult.batchLatencies);

  console.log("\n" + "=".repeat(80));
  console.log("TIMESTAR NODE.JS CLIENT BENCHMARK RESULTS");
  console.log("=".repeat(80));
  console.log(`Compression: ${USE_COMPRESS ? "ENABLED (FFOR + ALP + RLE + zstd)" : "DISABLED (raw protobuf)"}`);
  console.log(`Batches: ${NUM_BATCHES}, Batch size: ${BATCH_SIZE}, Concurrency: ${CONCURRENCY}`);
  console.log();

  console.log("--- INSERT PERFORMANCE ---");
  console.log(`  Total points:     ${writeResult.totalPoints.toLocaleString()}`);
  console.log(`  Wall time:        ${writeResult.wallSec.toFixed(2)}s`);
  console.log(`  Throughput:       ${(writeResult.ptsPerSec / 1e6).toFixed(2)}M pts/sec`);
  console.log(`  Batch latency:    mean=${batchStats.mean.toFixed(1)}ms  p50=${batchStats.median.toFixed(1)}ms  p95=${batchStats.p95.toFixed(1)}ms  p99=${batchStats.p99.toFixed(1)}ms`);

  console.log();
  console.log("--- QUERY PERFORMANCE ---");
  console.log(`${"Query".padEnd(40)} ${"p50 (ms)".padStart(10)} ${"p95 (ms)".padStart(10)} ${"p99 (ms)".padStart(10)} ${"mean (ms)".padStart(10)}`);
  console.log("-".repeat(80));

  let totalP50 = 0;
  for (const r of queryResult.results) {
    totalP50 += r.stats.median;
    console.log(
      `${r.name.padEnd(40)} ${r.stats.median.toFixed(2).padStart(10)} ${r.stats.p95.toFixed(2).padStart(10)} ${r.stats.p99.toFixed(2).padStart(10)} ${r.stats.mean.toFixed(2).padStart(10)}`
    );
  }
  console.log("-".repeat(80));
  console.log(`${"TOTAL (sum of p50)".padEnd(40)} ${totalP50.toFixed(2).padStart(10)}`);

  if (derivedResult.results.length > 0) {
    console.log();
    console.log("--- DERIVED QUERY PERFORMANCE ---");
    console.log(`${"Query".padEnd(40)} ${"p50 (ms)".padStart(10)} ${"p95 (ms)".padStart(10)} ${"mean (ms)".padStart(10)}`);
    console.log("-".repeat(70));
    for (const r of derivedResult.results) {
      console.log(
        `${r.name.padEnd(40)} ${r.stats.median.toFixed(2).padStart(10)} ${r.stats.p95.toFixed(2).padStart(10)} ${r.stats.mean.toFixed(2).padStart(10)}`
      );
    }
  }

  // Wire size measurement: compress one batch to measure actual ratio
  console.log();
  console.log("--- WIRE SIZE (MEASURED) ---");
  const rawBytesPerBatch = BATCH_SIZE * HOSTS.length * (FIELD_NAMES.length + 1) * 8; // fields + timestamps
  console.log(`  Raw array bytes per batch:  ${(rawBytesPerBatch / 1024).toFixed(0)} KB`);

  // Measure actual compression on a sample batch
  try {
    const sampleTs = Array.from({ length: BATCH_SIZE }, (_, i) => BASE_TS + i * MINUTE_NS);
    const tsCompressed = compressTimestamps(sampleTs);
    const sampleVals = Array.from({ length: BATCH_SIZE }, (_, i) => 22.5 + (i % 100) * 0.1);
    const valsCompressed = compressDoubles(sampleVals);
    const perHost = tsCompressed.length + valsCompressed.length * FIELD_NAMES.length;
    const totalCompressed = perHost * HOSTS.length;
    const ratio = rawBytesPerBatch / totalCompressed;
    console.log(`  Compressed (sensor-like):   ${(totalCompressed / 1024).toFixed(0)} KB (${ratio.toFixed(1)}x reduction)`);
    console.log(`    Timestamps:  ${BATCH_SIZE * 8} -> ${tsCompressed.length} bytes (${(BATCH_SIZE * 8 / tsCompressed.length).toFixed(0)}x)`);
    console.log(`    Doubles:     ${BATCH_SIZE * 8} -> ${valsCompressed.length} bytes (${(BATCH_SIZE * 8 / valsCompressed.length).toFixed(1)}x)`);
  } catch { /* skip if compression unavailable */ }
  console.log();
}

// ============================================================================
// Main
// ============================================================================

async function main() {
  console.log("TimeStar Node.js Client Benchmark");
  console.log(`Target: ${HOST}:${PORT}`);

  const client = new TimestarClient({ host: HOST, port: PORT, useProtobuf: true });

  // Check server
  const healthy = await client.isHealthy();
  if (!healthy) {
    console.error("ERROR: TimeStar server not reachable at " + HOST + ":" + PORT);
    process.exit(1);
  }
  console.log("Server: healthy");

  // Run benchmarks
  const writeResult = await benchmarkWrites(client);

  // Wait for WAL flush
  console.log("\nWaiting 5s for WAL-to-TSM flush...");
  await new Promise((r) => setTimeout(r, 5000));

  const queryResult = await benchmarkQueries(client);
  const derivedResult = await benchmarkDerived(client);

  printReport(writeResult, queryResult, derivedResult);
}

main().catch((err) => {
  console.error("Benchmark failed:", err);
  process.exit(1);
});
