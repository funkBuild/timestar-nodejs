# timestar

Node.js client for the TimeStar time series database with native compression.

Communicates over a protobuf binary protocol and compresses all data in-process using a native C++ addon, achieving up to 200x compression on timestamps and sub-millisecond query latency.

## Features

- **Protobuf binary protocol** -- all requests and responses use `application/protobuf` by default (JSON fallback available)
- **Native C++ compression** compiled via cmake-js and node-addon-api:
  - FFOR (Frame-of-Reference) for timestamps and integers
  - ALP (Adaptive Lossless floating-Point) for doubles
  - RLE (Run-Length Encoding) for booleans
  - zstd for strings
- **All 15 API endpoints** -- write, query, delete, metadata, retention, streaming, derived queries, anomaly detection, and forecasting
- **TypeScript-first** -- full type definitions for every request and response

## Compatibility

**Requires TimeStar server >= 1.0.7** (`funkbuild/timestar:1.0.7`).

Client 1.1.x is fully validated (the complete correctness suite in
`test/correctness/`) against TimeStar server builds with last-write-wins
duplicate semantics (commit `694b3b6`, 2026-07-17, or newer — see "Duplicate
points overwrite" below; the duplicate-write test fails against older
append-semantics servers). Compressed writes require server >= 1.0.7. The client
still splits large compressed writes into <= 1024-timestamp chunks — a
workaround for a compressed-timestamp truncation bug in servers older than
`8425b17` that is kept for old-server compatibility and is harmless on fixed
servers (consecutive chunks store identically to one large point — the
server preserves request order, so last-write-wins resolution of duplicate
timestamps is unaffected by the chunk boundaries).

Since client 1.1.0, compressed write payloads are sent **instead of** the raw
arrays. Servers older than 1.0.7 ignored the `compressed_*` protobuf fields
entirely, so compressed writes against them return success while **silently
storing zero points**. Do not use this client with older servers.

Other server 1.0.7 behaviors this client relies on / surfaces:

- **Flat error responses**: all endpoints return
  `{"status":"error","error_code":"<CODE>","message":"<msg>","error":"<msg>"}`
  (`error_code` omitted when uncoded). The client also still tolerates the
  legacy nested `{"error":{"code","message"}}` shape from older servers.
- **Protobuf content-type**: protobuf responses carry
  `Content-Type: application/x-protobuf` (older servers said `application/json`).
  The client accepts both.
- **Boolean query values are real `true`/`false`** on every query path.
  Booleans are non-numeric: the aggregation method named in the query is
  ignored for them, exactly as it is for strings (older servers coerced them
  to numeric `0`/`1`).
- **Deterministic response shape**: multi-field queries always return one
  series per measurement+tags with fields consolidated, regardless of shard
  placement.

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Node.js | >= 18 | Uses native `fetch` and `node:http` |
| C++ compiler | gcc or clang | Required to build the native addon |
| CMake | >= 3.15 | Build system for the native addon |
| zstd library | any | `sudo apt install libzstd-dev` (Debian/Ubuntu) or `brew install zstd` (macOS) |

## Install

```bash
npm install timestar
```

The native compression addon compiles automatically on install via cmake-js. If the build fails, ensure the prerequisites above are installed.

## Quick Start

### Connect

```ts
import { TimestarClient } from "timestar";

const client = new TimestarClient({
  host: "localhost",
  port: 8086,
});
```

### Write Points

```ts
// Array fields — type auto-detected from contents
await client.write({
  measurement: "cpu",
  tags: { host: "server-01", region: "us-east" },
  fields: {
    usage: [55.2, 62.8, 49.1],
    throttled: [false, false, true],
  },
  timestamps: [1700000000000, 1700000001000, 1700000002000],
});

// Single scalar shorthand
await client.write({
  measurement: "cpu",
  tags: { host: "server-01" },
  fields: { usage: 55.2 },
  timestamps: [1700000000000],
});

// Explicit type when needed (e.g., force int64 for whole numbers)
await client.write({
  measurement: "counters",
  tags: { host: "server-01" },
  fields: { requests: { int64Values: [1000, 2000, 3000] } },
  timestamps: [1700000000000, 1700000001000, 1700000002000],
});
```

Write semantics worth knowing:

- **`pointsWritten` counts field-points**: fields × timestamps per point. A
  point with 3 fields and 10 timestamps reports `pointsWritten: 30`.
- **Duplicate points overwrite — last write wins** (server commit `694b3b6`,
  2026-07-17, or newer): writing an identical
  measurement+tags+field+timestamp again REPLACES the earlier value
  (InfluxDB-compatible). Queries only ever see the newest write — raw reads
  return one point and aggregations count it once, wherever the copies live
  (same batch: last in request order wins; memory store; across a flush;
  across storage files; after compaction). Idempotent write retries are
  always safe. Older servers appended duplicates instead; if you must target
  one, deduplicate client-side.
- **Partial failures**: invalid points/fields are skipped and reported in
  `WriteResponse.errors` with `status: "partial"` — the write is not rolled
  back.

### Field Type Detection

Field values are auto-detected from their contents:

| Value | Detected type | Example |
|---|---|---|
| `number` | double (scalar) | `{ usage: 55.2 }` |
| `boolean` | bool (scalar) | `{ active: true }` |
| `string` | string (scalar) | `{ name: "sensor-1" }` |
| `bigint` | int64 (scalar) | `{ count: 42n }` |
| `number[]` | double array | `{ usage: [55.2, 62.8] }` |
| `boolean[]` | bool array | `{ active: [true, false] }` |
| `string[]` | string array | `{ tags: ["a", "b"] }` |

To override auto-detection, wrap in a `WriteField` object with the explicit type key:

```ts
// Whole numbers auto-detect as double — use int64Values to force integer storage
{ requests: { int64Values: [1000, 2000, 3000] } }

// All four explicit types:
{ temperature: { doubleValues: [22.5, 23.1] } }
{ active:      { boolValues: [true, false] } }
{ label:       { stringValues: ["a", "b"] } }
{ count:       { int64Values: [100n, 200n] } }
```

### Query

```ts
const result = await client.query(
  "SELECT usage FROM cpu WHERE host = 'server-01'",
  {
    startTime: 1700000000000,
    endTime: 1700000003000,
  },
);

for (const series of result.series) {
  console.log(series.fields.usage.timestamps);
  console.log(series.fields.usage.values);
}
```

Note: boolean and string fields are **non-numeric** — they come back in the
type they were written in (`true`/`false` for booleans), and the aggregation
method named in the query is ignored for them. Without an
`aggregationInterval` they pass through raw; with one they reduce to
LATEST-per-bucket.

### 64-bit Precision

JavaScript numbers hold at most 53 bits of integer precision, and nanosecond
epoch timestamps (~1.7e18) exceed that — so by default, query results round
timestamps (and int64 field values) to the nearest representable double, which
can be off by up to ~128ns at current epoch values.

Opt in to exact `bigint[]` results with `precise: true`, per query or
client-wide:

```ts
// Per query
const result = await client.query("avg:cpu(usage){}", {
  startTime: 1_700_000_000_000_000_000n,
  endTime: 1_700_000_060_000_000_000n,
  precise: true,
});
// result.series[0].fields.usage.timestamps is bigint[] — exact ns precision

// Client-wide default
const preciseClient = new TimestarClient({ port: 8086, precise: true });
```

Precision semantics:

- **Write side always preserves full 64-bit precision on the wire**, with or
  without `precise`. `bigint` timestamps and `int64Values` are never rounded
  by the client (values within ±2^53 may be sent as numbers; larger values are
  carried exactly).
- **Timestamps** round-trip exactly through the server: write `bigint`
  timestamps, query with `precise: true`, and you get the identical `bigint[]`
  back.
- **int64 field values**: with `precise: true` the client returns `bigint[]`
  whenever the server sends int64 wire data. Note that current servers
  aggregate int64 fields numerically and return them as doubles on the query
  path, so int64 values beyond 2^53 are read back as the nearest double
  (`number[]`) — a server-side limit; the stored data retains what was
  written.

### Derived Queries

Combine multiple queries with a formula:

```ts
const derived = await client.derived(
  {
    a: "SELECT usage FROM cpu WHERE host = 'server-01'",
    b: "SELECT usage FROM cpu WHERE host = 'server-02'",
  },
  "a + b",
  {
    startTime: 1700000000000,
    endTime: 1700000060000,
    aggregationInterval: "10s",
  },
);

console.log(derived.timestamps, derived.values);
```

### Subscribe to Live Data

```ts
for await (const batch of client.subscribe({ query: "SELECT usage FROM cpu" })) {
  for (const point of batch.points) {
    console.log(point.timestamp, point.value);
  }
}
```

## API Reference

### Health

| Method | Signature | Returns | Description |
|---|---|---|---|
| `health` | `health()` | `Promise<HealthResponse>` | Returns server health status |
| `isHealthy` | `isHealthy()` | `Promise<boolean>` | Lightweight check -- returns `true` if the server responds 200 |

### Write

| Method | Signature | Returns | Description |
|---|---|---|---|
| `write` | `write(points: WritePoint \| WritePoint[])` | `Promise<WriteResponse>` | Write one or more points with automatic compression |

### Query

| Method | Signature | Returns | Description |
|---|---|---|---|
| `query` | `query(query: string, options?: QueryOptions)` | `Promise<QueryResponse>` | Execute a query with optional time range and aggregation interval |

### Delete

| Method | Signature | Returns | Description |
|---|---|---|---|
| `delete` | `delete(items: DeleteRequestItem \| DeleteRequestItem[])` | `Promise<DeleteResponse>` | Delete series, measurements, or specific time ranges. Single items use `/delete`; arrays use batch delete. |

### Metadata

| Method | Signature | Returns | Description |
|---|---|---|---|
| `measurements` | `measurements(options?: MeasurementsOptions)` | `Promise<MeasurementsResponse>` | List measurements with optional prefix filter and pagination |
| `tags` | `tags(measurement: string, options?: TagsOptions)` | `Promise<TagsResponse>` | List tag keys and values for a measurement |
| `fields` | `fields(measurement: string)` | `Promise<FieldsResponse>` | List fields and their types for a measurement |
| `cardinality` | `cardinality(measurement: string)` | `Promise<CardinalityResponse>` | Get estimated series count and per-tag cardinality |

### Retention

| Method | Signature | Returns | Description |
|---|---|---|---|
| `setRetention` | `setRetention(measurement: string, ttl: string, downsample?: DownsamplePolicy)` | `Promise<void>` | Set a retention policy with optional downsampling |
| `getRetention` | `getRetention(measurement: string)` | `Promise<RetentionGetResponse>` | Get the retention policy for a measurement |
| `deleteRetention` | `deleteRetention(measurement: string)` | `Promise<void>` | Remove the retention policy for a measurement |

### Streaming

| Method | Signature | Returns | Description |
|---|---|---|---|
| `subscribe` | `subscribe(request: SubscribeRequest)` | `AsyncGenerator<StreamingBatch>` | Subscribe to live data via SSE. Supports single queries, multi-query with formulas, and backfill. |
| `subscriptions` | `subscriptions()` | `Promise<SubscriptionsResponse>` | List active subscriptions and their stats |

### Derived Queries, Anomaly Detection, and Forecasting

| Method | Signature | Returns | Description |
|---|---|---|---|
| `derived` | `derived(queries: Record<string, string>, formula: string, options: DerivedQueryOptions)` | `Promise<DerivedQueryResponse>` | Combine multiple queries with a formula (e.g. `"a + b"`, `"a / b * 100"`) |
| `anomalies` | `anomalies(queries: Record<string, string>, formula: string, options: DerivedQueryOptions)` | `Promise<AnomalyResponse>` | Run anomaly detection on derived data. Returns raw values, upper/lower bounds, scores, and ratings. |
| `forecast` | `forecast(queries: Record<string, string>, formula: string, options: DerivedQueryOptions)` | `Promise<ForecastResponse>` | Generate forecasts with confidence bounds. Returns past data, forecast values, and upper/lower bands. |

## Compression

The client uses **Approach B**: field values and timestamps are compressed client-side into `bytes` fields within the protobuf messages, **replacing** the raw repeated arrays on the wire. The server (>= 1.0.7) decodes them directly, avoiding any intermediate representation. Value arrays are compressed when their length matches the timestamp count (>= 2 values); scalars and mismatched-length arrays are sent raw so server-side validation semantics are unchanged.

Four compression algorithms are used, each matched to its data type:

| Algorithm | Data Type | How It Works | Typical Ratio |
|---|---|---|---|
| **FFOR** (Frame-of-Reference) | Timestamps, integers | Subtracts a per-block minimum and bit-packs the residuals. Exceptions (outliers) are stored separately. | ~200x |
| **ALP** (Adaptive Lossless floating-Point) | Doubles | Encodes IEEE 754 doubles by finding a decimal exponent/factor pair that converts most values to exact integers, then FFOR-compresses those integers. | ~15x |
| **RLE** (Run-Length Encoding) | Booleans | Stores alternating run lengths of `true`/`false` as varints. | ~200x |
| **zstd** | Strings | Newline-joins all strings and compresses with Zstandard. | ~22x |

All compression runs in the native C++ addon (no JS overhead in the hot path). Single scalar field values skip compression entirely to avoid overhead exceeding savings.

## Benchmarks

Measured performance on the native protobuf+compression path:

- **Write throughput**: 74M points/sec
- **Query latency**: sub-millisecond for typical queries

Run benchmarks locally:

```bash
# Full benchmark suite
npm run bench

# Compare compressed vs uncompressed
npm run bench:compare

# Benchmark without compression
npm run bench:no-compress
```

## Configuration

The `TimestarClient` constructor accepts a `TimestarClientOptions` object:

| Option | Type | Default | Description |
|---|---|---|---|
| `host` | `string` | `"localhost"` | TimeStar server hostname |
| `port` | `number` | `8086` | TimeStar server port |
| `authToken` | `string` | `undefined` | Bearer token for authentication |
| `useProtobuf` | `boolean` | `true` | Deprecated — the client always uses protobuf; this option is ignored. |
| `requestTimeoutMs` | `number` | `30000` | Per-request timeout in milliseconds |
| `precise` | `boolean` | `false` | Default for `QueryOptions.precise`: return timestamps and int64 field values as exact `bigint[]` instead of `number[]` (which rounds beyond 2^53). See "64-bit Precision". |

## Error Handling

All API methods throw `TimestarError` on failure:

```ts
import { TimestarClient, TimestarError } from "timestar";

try {
  await client.query("SELECT ...");
} catch (err) {
  if (err instanceof TimestarError) {
    console.error(err.message);    // Error message from the server
    console.error(err.statusCode); // HTTP status code
    console.error(err.code);       // Machine-readable code (e.g. "INVALID_QUERY"), when provided
  }
}
```

The client understands both the flat error shape emitted by server >= 1.0.7
(`{"status":"error","error_code","message","error"}` — where `error_code`
populates `err.code`) and the protobuf error messages each endpoint returns
to protobuf clients. Legacy nested JSON errors from older servers are also
parsed.

Note that per-point write failures do NOT throw: `/write` returns HTTP 200
with `status: "partial"` and the per-point messages in
`WriteResponse.errors` (see Write semantics above).

## License

MIT
