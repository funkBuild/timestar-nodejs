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
await client.write({
  measurement: "cpu",
  tags: { host: "server-01", region: "us-east" },
  fields: {
    usage: { doubleValues: [55.2, 62.8, 49.1] },
    throttled: { boolValues: [false, false, true] },
  },
  timestamps: [1700000000000, 1700000001000, 1700000002000],
});
```

Single scalar values are also accepted as a shorthand:

```ts
await client.write({
  measurement: "cpu",
  tags: { host: "server-01" },
  fields: { usage: 55.2 },
  timestamps: [1700000000000],
});
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

The client uses **Approach B**: field values and timestamps are compressed client-side into `bytes` fields within the protobuf messages. The server decompresses them directly, avoiding any intermediate representation.

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
| `useProtobuf` | `boolean` | `true` | Use protobuf binary protocol. Set to `false` for JSON. |

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
    console.error(err.code);       // Optional error code
  }
}
```

## License

MIT
