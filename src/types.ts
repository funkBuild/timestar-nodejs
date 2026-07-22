// ============================================================================
// Write API
// ============================================================================

/** Explicit typed field — use when auto-detection isn't sufficient (e.g., forcing int64 for whole numbers). */
export interface WriteField {
  doubleValues?: number[];
  boolValues?: boolean[];
  stringValues?: string[];
  int64Values?: Array<number | bigint>;
}

/** A field value: scalar, typed array, or plain array (auto-detected). */
export type FieldValue =
  | number                        // single double
  | boolean                       // single bool
  | string                        // single string
  | bigint                        // single int64
  | number[]                      // auto-detected as double[]
  | boolean[]                     // auto-detected as bool[]
  | string[]                      // auto-detected as string[]
  | WriteField;                   // explicit typed field

export interface WritePoint {
  measurement: string;
  tags?: Record<string, string>;
  fields: Record<string, FieldValue>;
  timestamps: Array<number | bigint>;
}

export interface WriteRequest {
  writes: WritePoint[];
}

export interface WriteResponse {
  /** "success", "partial" (some points/fields failed — see errors), or "error". */
  status: string;
  /**
   * Number of FIELD-points written: fields x timestamps per point.
   * A point with 3 fields and 10 timestamps counts as 30.
   */
  pointsWritten: number;
  failedWrites: number;
  /** Per-point/per-field error messages (capped by the server, currently at 10). */
  errors: string[];
}

// ============================================================================
// Query API
// ============================================================================

export interface QueryOptions {
  startTime?: number | bigint;
  endTime?: number | bigint;
  aggregationInterval?: string;
  /**
   * Bucket-grid alignment for interval queries.
   *
   * "start" (THIS CLIENT'S DEFAULT): buckets are anchored at startTime —
   * bucket = startTime + floor((ts − startTime)/interval)·interval — matching
   * rollup.js semantics. "epoch": the server's canonical epoch-aligned grid
   * (bucket = floor(ts/interval)·interval), whose boundaries never shift with
   * the query range.
   *
   * NOTE the asymmetry: the SERVER defaults to "epoch"; this client sends
   * "start" unless told otherwise (it exists to replace a rollup.js reader).
   * Pass "epoch" here — or set the client-level option — to get the server's
   * canonical grid. Requires server >= 1.3.0; older servers ignore the field
   * and answer epoch-aligned. No effect without an aggregationInterval.
   */
  bucketAlignment?: "epoch" | "start";
  /**
   * When true, boolean fields aggregate arithmetically as 1.0/0.0 (avg of
   * [t,t,f,t,f] is 0.6, matching rollup.js) and come back as number[] —
   * raw reads included. Default false: booleans are non-numeric (see
   * FieldData.values). Strings are unaffected either way. Requires
   * server >= 1.3.0.
   */
  booleansAsNumeric?: boolean;
  /**
   * Precision of decoded timestamps and int64 field values.
   *
   * Default (false): FieldData.timestamps and int64 values are returned as
   * number[]. JavaScript numbers hold at most 53 bits of integer precision,
   * so values beyond 2^53 — which includes ALL realistic nanosecond epoch
   * timestamps (~1.7e18) — are silently rounded to the nearest representable
   * double (off by up to ~128ns at current epoch values).
   *
   * true: timestamps and int64 field values are returned as bigint[] with
   * exact 64-bit precision. Doubles, booleans, and strings are unaffected.
   * Overrides the client-level `precise` option for this query.
   */
  precise?: boolean;
}

export interface FieldData {
  /**
   * Point timestamps (nanoseconds). number[] by default — values beyond 2^53
   * lose precision (see QueryOptions.precise); bigint[] when the query or
   * client was created with `precise: true`.
   */
  timestamps: Array<number | bigint>;
  /**
   * Field values, returned in the type they were written in.
   *
   * Boolean fields are returned as boolean[] (`true`/`false`). They are
   * non-numeric: the aggregation method named in the query is ignored for
   * them, exactly as it is for strings — without an aggregationInterval they
   * pass through raw, and with one they reduce to LATEST-per-bucket. (Servers
   * before this behaviour coerced them to numeric 0/1 on the query path.)
   *
   * int64 fields are number[] by default (values beyond 2^53 are rounded)
   * and bigint[] with `precise: true`. The write side always preserves full
   * 64-bit precision on the wire regardless of this option.
   */
  values: number[] | bigint[] | boolean[] | string[];
}

export interface SeriesResult {
  measurement: string;
  tags: Record<string, string>;
  groupTags?: string[];
  fields: Record<string, FieldData>;
}

export interface QueryStatistics {
  seriesCount: number;
  pointCount: number;
  executionTimeMs: number;
  shardsQueried: number[];
  failedSeriesCount: number;
  truncated: boolean;
  truncationReason: string;
}

export interface QueryResponse {
  status: string;
  series: SeriesResult[];
  statistics: QueryStatistics;
  errorCode?: string;
  errorMessage?: string;
}

// ============================================================================
// Delete API
// ============================================================================

export interface DeleteRequestItem {
  series?: string;
  measurement?: string;
  tags?: Record<string, string>;
  field?: string;
  fields?: string[];
  startTime?: number | bigint;
  endTime?: number | bigint;
}

export interface DeleteResponse {
  status: string;
  deletedCount: number;
  totalRequests: number;
  errorMessage?: string;
}

// ============================================================================
// Metadata API
// ============================================================================

export interface MeasurementsOptions {
  prefix?: string;
  limit?: number;
  offset?: number;
}

export interface MeasurementsResponse {
  status: string;
  measurements: string[];
  total: number;
}

export interface TagsOptions {
  tag?: string;
}

export interface TagsResponse {
  status: string;
  measurement: string;
  tags: Record<string, string[]>;
}

export interface FieldInfo {
  name: string;
  type: string;
}

export interface FieldsResponse {
  status: string;
  measurement: string;
  fields: FieldInfo[];
}

export interface TagCardinality {
  tagKey: string;
  estimatedCount: number;
}

export interface CardinalityResponse {
  status: string;
  measurement: string;
  estimatedSeriesCount: number;
  tagCardinalities: TagCardinality[];
}

// ============================================================================
// Retention API
// ============================================================================

export interface DownsamplePolicy {
  after: string;
  interval: string;
  method: "avg" | "min" | "max" | "sum" | "latest";
}

export interface RetentionPolicy {
  measurement: string;
  ttl: string;
  ttlNanos?: number;
  downsample?: DownsamplePolicy;
}

export interface RetentionGetResponse {
  status: string;
  policy: RetentionPolicy;
}

// ============================================================================
// Streaming / Subscribe API
// ============================================================================

export interface StreamQueryEntry {
  query: string;
  label: string;
}

export interface SubscribeRequest {
  query?: string;
  queries?: StreamQueryEntry[];
  formula?: string;
  startTime?: number | bigint;
  backfill?: boolean;
  aggregationInterval?: string;
}

export interface StreamingDataPoint {
  measurement: string;
  field: string;
  tags: Record<string, string>;
  timestamp: number | bigint;
  value: number | boolean | string | bigint;
}

export interface StreamingBatch {
  points: StreamingDataPoint[];
  sequenceId: number;
  label: string;
  isDrop: boolean;
  droppedCount: number;
}

export interface SubscriptionStats {
  id: number;
  measurement: string;
  scopes: Record<string, string>;
  fields: string[];
  label: string;
  handlerShard: number;
  queueDepth: number;
  queueCapacity: number;
  droppedPoints: number;
  eventsSent: number;
}

export interface SubscriptionsResponse {
  status: string;
  subscriptions: SubscriptionStats[];
}

// ============================================================================
// Derived Query API
// ============================================================================

export interface DerivedQueryOptions {
  startTime: number | bigint;
  endTime: number | bigint;
  aggregationInterval?: string;
}

export interface DerivedQueryStatistics {
  pointCount: number;
  executionTimeMs: number;
  subQueriesExecuted: number;
  pointsDroppedDueToAlignment: number;
}

export interface DerivedQueryResponse {
  status: string;
  timestamps: Array<number | bigint>;
  values: number[];
  formula: string;
  statistics: DerivedQueryStatistics;
  errorCode?: string;
  errorMessage?: string;
}

// ============================================================================
// Anomaly Detection
// ============================================================================

export type AnomalyAlgorithm = "basic" | "robust" | "agile";
export type AnomalySeasonality = "hourly" | "daily" | "weekly";

export interface AnomalySeriesPiece {
  piece: "raw" | "upper" | "lower" | "scores" | "ratings";
  groupTags: string[];
  values: number[];
  alertValue?: number;
  hasAlert: boolean;
}

export interface AnomalyStatistics {
  algorithm: string;
  bounds: number;
  seasonality: string;
  anomalyCount: number;
  totalPoints: number;
  executionTimeMs: number;
}

export interface AnomalyResponse {
  status: string;
  times: Array<number | bigint>;
  series: AnomalySeriesPiece[];
  statistics: AnomalyStatistics;
  errorMessage?: string;
}

// ============================================================================
// Forecast
// ============================================================================

export type ForecastAlgorithm = "linear" | "seasonal";

export interface ForecastSeriesPiece {
  piece: "past" | "forecast" | "upper" | "lower";
  groupTags: string[];
  values: number[];
}

export interface ForecastStatistics {
  algorithm: string;
  deviations: number;
  seasonality: string;
  slope: number;
  intercept: number;
  rSquared: number;
  residualStdDev: number;
  historicalPoints: number;
  forecastPoints: number;
  seriesCount: number;
  executionTimeMs: number;
}

export interface ForecastResponse {
  status: string;
  times: Array<number | bigint>;
  forecastStartIndex: number;
  series: ForecastSeriesPiece[];
  statistics: ForecastStatistics;
  errorMessage?: string;
}

// ============================================================================
// Health
// ============================================================================

export interface HealthResponse {
  status: string;
}

// ============================================================================
// Client Options
// ============================================================================

export interface TimestarClientOptions {
  host?: string;
  port?: number;
  authToken?: string;
  /** @deprecated The client always uses protobuf. This option is ignored. */
  useProtobuf?: boolean;
  requestTimeoutMs?: number;
  /**
   * Default for QueryOptions.precise on all queries from this client:
   * when true, query results return timestamps and int64 field values as
   * bigint[] (exact 64-bit) instead of number[] (rounded beyond 2^53).
   */
  precise?: boolean;
  /**
   * Total time budget (milliseconds) for transparent write retries when the
   * server responds 503 (congestion). Each retry waits the server's
   * Retry-After header (falling back to exponential backoff when absent);
   * once the accumulated wait would exceed this budget the 503 is thrown.
   * Set to 0 to disable retries. Default: 30000.
   */
  maxRetryDelayMs?: number;
  /**
   * Default for QueryOptions.bucketAlignment on all queries from this client.
   * Default "start" (rollup.js-compatible buckets anchored at startTime) —
   * NOT the server's canonical "epoch" grid; see QueryOptions.bucketAlignment.
   */
  bucketAlignment?: "epoch" | "start";
  /**
   * Default for QueryOptions.booleansAsNumeric on all queries from this
   * client. Default false.
   */
  booleansAsNumeric?: boolean;
}
