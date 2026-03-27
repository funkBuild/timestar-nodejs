// ============================================================================
// Write API
// ============================================================================

export interface WriteField {
  doubleValues?: number[];
  boolValues?: boolean[];
  stringValues?: string[];
  int64Values?: Array<number | bigint>;
}

export interface WritePoint {
  measurement: string;
  tags?: Record<string, string>;
  fields: Record<string, WriteField | number | boolean | string | bigint>;
  timestamps: Array<number | bigint>;
}

export interface WriteRequest {
  writes: WritePoint[];
}

export interface WriteResponse {
  status: string;
  pointsWritten: number;
  failedWrites: number;
  errors: string[];
}

// ============================================================================
// Query API
// ============================================================================

export interface QueryOptions {
  startTime?: number | bigint;
  endTime?: number | bigint;
  aggregationInterval?: string;
}

export interface FieldData {
  timestamps: Array<number | bigint>;
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
}
