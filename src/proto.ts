import * as protobuf from "protobufjs";
import * as path from "path";

let rootPromise: Promise<protobuf.Root> | null = null;

function getRoot(): Promise<protobuf.Root> {
  if (!rootPromise) {
    const protoPath = path.resolve(__dirname, "..", "proto", "timestar.proto");
    rootPromise = protobuf.load(protoPath);
  }
  return rootPromise;
}

async function getType(name: string): Promise<protobuf.Type> {
  const root = await getRoot();
  return root.lookupType(`timestar_pb.${name}`);
}

// [M7 fix] Removed dead makeCodec/ProtoCodec — only async codecs are used.

export interface AsyncProtoCodec<T = unknown> {
  encode(message: T): Promise<Uint8Array>;
  decode(buffer: Uint8Array): Promise<T>;
}

function makeAsyncCodec<T = unknown>(typeName: string): AsyncProtoCodec<T> {
  let typePromise: Promise<protobuf.Type> | null = null;
  const getTypeOnce = () => {
    if (!typePromise) typePromise = getType(typeName);
    return typePromise;
  };
  getTypeOnce();

  return {
    async encode(message: T): Promise<Uint8Array> {
      const type = await getTypeOnce();
      const msg = type.create(message as Record<string, unknown>);
      return type.encode(msg).finish();
    },
    async decode(buffer: Uint8Array): Promise<T> {
      const type = await getTypeOnce();
      const decoded = type.decode(buffer);
      return type.toObject(decoded, {
        longs: Number,
        defaults: true,
        arrays: true,
        objects: true,
      }) as T;
    },
  };
}

// Proto message interfaces (wire format — snake_case matching proto field names)
// These differ from the user-facing types in types.ts

export interface ProtoWriteField {
  doubleValues?: { values?: number[]; compressedAlp?: Uint8Array };
  boolValues?: { values?: boolean[]; compressedRle?: Uint8Array };
  stringValues?: { values?: string[]; compressedZstd?: Uint8Array; count?: number };
  int64Values?: { values?: Array<number | Long>; compressedFfor?: Uint8Array };
}

export interface ProtoWritePoint {
  measurement: string;
  tags?: Record<string, string>;
  fields?: Record<string, ProtoWriteField>;
  timestamps?: Array<number | Long>;
  compressedTimestamps?: Uint8Array;
}

export interface ProtoWriteRequest {
  writes: ProtoWritePoint[];
}

export interface ProtoWriteResponse {
  status: string;
  pointsWritten: number;
  failedWrites: number;
  errors: string[];
}

export interface ProtoQueryRequest {
  query: string;
  startTime?: number;
  endTime?: number;
  aggregationInterval?: string;
}

export interface ProtoFieldData {
  timestamps: number[];
  compressedTimestamps?: Uint8Array;
  doubleValues?: { values?: number[]; compressedAlp?: Uint8Array };
  int64Values?: { values?: number[]; compressedFfor?: Uint8Array };
  boolValues?: { values?: boolean[]; compressedRle?: Uint8Array };
  stringValues?: { values?: string[]; compressedZstd?: Uint8Array; count?: number };
}

export interface ProtoSeriesResult {
  measurement: string;
  tags: Record<string, string>;
  fields: Record<string, ProtoFieldData>;
}

export interface ProtoQueryStatistics {
  seriesCount: number;
  pointCount: number;
  executionTimeMs: number;
  shardsQueried: number[];
  failedSeriesCount: number;
  truncated: boolean;
  truncationReason: string;
}

export interface ProtoQueryResponse {
  status: string;
  series: ProtoSeriesResult[];
  statistics: ProtoQueryStatistics;
  errorCode: string;
  errorMessage: string;
}

export interface ProtoDeleteRequest {
  series?: string;
  measurement?: string;
  tags?: Record<string, string>;
  field?: string;
  fields?: string[];
  startTime?: number;
  endTime?: number;
}

export interface ProtoBatchDeleteRequest {
  deletes: ProtoDeleteRequest[];
}

export interface ProtoDeleteResponse {
  status: string;
  deletedCount: number;
  totalRequests: number;
  errorMessage: string;
}

export interface ProtoMeasurementsRequest {
  prefix?: string;
  limit?: number;
  offset?: number;
}

export interface ProtoMeasurementsResponse {
  status: string;
  measurements: string[];
  total: number;
}

export interface ProtoTagsRequest {
  measurement: string;
  tag?: string;
}

export interface ProtoTagValues {
  values: string[];
}

export interface ProtoTagsResponse {
  status: string;
  measurement: string;
  tags: Record<string, ProtoTagValues>;
}

export interface ProtoFieldInfo {
  name: string;
  type: string;
}

export interface ProtoFieldsRequest {
  measurement: string;
  tagFilters?: Record<string, string>;
}

export interface ProtoFieldsResponse {
  status: string;
  measurement: string;
  fields: ProtoFieldInfo[];
}

export interface ProtoCardinalityRequest {
  measurement: string;
}

export interface ProtoTagCardinality {
  tagKey: string;
  estimatedCount: number;
}

export interface ProtoCardinalityResponse {
  status: string;
  measurement: string;
  estimatedSeriesCount: number;
  tagCardinalities: ProtoTagCardinality[];
}

export interface ProtoRetentionPutRequest {
  measurement: string;
  ttl: string;
  downsample?: {
    after: string;
    interval: string;
    method: string;
  };
}

export interface ProtoRetentionGetRequest {
  measurement: string;
}

export interface ProtoRetentionGetResponse {
  status: string;
  policy: {
    measurement: string;
    ttl: string;
    ttlNanos: number;
    downsample?: {
      after: string;
      afterNanos: number;
      interval: string;
      intervalNanos: number;
      method: string;
    };
  };
}

export interface ProtoRetentionDeleteRequest {
  measurement: string;
}

export interface ProtoSubscribeRequest {
  query?: string;
  queries?: Array<{ query: string; label: string }>;
  formula?: string;
  startTime?: number;
  backfill?: boolean;
  aggregationInterval?: string;
}

export interface ProtoSubscriptionsResponse {
  status: string;
  subscriptions: Array<{
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
  }>;
}

export interface ProtoDerivedSubQuery {
  name: string;
  query: string;
}

export interface ProtoDerivedQueryRequest {
  queries: ProtoDerivedSubQuery[];
  formula: string;
  startTime?: number;
  endTime?: number;
  aggregationInterval?: string;
}

export interface ProtoDerivedQueryResponse {
  status: string;
  timestamps: number[];
  values: number[];
  formula: string;
  statistics: {
    pointCount: number;
    executionTimeMs: number;
    subQueriesExecuted: number;
    pointsDroppedDueToAlignment: number;
  };
  errorCode: string;
  errorMessage: string;
}

export interface ProtoAnomalyResponse {
  status: string;
  times: number[];
  series: Array<{
    piece: string;
    groupTags: string[];
    values: number[];
    alertValue: number;
    hasAlert: boolean;
  }>;
  statistics: {
    algorithm: string;
    bounds: number;
    seasonality: string;
    anomalyCount: number;
    totalPoints: number;
    executionTimeMs: number;
  };
  errorMessage: string;
}

export interface ProtoForecastResponse {
  status: string;
  times: number[];
  forecastStartIndex: number;
  series: Array<{
    piece: string;
    groupTags: string[];
    values: number[];
  }>;
  statistics: {
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
  };
  errorMessage: string;
}

export interface ProtoHealthResponse {
  status: string;
}

type Long = number;

// Codecs for all message types
export const codecs = {
  // Write
  WriteRequest: makeAsyncCodec<ProtoWriteRequest>("WriteRequest"),
  WriteResponse: makeAsyncCodec<ProtoWriteResponse>("WriteResponse"),

  // Query
  QueryRequest: makeAsyncCodec<ProtoQueryRequest>("QueryRequest"),
  QueryResponse: makeAsyncCodec<ProtoQueryResponse>("QueryResponse"),

  // Delete
  DeleteRequest: makeAsyncCodec<ProtoDeleteRequest>("DeleteRequest"),
  BatchDeleteRequest: makeAsyncCodec<ProtoBatchDeleteRequest>("BatchDeleteRequest"),
  DeleteResponse: makeAsyncCodec<ProtoDeleteResponse>("DeleteResponse"),

  // Metadata
  MeasurementsRequest: makeAsyncCodec<ProtoMeasurementsRequest>("MeasurementsRequest"),
  MeasurementsResponse: makeAsyncCodec<ProtoMeasurementsResponse>("MeasurementsResponse"),
  TagsRequest: makeAsyncCodec<ProtoTagsRequest>("TagsRequest"),
  TagsResponse: makeAsyncCodec<ProtoTagsResponse>("TagsResponse"),
  FieldsRequest: makeAsyncCodec<ProtoFieldsRequest>("FieldsRequest"),
  FieldsResponse: makeAsyncCodec<ProtoFieldsResponse>("FieldsResponse"),
  CardinalityRequest: makeAsyncCodec<ProtoCardinalityRequest>("CardinalityRequest"),
  CardinalityResponse: makeAsyncCodec<ProtoCardinalityResponse>("CardinalityResponse"),

  // Retention
  RetentionPutRequest: makeAsyncCodec<ProtoRetentionPutRequest>("RetentionPutRequest"),
  RetentionGetRequest: makeAsyncCodec<ProtoRetentionGetRequest>("RetentionGetRequest"),
  RetentionGetResponse: makeAsyncCodec<ProtoRetentionGetResponse>("RetentionGetResponse"),
  RetentionDeleteRequest: makeAsyncCodec<ProtoRetentionDeleteRequest>("RetentionDeleteRequest"),

  // Streaming
  SubscribeRequest: makeAsyncCodec<ProtoSubscribeRequest>("SubscribeRequest"),
  SubscriptionsResponse: makeAsyncCodec<ProtoSubscriptionsResponse>("SubscriptionsResponse"),
  StreamingBatch: makeAsyncCodec<{
    points: Array<{
      measurement: string;
      field: string;
      tags: Record<string, string>;
      timestamp: number;
      value: { doubleValue?: number; boolValue?: boolean; stringValue?: string; int64Value?: number };
    }>;
    sequenceId: number;
    label: string;
    isDrop: boolean;
    droppedCount: number;
  }>("StreamingBatch"),

  // Derived
  DerivedQueryRequest: makeAsyncCodec<ProtoDerivedQueryRequest>("DerivedQueryRequest"),
  DerivedQueryResponse: makeAsyncCodec<ProtoDerivedQueryResponse>("DerivedQueryResponse"),

  // Anomaly / Forecast
  AnomalyResponse: makeAsyncCodec<ProtoAnomalyResponse>("AnomalyResponse"),
  ForecastResponse: makeAsyncCodec<ProtoForecastResponse>("ForecastResponse"),

  // Health
  HealthResponse: makeAsyncCodec<ProtoHealthResponse>("HealthResponse"),
  StatusResponse: makeAsyncCodec<{ status: string; message: string; code: string }>("StatusResponse"),
  Empty: makeAsyncCodec<Record<string, never>>("Empty"),
};

/** Pre-load the proto file so subsequent encode/decode calls are fast. */
export async function init(): Promise<void> {
  await getRoot();
}
