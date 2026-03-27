export { TimestarClient, TimestarError } from "./client";
export { init as initProto } from "./proto";
export {
  compressTimestamps,
  decompressTimestamps,
  compressDoubles,
  decompressDoubles,
  compressIntegers,
  decompressIntegers,
  compressBooleans,
  decompressBooleans,
  compressStrings,
  decompressStrings,
} from "./compression";
export type {
  // Client options
  TimestarClientOptions,

  // Write
  WriteField,
  WritePoint,
  WriteRequest,
  WriteResponse,

  // Query
  QueryOptions,
  FieldData,
  SeriesResult,
  QueryStatistics,
  QueryResponse,

  // Delete
  DeleteRequestItem,
  DeleteResponse,

  // Metadata
  MeasurementsOptions,
  MeasurementsResponse,
  TagsOptions,
  TagsResponse,
  FieldInfo,
  FieldsResponse,
  TagCardinality,
  CardinalityResponse,

  // Retention
  DownsamplePolicy,
  RetentionPolicy,
  RetentionGetResponse,

  // Streaming
  StreamQueryEntry,
  SubscribeRequest,
  StreamingDataPoint,
  StreamingBatch,
  SubscriptionStats,
  SubscriptionsResponse,

  // Derived
  DerivedQueryOptions,
  DerivedQueryStatistics,
  DerivedQueryResponse,

  // Anomaly
  AnomalyAlgorithm,
  AnomalySeasonality,
  AnomalySeriesPiece,
  AnomalyStatistics,
  AnomalyResponse,

  // Forecast
  ForecastAlgorithm,
  ForecastSeriesPiece,
  ForecastStatistics,
  ForecastResponse,

  // Health
  HealthResponse,
} from "./types";
