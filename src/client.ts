import * as http from "http";
import { codecs, init as protoInit, ProtoFieldData, ProtoQueryResponse, ProtoWriteField, ProtoWritePoint } from "./proto";
import {
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
import type {
  TimestarClientOptions,
  WritePoint,
  WriteField,
  WriteResponse,
  QueryOptions,
  QueryResponse,
  SeriesResult,
  FieldData,
  DeleteRequestItem,
  DeleteResponse,
  MeasurementsOptions,
  MeasurementsResponse,
  TagsOptions,
  TagsResponse,
  FieldsResponse,
  CardinalityResponse,
  DownsamplePolicy,
  RetentionGetResponse,
  SubscribeRequest,
  StreamingBatch,
  StreamingDataPoint,
  SubscriptionsResponse,
  DerivedQueryOptions,
  DerivedQueryResponse,
  AnomalyResponse,
  ForecastResponse,
  HealthResponse,
} from "./types";

export class TimestarClient {
  private readonly baseUrl: string;
  private readonly authToken?: string;
  private readonly requestTimeoutMs: number;
  private initPromise: Promise<void> | null = null;

  constructor(options: TimestarClientOptions = {}) {
    const host = options.host ?? "localhost";
    const port = options.port ?? 8086;
    this.baseUrl = `http://${host}:${port}`;
    this.authToken = options.authToken;
    this.requestTimeoutMs = options.requestTimeoutMs ?? 30_000;
  }

  // Race-safe init: caches the Promise so concurrent callers share one init.
  private ensureInit(): Promise<void> | void {
    if (this.initPromise === null) {
      this.initPromise = protoInit();
    }
    return this.initPromise;
  }

  // ---------------------------------------------------------------------------
  // HTTP helpers — [H5 fix] native fetch (Node 18+, undici connection pooling)
  // ---------------------------------------------------------------------------

  private async request(
    method: string,
    path: string,
    body?: Uint8Array | null,
    contentType?: string,
    accept?: string,
  ): Promise<{ status: number; headers: Record<string, string>; body: Buffer }> {
    const url = new URL(path, this.baseUrl);
    const headers: Record<string, string> = {};
    if (this.authToken) headers["Authorization"] = `Bearer ${this.authToken}`;
    if (contentType) headers["Content-Type"] = contentType;
    if (accept) headers["Accept"] = accept;

    try {
      const res = await fetch(url, {
        method,
        headers,
        body: body ?? undefined,
        signal: AbortSignal.timeout(this.requestTimeoutMs),
      });

      const buf = Buffer.from(await res.arrayBuffer());
      const respHeaders: Record<string, string> = {};
      res.headers.forEach((v, k) => { respHeaders[k] = v; });
      return { status: res.status, headers: respHeaders, body: buf };
    } catch (err: any) {
      if (err.name === 'AbortError') throw new TimestarError('Request timeout', 0, 'TIMEOUT');
      throw new TimestarError(`Connection error: ${err.message}`, 0, 'CONNECTION_ERROR');
    }
  }

  private async protoPost<TReq, TRes>(
    path: string,
    reqCodecKey: keyof typeof codecs,
    resCodecKey: keyof typeof codecs,
    payload: TReq,
  ): Promise<TRes> {
    { const p = this.ensureInit(); if (p) await p; }

    const reqCodec = codecs[reqCodecKey] as { encode(m: TReq): Promise<Uint8Array> };
    const resCodec = codecs[resCodecKey] as { decode(b: Uint8Array): Promise<TRes> };
    const encoded = await reqCodec.encode(payload);
    const res = await this.request(
      "POST",
      path,
      encoded,
      "application/protobuf",
      "application/protobuf",
    );
    if (res.status >= 400) {
      // Try to decode error as protobuf StatusResponse, fall back to text
      try {
        const errRes = await codecs.StatusResponse.decode(new Uint8Array(res.body));
        throw new TimestarError(errRes.message || errRes.status, res.status, errRes.code);
      } catch (e) {
        if (e instanceof TimestarError) throw e;
        throw new TimestarError(res.body.toString("utf-8"), res.status);
      }
    }
    return resCodec.decode(new Uint8Array(res.body));
  }

  private async protoGet<TRes>(
    path: string,
    resCodecKey: keyof typeof codecs,
    params?: Record<string, string | number | undefined>,
  ): Promise<TRes> {
    { const p = this.ensureInit(); if (p) await p; }

    const url = new URL(path, this.baseUrl);
    if (params) {
      for (const [k, v] of Object.entries(params)) {
        if (v !== undefined) url.searchParams.set(k, String(v));
      }
    }

    const resCodec = codecs[resCodecKey] as { decode(b: Uint8Array): Promise<TRes> };
    const res = await this.request(
      "GET",
      url.pathname + url.search,
      null,
      undefined,
      "application/protobuf",
    );
    if (res.status >= 400) {
      try {
        const errRes = await codecs.StatusResponse.decode(new Uint8Array(res.body));
        throw new TimestarError(errRes.message || errRes.status, res.status, errRes.code);
      } catch (e) {
        if (e instanceof TimestarError) throw e;
        throw new TimestarError(res.body.toString("utf-8"), res.status);
      }
    }
    return resCodec.decode(new Uint8Array(res.body));
  }

  // ---------------------------------------------------------------------------
  // Health
  // ---------------------------------------------------------------------------

  async health(): Promise<HealthResponse> {
    return this.protoGet<HealthResponse>("/health", "HealthResponse");
  }

  // Lightweight health check that returns true/false
  async isHealthy(): Promise<boolean> {
    try {
      const res = await this.request("GET", "/health");
      return res.status === 200;
    } catch {
      return false;
    }
  }

  // ---------------------------------------------------------------------------
  // Write
  // ---------------------------------------------------------------------------

  async write(points: WritePoint | WritePoint[]): Promise<WriteResponse> {
    const arr = Array.isArray(points) ? points : [points];
    const protoPoints = arr.map(normalizeWritePoint);

    return this.protoPost<{ writes: ProtoWritePoint[] }, WriteResponse>(
      "/write",
      "WriteRequest",
      "WriteResponse",
      { writes: protoPoints },
    );
  }

  // ---------------------------------------------------------------------------
  // Query
  // ---------------------------------------------------------------------------

  async query(query: string, options: QueryOptions = {}): Promise<QueryResponse> {
    const payload: any = { query };
    if (options.startTime !== undefined) payload.startTime = Number(options.startTime);
    if (options.endTime !== undefined) payload.endTime = Number(options.endTime);
    if (options.aggregationInterval !== undefined) payload.aggregationInterval = options.aggregationInterval;

    { const p = this.ensureInit(); if (p) await p; }
    const encoded = await codecs.QueryRequest.encode(payload);
    const res = await this.request(
      "POST",
      "/query",
      encoded,
      "application/protobuf",
      "application/protobuf",
    );
    if (res.status >= 400) {
      try {
        const errRes = await codecs.StatusResponse.decode(new Uint8Array(res.body));
        throw new TimestarError(errRes.message || errRes.status, res.status, errRes.code);
      } catch (e) {
        if (e instanceof TimestarError) throw e;
        throw new TimestarError(res.body.toString("utf-8"), res.status);
      }
    }
    const proto = await codecs.QueryResponse.decode(new Uint8Array(res.body));
    return convertQueryResponse(proto);
  }

  // ---------------------------------------------------------------------------
  // Delete
  // ---------------------------------------------------------------------------

  async delete(req: DeleteRequestItem | DeleteRequestItem[]): Promise<DeleteResponse> {
    const items = Array.isArray(req) ? req : [req];

    if (items.length === 1) {
      const payload = normalizeDeleteRequest(items[0]);
      return this.protoPost<any, DeleteResponse>(
        "/delete",
        "DeleteRequest",
        "DeleteResponse",
        payload,
      );
    } else {
      const payload = { deletes: items.map(normalizeDeleteRequest) };
      return this.protoPost<any, DeleteResponse>(
        "/delete",
        "BatchDeleteRequest",
        "DeleteResponse",
        payload,
      );
    }
  }

  // ---------------------------------------------------------------------------
  // Metadata
  // ---------------------------------------------------------------------------

  async measurements(options: MeasurementsOptions = {}): Promise<MeasurementsResponse> {
    return this.protoGet<MeasurementsResponse>(
      "/measurements",
      "MeasurementsResponse",
      {
        prefix: options.prefix,
        limit: options.limit,
        offset: options.offset,
      },
    );
  }

  async tags(measurement: string, options: TagsOptions = {}): Promise<TagsResponse> {
    const raw = await this.protoGet<any>(
      "/tags",
      "TagsResponse",
      { measurement, tag: options.tag },
    );
    // Proto returns tags as map<string, TagValues> with .values arrays.
    // Flatten to map<string, string[]> for user convenience.
    if (raw.tags) {
      const flattened: Record<string, string[]> = {};
      for (const [k, v] of Object.entries(raw.tags)) {
        flattened[k] = (v as any).values ?? v;
      }
      raw.tags = flattened;
    }
    return raw as TagsResponse;
  }

  async fields(measurement: string): Promise<FieldsResponse> {
    return this.protoGet<FieldsResponse>(
      "/fields",
      "FieldsResponse",
      { measurement },
    );
  }

  async cardinality(measurement: string): Promise<CardinalityResponse> {
    return this.protoGet<CardinalityResponse>(
      "/cardinality",
      "CardinalityResponse",
      { measurement },
    );
  }

  // ---------------------------------------------------------------------------
  // Retention
  // ---------------------------------------------------------------------------

  async setRetention(
    measurement: string,
    ttl: string,
    downsample?: DownsamplePolicy,
  ): Promise<void> {
    { const p = this.ensureInit(); if (p) await p; }
    const payload: any = { measurement, ttl };
    if (downsample) {
      payload.downsample = {
        after: downsample.after,
        interval: downsample.interval,
        method: downsample.method,
      };
    }

    const encoded = await codecs.RetentionPutRequest.encode(payload);
    const res = await this.request(
      "PUT",
      "/retention",
      encoded,
      "application/protobuf",
      "application/protobuf",
    );
    if (res.status >= 400) {
      throw new TimestarError(res.body.toString("utf-8"), res.status);
    }
  }

  async getRetention(measurement: string): Promise<RetentionGetResponse> {
    return this.protoGet<RetentionGetResponse>(
      "/retention",
      "RetentionGetResponse",
      { measurement },
    );
  }

  async deleteRetention(measurement: string): Promise<void> {
    { const p = this.ensureInit(); if (p) await p; }
    const url = new URL("/retention", this.baseUrl);
    url.searchParams.set("measurement", measurement);

    const res = await this.request("DELETE", url.pathname + url.search);
    if (res.status >= 400) {
      throw new TimestarError(res.body.toString("utf-8"), res.status);
    }
  }

  // ---------------------------------------------------------------------------
  // Streaming / Subscribe
  // ---------------------------------------------------------------------------

  async *subscribe(req: SubscribeRequest): AsyncGenerator<StreamingBatch, void, undefined> {
    { const p = this.ensureInit(); if (p) await p; }
    const url = new URL("/subscribe", this.baseUrl);

    // Subscribe always uses SSE (text/event-stream). Request body is protobuf.
    const body = await codecs.SubscribeRequest.encode(normalizeSubscribeRequest(req));

    const headers: Record<string, string> = {
      "Content-Type": "application/protobuf",
      "Accept": "text/event-stream",
    };
    if (this.authToken) {
      headers["Authorization"] = `Bearer ${this.authToken}`;
    }
    headers["Content-Length"] = String(body.length);

    // [L5 fix] Use correct protocol from baseUrl (http vs https)
    const response = await new Promise<http.IncomingMessage>((resolve, reject) => {
      const nodeUrl = new URL(url.pathname, this.baseUrl);
      const transport = nodeUrl.protocol === "https:" ? require("https") : http;
      const req = transport.request(nodeUrl, { method: "POST", headers }, resolve);
      req.on("error", reject);
      req.write(body);
      req.end();
    });

    const HEARTBEAT_TIMEOUT = 60_000;
    response.setTimeout(HEARTBEAT_TIMEOUT, () => {
      response.destroy(new Error('SSE heartbeat timeout'));
    });

    if (response.statusCode && response.statusCode >= 400) {
      const chunks: Buffer[] = [];
      for await (const chunk of response) chunks.push(chunk as Buffer);
      throw new TimestarError(Buffer.concat(chunks).toString("utf-8"), response.statusCode);
    }

    // Parse SSE stream
    let buffer = "";
    let currentEvent = "";
    let currentData = "";

    for await (const chunk of response) {
      buffer += (chunk as Buffer).toString("utf-8");
      const lines = buffer.split("\n");
      buffer = lines.pop()!; // Keep incomplete line

      for (const line of lines) {
        if (line.startsWith("event:")) {
          currentEvent = line.slice(6).trim();
        } else if (line.startsWith("data:")) {
          // SSE spec: multiple data lines joined with newline
          const payload = line.slice(5);
          if (currentData.length > 0) currentData += "\n";
          currentData += payload.startsWith(" ") ? payload.slice(1) : payload;
        } else if (line === "") {
          // End of event
          if (currentEvent === "data" || currentEvent === "") {
            if (currentData) {
              try {
                const parsed = JSON.parse(currentData);
                yield convertStreamingBatch(parsed);
              } catch {
                // Skip unparseable events
              }
            }
          }
          currentEvent = "";
          currentData = "";
        }
      }
    }
  }

  async subscriptions(): Promise<SubscriptionsResponse> {
    return this.protoGet<SubscriptionsResponse>(
      "/subscriptions",
      "SubscriptionsResponse",
    );
  }

  // ---------------------------------------------------------------------------
  // Derived Queries
  // ---------------------------------------------------------------------------

  async derived(
    queries: Record<string, string>,
    formula: string,
    options: DerivedQueryOptions,
  ): Promise<DerivedQueryResponse> {
    const protoQueries = Object.entries(queries).map(([name, query]) => ({ name, query }));
    const payload: any = {
      queries: protoQueries,
      formula,
      startTime: Number(options.startTime),
      endTime: Number(options.endTime),
    };
    if (options.aggregationInterval) {
      payload.aggregationInterval = options.aggregationInterval;
    }

    return this.protoPost<any, DerivedQueryResponse>(
      "/derived",
      "DerivedQueryRequest",
      "DerivedQueryResponse",
      payload,
    );
  }

  // ---------------------------------------------------------------------------
  // Anomaly Detection
  // ---------------------------------------------------------------------------

  async anomalies(
    queries: Record<string, string>,
    formula: string,
    options: DerivedQueryOptions,
  ): Promise<AnomalyResponse> {
    const protoQueries = Object.entries(queries).map(([name, query]) => ({ name, query }));
    const payload: any = {
      queries: protoQueries,
      formula,
      startTime: Number(options.startTime),
      endTime: Number(options.endTime),
    };
    if (options.aggregationInterval) {
      payload.aggregationInterval = options.aggregationInterval;
    }

    // The /derived endpoint returns AnomalyResponse when formula contains anomalies()
    { const p = this.ensureInit(); if (p) await p; }

    const encoded = await codecs.DerivedQueryRequest.encode(payload);
    const res = await this.request(
      "POST",
      "/derived",
      encoded,
      "application/protobuf",
      "application/protobuf",
    );
    if (res.status >= 400) {
      try {
        const errRes = await codecs.StatusResponse.decode(new Uint8Array(res.body));
        throw new TimestarError(errRes.message || errRes.status, res.status, errRes.code);
      } catch (e) {
        if (e instanceof TimestarError) throw e;
        throw new TimestarError(res.body.toString("utf-8"), res.status);
      }
    }
    return codecs.AnomalyResponse.decode(new Uint8Array(res.body)) as unknown as AnomalyResponse;
  }

  // ---------------------------------------------------------------------------
  // Forecast
  // ---------------------------------------------------------------------------

  async forecast(
    queries: Record<string, string>,
    formula: string,
    options: DerivedQueryOptions,
  ): Promise<ForecastResponse> {
    const protoQueries = Object.entries(queries).map(([name, query]) => ({ name, query }));
    const payload: any = {
      queries: protoQueries,
      formula,
      startTime: Number(options.startTime),
      endTime: Number(options.endTime),
    };
    if (options.aggregationInterval) {
      payload.aggregationInterval = options.aggregationInterval;
    }

    { const p = this.ensureInit(); if (p) await p; }

    const encoded = await codecs.DerivedQueryRequest.encode(payload);
    const res = await this.request(
      "POST",
      "/derived",
      encoded,
      "application/protobuf",
      "application/protobuf",
    );
    if (res.status >= 400) {
      try {
        const errRes = await codecs.StatusResponse.decode(new Uint8Array(res.body));
        throw new TimestarError(errRes.message || errRes.status, res.status, errRes.code);
      } catch (e) {
        if (e instanceof TimestarError) throw e;
        throw new TimestarError(res.body.toString("utf-8"), res.status);
      }
    }
    return codecs.ForecastResponse.decode(new Uint8Array(res.body)) as unknown as ForecastResponse;
  }
}

// =============================================================================
// Error class
// =============================================================================

export class TimestarError extends Error {
  constructor(
    message: string,
    public readonly statusCode: number,
    public readonly code?: string,
  ) {
    super(message);
    this.name = "TimestarError";
  }
}

// =============================================================================
// Conversion helpers
// =============================================================================

function normalizeWritePoint(point: WritePoint): ProtoWritePoint {
  const protoFields: Record<string, ProtoWriteField> = {};

  for (const [key, val] of Object.entries(point.fields)) {
    if (typeof val === "number") {
      // [M10 fix] Single scalars: skip compression (overhead > savings)
      protoFields[key] = { doubleValues: { values: [val] } };
    } else if (typeof val === "boolean") {
      protoFields[key] = { boolValues: { values: [val] } };
    } else if (typeof val === "string") {
      protoFields[key] = { stringValues: { values: [val] } };
    } else if (typeof val === "bigint") {
      protoFields[key] = { int64Values: { values: [Number(val)] } };
    } else {
      const wf = val as WriteField;
      if (wf.doubleValues) {
        protoFields[key] = { doubleValues: { values: wf.doubleValues, compressedAlp: compressDoubles(wf.doubleValues) } };
      } else if (wf.boolValues) {
        protoFields[key] = { boolValues: { values: wf.boolValues, compressedRle: compressBooleans(wf.boolValues) } };
      } else if (wf.stringValues) {
        protoFields[key] = { stringValues: { values: wf.stringValues, compressedZstd: compressStrings(wf.stringValues), count: wf.stringValues.length } };
      } else if (wf.int64Values) {
        protoFields[key] = { int64Values: { values: wf.int64Values.map(Number), compressedFfor: compressIntegers(wf.int64Values.map(Number)) } };
      }
    }
  }

  return {
    measurement: point.measurement,
    tags: point.tags ?? {},
    fields: protoFields,
    timestamps: point.timestamps.map(Number),
    compressedTimestamps: compressTimestamps(point.timestamps),
  };
}

function normalizeDeleteRequest(item: DeleteRequestItem): any {
  const result: any = {};
  if (item.series) result.series = item.series;
  if (item.measurement) result.measurement = item.measurement;
  if (item.tags) result.tags = item.tags;
  if (item.field) result.field = item.field;
  if (item.fields) result.fields = item.fields;
  if (item.startTime !== undefined) result.startTime = Number(item.startTime);
  if (item.endTime !== undefined) result.endTime = Number(item.endTime);
  return result;
}

function normalizeSubscribeRequest(req: SubscribeRequest): any {
  const result: any = {};
  if (req.query) result.query = req.query;
  if (req.queries) result.queries = req.queries;
  if (req.formula) result.formula = req.formula;
  if (req.startTime !== undefined) result.startTime = Number(req.startTime);
  if (req.backfill !== undefined) result.backfill = req.backfill;
  if (req.aggregationInterval) result.aggregationInterval = req.aggregationInterval;
  return result;
}

// [M11 fix] Create Buffer view from Uint8Array without copying
function toBuffer(data: Uint8Array): Buffer {
  if (Buffer.isBuffer(data)) return data;
  return Buffer.from(data.buffer, data.byteOffset, data.byteLength);
}

// Read the total value count from a FFOR-compressed blob by scanning block headers.
// Each block header word0 has block_count in bits [0:10]. Sum across all blocks.
function readFforTotalCount(compressed: Uint8Array): number {
  if (compressed.length < 16) return 0;
  let total = 0;
  let offset = 0;
  while (offset + 16 <= compressed.length) {
    // Read word0 as uint64 LE, extract block_count from bits [0:10]
    const lo = compressed[offset] | (compressed[offset + 1] << 8);
    const blockCount = lo & 0x7FF;
    if (blockCount === 0) break;
    total += blockCount;

    // Read bw from bits [11:17] and exc_count from bits [18:27]
    const bw = (lo >> 11) & 0x7F | ((compressed[offset + 2] & 0x3) << 5);
    const excCountLo = (compressed[offset + 2] >> 2) | (compressed[offset + 3] << 6);
    const excCount = excCountLo & 0x3FF;

    // Skip: 16 header bytes + packed_words * 8 + exc_pos_words * 8 + exc_count * 8
    const packedWords = bw === 0 ? 0 : Math.ceil((blockCount * bw) / 64);
    const excPosWords = excCount > 0 ? Math.ceil(excCount / 4) : 0;
    offset += 16 + packedWords * 8 + excPosWords * 8 + excCount * 8;
  }
  return total;
}

// Decompress field data. Self-describing formats (ALP, zstd) are decompressed first
// to provide counts. For non-self-describing formats (FFOR, RLE), we extract the count
// from the FFOR timestamp header or decompress timestamps first.
function decompressFieldData(protoFd: ProtoFieldData): FieldData {
  // Step 1: Try self-describing value formats first
  let values: number[] | boolean[] | string[];
  let valueCount = 0;

  if (protoFd.doubleValues?.compressedAlp && protoFd.doubleValues.compressedAlp.length > 0) {
    values = decompressDoubles(toBuffer(protoFd.doubleValues.compressedAlp));
    valueCount = values.length;
  } else if (protoFd.stringValues?.compressedZstd && protoFd.stringValues.compressedZstd.length > 0) {
    values = decompressStrings(toBuffer(protoFd.stringValues.compressedZstd));
    valueCount = values.length;
  } else if (protoFd.doubleValues?.values && protoFd.doubleValues.values.length > 0) {
    values = protoFd.doubleValues.values;
    valueCount = values.length;
  } else if (protoFd.stringValues?.values && protoFd.stringValues.values.length > 0) {
    values = protoFd.stringValues.values;
    valueCount = values.length;
  } else {
    values = [];
    // For int64/bool: extract count from FFOR timestamp header if timestamps are compressed
    if (valueCount === 0 && protoFd.compressedTimestamps && protoFd.compressedTimestamps.length > 0) {
      valueCount = readFforTotalCount(protoFd.compressedTimestamps);
    }
  }

  // Step 2: Decompress timestamps
  let timestamps: number[];
  if (protoFd.compressedTimestamps && protoFd.compressedTimestamps.length > 0) {
    const count = valueCount > 0 ? valueCount : (protoFd.timestamps?.length ?? 0);
    timestamps = decompressTimestamps(toBuffer(protoFd.compressedTimestamps), count);
  } else {
    timestamps = protoFd.timestamps ?? [];
  }

  // Step 3: Decompress int64/bool values using timestamps.length as count
  if (values.length === 0) {
    if (protoFd.int64Values?.compressedFfor && protoFd.int64Values.compressedFfor.length > 0) {
      values = decompressIntegers(toBuffer(protoFd.int64Values.compressedFfor), timestamps.length);
    } else if (protoFd.boolValues?.compressedRle && protoFd.boolValues.compressedRle.length > 0) {
      values = decompressBooleans(toBuffer(protoFd.boolValues.compressedRle), timestamps.length);
    } else if (protoFd.int64Values?.values) {
      values = protoFd.int64Values.values;
    } else if (protoFd.boolValues?.values) {
      values = protoFd.boolValues.values;
    }
  }

  return { timestamps, values };
}

function convertQueryResponse(proto: ProtoQueryResponse): QueryResponse {
  const series: SeriesResult[] = (proto.series ?? []).map((s) => {
    const fields: Record<string, FieldData> = {};
    for (const [fieldName, fd] of Object.entries(s.fields ?? {})) {
      fields[fieldName] = decompressFieldData(fd);
    }
    return {
      measurement: s.measurement,
      tags: s.tags ?? {},
      fields,
    };
  });

  return {
    status: proto.status,
    series,
    statistics: proto.statistics ?? {
      seriesCount: 0,
      pointCount: 0,
      executionTimeMs: 0,
      shardsQueried: [],
      failedSeriesCount: 0,
      truncated: false,
      truncationReason: "",
    },
    errorCode: proto.errorCode || undefined,
    errorMessage: proto.errorMessage || undefined,
  };
}

function convertStreamingBatch(data: any): StreamingBatch {
  const points: StreamingDataPoint[] = (data.points ?? []).map((p: any) => {
    let value: any = p.value;
    if (typeof value === "object" && value !== null) {
      value = value.doubleValue ?? value.boolValue ?? value.stringValue ?? value.int64Value ?? 0;
    }
    return {
      measurement: p.measurement,
      field: p.field,
      tags: p.tags ?? {},
      timestamp: p.timestamp,
      value,
    };
  });

  return {
    points,
    sequenceId: data.sequence_id ?? data.sequenceId ?? 0,
    label: data.label ?? "",
    isDrop: data.is_drop ?? data.isDrop ?? false,
    droppedCount: data.dropped_count ?? data.droppedCount ?? 0,
  };
}
