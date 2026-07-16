import * as http from "http";
import {
  codecs,
  init as protoInit,
  ProtoAnomalyResponse,
  ProtoDerivedQueryResponse,
  ProtoFieldData,
  ProtoForecastResponse,
  ProtoQueryResponse,
  ProtoWriteField,
  ProtoWritePoint,
} from "./proto";
import {
  compressTimestamps,
  decompressTimestamps,
  decompressTimestampsBigInt,
  compressDoubles,
  decompressDoubles,
  compressIntegers,
  decompressIntegers,
  decompressIntegersBigInt,
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
  AnomalySeriesPiece,
  ForecastResponse,
  ForecastSeriesPiece,
  HealthResponse,
} from "./types";

export class TimestarClient {
  private readonly baseUrl: string;
  private readonly authToken?: string;
  private readonly requestTimeoutMs: number;
  private readonly precise: boolean;
  private initPromise: Promise<void> | null = null;

  constructor(options: TimestarClientOptions = {}) {
    const host = options.host ?? "localhost";
    const port = options.port ?? 8086;
    this.baseUrl = `http://${host}:${port}`;
    this.authToken = options.authToken;
    this.requestTimeoutMs = options.requestTimeoutMs ?? 30_000;
    this.precise = options.precise ?? false;
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

  // Throw a TimestarError extracted from an error response body.
  //
  // Server >= 1.0.7 tags protobuf responses with Content-Type
  // "application/x-protobuf" (older servers used "application/json" even for
  // protobuf bodies), and all JSON errors are the flat shape
  //   {"status":"error","error_code":"<CODE>","message":"<msg>","error":"<msg>"}
  // (error_code omitted when uncoded). Older servers used a NESTED shape
  // {"error":{"code","message"}} on metadata/cardinality/derived and a bare
  // {"message"} on the write handler — all are still tolerated here.
  //
  // `resCodecKey` selects the protobuf message the server encodes errors in
  // for this endpoint: /write errors arrive as WriteResponse (errors[]),
  // /query as QueryResponse (error_code/error_message), /derived as
  // DerivedQueryResponse; everything else uses StatusResponse.
  private async throwServerError(
    res: { status: number; headers: Record<string, string>; body: Buffer },
    resCodecKey?: keyof typeof codecs,
  ): Promise<never> {
    const contentType = (res.headers["content-type"] ?? "").toLowerCase();
    const bodyText = res.body.toString("utf-8");

    // JSON error body (flat >= 1.0.7, nested/bare-message for older servers)
    if (contentType.includes("json") || bodyText.trimStart().startsWith("{")) {
      try {
        const j = JSON.parse(bodyText);
        let message: string | undefined;
        let code: string | undefined = typeof j.error_code === "string" ? j.error_code : undefined;
        if (typeof j.error === "string") {
          message = j.error;
        } else if (j.error && typeof j.error === "object") {
          // Legacy nested {"error":{"code","message"}}
          message = typeof j.error.message === "string" ? j.error.message : undefined;
          if (!code && typeof j.error.code === "string") code = j.error.code;
        }
        if (!message && typeof j.message === "string") message = j.message;
        throw new TimestarError(message ?? bodyText, res.status, code);
      } catch (e) {
        if (e instanceof TimestarError) throw e;
        throw new TimestarError(bodyText, res.status);
      }
    }

    // Protobuf error body — decode with the endpoint's response message first
    try {
      if (resCodecKey === "WriteResponse") {
        const wr = await codecs.WriteResponse.decode(res.body);
        throw new TimestarError(wr.errors?.join("; ") || wr.status || bodyText, res.status);
      }
      if (resCodecKey === "QueryResponse") {
        const qr = await codecs.QueryResponse.decode(res.body);
        throw new TimestarError(qr.errorMessage || qr.status || bodyText, res.status, qr.errorCode || undefined);
      }
      if (resCodecKey === "DerivedQueryResponse") {
        const dr = await codecs.DerivedQueryResponse.decode(res.body);
        throw new TimestarError(dr.errorMessage || dr.status || bodyText, res.status, dr.errorCode || undefined);
      }
      const errRes = await codecs.StatusResponse.decode(res.body);
      throw new TimestarError(errRes.message || errRes.status, res.status, errRes.code || undefined);
    } catch (e) {
      if (e instanceof TimestarError) throw e;
      throw new TimestarError(bodyText, res.status);
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
      await this.throwServerError(res, resCodecKey);
    }
    return resCodec.decode(res.body);
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
      await this.throwServerError(res);
    }
    return resCodec.decode(res.body);
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
    const protoPoints = arr.flatMap(chunkWritePoint).map(normalizeWritePoint);

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
    if (options.startTime !== undefined) payload.startTime = timeToWire(options.startTime);
    if (options.endTime !== undefined) payload.endTime = timeToWire(options.endTime);
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
      await this.throwServerError(res, "QueryResponse");
    }
    const proto = await codecs.QueryResponse.decode(res.body);
    return convertQueryResponse(proto, options.precise ?? this.precise);
  }

  // ---------------------------------------------------------------------------
  // Delete
  // ---------------------------------------------------------------------------

  // Always sends a BatchDeleteRequest, even for a single item. The server
  // parses /delete protobuf bodies by trying BatchDeleteRequest FIRST and a
  // bare DeleteRequest only when batch parsing throws. A bare STRUCTURED
  // DeleteRequest (measurement=2/tags=3/fields=5, no series=1) happens to be
  // a valid-but-empty BatchDeleteRequest (all its fields are skipped as
  // unknown), so the server silently executed ZERO deletes for the old
  // single-request encoding. Wrapping in a batch of one is unambiguous and
  // works for every request shape.
  async delete(req: DeleteRequestItem | DeleteRequestItem[]): Promise<DeleteResponse> {
    const items = Array.isArray(req) ? req : [req];
    const payload = { deletes: items.map(normalizeDeleteRequest) };
    return this.protoPost<any, DeleteResponse>(
      "/delete",
      "BatchDeleteRequest",
      "DeleteResponse",
      payload,
    );
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
      await this.throwServerError(res);
    }
  }

  async getRetention(measurement: string): Promise<RetentionGetResponse> {
    return this.protoGet<RetentionGetResponse>(
      "/retention",
      "RetentionGetResponse",
      { measurement },
    );
  }

  // Note: server >= 1.0.7 returns a valid JSON body with a proper "message"
  // for DELETE /retention (earlier builds emitted corrupted bytes). The body
  // is intentionally not parsed on success, so no workaround was ever needed.
  async deleteRetention(measurement: string): Promise<void> {
    { const p = this.ensureInit(); if (p) await p; }
    const url = new URL("/retention", this.baseUrl);
    url.searchParams.set("measurement", measurement);

    const res = await this.request("DELETE", url.pathname + url.search);
    if (res.status >= 400) {
      await this.throwServerError(res);
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
      startTime: timeToWire(options.startTime),
      endTime: timeToWire(options.endTime),
    };
    if (options.aggregationInterval) {
      payload.aggregationInterval = options.aggregationInterval;
    }

    const proto = await this.protoPost<any, ProtoDerivedQueryResponse>(
      "/derived",
      "DerivedQueryRequest",
      "DerivedQueryResponse",
      payload,
    );
    return convertDerivedQueryResponse(proto);
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
      startTime: timeToWire(options.startTime),
      endTime: timeToWire(options.endTime),
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
      await this.throwServerError(res, "DerivedQueryResponse");
    }
    const proto = await codecs.AnomalyResponse.decode(res.body);
    return convertAnomalyResponse(proto);
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
      startTime: timeToWire(options.startTime),
      endTime: timeToWire(options.endTime),
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
      await this.throwServerError(res, "DerivedQueryResponse");
    }
    const proto = await codecs.ForecastResponse.decode(res.body);
    return convertForecastResponse(proto);
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

// Wire strategy (server >= 1.0.7): compressed payloads are sent INSTEAD of the
// raw repeated arrays. The server prefers the compressed_* bytes when present
// and decodes them natively (FFOR timestamps, ALP doubles, zigzag+FFOR int64,
// RLE bools, zstd strings). Servers older than 1.0.7 IGNORED the compressed
// fields entirely, so compressed-only writes would be silently dropped there —
// this client therefore requires server >= 1.0.7.
//
// Compression is used when the value array length matches the timestamp count
// (>= 2 values). Mismatched lengths are sent raw so the server's validation
// and single-timestamp replication semantics stay unchanged.
const shouldCompress = (len: number, tsCount: number): boolean => len >= 2 && len === tsCount;

// [C1] Convert a bigint to a proto-safe int64/uint64 wire value without
// precision loss. protobufjs does NOT accept bare bigint (it silently encodes
// 0), but decimal strings are converted via Long with full 64-bit precision.
// Numbers are cheaper to encode, so they're used when exactly representable.
function bigintToWire(v: bigint): number | string {
  return v >= -9007199254740991n && v <= 9007199254740991n ? Number(v) : v.toString();
}

// int64/uint64 wire value for a number-or-bigint input (raw proto paths).
function timeToWire(v: number | bigint): number | string {
  return typeof v === "bigint" ? bigintToWire(v) : v;
}

// Detect field type from a plain array by inspecting the first element.
// `tsCount` is the number of timestamps on the enclosing write point.
function normalizeFieldValue(val: unknown, tsCount: number): ProtoWriteField | null {
  // Single scalars — skip compression
  if (typeof val === "number") return { doubleValues: { values: [val] } };
  if (typeof val === "boolean") return { boolValues: { values: [val] } };
  if (typeof val === "string") return { stringValues: { values: [val] } };
  if (typeof val === "bigint") return { int64Values: { values: [bigintToWire(val)] } };

  // Plain array — auto-detect type from first element
  if (Array.isArray(val) && val.length > 0) {
    const first = val[0];
    if (typeof first === "number") {
      const nums = val as number[];
      return shouldCompress(nums.length, tsCount)
        ? { doubleValues: { compressedAlp: compressDoubles(nums) } }
        : { doubleValues: { values: nums } };
    }
    if (typeof first === "boolean") {
      const bools = val as boolean[];
      return shouldCompress(bools.length, tsCount)
        ? { boolValues: { compressedRle: compressBooleans(bools) } }
        : { boolValues: { values: bools } };
    }
    if (typeof first === "string") {
      const strs = val as string[];
      return shouldCompress(strs.length, tsCount)
        ? { stringValues: { compressedZstd: compressStrings(strs), count: strs.length } }
        : { stringValues: { values: strs } };
    }
    if (typeof first === "bigint") {
      // [C1] Carry bigints through at full 64-bit precision — no Number().
      const ints = val as bigint[];
      return shouldCompress(ints.length, tsCount)
        ? { int64Values: { compressedFfor: compressIntegers(ints) } }
        : { int64Values: { values: ints.map(bigintToWire) } };
    }
  }

  // Empty array — return empty doubles by default
  if (Array.isArray(val) && val.length === 0) {
    return { doubleValues: { values: [] } };
  }

  // Explicit WriteField — pass through with compression
  const wf = val as WriteField;
  if (wf.doubleValues) {
    return shouldCompress(wf.doubleValues.length, tsCount)
      ? { doubleValues: { compressedAlp: compressDoubles(wf.doubleValues) } }
      : { doubleValues: { values: wf.doubleValues } };
  }
  if (wf.boolValues) {
    return shouldCompress(wf.boolValues.length, tsCount)
      ? { boolValues: { compressedRle: compressBooleans(wf.boolValues) } }
      : { boolValues: { values: wf.boolValues } };
  }
  if (wf.stringValues) {
    return shouldCompress(wf.stringValues.length, tsCount)
      ? { stringValues: { compressedZstd: compressStrings(wf.stringValues), count: wf.stringValues.length } }
      : { stringValues: { values: wf.stringValues } };
  }
  if (wf.int64Values) {
    // [C1] Carry bigints through at full 64-bit precision — no Number().
    return shouldCompress(wf.int64Values.length, tsCount)
      ? { int64Values: { compressedFfor: compressIntegers(wf.int64Values) } }
      : { int64Values: { values: wf.int64Values.map(timeToWire) } };
  }

  return null;
}

// [S1] Maximum timestamps per point on the COMPRESSED-timestamp wire path.
//
// SERVER BUG WORKAROUND: the server decodes compressed_timestamps with an
// upper bound of `compressedBytes / 2 + 1024` values (proto_converters.cpp,
// "compressed data can't encode more values than bytes/2"). That heuristic is
// wrong for FFOR delta-of-delta: regular timestamps compress far below 2
// bytes/value (1000 x 1s-spaced timestamps -> 40 bytes), so any point with
// more than ~1024 well-compressible timestamps was silently TRUNCATED to
// (bytes/2 + 1024) points — reported as full success (e.g. 300k points ->
// 3380 stored). Keeping every compressed point at <= 1024 timestamps makes
// the server's cap always >= the true count. Large writes are split into
// consecutive chunks of the same measurement+tags, which the server treats
// identically to one large point (append semantics).
const MAX_COMPRESSED_TS_PER_POINT = 1024;

// Split a WritePoint with more than MAX_COMPRESSED_TS_PER_POINT timestamps
// into consecutive chunks, slicing every column-aligned field. Points whose
// fields are not column-aligned (scalars or length-mismatched arrays) are
// returned unchanged — normalizeWritePoint sends RAW timestamps for those
// instead (see [S1] above), preserving the server's mismatch validation and
// replication semantics.
function chunkWritePoint(point: WritePoint): WritePoint[] {
  const tsCount = point.timestamps.length;
  if (tsCount <= MAX_COMPRESSED_TS_PER_POINT) return [point];

  const sliceField = (val: WritePoint["fields"][string], lo: number, hi: number) => {
    if (Array.isArray(val)) return val.slice(lo, hi) as typeof val;
    const wf = val as WriteField;
    const out: WriteField = {};
    if (wf.doubleValues) out.doubleValues = wf.doubleValues.slice(lo, hi);
    if (wf.boolValues) out.boolValues = wf.boolValues.slice(lo, hi);
    if (wf.stringValues) out.stringValues = wf.stringValues.slice(lo, hi);
    if (wf.int64Values) out.int64Values = wf.int64Values.slice(lo, hi);
    return out;
  };

  // Only split when every field is a column of exactly tsCount values.
  for (const val of Object.values(point.fields)) {
    let len: number | undefined;
    if (Array.isArray(val)) {
      len = val.length;
    } else if (typeof val === "object" && val !== null) {
      const wf = val as WriteField;
      const arr = wf.doubleValues ?? wf.boolValues ?? wf.stringValues ?? wf.int64Values;
      len = arr?.length;
    }
    if (len !== tsCount) return [point];
  }

  const chunks: WritePoint[] = [];
  for (let lo = 0; lo < tsCount; lo += MAX_COMPRESSED_TS_PER_POINT) {
    const hi = Math.min(lo + MAX_COMPRESSED_TS_PER_POINT, tsCount);
    const fields: WritePoint["fields"] = {};
    for (const [k, v] of Object.entries(point.fields)) {
      fields[k] = sliceField(v, lo, hi);
    }
    chunks.push({
      measurement: point.measurement,
      tags: point.tags,
      fields,
      timestamps: point.timestamps.slice(lo, hi),
    });
  }
  return chunks;
}

function normalizeWritePoint(point: WritePoint): ProtoWritePoint {
  const tsCount = point.timestamps.length;
  const protoFields: Record<string, ProtoWriteField> = {};

  for (const [key, val] of Object.entries(point.fields)) {
    const field = normalizeFieldValue(val, tsCount);
    if (field) protoFields[key] = field;
  }

  const base: ProtoWritePoint = {
    measurement: point.measurement,
    tags: point.tags ?? {},
    fields: protoFields,
  };

  // Compressed timestamps replace the raw repeated field (server >= 1.0.7
  // prefers the compressed bytes; a single timestamp is smaller raw).
  // [S1] Points that could not be chunked to <= 1024 timestamps fall back to
  // raw timestamps so the server's bytes/2+1024 decode cap cannot truncate.
  if (tsCount >= 2 && tsCount <= MAX_COMPRESSED_TS_PER_POINT) {
    base.compressedTimestamps = compressTimestamps(point.timestamps);
  } else {
    // [C2] Preserve bigint timestamps exactly on the raw path (no Number()).
    base.timestamps = point.timestamps.map(timeToWire);
  }
  return base;
}

function normalizeDeleteRequest(item: DeleteRequestItem): any {
  const result: any = {};
  if (item.series) result.series = item.series;
  if (item.measurement) result.measurement = item.measurement;
  if (item.tags) result.tags = item.tags;
  if (item.field) result.field = item.field;
  if (item.fields) result.fields = item.fields;
  if (item.startTime !== undefined) result.startTime = timeToWire(item.startTime);
  if (item.endTime !== undefined) result.endTime = timeToWire(item.endTime);
  return result;
}

function normalizeSubscribeRequest(req: SubscribeRequest): any {
  const result: any = {};
  if (req.query) result.query = req.query;
  if (req.queries) result.queries = req.queries;
  if (req.formula) result.formula = req.formula;
  if (req.startTime !== undefined) result.startTime = timeToWire(req.startTime);
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
//
// [C2] `precise: true` returns bigint[] for timestamps and int64 field values
// (full 64-bit precision). The default returns number[], which silently
// rounds values beyond 2^53 — including realistic nanosecond timestamps.
// Raw (uncompressed) fallback arrays were already decoded by protobufjs with
// longs: Number, so in precise mode they are converted to bigint for a
// consistent return type, but precision beyond 2^53 cannot be recovered on
// that path (only older servers send raw arrays).
function decompressFieldData(protoFd: ProtoFieldData, precise: boolean): FieldData {
  // Step 1: Try self-describing value formats first
  let values: number[] | bigint[] | boolean[] | string[];
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
  let timestamps: number[] | bigint[];
  if (protoFd.compressedTimestamps && protoFd.compressedTimestamps.length > 0) {
    const count = valueCount > 0 ? valueCount : (protoFd.timestamps?.length ?? 0);
    timestamps = precise
      ? decompressTimestampsBigInt(toBuffer(protoFd.compressedTimestamps), count)
      : decompressTimestamps(toBuffer(protoFd.compressedTimestamps), count);
  } else {
    timestamps = precise
      ? (protoFd.timestamps ?? []).map(BigInt)
      : protoFd.timestamps ?? [];
  }

  // Step 3: Decompress int64/bool values using timestamps.length as count
  if (values.length === 0) {
    if (protoFd.int64Values?.compressedFfor && protoFd.int64Values.compressedFfor.length > 0) {
      values = precise
        ? decompressIntegersBigInt(toBuffer(protoFd.int64Values.compressedFfor), timestamps.length)
        : decompressIntegers(toBuffer(protoFd.int64Values.compressedFfor), timestamps.length);
    } else if (protoFd.boolValues?.compressedRle && protoFd.boolValues.compressedRle.length > 0) {
      values = decompressBooleans(toBuffer(protoFd.boolValues.compressedRle), timestamps.length);
    } else if (protoFd.int64Values?.values) {
      values = precise ? protoFd.int64Values.values.map(BigInt) : protoFd.int64Values.values;
    } else if (protoFd.boolValues?.values) {
      values = protoFd.boolValues.values;
    }
  }

  return { timestamps, values };
}

function convertQueryResponse(proto: ProtoQueryResponse, precise: boolean): QueryResponse {
  const series: SeriesResult[] = (proto.series ?? []).map((s) => {
    const fields: Record<string, FieldData> = {};
    for (const [fieldName, fd] of Object.entries(s.fields ?? {})) {
      fields[fieldName] = decompressFieldData(fd, precise);
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

// Decompress a FFOR-compressed times array (count derived from block headers).
function decompressTimesField(compressed: Uint8Array | undefined, raw: number[] | undefined): number[] {
  if (compressed && compressed.length > 0) {
    const count = readFforTotalCount(compressed);
    return decompressTimestamps(toBuffer(compressed), count);
  }
  return raw ?? [];
}

// Decompress an ALP-compressed values array (self-describing).
function decompressValuesField(compressed: Uint8Array | undefined, raw: number[] | undefined): number[] {
  if (compressed && compressed.length > 0) {
    return decompressDoubles(toBuffer(compressed));
  }
  return raw ?? [];
}

// The server compresses /derived response arrays for protobuf clients
// (FFOR timestamps, ALP values) — decompress to plain arrays for users.
function convertDerivedQueryResponse(proto: ProtoDerivedQueryResponse): DerivedQueryResponse {
  return {
    status: proto.status,
    timestamps: decompressTimesField(proto.compressedTimestamps, proto.timestamps),
    values: decompressValuesField(proto.compressedValues, proto.values),
    formula: proto.formula,
    statistics: proto.statistics ?? {
      pointCount: 0,
      executionTimeMs: 0,
      subQueriesExecuted: 0,
      pointsDroppedDueToAlignment: 0,
    },
    errorCode: proto.errorCode || undefined,
    errorMessage: proto.errorMessage || undefined,
  };
}

function convertAnomalyResponse(proto: ProtoAnomalyResponse): AnomalyResponse {
  return {
    status: proto.status,
    times: decompressTimesField(proto.compressedTimes, proto.times),
    series: (proto.series ?? []).map((p) => ({
      piece: p.piece as AnomalySeriesPiece["piece"],
      groupTags: p.groupTags ?? [],
      values: decompressValuesField(p.compressedValues, p.values),
      alertValue: p.hasAlert ? p.alertValue : undefined,
      hasAlert: p.hasAlert,
    })),
    statistics: proto.statistics,
    errorMessage: proto.errorMessage || undefined,
  };
}

function convertForecastResponse(proto: ProtoForecastResponse): ForecastResponse {
  return {
    status: proto.status,
    times: decompressTimesField(proto.compressedTimes, proto.times),
    forecastStartIndex: proto.forecastStartIndex,
    series: (proto.series ?? []).map((p) => ({
      piece: p.piece as ForecastSeriesPiece["piece"],
      groupTags: p.groupTags ?? [],
      values: decompressValuesField(p.compressedValues, p.values),
    })),
    statistics: proto.statistics,
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
