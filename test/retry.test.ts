import { describe, it, expect, beforeAll, afterEach } from "vitest";
import * as http from "http";
import { AddressInfo } from "net";
import { TimestarClient, TimestarError } from "../src/client";
import { codecs, init as protoInit } from "../src/proto";

// Mock server exercising the 503 + Retry-After congestion path. Each test
// configures a queue of canned responses; once the queue is empty the server
// answers 200 with a protobuf WriteResponse.

interface CannedResponse {
  status: number;
  headers?: Record<string, string>;
  body?: string;
}

let server: http.Server;
let port: number;
let queue: CannedResponse[] = [];
let requestTimes: number[] = [];
let successBody: Buffer;

beforeAll(async () => {
  await protoInit();
  successBody = Buffer.from(
    await codecs.WriteResponse.encode({ status: "success", pointsWritten: 1, failedWrites: 0, errors: [] }),
  );

  server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      requestTimes.push(Date.now());
      const canned = queue.shift();
      if (canned) {
        res.writeHead(canned.status, { "Content-Type": "application/json", ...canned.headers });
        res.end(canned.body ?? JSON.stringify({ status: "error", error: "server congested" }));
      } else {
        res.writeHead(200, { "Content-Type": "application/x-protobuf" });
        res.end(successBody);
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  port = (server.address() as AddressInfo).port;

  return async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  };
});

afterEach(() => {
  queue = [];
  requestTimes = [];
});

const makeClient = (maxRetryDelayMs?: number) =>
  new TimestarClient({ host: "127.0.0.1", port, maxRetryDelayMs });

const POINT = { measurement: "retry_test", fields: { v: 1 }, timestamps: [1n] };

describe("write retry on 503 congestion", () => {
  it("waits the Retry-After delta-seconds then retries and succeeds", async () => {
    queue = [{ status: 503, headers: { "Retry-After": "1" } }];
    const resp = await makeClient().write(POINT);
    expect(resp.status).toBe("success");
    expect(requestTimes.length).toBe(2);
    // Allow modest scheduling slop below the requested 1s.
    expect(requestTimes[1] - requestTimes[0]).toBeGreaterThanOrEqual(950);
  });

  it("honors an HTTP-date Retry-After", async () => {
    const retryAt = new Date(Date.now() + 1000).toUTCString();
    queue = [{ status: 503, headers: { "Retry-After": retryAt } }];
    const resp = await makeClient().write(POINT);
    expect(resp.status).toBe("success");
    expect(requestTimes.length).toBe(2);
    // HTTP-dates have 1s resolution, so the parsed delay may round down.
    expect(requestTimes[1] - requestTimes[0]).toBeGreaterThanOrEqual(400);
  });

  it("falls back to exponential backoff when Retry-After is missing", async () => {
    queue = [{ status: 503 }, { status: 503 }];
    const resp = await makeClient().write(POINT);
    expect(resp.status).toBe("success");
    expect(requestTimes.length).toBe(3);
    expect(requestTimes[1] - requestTimes[0]).toBeGreaterThanOrEqual(450); // ~500ms
    expect(requestTimes[2] - requestTimes[1]).toBeGreaterThanOrEqual(950); // ~1000ms
  });

  it("throws the 503 immediately when Retry-After exceeds the budget", async () => {
    queue = [{ status: 503, headers: { "Retry-After": "60" } }];
    const start = Date.now();
    const err = await makeClient(2000).write(POINT).catch((e) => e);
    expect(err).toBeInstanceOf(TimestarError);
    expect(err.statusCode).toBe(503);
    expect(err.message).toContain("congested");
    expect(requestTimes.length).toBe(1);
    expect(Date.now() - start).toBeLessThan(1000);
  });

  it("gives up once accumulated waits would exceed the budget", async () => {
    queue = [
      { status: 503, headers: { "Retry-After": "1" } },
      { status: 503, headers: { "Retry-After": "1" } },
      { status: 503, headers: { "Retry-After": "1" } },
    ];
    const err = await makeClient(1500).write(POINT).catch((e) => e);
    expect(err).toBeInstanceOf(TimestarError);
    expect(err.statusCode).toBe(503);
    // 1s waited, second 1s wait would exceed the 1.5s budget -> 2 requests.
    expect(requestTimes.length).toBe(2);
  });

  it("does not retry when maxRetryDelayMs is 0", async () => {
    queue = [{ status: 503, headers: { "Retry-After": "1" } }];
    const err = await makeClient(0).write(POINT).catch((e) => e);
    expect(err).toBeInstanceOf(TimestarError);
    expect(err.statusCode).toBe(503);
    expect(requestTimes.length).toBe(1);
  });

  it("does not retry non-503 errors", async () => {
    queue = [{ status: 500, body: JSON.stringify({ status: "error", error: "boom" }) }];
    const err = await makeClient().write(POINT).catch((e) => e);
    expect(err).toBeInstanceOf(TimestarError);
    expect(err.statusCode).toBe(500);
    expect(requestTimes.length).toBe(1);
  });
});
