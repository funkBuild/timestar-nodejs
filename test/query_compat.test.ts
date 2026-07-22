// bucketAlignment / booleansAsNumeric — the rollup.js compat options.
//
// This client DEFAULTS to bucketAlignment="start" (it replaces a rollup.js
// reader); the server's canonical default is "epoch". These tests pin the
// wire payload the client actually sends (fetch stubbed, body decoded with
// the real codec) so the default cannot silently regress in either direction.

import { describe, it, expect, vi, afterEach } from "vitest";
import { TimestarClient } from "../src/client";
import { codecs } from "../src/proto";

// Minimal valid protobuf QueryResponse ("status" field only is fine — the
// decoder tolerates an empty series list).
async function emptyQueryResponse(): Promise<Uint8Array> {
  return codecs.QueryResponse.encode({
    status: "success",
    series: [],
    statistics: {
      seriesCount: 0,
      pointCount: 0,
      failedSeriesCount: 0,
      executionTimeMs: 0,
      shardsQueried: [],
      truncated: false,
      truncationReason: "",
    },
    errorCode: "",
    errorMessage: "",
  } as any);
}

async function captureQueryPayload(
  clientOpts: ConstructorParameters<typeof TimestarClient>[0],
  queryOpts: Parameters<TimestarClient["query"]>[1],
): Promise<any> {
  let captured: Uint8Array | null = null;
  const responseBody = await emptyQueryResponse();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (_url: any, init: any) => {
      captured = new Uint8Array(init.body);
      return new Response(responseBody, {
        status: 200,
        headers: { "Content-Type": "application/protobuf" },
      });
    }),
  );
  const client = new TimestarClient(clientOpts);
  await client.query("avg:m(v){}", queryOpts);
  expect(captured).not.toBeNull();
  return codecs.QueryRequest.decode(captured!);
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("query compat options (wire payload)", () => {
  it("defaults to start-aligned buckets and non-numeric booleans", async () => {
    const req = await captureQueryPayload({}, { startTime: 1, endTime: 2, aggregationInterval: "10s" });
    expect(req.bucketAlignment).toBe("start");
    // proto3 default: absent/false — booleans stay non-numeric unless asked.
    expect(req.booleansAsNumeric ?? false).toBe(false);
  });

  it("per-query epoch opt-out restores the server's canonical grid", async () => {
    const req = await captureQueryPayload(
      {},
      { startTime: 1, endTime: 2, aggregationInterval: "10s", bucketAlignment: "epoch" },
    );
    expect(req.bucketAlignment).toBe("epoch");
  });

  it("client-level defaults apply and per-query options override them", async () => {
    const epochDefault = await captureQueryPayload(
      { bucketAlignment: "epoch", booleansAsNumeric: true },
      { startTime: 1, endTime: 2, aggregationInterval: "10s" },
    );
    expect(epochDefault.bucketAlignment).toBe("epoch");
    expect(epochDefault.booleansAsNumeric).toBe(true);

    const overridden = await captureQueryPayload(
      { bucketAlignment: "epoch" },
      { startTime: 1, endTime: 2, aggregationInterval: "10s", bucketAlignment: "start", booleansAsNumeric: true },
    );
    expect(overridden.bucketAlignment).toBe("start");
    expect(overridden.booleansAsNumeric).toBe(true);
  });
});
