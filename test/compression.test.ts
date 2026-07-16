import { describe, it, expect } from "vitest";
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
} from "../src/compression";

// ============================================================================
// FFOR Timestamp Compression
// ============================================================================

describe("FFOR Timestamps", () => {
  it("roundtrips constant-interval timestamps", () => {
    const now = Date.now() * 1e6;
    const ts = Array.from({ length: 1000 }, (_, i) => now + i * 1e9);
    const compressed = compressTimestamps(ts);
    const decompressed = decompressTimestamps(compressed, 1000);

    expect(decompressed).toHaveLength(1000);
    for (let i = 0; i < 1000; i++) {
      expect(Math.abs(ts[i] - decompressed[i])).toBeLessThan(2);
    }
    // Constant interval should compress very well
    expect(compressed.length).toBeLessThan(ts.length * 8 / 10);
  });

  it("roundtrips jittered timestamps", () => {
    const now = Date.now() * 1e6;
    const ts = Array.from({ length: 500 }, (_, i) =>
      now + i * 1e9 + Math.floor(Math.random() * 1000)
    );
    const compressed = compressTimestamps(ts);
    const decompressed = decompressTimestamps(compressed, 500);

    expect(decompressed).toHaveLength(500);
    for (let i = 0; i < 500; i++) {
      expect(Math.abs(ts[i] - decompressed[i])).toBeLessThan(2);
    }
  });

  it("handles single value", () => {
    const ts = [1000000000];
    const compressed = compressTimestamps(ts);
    const decompressed = decompressTimestamps(compressed, 1);
    expect(decompressed).toHaveLength(1);
    expect(Math.abs(ts[0] - decompressed[0])).toBeLessThan(2);
  });

  it("handles empty array", () => {
    const compressed = compressTimestamps([]);
    expect(compressed.length).toBe(0);
    const decompressed = decompressTimestamps(compressed, 0);
    expect(decompressed).toHaveLength(0);
  });

  it("handles multi-block (>1024 values)", () => {
    const now = Date.now() * 1e6;
    const ts = Array.from({ length: 2000 }, (_, i) => now + i * 1e9);
    const compressed = compressTimestamps(ts);
    const decompressed = decompressTimestamps(compressed, 2000);
    expect(decompressed).toHaveLength(2000);
    for (let i = 0; i < 2000; i++) {
      expect(Math.abs(ts[i] - decompressed[i])).toBeLessThan(2);
    }
  });
});

// ============================================================================
// FFOR Integer Compression
// ============================================================================

describe("FFOR Integers", () => {
  it("roundtrips small positive/negative integers", () => {
    const vals = Array.from({ length: 100 }, (_, i) => i - 50);
    const compressed = compressIntegers(vals);
    const decompressed = decompressIntegers(compressed, 100);
    expect(decompressed).toEqual(vals);
  });

  it("roundtrips large range integers", () => {
    const vals = Array.from({ length: 100 }, (_, i) =>
      Math.floor(Math.random() * 1000000) - 500000
    );
    const compressed = compressIntegers(vals);
    const decompressed = decompressIntegers(compressed, 100);
    expect(decompressed).toEqual(vals);
  });

  it("roundtrips all-identical values", () => {
    const vals = Array.from({ length: 500 }, () => 42);
    const compressed = compressIntegers(vals);
    const decompressed = decompressIntegers(compressed, 500);
    expect(decompressed).toEqual(vals);
  });

  it("achieves compression on typical data", () => {
    const vals = Array.from({ length: 1000 }, (_, i) =>
      Math.floor(Math.random() * 1000) - 500
    );
    const compressed = compressIntegers(vals);
    expect(compressed.length).toBeLessThan(vals.length * 8);
  });
});

// ============================================================================
// ALP Float Compression
// ============================================================================

describe("ALP Doubles", () => {
  it("roundtrips sensor-like decimal data losslessly", () => {
    const vals = Array.from({ length: 1000 }, (_, i) => 22.5 + (i % 10) * 0.1);
    const compressed = compressDoubles(vals);
    const decompressed = decompressDoubles(compressed);

    expect(decompressed).toHaveLength(1000);
    for (let i = 0; i < 1000; i++) {
      expect(decompressed[i]).toBe(vals[i]);
    }
    // Decimal sensor data should compress well with ALP
    expect(compressed.length).toBeLessThan(vals.length * 8 / 2);
  });

  it("roundtrips random doubles", () => {
    const vals = Array.from({ length: 100 }, () => Math.random() * 1000);
    const compressed = compressDoubles(vals);
    const decompressed = decompressDoubles(compressed);

    expect(decompressed).toHaveLength(100);
    for (let i = 0; i < 100; i++) {
      // ALP_RD fallback may be used but should still be lossless
      expect(decompressed[i]).toBe(vals[i]);
    }
  });

  it("handles special values: NaN, Infinity, -Infinity", () => {
    const vals = [NaN, Infinity, -Infinity, 0, -0, 42.5, 1e-10, 1e15];
    const compressed = compressDoubles(vals);
    const decompressed = decompressDoubles(compressed);

    expect(decompressed).toHaveLength(vals.length);
    expect(isNaN(decompressed[0])).toBe(true);
    expect(decompressed[1]).toBe(Infinity);
    expect(decompressed[2]).toBe(-Infinity);
    expect(decompressed[3]).toBe(0);
    // -0 check: 1/-0 === -Infinity
    expect(1 / decompressed[4]).toBe(-Infinity);
    expect(decompressed[5]).toBe(42.5);
  });

  it("handles empty array", () => {
    const compressed = compressDoubles([]);
    expect(compressed.length).toBe(0);
    const decompressed = decompressDoubles(compressed);
    expect(decompressed).toHaveLength(0);
  });

  it("handles single value", () => {
    const vals = [3.14];
    const compressed = compressDoubles(vals);
    const decompressed = decompressDoubles(compressed);
    expect(decompressed).toEqual(vals);
  });
});

// ============================================================================
// Boolean RLE Compression
// ============================================================================

describe("Boolean RLE", () => {
  it("roundtrips long runs", () => {
    const vals = [
      ...Array(500).fill(true),
      ...Array(300).fill(false),
      ...Array(200).fill(true),
    ];
    const compressed = compressBooleans(vals);
    const decompressed = decompressBooleans(compressed, 1000);

    expect(decompressed).toEqual(vals);
    // Long runs should compress extremely well
    expect(compressed.length).toBeLessThan(20);
  });

  it("roundtrips alternating values", () => {
    const vals = Array.from({ length: 100 }, (_, i) => i % 2 === 0);
    const compressed = compressBooleans(vals);
    const decompressed = decompressBooleans(compressed, 100);
    expect(decompressed).toEqual(vals);
  });

  it("roundtrips all-true", () => {
    const vals = Array(1000).fill(true);
    const compressed = compressBooleans(vals);
    const decompressed = decompressBooleans(compressed, 1000);
    expect(decompressed).toEqual(vals);
    expect(compressed.length).toBeLessThan(10);
  });

  it("roundtrips all-false", () => {
    const vals = Array(1000).fill(false);
    const compressed = compressBooleans(vals);
    const decompressed = decompressBooleans(compressed, 1000);
    expect(decompressed).toEqual(vals);
  });

  it("handles single value", () => {
    expect(decompressBooleans(compressBooleans([true]), 1)).toEqual([true]);
    expect(decompressBooleans(compressBooleans([false]), 1)).toEqual([false]);
  });

  it("handles empty array", () => {
    const compressed = compressBooleans([]);
    expect(compressed.length).toBe(0);
    const decompressed = decompressBooleans(compressed, 0);
    expect(decompressed).toHaveLength(0);
  });
});

// ============================================================================
// String zstd Compression
// ============================================================================

describe("String zstd", () => {
  it("roundtrips low-cardinality strings", () => {
    const vals = Array.from({ length: 100 }, (_, i) => "sensor_" + (i % 5));
    const compressed = compressStrings(vals);
    const decompressed = decompressStrings(compressed);
    expect(decompressed).toEqual(vals);
    // Low cardinality should compress well
    const rawSize = vals.reduce((a, s) => a + s.length, 0);
    expect(compressed.length).toBeLessThan(rawSize);
  });

  it("roundtrips high-cardinality strings", () => {
    const vals = Array.from({ length: 100 }, (_, i) => "unique_value_" + i);
    const compressed = compressStrings(vals);
    const decompressed = decompressStrings(compressed);
    expect(decompressed).toEqual(vals);
  });

  it("roundtrips empty strings", () => {
    const vals = ["", "", ""];
    const compressed = compressStrings(vals);
    const decompressed = decompressStrings(compressed);
    expect(decompressed).toEqual(vals);
  });

  it("roundtrips unicode strings", () => {
    const vals = ["こんにちは", "世界", "🌍🌎🌏"];
    const compressed = compressStrings(vals);
    const decompressed = decompressStrings(compressed);
    expect(decompressed).toEqual(vals);
  });

  it("handles empty array", () => {
    const compressed = compressStrings([]);
    expect(compressed.length).toBe(0);
    const decompressed = decompressStrings(compressed);
    expect(decompressed).toHaveLength(0);
  });

  it("handles single string", () => {
    const vals = ["hello world"];
    const compressed = compressStrings(vals);
    const decompressed = decompressStrings(compressed);
    expect(decompressed).toEqual(vals);
  });
});

// ============================================================================
// Compression Ratio Verification
// ============================================================================

describe("Compression Ratios", () => {
  it("FFOR achieves >50x on constant-interval timestamps", () => {
    const now = Date.now() * 1e6;
    const ts = Array.from({ length: 1000 }, (_, i) => now + i * 1e9);
    const compressed = compressTimestamps(ts);
    const ratio = (ts.length * 8) / compressed.length;
    console.log(`  FFOR constant timestamps: ${ts.length * 8} -> ${compressed.length} bytes (${ratio.toFixed(1)}x)`);
    expect(ratio).toBeGreaterThan(50);
  });

  it("ALP achieves >5x on decimal sensor data", () => {
    const vals = Array.from({ length: 1000 }, (_, i) => 22.5 + (i % 10) * 0.1);
    const compressed = compressDoubles(vals);
    const ratio = (vals.length * 8) / compressed.length;
    console.log(`  ALP decimal data: ${vals.length * 8} -> ${compressed.length} bytes (${ratio.toFixed(1)}x)`);
    expect(ratio).toBeGreaterThan(5);
  });

  it("Bool RLE achieves >100x on long runs", () => {
    const vals = [...Array(500).fill(true), ...Array(500).fill(false)];
    const compressed = compressBooleans(vals);
    const ratio = vals.length / compressed.length;
    console.log(`  Bool RLE runs: ${vals.length} -> ${compressed.length} bytes (${ratio.toFixed(1)}x)`);
    expect(ratio).toBeGreaterThan(100);
  });
});

// ============================================================================
// readFforTotalCount verification (indirect via compress/decompress roundtrip)
// ============================================================================

describe("readFforTotalCount verification", () => {
  const now = Date.now() * 1e6;

  it("roundtrips N=1 timestamps", () => {
    const ts = [now];
    const compressed = compressTimestamps(ts);
    const decompressed = decompressTimestamps(compressed, 1);
    expect(decompressed).toHaveLength(1);
    expect(Math.abs(ts[0] - decompressed[0])).toBeLessThan(2);
  });

  it("roundtrips N=500 timestamps", () => {
    const ts = Array.from({ length: 500 }, (_, i) => now + i * 1e9);
    const compressed = compressTimestamps(ts);
    const decompressed = decompressTimestamps(compressed, 500);
    expect(decompressed).toHaveLength(500);
    for (let i = 0; i < 500; i++) {
      expect(Math.abs(ts[i] - decompressed[i])).toBeLessThan(2);
    }
  });

  it("roundtrips N=1024 timestamps (exactly one block)", () => {
    const ts = Array.from({ length: 1024 }, (_, i) => now + i * 1e9);
    const compressed = compressTimestamps(ts);
    const decompressed = decompressTimestamps(compressed, 1024);
    expect(decompressed).toHaveLength(1024);
    for (let i = 0; i < 1024; i++) {
      expect(Math.abs(ts[i] - decompressed[i])).toBeLessThan(2);
    }
  });

  it("roundtrips N=1025 timestamps (one full block + 1-element tail)", () => {
    const ts = Array.from({ length: 1025 }, (_, i) => now + i * 1e9);
    const compressed = compressTimestamps(ts);
    const decompressed = decompressTimestamps(compressed, 1025);
    expect(decompressed).toHaveLength(1025);
    for (let i = 0; i < 1025; i++) {
      expect(Math.abs(ts[i] - decompressed[i])).toBeLessThan(2);
    }
  });

  it("roundtrips N=2048 timestamps (exactly two blocks)", () => {
    const ts = Array.from({ length: 2048 }, (_, i) => now + i * 1e9);
    const compressed = compressTimestamps(ts);
    const decompressed = decompressTimestamps(compressed, 2048);
    expect(decompressed).toHaveLength(2048);
    for (let i = 0; i < 2048; i++) {
      expect(Math.abs(ts[i] - decompressed[i])).toBeLessThan(2);
    }
  });
});

// ============================================================================
// Extreme integer values
// ============================================================================

describe("Extreme integer values", () => {
  it("roundtrips boundary integers including MAX_SAFE_INTEGER and MIN_SAFE_INTEGER", () => {
    const vals = [0, 1, -1, 2147483647, -2147483648, Number.MAX_SAFE_INTEGER, Number.MIN_SAFE_INTEGER];
    const compressed = compressIntegers(vals);
    const decompressed = decompressIntegers(compressed, vals.length);
    expect(decompressed).toEqual(vals);
  });

  it("roundtrips 100 alternating MAX_SAFE_INTEGER and MIN_SAFE_INTEGER", () => {
    const vals = Array.from({ length: 100 }, (_, i) =>
      i % 2 === 0 ? Number.MAX_SAFE_INTEGER : Number.MIN_SAFE_INTEGER
    );
    const compressed = compressIntegers(vals);
    const decompressed = decompressIntegers(compressed, 100);
    expect(decompressed).toEqual(vals);
  });

  it("roundtrips 100 identical -2147483648 values", () => {
    const vals = Array.from({ length: 100 }, () => -2147483648);
    const compressed = compressIntegers(vals);
    const decompressed = decompressIntegers(compressed, 100);
    expect(decompressed).toEqual(vals);
  });
});

// ============================================================================
// Deterministic ALP_RD trigger
// ============================================================================

describe("Deterministic ALP_RD trigger", () => {
  it("roundtrips irrational multiples of PI losslessly via ALP_RD", () => {
    const vals = Array.from({ length: 300 }, (_, i) => Math.PI * (i + 1));
    const compressed = compressDoubles(vals);
    const decompressed = decompressDoubles(compressed);

    expect(decompressed).toHaveLength(300);
    for (let i = 0; i < 300; i++) {
      expect(decompressed[i]).toBe(vals[i]);
    }
  });
});

// ============================================================================
// Block boundary tests (1024 = one full block, 1025 = full block + 1 tail)
// ============================================================================

describe("Block boundary tests", () => {
  const now = Date.now() * 1e6;

  describe("timestamps", () => {
    it("roundtrips exactly 1024 values (one full block)", () => {
      const ts = Array.from({ length: 1024 }, (_, i) => now + i * 1e9);
      const compressed = compressTimestamps(ts);
      const decompressed = decompressTimestamps(compressed, 1024);
      expect(decompressed).toHaveLength(1024);
      for (let i = 0; i < 1024; i++) {
        expect(Math.abs(ts[i] - decompressed[i])).toBeLessThan(2);
      }
    });

    it("roundtrips exactly 1025 values (full block + 1-element tail)", () => {
      const ts = Array.from({ length: 1025 }, (_, i) => now + i * 1e9);
      const compressed = compressTimestamps(ts);
      const decompressed = decompressTimestamps(compressed, 1025);
      expect(decompressed).toHaveLength(1025);
      for (let i = 0; i < 1025; i++) {
        expect(Math.abs(ts[i] - decompressed[i])).toBeLessThan(2);
      }
    });
  });

  describe("integers", () => {
    it("roundtrips exactly 1024 values (one full block)", () => {
      const vals = Array.from({ length: 1024 }, (_, i) => i - 512);
      const compressed = compressIntegers(vals);
      const decompressed = decompressIntegers(compressed, 1024);
      expect(decompressed).toEqual(vals);
    });

    it("roundtrips exactly 1025 values (full block + 1-element tail)", () => {
      const vals = Array.from({ length: 1025 }, (_, i) => i - 512);
      const compressed = compressIntegers(vals);
      const decompressed = decompressIntegers(compressed, 1025);
      expect(decompressed).toEqual(vals);
    });
  });

  describe("doubles", () => {
    it("roundtrips exactly 1024 values (one full block)", () => {
      const vals = Array.from({ length: 1024 }, (_, i) => 22.5 + (i % 10) * 0.1);
      const compressed = compressDoubles(vals);
      const decompressed = decompressDoubles(compressed);
      expect(decompressed).toHaveLength(1024);
      for (let i = 0; i < 1024; i++) {
        expect(decompressed[i]).toBe(vals[i]);
      }
    });

    it("roundtrips exactly 1025 values (full block + 1-element tail)", () => {
      const vals = Array.from({ length: 1025 }, (_, i) => 22.5 + (i % 10) * 0.1);
      const compressed = compressDoubles(vals);
      const decompressed = decompressDoubles(compressed);
      expect(decompressed).toHaveLength(1025);
      for (let i = 0; i < 1025; i++) {
        expect(decompressed[i]).toBe(vals[i]);
      }
    });
  });
});

// ============================================================================
// 64-bit precision (Q2/C1/C2)
//
// The number-returning decoders combine two uint32 reads instead of allocating
// a BigInt per element. These tests pin the required invariant: the result is
// value-identical to Number(bigint) for EVERY 64-bit value, including the
// nearest-even rounding beyond 2^53. The bigint decoders must be exact.
// ============================================================================

describe("64-bit precision", () => {
  // Deterministic 32-bit LCG for reproducible property tests
  function makeLcg(seed: number) {
    let s = seed >>> 0;
    return () => {
      s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
      return s;
    };
  }

  const U64_BOUNDARIES: bigint[] = [
    0n, 1n, 2n, 127n, 128n,
    2n ** 31n - 1n, 2n ** 31n, 2n ** 32n - 1n, 2n ** 32n, 2n ** 32n + 1n,
    2n ** 53n - 1n, 2n ** 53n, 2n ** 53n + 1n, 2n ** 53n + 2n, 2n ** 53n + 3n,
    2n ** 60n + 12345n,
    1_700_000_000_000_000_001n, // realistic ns timestamp, not representable as double
    2n ** 63n - 1n, 2n ** 63n, 2n ** 63n + 1n, 2n ** 64n - 1n,
  ];

  const I64_BOUNDARIES: bigint[] = [
    0n, 1n, -1n, 127n, -128n,
    2n ** 31n - 1n, -(2n ** 31n), 2n ** 32n - 1n, -(2n ** 32n),
    2n ** 53n - 1n, 2n ** 53n + 1n, -(2n ** 53n) - 1n, -(2n ** 53n) - 3n,
    2n ** 60n + 12345n, -(2n ** 60n) - 12345n,
    2n ** 63n - 1n, -(2n ** 63n),
  ];

  it("uint64 decode-as-number is value-identical to Number(bigint) at boundaries", () => {
    const compressed = compressTimestamps(U64_BOUNDARIES);
    const asNumbers = decompressTimestamps(compressed, U64_BOUNDARIES.length);
    for (let i = 0; i < U64_BOUNDARIES.length; i++) {
      expect(asNumbers[i]).toBe(Number(U64_BOUNDARIES[i]));
    }
  });

  it("int64 decode-as-number is value-identical to Number(bigint) at boundaries", () => {
    const compressed = compressIntegers(I64_BOUNDARIES);
    const asNumbers = decompressIntegers(compressed, I64_BOUNDARIES.length);
    for (let i = 0; i < I64_BOUNDARIES.length; i++) {
      expect(asNumbers[i]).toBe(Number(I64_BOUNDARIES[i]));
    }
  });

  it("property: random uint64 values decode-as-number identically to Number(bigint)", () => {
    const rng = makeLcg(0xC0FFEE);
    const vals: bigint[] = Array.from({ length: 4096 }, () => {
      // Vary magnitude: mask hi word by a random bit width to hit all ranges
      const hi = BigInt(rng() >>> (rng() % 33));
      return (hi << 32n) | BigInt(rng());
    });
    const asNumbers = decompressTimestamps(compressTimestamps(vals), vals.length);
    const asBigints = decompressTimestampsBigInt(compressTimestamps(vals), vals.length);
    for (let i = 0; i < vals.length; i++) {
      expect(asNumbers[i]).toBe(Number(vals[i]));
      expect(asBigints[i]).toBe(vals[i]);
    }
  });

  it("property: random int64 values decode-as-number identically to Number(bigint)", () => {
    const rng = makeLcg(0xDECAF);
    const vals: bigint[] = Array.from({ length: 4096 }, () => {
      const hi = BigInt(rng() >>> (rng() % 33));
      const u = (hi << 32n) | BigInt(rng());
      return BigInt.asIntN(64, u);
    });
    const asNumbers = decompressIntegers(compressIntegers(vals), vals.length);
    const asBigints = decompressIntegersBigInt(compressIntegers(vals), vals.length);
    for (let i = 0; i < vals.length; i++) {
      expect(asNumbers[i]).toBe(Number(vals[i]));
      expect(asBigints[i]).toBe(vals[i]);
    }
  });

  it("bigint int64 write values beyond 2^53 round-trip exactly (C1)", () => {
    const vals = [2n ** 60n + 12345n, -(2n ** 60n) - 12345n, 2n ** 63n - 1n, -(2n ** 63n), 42n];
    const decoded = decompressIntegersBigInt(compressIntegers(vals), vals.length);
    expect(decoded).toEqual(vals);
  });

  it("bigint timestamps beyond 2^53 round-trip exactly (C2)", () => {
    const ts = Array.from({ length: 500 }, (_, i) => 1_700_000_000_000_000_001n + BigInt(i) * 60_000_000_001n);
    const decoded = decompressTimestampsBigInt(compressTimestamps(ts), ts.length);
    expect(decoded).toEqual(ts);
  });

  it("number write path encodes identically to bigint write path (W2)", () => {
    // 2^60-magnitude doubles are spaced 256 apart; use representable offsets
    const base = 2 ** 60;
    const nums = [base, base + 256, base + 512, base + 1024];
    const bigs = nums.map(BigInt); // exact: each is representable
    expect(Buffer.compare(compressTimestamps(nums), compressTimestamps(bigs))).toBe(0);

    const inums = [-1, -4294967296, -9007199254740991, 9007199254740991, 0, 12345];
    const ibigs = inums.map(BigInt);
    expect(Buffer.compare(compressIntegers(inums), compressIntegers(ibigs))).toBe(0);
  });

  it("mixed number/bigint timestamp arrays encode consistently", () => {
    const mixed: Array<number | bigint> = [1_600_000_000_000_000, 1_600_000_060_000_000n, 1_600_000_120_000_000];
    const allBig = mixed.map((v) => BigInt(v));
    expect(Buffer.compare(compressTimestamps(mixed), compressTimestamps(allBig))).toBe(0);
  });
});

// ============================================================================
// Single-scalar verification
// ============================================================================

describe("Single-scalar verification", () => {
  it("roundtrips single double", () => {
    const vals = [42.5];
    const compressed = compressDoubles(vals);
    const decompressed = decompressDoubles(compressed);
    expect(decompressed).toEqual(vals);
  });

  it("roundtrips single integer zero", () => {
    const vals = [0];
    const compressed = compressIntegers(vals);
    const decompressed = decompressIntegers(compressed, 1);
    expect(decompressed).toEqual(vals);
  });

  it("roundtrips single negative integer", () => {
    const vals = [-1];
    const compressed = compressIntegers(vals);
    const decompressed = decompressIntegers(compressed, 1);
    expect(decompressed).toEqual(vals);
  });

  it("roundtrips single timestamp", () => {
    const ts = [Date.now() * 1e6];
    const compressed = compressTimestamps(ts);
    const decompressed = decompressTimestamps(compressed, 1);
    expect(decompressed).toHaveLength(1);
    expect(Math.abs(ts[0] - decompressed[0])).toBeLessThan(2);
  });

  it("roundtrips single boolean true", () => {
    const compressed = compressBooleans([true]);
    const decompressed = decompressBooleans(compressed, 1);
    expect(decompressed).toEqual([true]);
  });

  it("roundtrips single boolean false", () => {
    const compressed = compressBooleans([false]);
    const decompressed = decompressBooleans(compressed, 1);
    expect(decompressed).toEqual([false]);
  });

  it("roundtrips single string", () => {
    const vals = ["hello"];
    const compressed = compressStrings(vals);
    const decompressed = decompressStrings(compressed);
    expect(decompressed).toEqual(vals);
  });
});
