import { describe, it, expect } from "vitest";
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
