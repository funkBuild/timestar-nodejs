// Wrapper around the native compression addon.

import * as path from "path";

interface NativeAddon {
  timestampEncode(buf: Buffer): Buffer;
  timestampDecode(buf: Buffer, count: number): Buffer;
  integerEncode(buf: Buffer): Buffer;
  integerDecode(buf: Buffer, count: number): Buffer;
  doubleEncode(buf: Buffer): Buffer;
  doubleDecode(buf: Buffer): Buffer;
  boolEncode(buf: Buffer): Buffer;
  boolDecode(buf: Buffer, count: number): Buffer;
  stringEncode(values: string[]): Buffer;
  stringDecode(buf: Buffer): string[];
}

let addon: NativeAddon | null = null;

function getAddon(): NativeAddon {
  if (!addon) {
    const addonPath = path.resolve(__dirname, "..", "native", "build", "Release", "timestar_compression.node");
    addon = require(addonPath) as NativeAddon;
  }
  return addon;
}

// ============================================================================
// uint64/int64 <-> number conversion helpers
//
// [W2/Q2] Splitting a 64-bit word into two uint32 halves avoids allocating a
// BigInt per element on the hot encode/decode paths.
//
// Write side (numbers): for any non-negative integer-valued double,
//   hi = floor(v / 2^32) and lo = v mod 2^32 (ToUint32) are computed exactly
//   (division by a power of two only shifts the exponent), so hi*2^32+lo === v.
//   Same holds for signed values with hi = floor(v / 2^32) (negative hi).
//
// Read side: hi * 2^32 is exact (power-of-two scaling), and the single
//   addition of lo rounds the true mathematical sum to nearest-even — which is
//   exactly what Number(BigInt(v)) does. The results are therefore
//   value-identical to the previous Number(getBigUint64/getBigInt64)
//   implementation, including rounding behavior above 2^53 (verified by
//   property tests in test/compression.test.ts).
// ============================================================================

const TWO_32 = 0x1_0000_0000;

function u64ToNumber(lo: number, hi: number): number {
  return hi * TWO_32 + lo;
}

function i64ToNumber(lo: number, hiSigned: number): number {
  return hiSigned * TWO_32 + lo;
}

// ============================================================================
// Timestamp compression (delta-of-delta + zigzag + FFOR)
// ============================================================================

export function compressTimestamps(timestamps: Array<number | bigint>): Buffer {
  if (timestamps.length === 0) return Buffer.alloc(0);
  // [W3] allocUnsafe: every byte is overwritten below.
  const buf = Buffer.allocUnsafe(timestamps.length * 8);
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  for (let i = 0; i < timestamps.length; i++) {
    const ts = timestamps[i];
    if (typeof ts === "bigint") {
      // BigInt path — full 64-bit precision for bigint inputs.
      view.setBigUint64(i * 8, BigInt.asUintN(64, ts), true);
    } else {
      // [W2] Number path — lo/hi uint32 pair, no BigInt allocation.
      view.setUint32(i * 8, ts >>> 0, true);
      view.setUint32(i * 8 + 4, Math.floor(ts / TWO_32), true);
    }
  }
  return getAddon().timestampEncode(buf);
}

export function decompressTimestamps(compressed: Buffer, count: number): number[] {
  if (count === 0 || compressed.length === 0) return [];
  const decoded = getAddon().timestampDecode(compressed, count);
  const view = new DataView(decoded.buffer, decoded.byteOffset, decoded.byteLength);
  const result: number[] = new Array(count);
  for (let i = 0; i < count; i++) {
    // [Q2] Two uint32 reads — value-identical to Number(view.getBigUint64(...)).
    result[i] = u64ToNumber(view.getUint32(i * 8, true), view.getUint32(i * 8 + 4, true));
  }
  return result;
}

/**
 * [C2] Precise timestamp decode: returns bigint[] with full 64-bit precision.
 * Use for nanosecond timestamps beyond 2^53 (any realistic ns epoch time).
 */
export function decompressTimestampsBigInt(compressed: Buffer, count: number): bigint[] {
  if (count === 0 || compressed.length === 0) return [];
  const decoded = getAddon().timestampDecode(compressed, count);
  const view = new DataView(decoded.buffer, decoded.byteOffset, decoded.byteLength);
  const result: bigint[] = new Array(count);
  for (let i = 0; i < count; i++) {
    result[i] = view.getBigUint64(i * 8, true);
  }
  return result;
}

// ============================================================================
// Double compression (ALP)
// [M12 fix] Write Float64Array directly into Buffer to avoid extra copy
// [H7 fix] Manual loop instead of Array.from for decompression
// ============================================================================

export function compressDoubles(values: number[]): Buffer {
  if (values.length === 0) return Buffer.alloc(0);
  // Allocate Buffer, create F64 view over it — single allocation, no copy.
  // [W3] allocUnsafe: fully overwritten. Node pools are 8-byte aligned, which
  // the Float64Array view requires.
  const buf = Buffer.allocUnsafe(values.length * 8);
  const f64 = new Float64Array(buf.buffer, buf.byteOffset, values.length);
  for (let i = 0; i < values.length; i++) f64[i] = values[i];
  return getAddon().doubleEncode(buf);
}

export function decompressDoubles(compressed: Buffer): number[] {
  if (compressed.length === 0) return [];
  const decoded = getAddon().doubleDecode(compressed);
  const f64 = new Float64Array(decoded.buffer, decoded.byteOffset, decoded.length / 8);
  // [H7 fix] Manual loop is faster than Array.from for typed arrays
  const result = new Array(f64.length);
  for (let i = 0; i < f64.length; i++) result[i] = f64[i];
  return result;
}

// ============================================================================
// Integer compression (zigzag + FFOR)
// [C1] Accepts bigint elements and carries them to the wire at full 64-bit
// precision (no Number() rounding).
// ============================================================================

export function compressIntegers(values: Array<number | bigint>): Buffer {
  if (values.length === 0) return Buffer.alloc(0);
  // [W3] allocUnsafe: every byte is overwritten below.
  const buf = Buffer.allocUnsafe(values.length * 8);
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    if (typeof v === "bigint") {
      view.setBigInt64(i * 8, BigInt.asIntN(64, v), true);
    } else {
      // [W2-style] lo/hi split, exact for integer-valued doubles (see header).
      view.setUint32(i * 8, v >>> 0, true);
      view.setInt32(i * 8 + 4, Math.floor(v / TWO_32), true);
    }
  }
  return getAddon().integerEncode(buf);
}

export function decompressIntegers(compressed: Buffer, count: number): number[] {
  if (count === 0 || compressed.length === 0) return [];
  const decoded = getAddon().integerDecode(compressed, count);
  const view = new DataView(decoded.buffer, decoded.byteOffset, decoded.byteLength);
  const result: number[] = new Array(count);
  for (let i = 0; i < count; i++) {
    // [Q2] uint32 lo + signed int32 hi — value-identical to
    // Number(view.getBigInt64(...)), sign carried by the hi word.
    result[i] = i64ToNumber(view.getUint32(i * 8, true), view.getInt32(i * 8 + 4, true));
  }
  return result;
}

/**
 * [C2] Precise int64 decode: returns bigint[] with full 64-bit precision.
 * Use when int64 field values may exceed 2^53.
 */
export function decompressIntegersBigInt(compressed: Buffer, count: number): bigint[] {
  if (count === 0 || compressed.length === 0) return [];
  const decoded = getAddon().integerDecode(compressed, count);
  const view = new DataView(decoded.buffer, decoded.byteOffset, decoded.byteLength);
  const result: bigint[] = new Array(count);
  for (let i = 0; i < count; i++) {
    result[i] = view.getBigInt64(i * 8, true);
  }
  return result;
}

// ============================================================================
// Boolean compression (RLE)
// ============================================================================

export function compressBooleans(values: boolean[]): Buffer {
  if (values.length === 0) return Buffer.alloc(0);
  // [W3] allocUnsafe: every byte is overwritten below.
  const buf = Buffer.allocUnsafe(values.length);
  for (let i = 0; i < values.length; i++) buf[i] = values[i] ? 1 : 0;
  return getAddon().boolEncode(buf);
}

export function decompressBooleans(compressed: Buffer, count: number): boolean[] {
  if (count === 0 || compressed.length === 0) return [];
  const decoded = getAddon().boolDecode(compressed, count);
  const result: boolean[] = new Array(count);
  for (let i = 0; i < count; i++) result[i] = decoded[i] !== 0;
  return result;
}

// ============================================================================
// String compression (zstd)
// ============================================================================

export function compressStrings(values: string[]): Buffer {
  if (values.length === 0) return Buffer.alloc(0);
  return getAddon().stringEncode(values);
}

export function decompressStrings(compressed: Buffer): string[] {
  if (compressed.length === 0) return [];
  return getAddon().stringDecode(compressed);
}
