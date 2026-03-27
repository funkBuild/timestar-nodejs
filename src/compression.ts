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
// Timestamp compression (delta-of-delta + zigzag + FFOR)
// [H6 fix] Use BigUint64Array for clean uint64 conversion
// ============================================================================

export function compressTimestamps(timestamps: Array<number | bigint>): Buffer {
  if (timestamps.length === 0) return Buffer.alloc(0);
  const arr = new BigUint64Array(timestamps.length);
  for (let i = 0; i < timestamps.length; i++) {
    const ts = timestamps[i];
    arr[i] = typeof ts === 'bigint' ? ts : BigInt(ts);
  }
  return getAddon().timestampEncode(Buffer.from(arr.buffer));
}

export function decompressTimestamps(compressed: Buffer, count: number): number[] {
  if (count === 0 || compressed.length === 0) return [];
  const decoded = getAddon().timestampDecode(compressed, count);
  const view = new DataView(decoded.buffer, decoded.byteOffset, decoded.byteLength);
  const result: number[] = new Array(count);
  for (let i = 0; i < count; i++) {
    result[i] = Number(view.getBigUint64(i * 8, true));
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
  // Allocate Buffer, create F64 view over it — single allocation, no copy
  const buf = Buffer.alloc(values.length * 8);
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
// ============================================================================

export function compressIntegers(values: number[]): Buffer {
  if (values.length === 0) return Buffer.alloc(0);
  const buf = Buffer.alloc(values.length * 8);
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  for (let i = 0; i < values.length; i++) {
    view.setBigInt64(i * 8, BigInt(values[i]), true);
  }
  return getAddon().integerEncode(buf);
}

export function decompressIntegers(compressed: Buffer, count: number): number[] {
  if (count === 0 || compressed.length === 0) return [];
  const decoded = getAddon().integerDecode(compressed, count);
  const view = new DataView(decoded.buffer, decoded.byteOffset, decoded.byteLength);
  const result: number[] = new Array(count);
  for (let i = 0; i < count; i++) {
    result[i] = Number(view.getBigInt64(i * 8, true));
  }
  return result;
}

// ============================================================================
// Boolean compression (RLE)
// ============================================================================

export function compressBooleans(values: boolean[]): Buffer {
  if (values.length === 0) return Buffer.alloc(0);
  const buf = Buffer.alloc(values.length);
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
