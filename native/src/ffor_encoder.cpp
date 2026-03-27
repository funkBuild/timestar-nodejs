// FFOR timestamp/integer encoder — ported from tsdb integer_encoder_ffor.cpp
// Scalar only (no Highway SIMD). Uses ts::Buffer/ts::Slice instead of AlignedBuffer/Slice.

#include <napi.h>
#include "ts_buffer.hpp"
#include "alp_ffor.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>

namespace {

constexpr size_t BLOCK_SIZE = 1024;

inline uint64_t zigzagEncode(int64_t x) {
    return (static_cast<uint64_t>(x) << 1) ^ static_cast<uint64_t>(x >> 63);
}
inline int64_t zigzagDecode(uint64_t y) {
    return static_cast<int64_t>((y >> 1) ^ -(y & 0x1));
}

inline uint8_t bitsForRange(uint64_t range) {
    if (range == 0) return 0;
    return static_cast<uint8_t>(64 - __builtin_clzll(range));
}

void writeBlockHeader(ts::Buffer& buf, uint16_t block_count, uint8_t bw, uint16_t exc_count, uint64_t base) {
    uint64_t w0 = static_cast<uint64_t>(block_count) |
                  (static_cast<uint64_t>(bw) << 11) |
                  (static_cast<uint64_t>(exc_count) << 18);
    buf.appendWord(w0);
    buf.appendWord(base);
}

struct BlockHeader {
    uint16_t block_count;
    uint8_t bw;
    uint16_t exc_count;
    uint64_t base;
};

BlockHeader readBlockHeader(ts::Slice& s) {
    uint64_t w0 = s.readWord();
    uint64_t base = s.readWord();
    return {
        static_cast<uint16_t>(w0 & 0x7FF),
        static_cast<uint8_t>((w0 >> 11) & 0x7F),
        static_cast<uint16_t>((w0 >> 18) & 0x3FF),
        base
    };
}

// Thread-local scratch buffers — allocated once, reused across calls
struct FFORScratch {
    std::vector<uint64_t> zigzag = std::vector<uint64_t>(1024);
    std::vector<uint64_t> clean = std::vector<uint64_t>(1024);
    std::vector<uint16_t> exc_positions = std::vector<uint16_t>(256);
    std::vector<uint64_t> exc_values = std::vector<uint64_t>(256);
    // Reusable zigzag buffer for integerEncode (avoids per-call allocation)
    std::vector<uint64_t> int_zigzag;
};

static FFORScratch& getScratch() {
    static thread_local FFORScratch s;
    return s;
}

void encodeBlock(const uint64_t* values, size_t count, ts::Buffer& buf, uint64_t min_val, uint64_t max_val) {
    if (min_val == max_val) {
        writeBlockHeader(buf, static_cast<uint16_t>(count), 0, 0, min_val);
        return;
    }

    uint8_t bw_full = bitsForRange(max_val - min_val);

    std::array<uint32_t, 65> bw_hist{};
    for (size_t i = 0; i < count; ++i) {
        uint64_t delta = values[i] - min_val;
        uint8_t vbw = (delta == 0) ? 0 : static_cast<uint8_t>(64 - __builtin_clzll(delta));
        bw_hist[vbw]++;
    }

    std::array<uint32_t, 65> suffix{};
    for (int k = 63; k >= 0; --k) {
        suffix[k] = suffix[k + 1] + bw_hist[k + 1];
    }

    const uint32_t max_exc = static_cast<uint32_t>(count / 4);
    size_t best_size = SIZE_MAX;
    uint8_t best_bw = bw_full;

    for (uint8_t cand = 0; cand <= bw_full; ++cand) {
        uint32_t exc = suffix[cand];
        if (exc > max_exc) continue;
        size_t ffor_bytes = alp::ffor_packed_words(count, cand) * 8;
        size_t exc_pos_bytes = (exc > 0) ? (static_cast<size_t>(exc + 3) / 4) * 8 : 0;
        size_t exc_val_bytes = static_cast<size_t>(exc) * 8;
        size_t total = 16 + ffor_bytes + exc_pos_bytes + exc_val_bytes;
        if (total < best_size) { best_size = total; best_bw = cand; }
    }

    // No-exception fast path
    if (suffix[best_bw] == 0) {
        writeBlockHeader(buf, static_cast<uint16_t>(count), best_bw, 0, min_val);
        size_t packed_words = alp::ffor_packed_words(count, best_bw);
        if (packed_words > 0) {
            size_t packed_bytes = packed_words * sizeof(uint64_t);
            uint8_t* dest = buf.grow(packed_bytes);
            std::memset(dest, 0, packed_bytes);
            alp::ffor_pack_u64(values, count, min_val, best_bw, reinterpret_cast<uint64_t*>(dest));
        }
        return;
    }

    // Exception path
    uint64_t threshold = (best_bw >= 64) ? UINT64_MAX : (best_bw == 0) ? 0 : (1ULL << best_bw) - 1;
    auto& sc = getScratch();
    sc.exc_positions.clear();
    sc.exc_values.clear();
    sc.clean.resize(count);

    for (size_t i = 0; i < count; ++i) {
        uint64_t delta = values[i] - min_val;
        if (delta > threshold) {
            sc.exc_positions.push_back(static_cast<uint16_t>(i));
            sc.exc_values.push_back(values[i]);
            sc.clean[i] = min_val;
        } else {
            sc.clean[i] = values[i];
        }
    }

    uint16_t actual_exc = static_cast<uint16_t>(sc.exc_positions.size());
    writeBlockHeader(buf, static_cast<uint16_t>(count), best_bw, actual_exc, min_val);

    size_t packed_words = alp::ffor_packed_words(count, best_bw);
    if (packed_words > 0) {
        size_t packed_bytes = packed_words * sizeof(uint64_t);
        uint8_t* dest = buf.grow(packed_bytes);
        std::memset(dest, 0, packed_bytes);
        alp::ffor_pack_u64(sc.clean.data(), count, min_val, best_bw, reinterpret_cast<uint64_t*>(dest));
    }

    if (actual_exc > 0) {
        size_t pos_words = (actual_exc + 3) / 4;
        for (size_t w = 0; w < pos_words; ++w) {
            uint64_t word = 0;
            for (size_t j = 0; j < 4; ++j) {
                size_t idx = w * 4 + j;
                if (idx < actual_exc) word |= static_cast<uint64_t>(sc.exc_positions[idx]) << (j * 16);
            }
            buf.appendWord(word);
        }
        buf.append(sc.exc_values.data(), actual_exc * sizeof(uint64_t));
    }
}

// [M4 fix] Removed unnecessary aligned_packed copy — Node.js Buffers are 8-byte aligned
size_t decodeBlockInto(ts::Slice& s, uint64_t* out) {
    auto hdr = readBlockHeader(s);
    if (hdr.bw > 64) throw std::runtime_error("Corrupt FFOR block: bw > 64");
    if (hdr.block_count > BLOCK_SIZE) throw std::runtime_error("Corrupt FFOR block: count > BLOCK_SIZE");
    if (hdr.exc_count > hdr.block_count) throw std::runtime_error("Corrupt FFOR block: exc > count");

    if (hdr.bw == 0) {
        std::fill_n(out, hdr.block_count, hdr.base);
    } else {
        size_t packed_words = alp::ffor_packed_words(hdr.block_count, hdr.bw);
        const uint64_t* packed_ptr = s.readWords(packed_words);
        alp::ffor_unpack_u64(packed_ptr, hdr.block_count, hdr.base, hdr.bw, out);
    }

    if (hdr.exc_count > 0) {
        size_t pos_words = (hdr.exc_count + 3) / 4;
        const uint64_t* pos_ptr = s.readWords(pos_words);
        const uint64_t* val_ptr = s.readWords(hdr.exc_count);
        for (size_t w = 0; w < pos_words; ++w) {
            uint64_t word = pos_ptr[w];
            size_t base_idx = w * 4;
            size_t remaining = hdr.exc_count - base_idx;
            size_t cnt = remaining < 4 ? remaining : 4;
            for (size_t j = 0; j < cnt; ++j) {
                uint16_t pos = static_cast<uint16_t>((word >> (j * 16)) & 0xFFFF);
                if (pos >= hdr.block_count) throw std::runtime_error("Corrupt FFOR: exc pos out of range");
                out[pos] = val_ptr[base_idx + j];
            }
        }
    }
    return hdr.block_count;
}

// Timestamp encode: delta-of-delta + zigzag + FFOR
ts::Buffer fforEncode(const uint64_t* values, size_t sz) {
    ts::Buffer buf;
    if (sz == 0) return buf;

    const size_t num_blocks = (sz + BLOCK_SIZE - 1) / BLOCK_SIZE;
    auto& scratch = getScratch();
    scratch.zigzag.resize(BLOCK_SIZE);

    buf.reserve(num_blocks * (16 + BLOCK_SIZE * 8));
    size_t val_idx = 0;

    for (size_t b = 0; b < num_blocks; ++b) {
        const size_t block_count = std::min(BLOCK_SIZE, sz - b * BLOCK_SIZE);
        uint64_t block_min = UINT64_MAX, block_max = 0;
        size_t zz_idx = 0;

        if (val_idx == 0) {
            uint64_t zz = values[0];
            scratch.zigzag[0] = zz;
            block_min = zz; block_max = zz;
            zz_idx = 1; val_idx = 1;

            if (zz_idx < block_count && val_idx < sz) {
                // Unsigned subtraction wraps correctly; cast to signed only for zigzag
                uint64_t udelta = values[1] - values[0];
                zz = zigzagEncode(static_cast<int64_t>(udelta));
                scratch.zigzag[1] = zz;
                if (zz < block_min) block_min = zz;
                if (zz > block_max) block_max = zz;
                zz_idx = 2; val_idx = 2;
            }
        }

        for (; zz_idx < block_count && val_idx < sz; ++zz_idx, ++val_idx) {
            // All delta arithmetic in unsigned to avoid signed overflow UB
            uint64_t d1 = values[val_idx] - values[val_idx - 1];
            uint64_t d2 = values[val_idx - 1] - values[val_idx - 2];
            uint64_t D = d1 - d2;
            uint64_t zz = zigzagEncode(static_cast<int64_t>(D));
            scratch.zigzag[zz_idx] = zz;
            if (zz < block_min) block_min = zz;
            if (zz > block_max) block_max = zz;
        }

        if (block_min > block_max) { block_min = block_max = 0; }
        encodeBlock(scratch.zigzag.data(), block_count, buf, block_min, block_max);
    }
    return buf;
}

// Decode with delta-of-delta reconstruction for timestamps.
// Fixes: clamp writes to expected_count (heap overflow on corrupt input),
//        use unsigned arithmetic for delta (avoid signed overflow UB).
std::vector<uint64_t> fforDecode(const uint8_t* data, size_t len, size_t expected_count) {
    ts::Slice s(data, len);
    std::vector<uint64_t> values(expected_count);
    size_t write_idx = 0;

    alignas(64) uint64_t blockBuf[BLOCK_SIZE];
    uint64_t last_decoded = 0;
    int64_t delta = 0;
    size_t global_idx = 0;

    while (write_idx < expected_count && s.remaining() >= 16) {
        size_t block_count = decodeBlockInto(s, blockBuf);
        // Clamp to prevent writing past output buffer on corrupt block_count
        size_t remaining = expected_count - write_idx;
        if (block_count > remaining) block_count = remaining;
        size_t local_i = 0;

        if (global_idx == 0 && local_i < block_count) {
            last_decoded = blockBuf[0];
            values[write_idx++] = last_decoded;
            local_i = 1; global_idx = 1;
        }
        if (global_idx == 1 && local_i < block_count) {
            delta = zigzagDecode(blockBuf[local_i]);
            last_decoded += static_cast<uint64_t>(delta); // unsigned add avoids signed overflow UB
            values[write_idx++] = last_decoded;
            local_i++; global_idx = 2;
        }

        for (; local_i + 3 < block_count; local_i += 4) {
            int64_t dd0 = zigzagDecode(blockBuf[local_i]);
            int64_t dd1 = zigzagDecode(blockBuf[local_i + 1]);
            int64_t dd2 = zigzagDecode(blockBuf[local_i + 2]);
            int64_t dd3 = zigzagDecode(blockBuf[local_i + 3]);
            delta += dd0; last_decoded += static_cast<uint64_t>(delta); values[write_idx++] = last_decoded;
            delta += dd1; last_decoded += static_cast<uint64_t>(delta); values[write_idx++] = last_decoded;
            delta += dd2; last_decoded += static_cast<uint64_t>(delta); values[write_idx++] = last_decoded;
            delta += dd3; last_decoded += static_cast<uint64_t>(delta); values[write_idx++] = last_decoded;
        }
        for (; local_i < block_count; ++local_i) {
            int64_t dd = zigzagDecode(blockBuf[local_i]);
            delta += dd;
            last_decoded += static_cast<uint64_t>(delta);
            values[write_idx++] = last_decoded;
        }
    }
    values.resize(write_idx);
    return values;
}

// [H2 fix] Integer encode: zigzag only (no delta-of-delta) then raw FFOR block packing.
// Delta-of-delta is for timestamps; arbitrary integers just need zigzag + FFOR.
ts::Buffer integerEncodeRaw(const uint64_t* values, size_t sz) {
    ts::Buffer buf;
    if (sz == 0) return buf;

    const size_t num_blocks = (sz + BLOCK_SIZE - 1) / BLOCK_SIZE;
    buf.reserve(num_blocks * (16 + BLOCK_SIZE * 8));

    for (size_t b = 0; b < num_blocks; ++b) {
        const size_t block_start = b * BLOCK_SIZE;
        const size_t block_count = std::min(BLOCK_SIZE, sz - block_start);
        const uint64_t* block_vals = values + block_start;

        uint64_t block_min = UINT64_MAX, block_max = 0;
        for (size_t i = 0; i < block_count; ++i) {
            if (block_vals[i] < block_min) block_min = block_vals[i];
            if (block_vals[i] > block_max) block_max = block_vals[i];
        }
        encodeBlock(block_vals, block_count, buf, block_min, block_max);
    }
    return buf;
}

// Raw FFOR decode: just unpack blocks, no delta reconstruction
std::vector<uint64_t> integerDecodeRaw(const uint8_t* data, size_t len, size_t expected_count) {
    ts::Slice s(data, len);
    std::vector<uint64_t> values(expected_count);
    size_t write_idx = 0;
    alignas(64) uint64_t blockBuf[BLOCK_SIZE];

    while (write_idx < expected_count && s.remaining() >= 16) {
        size_t block_count = decodeBlockInto(s, blockBuf);
        size_t to_copy = std::min(block_count, expected_count - write_idx);
        std::memcpy(&values[write_idx], blockBuf, to_copy * sizeof(uint64_t));
        write_idx += to_copy;
    }
    values.resize(write_idx);
    return values;
}

// Integer encode: zigzag first, then delta-of-delta + FFOR (same as timestamps).
// This matches the server's IntegerEncoder::encode(zigzag(values)) format.
ts::Buffer integerEncode(const int64_t* values, size_t sz) {
    if (sz == 0) return ts::Buffer();

    auto& scratch = getScratch();
    scratch.int_zigzag.resize(sz);
    for (size_t i = 0; i < sz; ++i) {
        scratch.int_zigzag[i] = zigzagEncode(values[i]);
    }
    // Use fforEncode (delta-of-delta) to match server's IntegerEncoder::encode
    return fforEncode(scratch.int_zigzag.data(), sz);
}

// Integer decode: delta-of-delta FFOR decode, then zigzag decode.
// Returns zigzag-decoded values as uint64_t (bits represent int64_t).
std::vector<uint64_t> integerDecode(const uint8_t* data, size_t len, size_t expected_count) {
    // fforDecode does delta-of-delta reconstruction (matching IntegerEncoder format)
    auto decoded = fforDecode(data, len, expected_count);
    // Zigzag decode in-place
    for (size_t i = 0; i < decoded.size(); ++i) {
        int64_t val = zigzagDecode(decoded[i]);
        std::memcpy(&decoded[i], &val, sizeof(uint64_t));
    }
    return decoded;
}

// =============================================================================
// [H1 fix] Zero-copy N-API returns using release callbacks
// =============================================================================

// Release callback that deletes a ts::Buffer
static void releaseTsBuffer(Napi::Env, void*, void* hint) {
    delete static_cast<ts::Buffer*>(hint);
}

// Release callback that deletes a std::vector<uint64_t>
static void releaseVecU64(Napi::Env, void*, void* hint) {
    delete static_cast<std::vector<uint64_t>*>(hint);
}

// (integerDecode now returns vector<uint64_t>, use releaseVecU64 for it)

} // anonymous namespace

// =============================================================================
// N-API bindings
// =============================================================================

static Napi::Value TimestampEncode(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 1 || !info[0].IsBuffer()) {
        Napi::TypeError::New(env, "Expected Buffer of uint64 values").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    auto buf = info[0].As<Napi::Buffer<uint8_t>>();
    size_t byte_len = buf.Length();
    if (byte_len % 8 != 0) {
        Napi::TypeError::New(env, "Buffer length must be multiple of 8").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    size_t count = byte_len / 8;
    const uint64_t* values = reinterpret_cast<const uint64_t*>(buf.Data());

    auto* encoded = new ts::Buffer(fforEncode(values, count));
    return Napi::Buffer<uint8_t>::New(env, encoded->data(), encoded->size(), releaseTsBuffer, encoded);
}

static Napi::Value TimestampDecode(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 2 || !info[0].IsBuffer() || !info[1].IsNumber()) {
        Napi::TypeError::New(env, "Expected (Buffer, count)").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    auto buf = info[0].As<Napi::Buffer<uint8_t>>();
    size_t count = info[1].As<Napi::Number>().Uint32Value();

    auto* decoded = new std::vector<uint64_t>(fforDecode(buf.Data(), buf.Length(), count));
    size_t byte_size = decoded->size() * sizeof(uint64_t);
    return Napi::Buffer<uint8_t>::New(env, reinterpret_cast<uint8_t*>(decoded->data()), byte_size, releaseVecU64, decoded);
}

static Napi::Value IntegerEncode(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 1 || !info[0].IsBuffer()) {
        Napi::TypeError::New(env, "Expected Buffer of int64 values").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    auto buf = info[0].As<Napi::Buffer<uint8_t>>();
    size_t byte_len = buf.Length();
    if (byte_len % 8 != 0) {
        Napi::TypeError::New(env, "Buffer length must be multiple of 8").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    size_t count = byte_len / 8;
    const int64_t* values = reinterpret_cast<const int64_t*>(buf.Data());

    auto* encoded = new ts::Buffer(integerEncode(values, count));
    return Napi::Buffer<uint8_t>::New(env, encoded->data(), encoded->size(), releaseTsBuffer, encoded);
}

static Napi::Value IntegerDecode(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 2 || !info[0].IsBuffer() || !info[1].IsNumber()) {
        Napi::TypeError::New(env, "Expected (Buffer, count)").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    auto buf = info[0].As<Napi::Buffer<uint8_t>>();
    size_t count = info[1].As<Napi::Number>().Uint32Value();

    auto* decoded = new std::vector<uint64_t>(integerDecode(buf.Data(), buf.Length(), count));
    size_t byte_size = decoded->size() * sizeof(uint64_t);
    return Napi::Buffer<uint8_t>::New(env, reinterpret_cast<uint8_t*>(decoded->data()), byte_size, releaseVecU64, decoded);
}

Napi::Object InitFFOR(Napi::Env env, Napi::Object exports) {
    exports.Set("timestampEncode", Napi::Function::New(env, TimestampEncode));
    exports.Set("timestampDecode", Napi::Function::New(env, TimestampDecode));
    exports.Set("integerEncode", Napi::Function::New(env, IntegerEncode));
    exports.Set("integerDecode", Napi::Function::New(env, IntegerDecode));
    return exports;
}
