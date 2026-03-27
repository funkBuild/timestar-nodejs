// String zstd encoder — ported from tsdb string_encoder.cpp
// Uses zstd for compression. No dictionary encoding for simplicity in v1.

#include <napi.h>
#include "ts_buffer.hpp"

#include <zstd.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

static constexpr uint32_t STRG_MAGIC = 0x53545247;
static constexpr uint32_t MAX_UNCOMPRESSED_SIZE = 256 * 1024 * 1024;

// Thread-local zstd contexts
struct ZstdCCtxDeleter { void operator()(ZSTD_CCtx* p) const { ZSTD_freeCCtx(p); } };
struct ZstdDCtxDeleter { void operator()(ZSTD_DCtx* p) const { ZSTD_freeDCtx(p); } };

static ZSTD_CCtx* getThreadCCtx() {
    static thread_local std::unique_ptr<ZSTD_CCtx, ZstdCCtxDeleter> ctx(ZSTD_createCCtx());
    return ctx.get();
}
static ZSTD_DCtx* getThreadDCtx() {
    static thread_local std::unique_ptr<ZSTD_DCtx, ZstdDCtxDeleter> ctx(ZSTD_createDCtx());
    return ctx.get();
}

void writeVarInt(ts::Buffer& buf, uint32_t value) {
    while (value >= 0x80) {
        buf.appendU8(static_cast<uint8_t>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    buf.appendU8(static_cast<uint8_t>(value & 0x7F));
}

uint32_t readVarInt(ts::Slice& s) {
    uint32_t value = 0;
    uint32_t shift = 0;
    uint8_t byte;
    do {
        if (s.remaining() == 0) throw std::runtime_error("StringEncoder: truncated varint");
        byte = s.readU8();
        value |= (uint32_t(byte & 0x7F) << shift);
        shift += 7;
        if (shift > 28 && (byte & 0x80)) throw std::runtime_error("StringEncoder: varint overflow");
    } while (byte & 0x80);
    return value;
}

// Encode: string array -> varint-prefixed concatenation -> zstd -> header + compressed
ts::Buffer stringEncode(const std::vector<std::string>& values) {
    ts::Buffer buf;
    uint32_t count = static_cast<uint32_t>(values.size());

    // Build uncompressed buffer (varint len + string data)
    ts::Buffer uncompressed;
    for (const auto& s : values) {
        writeVarInt(uncompressed, static_cast<uint32_t>(s.size()));
        uncompressed.append(s.data(), s.size());
    }

    if (uncompressed.size() > MAX_UNCOMPRESSED_SIZE)
        throw std::runtime_error("StringEncoder: uncompressed size exceeds 256MB limit");
    uint32_t uncompSize = static_cast<uint32_t>(uncompressed.size());

    // [M6 fix] Reuse thread-local compress buffer
    size_t compBound = ZSTD_compressBound(uncompSize);
    static thread_local std::vector<uint8_t> compBuf;
    compBuf.resize(compBound);
    size_t compSize = ZSTD_compressCCtx(getThreadCCtx(), compBuf.data(), compBound,
                                        uncompressed.data(), uncompSize, 1);
    if (ZSTD_isError(compSize))
        throw std::runtime_error(std::string("zstd compress failed: ") + ZSTD_getErrorName(compSize));

    // Header: magic(4) + uncompSize(4) + compSize(4) + count(4)
    buf.appendU32(STRG_MAGIC);
    buf.appendU32(uncompSize);
    buf.appendU32(static_cast<uint32_t>(compSize));
    buf.appendU32(count);
    buf.append(compBuf.data(), compSize);
    return buf;
}

std::vector<std::string> stringDecode(const uint8_t* data, size_t len) {
    if (len < 16) throw std::runtime_error("StringEncoder: too small for header");

    uint32_t magic, uncompSize, compSize, count;
    std::memcpy(&magic, data, 4);
    std::memcpy(&uncompSize, data + 4, 4);
    std::memcpy(&compSize, data + 8, 4);
    std::memcpy(&count, data + 12, 4);

    if (magic != STRG_MAGIC) throw std::runtime_error("StringEncoder: invalid magic");
    if (len < 16 + compSize) throw std::runtime_error("StringEncoder: truncated data");
    if (uncompSize > MAX_UNCOMPRESSED_SIZE) throw std::runtime_error("StringEncoder: uncompSize too large");

    std::vector<std::string> out;
    if (uncompSize == 0) return out;

    std::vector<uint8_t> decompBuf(uncompSize);
    size_t ret = ZSTD_decompressDCtx(getThreadDCtx(), decompBuf.data(), uncompSize,
                                     data + 16, compSize);
    if (ZSTD_isError(ret))
        throw std::runtime_error(std::string("zstd decompress failed: ") + ZSTD_getErrorName(ret));
    if (ret != uncompSize) throw std::runtime_error("StringEncoder: decompressed size mismatch");

    ts::Slice s(decompBuf.data(), uncompSize);
    out.reserve(count);
    for (uint32_t i = 0; i < count && s.remaining() > 0; ++i) {
        uint32_t strLen = readVarInt(s);
        if (strLen > s.remaining()) throw std::runtime_error("StringEncoder: invalid string length");
        out.emplace_back(reinterpret_cast<const char*>(s.ptr()), strLen);
        s.skip(strLen);
    }
    return out;
}

static void releaseTsBuffer(Napi::Env, void*, void* hint) {
    delete static_cast<ts::Buffer*>(hint);
}

} // anonymous namespace

// [H1 fix] Zero-copy N-API returns

static Napi::Value StringEnc(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 1 || !info[0].IsArray()) {
        Napi::TypeError::New(env, "Expected Array of strings").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    auto arr = info[0].As<Napi::Array>();
    std::vector<std::string> values;
    values.reserve(arr.Length());
    for (uint32_t i = 0; i < arr.Length(); ++i) {
        values.push_back(arr.Get(i).As<Napi::String>().Utf8Value());
    }

    auto* encoded = new ts::Buffer(stringEncode(values));
    return Napi::Buffer<uint8_t>::New(env, encoded->data(), encoded->size(), releaseTsBuffer, encoded);
}

static Napi::Value StringDec(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 1 || !info[0].IsBuffer()) {
        Napi::TypeError::New(env, "Expected Buffer").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    auto buf = info[0].As<Napi::Buffer<uint8_t>>();
    auto decoded = stringDecode(buf.Data(), buf.Length());

    auto arr = Napi::Array::New(env, decoded.size());
    for (size_t i = 0; i < decoded.size(); ++i) {
        arr.Set(static_cast<uint32_t>(i), Napi::String::New(env, decoded[i]));
    }
    return arr;
}

Napi::Object InitString(Napi::Env env, Napi::Object exports) {
    exports.Set("stringEncode", Napi::Function::New(env, StringEnc));
    exports.Set("stringDecode", Napi::Function::New(env, StringDec));
    return exports;
}
