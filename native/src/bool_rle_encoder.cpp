// Boolean RLE encoder — ported from tsdb bool_encoder_rle.cpp

#include <napi.h>
#include "ts_buffer.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <vector>

namespace {

void writeVarint(ts::Buffer& buf, uint64_t value) {
    if (value < 0x80) { buf.appendU8(static_cast<uint8_t>(value)); return; }
    while (value >= 0x80) { buf.appendU8(static_cast<uint8_t>(value | 0x80)); value >>= 7; }
    buf.appendU8(static_cast<uint8_t>(value));
}

uint64_t readVarint(ts::Slice& s) {
    if (s.remaining() == 0) throw std::runtime_error("BoolRLE: unexpected end in varint");
    uint8_t first = s.readU8();
    if ((first & 0x80) == 0) return first;
    uint64_t result = first & 0x7F;
    int shift = 7;
    while (s.remaining() > 0) {
        uint8_t byte = s.readU8();
        result |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) return result;
        shift += 7;
        if (shift >= 64) throw std::runtime_error("BoolRLE: varint overflow");
    }
    throw std::runtime_error("BoolRLE: truncated varint");
}

ts::Buffer boolEncode(const uint8_t* values, size_t n) {
    ts::Buffer buf;
    if (n == 0) return buf;
    buf.reserve(1 + n / 4);
    bool currentValue = values[0] != 0;
    buf.appendU8(currentValue ? 1 : 0);
    uint64_t runLength = 1;
    for (size_t i = 1; i < n; ++i) {
        bool v = values[i] != 0;
        if (v == currentValue) { ++runLength; }
        else { writeVarint(buf, runLength); currentValue = !currentValue; runLength = 1; }
    }
    writeVarint(buf, runLength);
    return buf;
}

// [L1 fix] Use memset for run fills instead of per-element push_back
std::vector<uint8_t> boolDecode(const uint8_t* data, size_t len, size_t count) {
    ts::Slice s(data, len);
    std::vector<uint8_t> out(count);
    size_t write_pos = 0;

    bool currentValue = s.readU8() != 0;
    size_t remaining = count;

    while (remaining > 0 && s.remaining() > 0) {
        uint64_t runLen = readVarint(s);
        size_t toEmit = std::min(static_cast<size_t>(runLen), remaining);
        std::memset(out.data() + write_pos, currentValue ? 1 : 0, toEmit);
        write_pos += toEmit;
        remaining -= toEmit;
        currentValue = !currentValue;
    }
    out.resize(write_pos);
    return out;
}

static void releaseTsBuffer(Napi::Env, void*, void* hint) {
    delete static_cast<ts::Buffer*>(hint);
}
static void releaseVecU8(Napi::Env, void*, void* hint) {
    delete static_cast<std::vector<uint8_t>*>(hint);
}

} // anonymous namespace

// [H1 fix] Zero-copy N-API returns

static Napi::Value BoolEncode(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 1 || !info[0].IsBuffer()) {
        Napi::TypeError::New(env, "Expected Buffer of uint8 values (0/1)").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    auto buf = info[0].As<Napi::Buffer<uint8_t>>();

    auto* encoded = new ts::Buffer(boolEncode(buf.Data(), buf.Length()));
    return Napi::Buffer<uint8_t>::New(env, encoded->data(), encoded->size(), releaseTsBuffer, encoded);
}

static Napi::Value BoolDecode(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 2 || !info[0].IsBuffer() || !info[1].IsNumber()) {
        Napi::TypeError::New(env, "Expected (Buffer, count)").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    auto buf = info[0].As<Napi::Buffer<uint8_t>>();
    size_t count = info[1].As<Napi::Number>().Uint32Value();

    auto* decoded = new std::vector<uint8_t>(boolDecode(buf.Data(), buf.Length(), count));
    return Napi::Buffer<uint8_t>::New(env, decoded->data(), decoded->size(), releaseVecU8, decoded);
}

Napi::Object InitBoolRLE(Napi::Env env, Napi::Object exports) {
    exports.Set("boolEncode", Napi::Function::New(env, BoolEncode));
    exports.Set("boolDecode", Napi::Function::New(env, BoolDecode));
    return exports;
}
