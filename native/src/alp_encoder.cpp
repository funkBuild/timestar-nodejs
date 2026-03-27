// ALP float encoder — ported from tsdb alp_encoder.cpp + alp_decoder.cpp
// Scalar only (no Highway SIMD).

#include <napi.h>
#include "ts_buffer.hpp"
#include "alp_constants.hpp"
#include "alp_ffor.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <unordered_map>
#include <vector>

namespace {

// ============================================================================
// ALP_RD (Real Doubles) — fallback for doubles resistant to decimal encoding
// ============================================================================

struct ALPRDBlockResult {
    std::vector<uint64_t> dictionary;
    std::vector<uint8_t> left_indices;
    std::vector<uint64_t> right_parts;
    std::vector<uint16_t> exception_positions;
    std::vector<uint64_t> exception_values;
    uint8_t right_bit_count = 0;
    uint8_t left_bw = 0, right_bw = 0;
    uint64_t right_for_base = 0;
};

uint8_t findBestSplit(const double* values, size_t count) {
    const size_t sample_count = std::min(count, alp::ALP_SAMPLE_SIZE);
    uint8_t best_right_bits = 32;
    size_t best_exceptions = count + 1;

    thread_local std::unordered_map<uint64_t, size_t> left_freq;

    for (uint8_t rb = 8; rb <= 56; rb += 4) {
        left_freq.clear();
        for (size_t i = 0; i < sample_count; ++i) {
            uint64_t bits; std::memcpy(&bits, &values[i], 8);
            left_freq[bits >> rb]++;
        }
        if (left_freq.size() <= alp::ALP_RD_MAX_DICT_SIZE) {
            if (0 < best_exceptions) { best_exceptions = 0; best_right_bits = rb; }
        } else {
            std::vector<std::pair<uint64_t, size_t>> freq_vec(left_freq.begin(), left_freq.end());
            std::partial_sort(freq_vec.begin(),
                freq_vec.begin() + std::min(freq_vec.size(), alp::ALP_RD_MAX_DICT_SIZE),
                freq_vec.end(),
                [](const auto& a, const auto& b) { return a.second > b.second; });
            std::array<uint64_t, alp::ALP_RD_MAX_DICT_SIZE> top8;
            size_t top8_count = std::min(freq_vec.size(), alp::ALP_RD_MAX_DICT_SIZE);
            for (size_t i = 0; i < top8_count; ++i) top8[i] = freq_vec[i].first;
            size_t exceptions = 0;
            for (size_t i = 0; i < sample_count; ++i) {
                uint64_t bits; std::memcpy(&bits, &values[i], 8);
                uint64_t left = bits >> rb;
                bool found = false;
                for (size_t j = 0; j < top8_count; ++j) {
                    if (top8[j] == left) { found = true; break; }
                }
                if (!found) exceptions++;
            }
            size_t estimated = (exceptions * count + sample_count - 1) / sample_count;
            if (estimated < best_exceptions) { best_exceptions = estimated; best_right_bits = rb; }
        }
    }
    return best_right_bits;
}

ALPRDBlockResult encodeRDBlock(const double* values, size_t count, uint8_t right_bit_count) {
    ALPRDBlockResult result;
    result.right_bit_count = right_bit_count;
    const uint64_t right_mask = (right_bit_count == 64) ? ~0ULL : ((1ULL << right_bit_count) - 1);

    thread_local std::unordered_map<uint64_t, size_t> left_freq;
    left_freq.clear();
    for (size_t i = 0; i < count; ++i) {
        uint64_t bits; std::memcpy(&bits, &values[i], 8);
        left_freq[bits >> right_bit_count]++;
    }

    std::vector<std::pair<uint64_t, size_t>> freq_vec(left_freq.begin(), left_freq.end());
    std::sort(freq_vec.begin(), freq_vec.end(), [](const auto& a, const auto& b) { return a.second > b.second; });

    const size_t dict_size = std::min(freq_vec.size(), alp::ALP_RD_MAX_DICT_SIZE);
    result.dictionary.resize(dict_size);
    std::array<std::pair<uint64_t, uint8_t>, alp::ALP_RD_MAX_DICT_SIZE> dict_arr;
    for (size_t i = 0; i < dict_size; ++i) {
        result.dictionary[i] = freq_vec[i].first;
        dict_arr[i] = {freq_vec[i].first, static_cast<uint8_t>(i)};
    }
    result.left_bw = (dict_size <= 1) ? 0 : static_cast<uint8_t>(64 - __builtin_clzll(dict_size - 1));

    result.left_indices.resize(count);
    result.right_parts.resize(count);
    uint64_t right_min = ~0ULL, right_max = 0;

    for (size_t i = 0; i < count; ++i) {
        uint64_t bits; std::memcpy(&bits, &values[i], 8);
        uint64_t left = bits >> right_bit_count;
        uint64_t right = bits & right_mask;
        result.right_parts[i] = right;
        if (right < right_min) right_min = right;
        if (right > right_max) right_max = right;
        uint8_t dict_idx = 0;
        bool found = false;
        for (size_t d = 0; d < dict_size; ++d) {
            if (dict_arr[d].first == left) { dict_idx = dict_arr[d].second; found = true; break; }
        }
        if (found) {
            result.left_indices[i] = dict_idx;
        } else {
            result.left_indices[i] = 0;
            result.exception_positions.push_back(static_cast<uint16_t>(i));
            result.exception_values.push_back(bits);
        }
    }
    result.right_for_base = right_min;
    uint64_t right_range = right_max - right_min;
    result.right_bw = (right_range == 0) ? 0 : static_cast<uint8_t>(64 - __builtin_clzll(right_range));
    return result;
}

// ============================================================================
// ALP Encoder
// ============================================================================

struct ScaleResult { int64_t encoded; bool exact; };

inline ScaleResult scaleValue(double value, uint8_t exp, uint8_t fac) {
    double scaled = value * alp::FACT_ARR[exp];
    double rounded = std::round(scaled);
    if (rounded > static_cast<double>(alp::MAX_SAFE_INT) || rounded < static_cast<double>(alp::MIN_SAFE_INT))
        return {0, false};
    int64_t encoded = static_cast<int64_t>(rounded);
    if (fac > 0) encoded = encoded / static_cast<int64_t>(alp::FACT_ARR[fac]);
    double decoded = static_cast<double>(encoded) * alp::FRAC_ARR[fac] / alp::FACT_ARR[exp];
    return {encoded, decoded == value};
}

struct BestPair { uint8_t exp = 0, fac = 0; size_t exceptions = SIZE_MAX; };

BestPair findBestExpFac(const double* values, size_t count) {
    const size_t sample_size = std::min(count, alp::ALP_SAMPLE_SIZE);
    std::vector<size_t> sample_indices(sample_size);
    if (sample_size == count) {
        for (size_t i = 0; i < count; ++i) sample_indices[i] = i;
    } else {
        for (size_t i = 0; i < sample_size; ++i) sample_indices[i] = (i * count) / sample_size;
    }

    BestPair best;
    for (uint8_t exp = 0; exp < alp::EXP_COUNT; ++exp) {
        for (uint8_t fac = 0; fac <= exp; ++fac) {
            size_t exceptions = 0;
            for (size_t idx : sample_indices) {
                double v = values[idx];
                if (std::isnan(v) || std::isinf(v) || (v == 0.0 && std::signbit(v))) { exceptions++; continue; }
                if (!scaleValue(v, exp, fac).exact) exceptions++;
            }
            if (exceptions < best.exceptions) { best = {exp, fac, exceptions}; }
            if (exceptions == 0) return best;
        }
    }
    return best;
}

uint8_t requiredBitWidth(int64_t min_val, int64_t max_val) {
    if (min_val == max_val) return 0;
    uint64_t range = static_cast<uint64_t>(max_val) - static_cast<uint64_t>(min_val);
    return range == 0 ? 0 : static_cast<uint8_t>(64 - __builtin_clzll(range));
}

ts::Buffer alpEncode(const double* values, size_t total_values) {
    ts::Buffer buf;
    if (total_values == 0) return buf;

    const size_t num_blocks = (total_values + alp::ALP_VECTOR_SIZE - 1) / alp::ALP_VECTOR_SIZE;
    const size_t tail_count = total_values % alp::ALP_VECTOR_SIZE;

    auto best = findBestExpFac(values, total_values);
    double exception_rate = static_cast<double>(best.exceptions) / std::min(total_values, alp::ALP_SAMPLE_SIZE);
    uint8_t scheme = (exception_rate > alp::ALP_RD_EXCEPTION_THRESHOLD) ? alp::SCHEME_ALP_RD : alp::SCHEME_ALP;

    // Stream header — must match server's ALP format exactly:
    //   header0: [0:31] magic, [32:63] total_values
    //   header1: [0:15] num_blocks, [16:31] tail_count, [32:39] scheme
    if (num_blocks > UINT16_MAX)
        throw std::runtime_error("ALP: num_blocks exceeds 16-bit header capacity");
    uint64_t header0 = static_cast<uint64_t>(alp::ALP_MAGIC) | (static_cast<uint64_t>(total_values) << 32);
    buf.appendWord(header0);
    uint64_t header1 = static_cast<uint64_t>(num_blocks) |
                       (static_cast<uint64_t>(tail_count) << 16) |
                       (static_cast<uint64_t>(scheme) << 32);
    buf.appendWord(header1);

    if (scheme == alp::SCHEME_ALP) {
        const uint8_t exp = best.exp, fac = best.fac;
        std::vector<int64_t> encoded(alp::ALP_VECTOR_SIZE);
        std::vector<uint16_t> exc_positions;
        std::vector<uint64_t> exc_values;
        std::vector<uint64_t> packed;

        for (size_t block = 0; block < num_blocks; ++block) {
            const size_t block_start = block * alp::ALP_VECTOR_SIZE;
            const size_t block_count = (block == num_blocks - 1 && tail_count > 0) ? tail_count : alp::ALP_VECTOR_SIZE;
            encoded.resize(block_count);
            exc_positions.clear(); exc_values.clear();

            int64_t min_val = std::numeric_limits<int64_t>::max();
            int64_t max_val = std::numeric_limits<int64_t>::min();

            for (size_t i = 0; i < block_count; ++i) {
                double v = values[block_start + i];
                bool is_special = std::isnan(v) || std::isinf(v) || (v == 0.0 && std::signbit(v));
                if (!is_special) {
                    auto result = scaleValue(v, exp, fac);
                    if (result.exact) {
                        encoded[i] = result.encoded;
                        if (result.encoded < min_val) min_val = result.encoded;
                        if (result.encoded > max_val) max_val = result.encoded;
                        continue;
                    }
                }
                exc_positions.push_back(static_cast<uint16_t>(i));
                uint64_t bits; std::memcpy(&bits, &v, 8);
                exc_values.push_back(bits);
                encoded[i] = 0;
            }

            if (min_val > max_val) { min_val = 0; max_val = 0; }
            for (auto pos : exc_positions) encoded[pos] = min_val;

            const uint8_t bw = requiredBitWidth(min_val, max_val);
            const uint16_t exception_count = static_cast<uint16_t>(exc_positions.size());

            uint64_t bh0 = static_cast<uint64_t>(exp) | (static_cast<uint64_t>(fac) << 8) |
                           (static_cast<uint64_t>(bw) << 16) | (static_cast<uint64_t>(exception_count) << 32) |
                           (static_cast<uint64_t>(block_count) << 48);
            buf.appendWord(bh0);
            uint64_t base_bits; std::memcpy(&base_bits, &min_val, 8);
            buf.appendWord(base_bits);

            size_t packed_words = alp::ffor_packed_words(block_count, bw);
            if (packed_words > 0) {
                packed.assign(packed_words, 0);
                alp::ffor_pack(encoded.data(), block_count, min_val, bw, packed.data());
                buf.append(packed.data(), packed_words * 8);
            }

            if (exception_count > 0) {
                size_t pos_words = (exception_count * 2 + 7) / 8;
                for (size_t w = 0; w < pos_words; ++w) {
                    uint64_t word = 0;
                    for (size_t j = 0; j < 4; ++j) {
                        size_t idx = w * 4 + j;
                        if (idx < exception_count) word |= static_cast<uint64_t>(exc_positions[idx]) << (j * 16);
                    }
                    buf.appendWord(word);
                }
                buf.append(exc_values.data(), exception_count * 8);
            }
        }
    } else {
        // ALP_RD
        uint8_t right_bit_count = findBestSplit(values, total_values);
        std::vector<int64_t> left_as_i64(alp::ALP_VECTOR_SIZE);
        std::vector<uint64_t> left_packed, right_packed;

        for (size_t block = 0; block < num_blocks; ++block) {
            const size_t block_start = block * alp::ALP_VECTOR_SIZE;
            const size_t block_count = (block == num_blocks - 1 && tail_count > 0) ? tail_count : alp::ALP_VECTOR_SIZE;

            auto rd = encodeRDBlock(values + block_start, block_count, right_bit_count);
            const uint16_t exception_count = static_cast<uint16_t>(rd.exception_positions.size());

            uint64_t bh0 = static_cast<uint64_t>(rd.right_bw) | (static_cast<uint64_t>(rd.left_bw) << 8) |
                           (static_cast<uint64_t>(rd.dictionary.size()) << 16) |
                           (static_cast<uint64_t>(right_bit_count) << 24) |
                           (static_cast<uint64_t>(exception_count) << 32) | (static_cast<uint64_t>(block_count) << 48);
            buf.appendWord(bh0);
            buf.appendWord(rd.right_for_base);

            if (!rd.dictionary.empty()) buf.append(rd.dictionary.data(), rd.dictionary.size() * 8);

            if (rd.left_bw > 0) {
                left_as_i64.resize(block_count);
                for (size_t i = 0; i < block_count; ++i) left_as_i64[i] = static_cast<int64_t>(rd.left_indices[i]);
                size_t lw = alp::ffor_packed_words(block_count, rd.left_bw);
                left_packed.assign(lw, 0);
                alp::ffor_pack(left_as_i64.data(), block_count, 0, rd.left_bw, left_packed.data());
                buf.append(left_packed.data(), lw * 8);
            }
            if (rd.right_bw > 0) {
                size_t rw = alp::ffor_packed_words(block_count, rd.right_bw);
                right_packed.assign(rw, 0);
                alp::ffor_pack_u64(rd.right_parts.data(), block_count, rd.right_for_base, rd.right_bw, right_packed.data());
                buf.append(right_packed.data(), rw * 8);
            }
            if (exception_count > 0) {
                size_t pos_words = (exception_count * 2 + 7) / 8;
                for (size_t w = 0; w < pos_words; ++w) {
                    uint64_t word = 0;
                    for (size_t j = 0; j < 4; ++j) {
                        size_t idx = w * 4 + j;
                        if (idx < exception_count) word |= static_cast<uint64_t>(rd.exception_positions[idx]) << (j * 16);
                    }
                    buf.appendWord(word);
                }
                buf.append(rd.exception_values.data(), exception_count * 8);
            }
        }
    }
    return buf;
}

// ============================================================================
// ALP Decoder
// ============================================================================

std::vector<double> alpDecode(const uint8_t* data, size_t len) {
    if (len < 16) throw std::runtime_error("ALP: data too short");
    ts::Slice s(data, len);

    uint64_t header0 = s.readWord();
    uint64_t header1 = s.readWord();
    uint32_t magic = static_cast<uint32_t>(header0 & 0xFFFFFFFF);
    if (magic != alp::ALP_MAGIC) throw std::runtime_error("ALP: invalid magic");

    uint32_t total_values = static_cast<uint32_t>(header0 >> 32);
    uint16_t num_blocks = static_cast<uint16_t>(header1 & 0xFFFF);
    uint8_t scheme = static_cast<uint8_t>((header1 >> 32) & 0xFF);

    std::vector<double> out;
    out.reserve(total_values);

    // Scratch buffers
    std::vector<uint64_t> packed_data(1024);
    std::vector<int64_t> decoded_ints(1024);

    for (uint16_t block = 0; block < num_blocks; ++block) {
        if (scheme == alp::SCHEME_ALP || scheme == alp::SCHEME_ALP_DELTA) {
            uint64_t bh0 = s.readWord();
            uint64_t bh1 = s.readWord();

            uint8_t exp = static_cast<uint8_t>(bh0 & 0xFF);
            uint8_t fac = static_cast<uint8_t>((bh0 >> 8) & 0xFF);
            uint8_t bw = static_cast<uint8_t>((bh0 >> 16) & 0xFF);
            uint16_t exception_count = static_cast<uint16_t>((bh0 >> 32) & 0xFFFF);
            uint16_t block_count = static_cast<uint16_t>((bh0 >> 48) & 0xFFFF);
            int64_t for_base; std::memcpy(&for_base, &bh1, 8);

            int64_t first_value = 0;
            if (scheme == alp::SCHEME_ALP_DELTA) {
                uint64_t fv = s.readWord();
                std::memcpy(&first_value, &fv, 8);
            }

            size_t packed_words = alp::ffor_packed_words(block_count, bw);
            packed_data.resize(packed_words);
            if (packed_words > 0) {
                const uint64_t* pw = s.readWords(packed_words);
                std::memcpy(packed_data.data(), pw, packed_words * 8);
            }
            decoded_ints.resize(block_count);
            alp::ffor_unpack(packed_data.data(), block_count, for_base, bw, decoded_ints.data());

            // Read exceptions
            std::vector<uint16_t> exc_pos(exception_count);
            std::vector<uint64_t> exc_vals(exception_count);
            if (exception_count > 0) {
                size_t pos_words = (exception_count * 2 + 7) / 8;
                const uint64_t* pw = s.readWords(pos_words);
                for (size_t w = 0; w < pos_words; ++w) {
                    size_t base_idx = w * 4;
                    size_t rem = exception_count - base_idx;
                    size_t cnt = rem < 4 ? rem : 4;
                    for (size_t j = 0; j < cnt; ++j)
                        exc_pos[base_idx + j] = static_cast<uint16_t>((pw[w] >> (j * 16)) & 0xFFFF);
                }
                const uint64_t* vw = s.readWords(exception_count);
                std::memcpy(exc_vals.data(), vw, exception_count * 8);
            }

            // Delta reconstruction
            if (scheme == alp::SCHEME_ALP_DELTA) {
                int64_t running = first_value;
                size_t exc_scan = 0;
                bool is_first = true;
                for (size_t i = 0; i < block_count; ++i) {
                    if (exc_scan < exception_count && exc_pos[exc_scan] == i) { exc_scan++; continue; }
                    if (is_first) { decoded_ints[i] = first_value; running = first_value; is_first = false; }
                    else {
                        uint64_t zz = static_cast<uint64_t>(decoded_ints[i]);
                        int64_t delta = static_cast<int64_t>((zz >> 1) ^ -(zz & 1));
                        running += delta;
                        decoded_ints[i] = running;
                    }
                }
            }

            // Convert to doubles
            double frac_val = alp::FRAC_ARR[fac];
            double fact_val = alp::FACT_ARR[exp];
            size_t exc_idx = 0;
            for (size_t i = 0; i < block_count; ++i) {
                if (exc_idx < exception_count && exc_pos[exc_idx] == i) {
                    double v; std::memcpy(&v, &exc_vals[exc_idx], 8);
                    out.push_back(v);
                    exc_idx++;
                } else {
                    out.push_back(static_cast<double>(decoded_ints[i]) * frac_val / fact_val);
                }
            }
        } else if (scheme == alp::SCHEME_ALP_RD) {
            uint64_t bh0 = s.readWord();
            uint64_t bh1 = s.readWord();

            uint8_t right_bw = static_cast<uint8_t>(bh0 & 0xFF);
            uint8_t left_bw = static_cast<uint8_t>((bh0 >> 8) & 0xFF);
            uint8_t dict_size = static_cast<uint8_t>((bh0 >> 16) & 0xFF);
            uint8_t right_bit_count = static_cast<uint8_t>((bh0 >> 24) & 0xFF);
            uint16_t exception_count = static_cast<uint16_t>((bh0 >> 32) & 0xFFFF);
            uint16_t block_count = static_cast<uint16_t>((bh0 >> 48) & 0xFFFF);
            uint64_t right_for_base = bh1;

            std::vector<uint64_t> dictionary(dict_size);
            for (size_t i = 0; i < dict_size; ++i) dictionary[i] = s.readWord();

            // Left indices
            std::vector<int64_t> left_indices(block_count, 0);
            if (left_bw > 0) {
                size_t lw = alp::ffor_packed_words(block_count, left_bw);
                const uint64_t* pw = s.readWords(lw);
                std::vector<uint64_t> lp(lw);
                std::memcpy(lp.data(), pw, lw * 8);
                alp::ffor_unpack(lp.data(), block_count, 0, left_bw, left_indices.data());
            }

            // Right parts
            std::vector<uint64_t> right_parts(block_count, right_for_base);
            if (right_bw > 0) {
                size_t rw = alp::ffor_packed_words(block_count, right_bw);
                const uint64_t* pw = s.readWords(rw);
                std::vector<uint64_t> rp(rw);
                std::memcpy(rp.data(), pw, rw * 8);
                alp::ffor_unpack_u64(rp.data(), block_count, right_for_base, right_bw, right_parts.data());
            }

            // Exceptions
            std::vector<uint16_t> exc_pos(exception_count);
            std::vector<uint64_t> exc_vals(exception_count);
            if (exception_count > 0) {
                size_t pos_words = (exception_count * 2 + 7) / 8;
                const uint64_t* pw = s.readWords(pos_words);
                for (size_t w = 0; w < pos_words; ++w) {
                    size_t base_idx = w * 4;
                    size_t rem = exception_count - base_idx;
                    size_t cnt = rem < 4 ? rem : 4;
                    for (size_t j = 0; j < cnt; ++j)
                        exc_pos[base_idx + j] = static_cast<uint16_t>((pw[w] >> (j * 16)) & 0xFFFF);
                }
                const uint64_t* vw = s.readWords(exception_count);
                std::memcpy(exc_vals.data(), vw, exception_count * 8);
            }

            // Reconstruct doubles
            const uint64_t right_mask = (right_bit_count >= 64) ? ~0ULL : ((1ULL << right_bit_count) - 1);
            size_t exc_idx = 0;
            for (size_t i = 0; i < block_count; ++i) {
                if (exc_idx < exception_count && exc_pos[exc_idx] == i) {
                    double v; std::memcpy(&v, &exc_vals[exc_idx], 8);
                    out.push_back(v);
                    exc_idx++;
                } else {
                    auto lidx = static_cast<uint8_t>(left_indices[i]);
                    if (lidx >= dict_size) throw std::runtime_error("ALP_RD: dictionary index out of range");
                    uint64_t left = dictionary[lidx];
                    uint64_t right = right_parts[i] & right_mask;
                    uint64_t combined = (left << right_bit_count) | right;
                    double v; std::memcpy(&v, &combined, 8);
                    out.push_back(v);
                }
            }
        }
    }
    return out;
}

static void releaseTsBuffer(Napi::Env, void*, void* hint) {
    delete static_cast<ts::Buffer*>(hint);
}
static void releaseVecDouble(Napi::Env, void*, void* hint) {
    delete static_cast<std::vector<double>*>(hint);
}

} // anonymous namespace

// =============================================================================
// N-API bindings — [H1 fix] zero-copy returns
// =============================================================================

static Napi::Value DoubleEncode(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 1 || !info[0].IsBuffer()) {
        Napi::TypeError::New(env, "Expected Buffer of float64 values").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    auto buf = info[0].As<Napi::Buffer<uint8_t>>();
    size_t count = buf.Length() / 8;
    const double* values = reinterpret_cast<const double*>(buf.Data());

    auto* encoded = new ts::Buffer(alpEncode(values, count));
    return Napi::Buffer<uint8_t>::New(env, encoded->data(), encoded->size(), releaseTsBuffer, encoded);
}

static Napi::Value DoubleDecode(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 1 || !info[0].IsBuffer()) {
        Napi::TypeError::New(env, "Expected Buffer").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    auto buf = info[0].As<Napi::Buffer<uint8_t>>();

    auto* decoded = new std::vector<double>(alpDecode(buf.Data(), buf.Length()));
    size_t byte_size = decoded->size() * sizeof(double);
    return Napi::Buffer<uint8_t>::New(env, reinterpret_cast<uint8_t*>(decoded->data()), byte_size, releaseVecDouble, decoded);
}

Napi::Object InitALP(Napi::Env env, Napi::Object exports) {
    exports.Set("doubleEncode", Napi::Function::New(env, DoubleEncode));
    exports.Set("doubleDecode", Napi::Function::New(env, DoubleDecode));
    return exports;
}
