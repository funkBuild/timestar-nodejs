#pragma once
// ALP constants — ported from tsdb/lib/encoding/alp/alp_constants.hpp

#include <array>
#include <cstddef>
#include <cstdint>

namespace alp {

static constexpr uint32_t ALP_MAGIC = 0x414C5001;
static constexpr size_t ALP_VECTOR_SIZE = 1024;
static constexpr double ALP_RD_EXCEPTION_THRESHOLD = 0.50;
static constexpr size_t ALP_SAMPLE_SIZE = 256;
static constexpr uint8_t SCHEME_ALP = 0;
static constexpr uint8_t SCHEME_ALP_RD = 1;
static constexpr uint8_t SCHEME_ALP_DELTA = 2;
static constexpr uint8_t MAX_BIT_WIDTH = 64;
static constexpr size_t ALP_RD_MAX_DICT_SIZE = 8;

static constexpr std::array<double, 19> FACT_ARR = {
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
    1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
};
static constexpr std::array<double, 19> FRAC_ARR = {
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
    1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
};

static constexpr int64_t MAX_SAFE_INT = (1LL << 53);
static constexpr int64_t MIN_SAFE_INT = -(1LL << 53);
static constexpr size_t EXP_COUNT = 19;

} // namespace alp
