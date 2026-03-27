#pragma once
// Thin buffer primitives replacing AlignedBuffer / Slice / CompressedBuffer
// from the tsdb codebase. No DMA alignment needed in Node.js context.

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

namespace ts {

// Simple growable byte buffer (replaces AlignedBuffer)
class Buffer {
public:
    Buffer() = default;
    explicit Buffer(size_t initial_capacity) { data_.reserve(initial_capacity); }

    void append(const void* src, size_t len) {
        const auto* bytes = static_cast<const uint8_t*>(src);
        data_.insert(data_.end(), bytes, bytes + len);
    }

    void appendWord(uint64_t word) {
        append(&word, sizeof(word));
    }

    void appendU32(uint32_t val) {
        append(&val, sizeof(val));
    }

    void appendU8(uint8_t val) {
        data_.push_back(val);
    }

    void resize(size_t n) { data_.resize(n); }
    void reserve(size_t n) { data_.reserve(n); }
    void clear() { data_.clear(); }

    uint8_t* data() { return data_.data(); }
    const uint8_t* data() const { return data_.data(); }
    size_t size() const { return data_.size(); }
    bool empty() const { return data_.empty(); }

    // Word-aligned access (used by FFOR)
    uint64_t* wordPtr(size_t byteOffset) {
        return reinterpret_cast<uint64_t*>(data_.data() + byteOffset);
    }

    // Ensure room for N more bytes, return pointer to start of reserved region
    uint8_t* grow(size_t n) {
        size_t pos = data_.size();
        data_.resize(pos + n);
        return data_.data() + pos;
    }

private:
    std::vector<uint8_t> data_;
};

// Read-only view over a byte buffer (replaces Slice)
class Slice {
public:
    Slice() : ptr_(nullptr), end_(nullptr) {}
    Slice(const uint8_t* data, size_t len) : ptr_(data), end_(data + len) {}

    uint64_t readWord() {
        if (ptr_ + 8 > end_) throw std::runtime_error("Slice: read past end");
        uint64_t val;
        std::memcpy(&val, ptr_, 8);
        ptr_ += 8;
        return val;
    }

    uint32_t readU32() {
        if (ptr_ + 4 > end_) throw std::runtime_error("Slice: read past end");
        uint32_t val;
        std::memcpy(&val, ptr_, 4);
        ptr_ += 4;
        return val;
    }

    uint8_t readU8() {
        if (ptr_ >= end_) throw std::runtime_error("Slice: read past end");
        return *ptr_++;
    }

    // Read N words into dest. Returns pointer to first word.
    const uint64_t* readWords(size_t n) {
        size_t bytes = n * 8;
        if (ptr_ + bytes > end_) throw std::runtime_error("Slice: read past end");
        const auto* result = reinterpret_cast<const uint64_t*>(ptr_);
        ptr_ += bytes;
        return result;
    }

    void skip(size_t n) {
        if (ptr_ + n > end_) throw std::runtime_error("Slice: skip past end");
        ptr_ += n;
    }

    const uint8_t* ptr() const { return ptr_; }
    size_t remaining() const { return static_cast<size_t>(end_ - ptr_); }
    bool empty() const { return ptr_ >= end_; }

private:
    const uint8_t* ptr_;
    const uint8_t* end_;
};

// Word-oriented compressed buffer (replaces CompressedBuffer for ALP)
class CompressedBuffer {
public:
    CompressedBuffer() = default;

    void writeWord(uint64_t word) { words_.push_back(word); }

    void writeBits(uint64_t value, unsigned int nbits) {
        if (nbits == 0) return;
        if (bitPos_ == 0) words_.push_back(0);
        auto& cur = words_.back();
        cur |= (value << bitPos_);
        if (bitPos_ + nbits >= 64) {
            words_.push_back(value >> (64 - bitPos_));
            bitPos_ = (bitPos_ + nbits) - 64;
        } else {
            bitPos_ += nbits;
        }
    }

    const uint64_t* data() const { return words_.data(); }
    size_t wordCount() const { return words_.size(); }
    size_t byteSize() const { return words_.size() * 8; }

    // Flatten to byte buffer
    Buffer toBuffer() const {
        Buffer buf(byteSize());
        buf.append(words_.data(), byteSize());
        return buf;
    }

private:
    std::vector<uint64_t> words_;
    unsigned int bitPos_ = 0;
};

// Read view over CompressedBuffer data
class CompressedSlice {
public:
    CompressedSlice(const uint64_t* data, size_t wordCount)
        : data_(data), count_(wordCount), pos_(0), bitPos_(0) {}

    uint64_t readWord() {
        if (pos_ >= count_) throw std::runtime_error("CompressedSlice: read past end");
        return data_[pos_++];
    }

    uint64_t readBits(unsigned int nbits) {
        if (nbits == 0) return 0;
        if (pos_ >= count_) throw std::runtime_error("CompressedSlice: read past end");
        uint64_t val = data_[pos_] >> bitPos_;
        if (bitPos_ + nbits >= 64) {
            pos_++;
            if (bitPos_ + nbits > 64 && pos_ < count_) {
                val |= data_[pos_] << (64 - bitPos_);
            }
            bitPos_ = (bitPos_ + nbits) - 64;
        } else {
            bitPos_ += nbits;
        }
        if (nbits < 64) val &= (1ULL << nbits) - 1;
        return val;
    }

    size_t remaining() const { return count_ - pos_; }

private:
    const uint64_t* data_;
    size_t count_;
    size_t pos_;
    unsigned int bitPos_;
};

} // namespace ts
