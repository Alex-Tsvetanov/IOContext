#pragma once
#include <vector>
#include <cstddef>
#include <span>
#include <cstring>

namespace xhttp {

class Buffer {
    std::vector<std::byte> data_;
    std::size_t r_{0};
public:
    explicit Buffer(std::size_t cap = 8192) : data_(cap) {}
    std::span<std::byte> write_span() { return { data_.data()+r_, data_.size()-r_ }; }
    std::span<const std::byte> read_span() const { return { data_.data(), r_ }; }
    std::size_t size() const { return r_; }
    void clear() { r_ = 0; }
    void ensure(std::size_t cap) { if (data_.size() < cap) data_.resize(cap); }
    void wrote(std::size_t n) { r_ += n; if (r_ > data_.size()) r_ = data_.size(); }
};

class OutQueue {
    std::vector<std::byte> buf_;
    std::size_t sent_{0};
public:
    void append(const void* p, std::size_t n) {
        auto old = buf_.size();
        buf_.resize(old+n);
        std::memcpy(buf_.data()+old, p, n);
    }
    std::span<const std::byte> unsent() const {
        return { buf_.data()+sent_, buf_.size()-sent_ };
    }
    void consumed(std::size_t n) {
        sent_ += n;
        if (sent_ >= buf_.size()) { buf_.clear(); sent_ = 0; }
    }
    bool empty() const { return buf_.empty() || sent_>=buf_.size(); }
    void clear() { buf_.clear(); sent_ = 0; }
};

} // namespace xhttp
