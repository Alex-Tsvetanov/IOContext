#pragma once
#include <vector>
#include <chrono>
#include <cstdint>
#include <functional>
#include <algorithm>

namespace xhttp {

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

struct Timer {
    TimePoint when;
    uint64_t id;
    std::function<void()> fn;
    bool operator<(Timer const& other) const { return when > other.when; } // min-heap
};

class TimerHeap {
    std::vector<Timer> heap_;
    uint64_t next_id_{1};
public:
    uint64_t add_after(std::chrono::milliseconds dur, std::function<void()> fn);
    void cancel(uint64_t id);
    void tick();
    TimePoint next_deadline() const;
};

} // namespace xhttp
