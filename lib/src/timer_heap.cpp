#include "xhttp/timer_heap.hpp"

namespace xhttp {

uint64_t TimerHeap::add_after(std::chrono::milliseconds dur, std::function<void()> fn) {
    Timer t;
    t.when = Clock::now() + dur;
    t.id = next_id_++;
    t.fn = std::move(fn);
    heap_.push_back(std::move(t));
    std::push_heap(heap_.begin(), heap_.end());
    return heap_.back().id;
}

void TimerHeap::cancel(uint64_t id) {
    for (auto &t : heap_) {
        if (t.id == id) {
            t.fn = []{}; // lazy cancel
            t.id = 0;
            break;
        }
    }
}

void TimerHeap::tick() {
    auto now = Clock::now();
    while (!heap_.empty()) {
        std::pop_heap(heap_.begin(), heap_.end());
        Timer t = std::move(heap_.back());
        heap_.pop_back();
        if (t.when > now) {
            heap_.push_back(std::move(t));
            std::push_heap(heap_.begin(), heap_.end());
            break;
        }
        if (t.fn) t.fn();
    }
}

TimePoint TimerHeap::next_deadline() const {
    if (heap_.empty()) return Clock::now() + std::chrono::hours(24);
    return heap_.front().when;
}

} // namespace xhttp
