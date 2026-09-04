#pragma once
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <chrono>

namespace decoder {

// LatestQueue: bounded queue that drops the oldest item
// when full instead of blocking the producer.
template <typename T>
class LatestQueue {
public:
    explicit LatestQueue(size_t maxsize = 1) : maxsize_(maxsize) {}

    // Puts an item into the queue, dropping the oldest item if full.
    void put_latest(T item) {
        std::lock_guard<std::mutex> lock(mutex_);
        while (queue_.size() >= maxsize_) {
            queue_.pop_front();
        }
        queue_.push_back(std::move(item));
        cv_.notify_one();
    }

    // Blocks up to timeout_seconds (if provided) waiting for an item.
    // Returns nullopt on timeout.
    std::optional<T> get(std::optional<double> timeout_seconds = std::nullopt) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (timeout_seconds.has_value()) {
            auto duration = std::chrono::duration<double>(*timeout_seconds);
            if (!cv_.wait_for(lock, duration, [this] { return !queue_.empty(); })) {
                return std::nullopt;
            }
        } else {
            cv_.wait(lock, [this] { return !queue_.empty(); });
        }
        T item = std::move(queue_.front());
        queue_.pop_front();
        return item;
    }

    // Non-blocking pop; returns nullopt if empty (mirrors queue.Queue.get_nowait()).
    std::optional<T> get_nowait() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return std::nullopt;
        }
        T item = std::move(queue_.front());
        queue_.pop_front();
        return item;
    }

private:
    size_t maxsize_;
    std::deque<T> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
};

} // namespace decoder
