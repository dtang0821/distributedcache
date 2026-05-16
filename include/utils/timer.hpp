#pragma once

#include <chrono>

namespace distributed_cache {

class Timer {
public:
    using Clock = std::chrono::steady_clock;

    Timer() : start_(Clock::now()) {}

    void reset()
    {
        start_ = Clock::now();
    }

    double elapsedSeconds() const
    {
        const auto elapsed = Clock::now() - start_;
        return std::chrono::duration<double>(elapsed).count();
    }

    std::chrono::nanoseconds elapsedNanoseconds() const
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start_);
    }

private:
    Clock::time_point start_;
};

} // namespace distributed_cache
