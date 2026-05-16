#pragma once

#include <chrono>
#include <list>
#include <string>

namespace distributed_cache {

using Key = std::string;
using Value = std::string;

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
using Duration = Clock::duration;

using LruList = std::list<Key>;
using LruIterator = LruList::iterator;

} // namespace distributed_cache
