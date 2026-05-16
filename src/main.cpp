#include "cache/cachestore.hpp"

#include <chrono>
#include <iostream>

int main()
{
    using namespace distributed_cache;
    using namespace std::chrono_literals;

    CacheStore cache(3);
    cache.put("alpha", "1");
    cache.put("beta", "2", 5s);

    if (const auto value = cache.get("alpha")) {
        std::cout << "alpha=" << value.value() << '\n';
    }

    std::cout << "cache size=" << cache.size() << '\n';
    return 0;
}
