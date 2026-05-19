#include "cache/cachestore.hpp"
#include "net/tcpserver.hpp"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

namespace {

std::size_t parseSizeArg(char* value, std::size_t fallback)
{
    if (value == nullptr) {
        return fallback;
    }

    char* end = nullptr;
    const auto parsed = std::strtoull(value, &end, 10);
    if (end == value || *end != '\0') {
        return fallback;
    }
    return static_cast<std::size_t>(parsed);
}

} // namespace

int main(int argc, char** argv)
{
    const std::string host = argc > 1 ? argv[1] : "0.0.0.0";
    const auto port = static_cast<std::uint16_t>(parseSizeArg(argc > 2 ? argv[2] : nullptr, 6379));
    const auto capacity = parseSizeArg(argc > 3 ? argv[3] : nullptr, 100000);
    const auto workers = parseSizeArg(argc > 4 ? argv[4] : nullptr, 4);

    try {
        auto cache = std::make_shared<distributed_cache::CacheStore>(capacity);
        distributed_cache::TcpServer server(host, port, cache, workers);
        server.run();
    } catch (const std::exception& error) {
        std::cerr << "server failed: " << error.what() << '\n';
        return 1;
    }

    return 0;
}
