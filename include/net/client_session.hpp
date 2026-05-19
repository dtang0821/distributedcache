#pragma once

#include "cache/cachestore.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace distributed_cache {

#ifdef _WIN32
using SocketHandle = uintptr_t;
#else
using SocketHandle = int;
#endif

class ClientSession {
public:
    ClientSession(SocketHandle socket, std::shared_ptr<CacheStore> cache);

    void run();

private:
    bool readLine(std::string& line);
    bool sendResponse(const std::string& response);
    std::string executeCommand(const std::string& line);
    void closeSocket();

    SocketHandle socket_;
    std::shared_ptr<CacheStore> cache_;
};

} // namespace distributed_cache
