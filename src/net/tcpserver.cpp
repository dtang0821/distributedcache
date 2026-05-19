#include "net/tcpserver.hpp"

#include "net/client_session.hpp"

#include <cstring>
#include <iostream>
#include <stdexcept>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace distributed_cache {
namespace {

#ifdef _WIN32
constexpr SocketHandle invalidSocket()
{
    return static_cast<SocketHandle>(INVALID_SOCKET);
}

SOCKET toNativeSocket(SocketHandle socket)
{
    return static_cast<SOCKET>(socket);
}

void closeNativeSocket(SocketHandle socket)
{
    if (socket != invalidSocket()) {
        closesocket(toNativeSocket(socket));
    }
}
#else
constexpr SocketHandle invalidSocket()
{
    return -1;
}

int toNativeSocket(SocketHandle socket)
{
    return socket;
}

void closeNativeSocket(SocketHandle socket)
{
    if (socket != invalidSocket()) {
        close(toNativeSocket(socket));
    }
}
#endif

} // namespace

TcpServer::TcpServer(std::string host,
                     std::uint16_t port,
                     std::shared_ptr<CacheStore> cache,
                     std::size_t workerThreads)
    : host_(std::move(host)),
      port_(port),
      cache_(std::move(cache)),
      threadPool_(workerThreads),
      listenSocket_(invalidSocket())
{
    if (!cache_) {
        throw std::invalid_argument("cache instance is required");
    }
}

TcpServer::~TcpServer()
{
    stop();
#ifdef _WIN32
    if (socketsInitialized_) {
        WSACleanup();
    }
#endif
}

void TcpServer::run()
{
    initializeSockets();
    listenSocket_ = createListenSocket();
    running_ = true;

    std::cout << "distributed-cache TCP server listening on "
              << host_ << ":" << port_ << '\n';

    while (running_) {
#ifdef _WIN32
        const SOCKET clientSocket = accept(toNativeSocket(listenSocket_), nullptr, nullptr);
        if (clientSocket == INVALID_SOCKET) {
#else
        const int clientSocket = accept(toNativeSocket(listenSocket_), nullptr, nullptr);
        if (clientSocket < 0) {
#endif
            if (running_) {
                std::cerr << "accept failed" << '\n';
            }
            continue;
        }

        const SocketHandle sessionSocket = static_cast<SocketHandle>(clientSocket);
        try {
            threadPool_.submit([sessionSocket, cache = cache_] {
                ClientSession session(sessionSocket, cache);
                session.run();
            });
        } catch (...) {
            closeNativeSocket(sessionSocket);
            throw;
        }
    }
}

void TcpServer::stop()
{
    running_ = false;
    closeListenSocket();
}

void TcpServer::initializeSockets()
{
#ifdef _WIN32
    if (socketsInitialized_) {
        return;
    }

    WSADATA data;
    const int result = WSAStartup(MAKEWORD(2, 2), &data);
    if (result != 0) {
        throw std::runtime_error("WSAStartup failed");
    }
#endif
    socketsInitialized_ = true;
}

SocketHandle TcpServer::createListenSocket()
{
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    addrinfo* rawResult = nullptr;
    const std::string port = std::to_string(port_);
    const char* host = host_.empty() ? nullptr : host_.c_str();
    const int lookup = getaddrinfo(host, port.c_str(), &hints, &rawResult);
    if (lookup != 0) {
        throw std::runtime_error("getaddrinfo failed");
    }

    SocketHandle created = invalidSocket();
    for (addrinfo* address = rawResult; address != nullptr; address = address->ai_next) {
#ifdef _WIN32
        const SOCKET candidate = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
        if (candidate == INVALID_SOCKET) {
#else
        const int candidate = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
        if (candidate < 0) {
#endif
            continue;
        }

        const int reuse = 1;
        setsockopt(candidate, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));

        if (bind(candidate, address->ai_addr, static_cast<int>(address->ai_addrlen)) == 0 &&
            listen(candidate, SOMAXCONN) == 0) {
            created = static_cast<SocketHandle>(candidate);
            break;
        }

#ifdef _WIN32
        closesocket(candidate);
#else
        close(candidate);
#endif
    }

    freeaddrinfo(rawResult);

    if (created == invalidSocket()) {
        throw std::runtime_error("failed to bind TCP listen socket");
    }

    return created;
}

void TcpServer::closeListenSocket()
{
    const SocketHandle socket = listenSocket_;
    listenSocket_ = invalidSocket();
    closeNativeSocket(socket);
}

} // namespace distributed_cache
