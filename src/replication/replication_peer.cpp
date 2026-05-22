#include "replication/replication_peer.hpp"

#include <sstream>
#include <stdexcept>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <ws2tcpip.h>
#else
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace distributed_cache {
namespace {

#ifdef _WIN32
void closeSocket(SOCKET socket)
{
    closesocket(socket);
}
#else
void closeSocket(int socket)
{
    close(socket);
}
#endif

} // namespace

namespace {

#ifdef _WIN32
using NativeSocket = SOCKET;
constexpr NativeSocket invalidNativeSocket()
{
    return INVALID_SOCKET;
}
#else
using NativeSocket = int;
constexpr NativeSocket invalidNativeSocket()
{
    return -1;
}
#endif

bool sendAll(NativeSocket socketHandle, const std::string& payload)
{
    const char* data = payload.data();
    std::size_t remaining = payload.size();
    while (remaining > 0) {
#ifdef _WIN32
        const int sent = ::send(socketHandle, data, static_cast<int>(remaining), 0);
#else
        const ssize_t sent = ::send(socketHandle, data, remaining, 0);
#endif
        if (sent <= 0) {
            return false;
        }
        data += sent;
        remaining -= static_cast<std::size_t>(sent);
    }
    return true;
}

bool readLine(NativeSocket socketHandle, std::string& line)
{
    line.clear();
    char character = '\0';
    while (true) {
#ifdef _WIN32
        const int received = recv(socketHandle, &character, 1, 0);
#else
        const ssize_t received = recv(socketHandle, &character, 1, 0);
#endif
        if (received <= 0) {
            return false;
        }
        if (character == '\n') {
            return true;
        }
        if (character != '\r') {
            line.push_back(character);
        }
    }
}

NativeSocket connectToEndpoint(const std::string& host, std::uint16_t port)
{
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* rawResult = nullptr;
    const std::string portString = std::to_string(port);
    if (getaddrinfo(host.c_str(), portString.c_str(), &hints, &rawResult) != 0) {
        return invalidNativeSocket();
    }

    NativeSocket connected = invalidNativeSocket();
    for (addrinfo* address = rawResult; address != nullptr; address = address->ai_next) {
        const NativeSocket candidate = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
        if (candidate == invalidNativeSocket()) {
            continue;
        }
        if (connect(candidate, address->ai_addr, static_cast<int>(address->ai_addrlen)) == 0) {
            connected = candidate;
            break;
        }
        closeSocket(candidate);
    }

    freeaddrinfo(rawResult);
    return connected;
}

} // namespace

ReplicationPeer::ReplicationPeer(std::string host, std::uint16_t port)
    : host_(std::move(host)),
      port_(port)
{
}

bool ReplicationPeer::send(const ReplicationMessage& message)
{
#ifdef _WIN32
    WSADATA data;
    if (WSAStartup(MAKEWORD(2, 2), &data) != 0) {
        return false;
    }
#endif

    const auto socketHandle = connectToEndpoint(host_, port_);
    bool delivered = socketHandle != invalidNativeSocket() &&
                     sendAll(socketHandle, message.serialize() + "\n");
    if (socketHandle != invalidNativeSocket()) {
        closeSocket(socketHandle);
    }
    if (delivered) {
        markAcked(message.sequence);
    }
#ifdef _WIN32
    WSACleanup();
#endif
    return delivered;
}

const std::string& ReplicationPeer::host() const
{
    return host_;
}

std::uint16_t ReplicationPeer::port() const
{
    return port_;
}

std::uint64_t ReplicationPeer::lastAckedSequence() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return lastAckedSequence_;
}

void ReplicationPeer::markAcked(std::uint64_t sequence)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (sequence > lastAckedSequence_) {
        lastAckedSequence_ = sequence;
    }
}

std::vector<ReplicationMessage> ReplicationPeer::fetchFromLeader(const std::string& host,
                                                                 std::uint16_t port,
                                                                 std::uint64_t afterSequence)
{
#ifdef _WIN32
    WSADATA data;
    if (WSAStartup(MAKEWORD(2, 2), &data) != 0) {
        return {};
    }
#endif

    std::vector<ReplicationMessage> messages;
    const auto socketHandle = connectToEndpoint(host, port);
    if (socketHandle == invalidNativeSocket()) {
#ifdef _WIN32
        WSACleanup();
#endif
        return messages;
    }

    if (sendAll(socketHandle, "SYNC_FROM " + std::to_string(afterSequence) + "\n")) {
        std::string line;
        while (readLine(socketHandle, line)) {
            if (line.rfind("END ", 0) == 0) {
                break;
            }
            const auto parsed = ReplicationMessage::parse(line);
            if (parsed.has_value()) {
                messages.push_back(parsed.value());
            }
        }
    }

    closeSocket(socketHandle);
#ifdef _WIN32
    WSACleanup();
#endif
    return messages;
}

} // namespace distributed_cache
