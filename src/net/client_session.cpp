#include "net/client_session.hpp"

#include "net/commandparser.hpp"

#include <sstream>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#else
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace distributed_cache {
namespace {

#ifdef _WIN32
SOCKET toNativeSocket(SocketHandle socket)
{
    return static_cast<SOCKET>(socket);
}
#else
int toNativeSocket(SocketHandle socket)
{
    return socket;
}
#endif

} // namespace

ClientSession::ClientSession(SocketHandle socket, std::shared_ptr<CacheStore> cache)
    : socket_(socket),
      cache_(std::move(cache))
{
}

void ClientSession::run()
{
    sendResponse("OK distributed-cache ready");

    std::string line;
    while (readLine(line)) {
        if (line == "QUIT" || line == "quit") {
            sendResponse("OK bye");
            break;
        }
        sendResponse(executeCommand(line));
    }

    closeSocket();
}

bool ClientSession::readLine(std::string& line)
{
    line.clear();
    char character = '\0';

    while (true) {
#ifdef _WIN32
        const int received = recv(toNativeSocket(socket_), &character, 1, 0);
#else
        const ssize_t received = recv(toNativeSocket(socket_), &character, 1, 0);
#endif
        if (received <= 0) {
            return false;
        }

        if (character == '\n') {
            return true;
        }

        line.push_back(character);
        if (line.size() > 1024 * 1024) {
            return false;
        }
    }
}

bool ClientSession::sendResponse(const std::string& response)
{
    const std::string payload = response + "\n";
    const char* data = payload.data();
    std::size_t remaining = payload.size();

    while (remaining > 0) {
#ifdef _WIN32
        const int sent = send(toNativeSocket(socket_), data, static_cast<int>(remaining), 0);
#else
        const ssize_t sent = send(toNativeSocket(socket_), data, remaining, 0);
#endif
        if (sent <= 0) {
            return false;
        }
        data += sent;
        remaining -= static_cast<std::size_t>(sent);
    }

    return true;
}

std::string ClientSession::executeCommand(const std::string& line)
{
    CommandParser parser;
    const auto parsed = parser.parse(line);
    if (!parsed.ok) {
        return "ERROR " + parsed.error;
    }

    const auto& command = parsed.command;
    switch (command.type) {
    case CommandType::Put:
        return cache_->put(command.key, command.value) ? "OK" : "ERROR cache capacity is zero";
    case CommandType::Get: {
        const auto value = cache_->get(command.key);
        if (!value.has_value()) {
            return "NOT_FOUND";
        }
        return "VALUE " + value.value();
    }
    case CommandType::Delete:
        return cache_->remove(command.key) ? "DELETED" : "NOT_FOUND";
    case CommandType::Exists:
        return cache_->exists(command.key) ? "YES" : "NO";
    case CommandType::Size:
        return "SIZE " + std::to_string(cache_->size());
    case CommandType::Unknown:
        break;
    }

    return "ERROR unknown command";
}

void ClientSession::closeSocket()
{
#ifdef _WIN32
    closesocket(toNativeSocket(socket_));
#else
    close(toNativeSocket(socket_));
#endif
}

} // namespace distributed_cache
