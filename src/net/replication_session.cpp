#include "net/replication_session.hpp"

#include "replication/replication_message.hpp"

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

ReplicationSession::ReplicationSession(SocketHandle socket,
                                       std::shared_ptr<ReplicationManager> replicationManager)
    : socket_(socket),
      replicationManager_(std::move(replicationManager))
{
}

void ReplicationSession::run()
{
    std::string line;
    while (readLine(line)) {
        handleLine(line);
    }
    closeSocket();
}

bool ReplicationSession::readLine(std::string& line)
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
        if (character != '\r') {
            line.push_back(character);
        }
        if (line.size() > 1024 * 1024) {
            return false;
        }
    }
}

bool ReplicationSession::sendLine(const std::string& line)
{
    const std::string payload = line + "\n";
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

void ReplicationSession::handleLine(const std::string& line)
{
    if (line.rfind("SYNC_FROM ", 0) == 0) {
        handleSyncFrom(line);
        return;
    }

    const auto message = ReplicationMessage::parse(line);
    if (!message.has_value()) {
        sendLine("ERROR invalid replication message");
        return;
    }

    replicationManager_->applyReplicaMessage(message.value());
    sendLine("ACK " + std::to_string(message->sequence));
}

void ReplicationSession::handleSyncFrom(const std::string& line)
{
    std::istringstream stream(line);
    std::string verb;
    std::uint64_t sequence = 0;
    stream >> verb >> sequence;

    const auto entries = replicationManager_->entriesAfter(sequence);
    for (const auto& entry : entries) {
        if (!sendLine(entry.serialize())) {
            return;
        }
    }
    sendLine("END " + std::to_string(replicationManager_->lastSequence()));
}

void ReplicationSession::closeSocket()
{
#ifdef _WIN32
    closesocket(toNativeSocket(socket_));
#else
    close(toNativeSocket(socket_));
#endif
}

} // namespace distributed_cache
