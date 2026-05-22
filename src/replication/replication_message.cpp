#include "replication/replication_message.hpp"

#include <chrono>
#include <sstream>

namespace distributed_cache {

std::string ReplicationMessage::serialize() const
{
    std::ostringstream stream;
    if (operation == ReplicationOperation::Put) {
        stream << "REPL_PUT " << sequence << ' ' << key << ' ' << value << ' ' << timestampMillis;
    } else {
        stream << "REPL_DELETE " << sequence << ' ' << key << ' ' << timestampMillis;
    }
    return stream.str();
}

std::optional<ReplicationMessage> ReplicationMessage::parse(const std::string& line)
{
    std::istringstream stream(line);
    std::string verb;
    stream >> verb;

    if (verb == "REPL_PUT") {
        ReplicationMessage message;
        message.operation = ReplicationOperation::Put;
        stream >> message.sequence >> message.key;
        std::string remainder;
        std::getline(stream, remainder);
        if (!remainder.empty() && remainder.front() == ' ') {
            remainder.erase(remainder.begin());
        }

        const auto lastSpace = remainder.find_last_of(' ');
        if (message.sequence == 0 || message.key.empty() || lastSpace == std::string::npos) {
            return std::nullopt;
        }

        message.value = remainder.substr(0, lastSpace);
        try {
            message.timestampMillis = std::stoull(remainder.substr(lastSpace + 1));
        } catch (...) {
            return std::nullopt;
        }

        if (message.value.empty()) {
            return std::nullopt;
        }
        return message;
    }

    if (verb == "REPL_DELETE") {
        ReplicationMessage message;
        message.operation = ReplicationOperation::Delete;
        stream >> message.sequence >> message.key >> message.timestampMillis;
        if (message.sequence == 0 || message.key.empty()) {
            return std::nullopt;
        }
        return message;
    }

    return std::nullopt;
}

std::uint64_t ReplicationMessage::nowMillis()
{
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

} // namespace distributed_cache
