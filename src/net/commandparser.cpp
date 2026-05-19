#include "net/commandparser.hpp"

#include <algorithm>
#include <cctype>
#include <sstream>

namespace distributed_cache {

ParseResult CommandParser::parse(const std::string& rawLine) const
{
    std::string line = trimTrailingCarriageReturn(rawLine);
    std::istringstream stream(line);

    std::string verb;
    stream >> verb;
    std::transform(verb.begin(), verb.end(), verb.begin(), [](unsigned char character) {
        return static_cast<char>(std::toupper(character));
    });

    if (verb == "PUT") {
        std::string key;
        stream >> key;

        std::string value;
        std::getline(stream, value);
        if (!value.empty() && value.front() == ' ') {
            value.erase(value.begin());
        }

        if (key.empty() || value.empty()) {
            return {false, {}, "PUT requires a key and value"};
        }

        return {true, {CommandType::Put, key, value}, {}};
    }

    if (verb == "GET") {
        std::string key;
        std::string extra;
        stream >> key >> extra;
        if (key.empty() || !extra.empty()) {
            return {false, {}, "GET requires exactly one key"};
        }
        return {true, {CommandType::Get, key, {}}, {}};
    }

    if (verb == "DELETE" || verb == "DEL") {
        std::string key;
        std::string extra;
        stream >> key >> extra;
        if (key.empty() || !extra.empty()) {
            return {false, {}, "DELETE requires exactly one key"};
        }
        return {true, {CommandType::Delete, key, {}}, {}};
    }

    if (verb == "EXISTS") {
        std::string key;
        std::string extra;
        stream >> key >> extra;
        if (key.empty() || !extra.empty()) {
            return {false, {}, "EXISTS requires exactly one key"};
        }
        return {true, {CommandType::Exists, key, {}}, {}};
    }

    if (verb == "SIZE") {
        std::string extra;
        stream >> extra;
        if (!extra.empty()) {
            return {false, {}, "SIZE does not accept arguments"};
        }
        return {true, {CommandType::Size, {}, {}}, {}};
    }

    return {false, {}, "unknown command"};
}

std::string CommandParser::trimTrailingCarriageReturn(std::string line)
{
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    return line;
}

} // namespace distributed_cache
