#pragma once

#include "common/types.hpp"

#include <optional>
#include <string>

namespace distributed_cache {

enum class CommandType {
    Put,
    Get,
    Delete,
    Exists,
    Size,
    Unknown
};

struct Command {
    CommandType type = CommandType::Unknown;
    Key key;
    Value value;
};

struct ParseResult {
    bool ok = false;
    Command command;
    std::string error;
};

class CommandParser {
public:
    ParseResult parse(const std::string& line) const;

private:
    static std::string trimTrailingCarriageReturn(std::string line);
};

} // namespace distributed_cache
