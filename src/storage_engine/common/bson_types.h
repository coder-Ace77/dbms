#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <variant>
#include <map>
#include <memory>

enum class BsonType : uint8_t{
    DOUBLE = 0x01,
    STRING = 0x02,
    DOCUMENT = 0x03,
    ARRAY = 0x04,
    BINARY = 0x05,
    BOOLEAN = 0x08,
    NULL_TYPE = 0x0A,
    INT32 = 0x10,
    INT64 = 0x12
};

struct BsonDocument;

using BsonValue = std::variant<
    double,
    std::string,
    std::shared_ptr<BsonDocument>, 
    bool,
    int32_t,
    int64_t,
    std::nullptr_t
>;

struct BsonDocument{
    std::map<std::string,BsonValue> elements;

    void Add(const std::string& key, BsonValue value) {
        elements[key] = value;
    }
};