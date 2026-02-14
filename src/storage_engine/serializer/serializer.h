#pragma once
#include "storage_engine/common/bson_types.h"
#include <vector>
#include <cstring>
#include <stdexcept>

class BsonSerializer{
public:
    static std::vector<uint8_t> Serialize(const BsonDocument& doc);

    static BsonDocument Deserialize(const std::vector<uint8_t>& data);
    static BsonDocument Deserialize(const uint8_t* data,size_t size);

private:
    static void AppendInt32(std::vector<uint8_t>& buffer, int32_t value);
    static void AppendInt64(std::vector<uint8_t>& buffer, int64_t value);
    static void AppendDouble(std::vector<uint8_t>& buffer, double value);
    static void AppendString(std::vector<uint8_t>& buffer, const std::string& value);
    static void AppendCString(std::vector<uint8_t>& buffer, const std::string& value); 
    
    static int32_t ReadInt32(const uint8_t* data, size_t& offset);
    static int64_t ReadInt64(const uint8_t* data, size_t& offset);
    static double ReadDouble(const uint8_t* data, size_t& offset);
    static std::string ReadString(const uint8_t* data, size_t& offset);
    static std::string ReadCString(const uint8_t* data, size_t& offset);
};