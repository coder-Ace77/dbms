#include "serializer.h"
#include<iostream>

// Format of bson serilized doc 
// sizefield1|field2|field3...null
// field - type|key|val strings end by null

std::vector<uint8_t> BsonSerializer::Serialize(const BsonDocument& doc){
    std::vector<uint8_t> buffer;

    // Total document size
    AppendInt32(buffer,0);


    //type,key,val
    for(auto& [key,value]:doc.elements){
        if(std::holds_alternative<double>(value)){

            buffer.push_back(static_cast<uint8_t>(BsonType::DOUBLE));
            AppendCString(buffer,key);
            AppendDouble(buffer,std::get<double>(value));

        }else if(std::holds_alternative<std::string>(value)){

            buffer.push_back(static_cast<uint8_t>(BsonType::STRING));
            AppendCString(buffer,key);
            AppendString(buffer,std::get<std::string>(value));

        }else if(std::holds_alternative<int32_t>(value)){

            buffer.push_back(static_cast<uint8_t>(BsonType::INT32));
            AppendCString(buffer, key);
            AppendInt32(buffer, std::get<int32_t>(value));
            
        }else if(std::holds_alternative<std::int64_t>(value)){

            buffer.push_back(static_cast<uint8_t>(BsonType::INT64));
            AppendCString(buffer, key);
            AppendInt64(buffer, std::get<int64_t>(value));
            
        }else if(std::holds_alternative<bool>(value)){
            
            buffer.push_back(static_cast<uint8_t>(BsonType::BOOLEAN));
            AppendCString(buffer, key);
            buffer.push_back(std::get<bool>(value) ? 0x01 : 0x00);

        }else if(std::holds_alternative<std::shared_ptr<BsonDocument>>(value)){

            buffer.push_back(static_cast<uint8_t>(BsonType::DOCUMENT));
            AppendCString(buffer,key);

            std::vector<uint8_t> sub_doc = Serialize(*std::get<std::shared_ptr<BsonDocument>>(value));
            buffer.insert(buffer.end(),sub_doc.begin(),sub_doc.end());
        }
    }

        buffer.push_back(0x00);

        // putting size again
        int32_t total_size = static_cast<int32_t>(buffer.size());
        std::memcpy(buffer.data(), &total_size, sizeof(int32_t));

        return buffer;
}

BsonDocument BsonSerializer::Deserialize(const std::vector<uint8_t>& data){
    return Deserialize(data.data(), data.size());
}

BsonDocument BsonSerializer::Deserialize(const uint8_t* data,size_t size){
    
    BsonDocument doc;
    size_t offset = 0;

    // 1. Read Total Size
    int32_t doc_size = ReadInt32(data, offset);
    if(doc_size > size)throw std::runtime_error("Corrupted BSON: Size mismatch");

    // 2. Loop until we hit the null terminator
    // last filed val is null = 0x00
    while (offset < (size_t)doc_size - 1){ 

        uint8_t type_byte = data[offset++];
        
        if(type_byte==0x00)break;

        std::string key=ReadCString(data, offset);

        switch(static_cast<BsonType>(type_byte)){
            case BsonType::INT32:
                doc.Add(key, ReadInt32(data, offset));
                break;
            case BsonType::INT64:
                doc.Add(key, ReadInt64(data, offset));
                break;
            case BsonType::DOUBLE:
                doc.Add(key, ReadDouble(data, offset));
                break;
            case BsonType::STRING:
                doc.Add(key, ReadString(data, offset));
                break;
            case BsonType::BOOLEAN:
                doc.Add(key, data[offset++] == 0x01);
                break;
            case BsonType::DOCUMENT: {                    
                int32_t sub_len;
                std::memcpy(&sub_len, data + offset, sizeof(int32_t));
                doc.Add(key, std::make_shared<BsonDocument>(Deserialize(data + offset, sub_len)));
                offset += sub_len;
                break;
            }
            default:
                throw std::runtime_error("Unknown BSON Type: " + std::to_string(type_byte));
        }
    }
    return doc;
}

void BsonSerializer::AppendInt32(std::vector<uint8_t> &buffer,int32_t value){

    uint8_t bytes[4];
    std::memcpy(bytes,&value,4);
    for(int i=0;i<4;i++){
        buffer.push_back(bytes[i]);
    }

}

void BsonSerializer::AppendInt64(std::vector<uint8_t>& buffer, int64_t value){
    uint8_t bytes[8];
    std::memcpy(bytes, &value, 8);
    for(int i=0; i<8; i++) buffer.push_back(bytes[i]);
}

void BsonSerializer::AppendDouble(std::vector<uint8_t>& buffer, double value) {
    uint8_t bytes[8];
    std::memcpy(bytes, &value, 8);
    for(int i=0; i<8; i++) buffer.push_back(bytes[i]);
}

void BsonSerializer::AppendString(std::vector<uint8_t>& buffer, const std::string& value){
    // BSON String: int32 (length including null) + bytes + 0x00
    AppendInt32(buffer, value.size() + 1);
    AppendCString(buffer, value);
}

void BsonSerializer::AppendCString(std::vector<uint8_t>& buffer, const std::string& value) {
    // Write characters
    for (char c : value) {
        buffer.push_back(static_cast<uint8_t>(c));
    }
    // Write Null Terminator
    buffer.push_back(0x00);
}


int32_t BsonSerializer::ReadInt32(const uint8_t* data, size_t& offset) {
    int32_t val;
    std::memcpy(&val, data + offset, 4);
    offset += 4;
    return val;
}

int64_t BsonSerializer::ReadInt64(const uint8_t* data, size_t& offset) {
    int64_t val;
    std::memcpy(&val, data + offset, 8);
    offset += 8;
    return val;
}

double BsonSerializer::ReadDouble(const uint8_t* data, size_t& offset) {
    double val;
    std::memcpy(&val, data + offset, 8);
    offset += 8;
    return val;
}

std::string BsonSerializer::ReadString(const uint8_t* data, size_t& offset) {
    int32_t len = ReadInt32(data, offset); // Length includes null terminator
    std::string str(reinterpret_cast<const char*>(data + offset), len - 1);
    offset += len;
    return str;
}

std::string BsonSerializer::ReadCString(const uint8_t* data, size_t& offset) {
    // Find null terminator
    size_t len = strlen(reinterpret_cast<const char*>(data + offset));
    std::string str(reinterpret_cast<const char*>(data + offset), len);
    offset += len + 1; // +1 for 0x00
    return str;
}
