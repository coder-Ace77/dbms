#include "wal.h"
#include <iostream>
#include <stdexcept>
#include <cstring>

// ============================================================================
// LogRecord Serialization
//
// Format: [total_size(4)] [lsn(8)] [txn_id(8)] [prev_lsn(8)] [type(1)]
//         [page_id(4)] [slot_id(2)] [before_len(4)] [before_data...]
//         [after_len(4)] [after_data...]
// ============================================================================

std::vector<uint8_t> LogRecord::Serialize() const {
    std::vector<uint8_t> buf;

    // Placeholder for total size
    uint32_t placeholder = 0;
    buf.insert(buf.end(), reinterpret_cast<const uint8_t*>(&placeholder), 
               reinterpret_cast<const uint8_t*>(&placeholder) + 4);

    // LSN
    buf.insert(buf.end(), reinterpret_cast<const uint8_t*>(&lsn),
               reinterpret_cast<const uint8_t*>(&lsn) + 8);
    // txn_id
    buf.insert(buf.end(), reinterpret_cast<const uint8_t*>(&txn_id),
               reinterpret_cast<const uint8_t*>(&txn_id) + 8);
    // prev_lsn
    buf.insert(buf.end(), reinterpret_cast<const uint8_t*>(&prev_lsn),
               reinterpret_cast<const uint8_t*>(&prev_lsn) + 8);
    // type
    buf.push_back(static_cast<uint8_t>(type));
    // page_id
    buf.insert(buf.end(), reinterpret_cast<const uint8_t*>(&page_id),
               reinterpret_cast<const uint8_t*>(&page_id) + 4);
    // slot_id
    buf.insert(buf.end(), reinterpret_cast<const uint8_t*>(&slot_id),
               reinterpret_cast<const uint8_t*>(&slot_id) + 2);

    // before_image
    uint32_t before_len = static_cast<uint32_t>(before_image.size());
    buf.insert(buf.end(), reinterpret_cast<const uint8_t*>(&before_len),
               reinterpret_cast<const uint8_t*>(&before_len) + 4);
    buf.insert(buf.end(), before_image.begin(), before_image.end());

    // after_image
    uint32_t after_len = static_cast<uint32_t>(after_image.size());
    buf.insert(buf.end(), reinterpret_cast<const uint8_t*>(&after_len),
               reinterpret_cast<const uint8_t*>(&after_len) + 4);
    buf.insert(buf.end(), after_image.begin(), after_image.end());

    // Write total size at the beginning
    uint32_t total_size = static_cast<uint32_t>(buf.size());
    std::memcpy(buf.data(), &total_size, 4);

    return buf;
}

LogRecord LogRecord::Deserialize(const uint8_t* data, size_t size, size_t& offset) {
    LogRecord record;

    if (offset + 4 > size) throw std::runtime_error("WAL: Truncated log record (size)");
    uint32_t total_size;
    std::memcpy(&total_size, data + offset, 4);
    offset += 4;

    size_t start = offset;

    std::memcpy(&record.lsn, data + offset, 8); offset += 8;
    std::memcpy(&record.txn_id, data + offset, 8); offset += 8;
    std::memcpy(&record.prev_lsn, data + offset, 8); offset += 8;
    record.type = static_cast<LogRecordType>(data[offset]); offset += 1;
    std::memcpy(&record.page_id, data + offset, 4); offset += 4;
    std::memcpy(&record.slot_id, data + offset, 2); offset += 2;

    uint32_t before_len;
    std::memcpy(&before_len, data + offset, 4); offset += 4;
    record.before_image.assign(data + offset, data + offset + before_len);
    offset += before_len;

    uint32_t after_len;
    std::memcpy(&after_len, data + offset, 4); offset += 4;
    record.after_image.assign(data + offset, data + offset + after_len);
    offset += after_len;

    return record;
}

// ============================================================================
// WAL Implementation
// ============================================================================

WAL::WAL(const std::string& log_file_name) : log_file_name_(log_file_name) {
    log_file_.open(log_file_name_, std::ios::binary | std::ios::app);
    if (!log_file_.is_open()) {
        throw std::runtime_error("WAL: Failed to open log file: " + log_file_name_);
    }
}

WAL::~WAL() {
    if (!buffer_.empty()) {
        Flush();
    }
    if (log_file_.is_open()) {
        log_file_.close();
    }
}

lsn_t WAL::AppendLogRecord(LogRecord& record) {
    std::lock_guard<std::mutex> guard(latch_);

    record.lsn = next_lsn_++;
    record.prev_lsn = GetPrevLSN(record.txn_id);
    txn_prev_lsn_[record.txn_id] = record.lsn;

    std::vector<uint8_t> serialized = record.Serialize();
    buffer_.insert(buffer_.end(), serialized.begin(), serialized.end());

    // Force flush on COMMIT
    if (record.type == LogRecordType::COMMIT) {
        // Write buffer to file
        log_file_.write(reinterpret_cast<const char*>(buffer_.data()), buffer_.size());
        log_file_.flush();
        buffer_.clear();
    }

    return record.lsn;
}

void WAL::Flush() {
    std::lock_guard<std::mutex> guard(latch_);
    if (!buffer_.empty()) {
        log_file_.write(reinterpret_cast<const char*>(buffer_.data()), buffer_.size());
        log_file_.flush();
        buffer_.clear();
    }
}

std::vector<LogRecord> WAL::ReadAllRecords() {
    std::vector<LogRecord> records;

    std::ifstream in(log_file_name_, std::ios::binary | std::ios::ate);
    if (!in.is_open()) return records;

    size_t file_size = in.tellg();
    in.seekg(0);

    std::vector<uint8_t> data(file_size);
    in.read(reinterpret_cast<char*>(data.data()), file_size);

    size_t offset = 0;
    while (offset < file_size) {
        try {
            records.push_back(LogRecord::Deserialize(data.data(), file_size, offset));
        } catch (...) {
            break;  // Truncated or corrupt — stop
        }
    }

    return records;
}

lsn_t WAL::GetPrevLSN(txn_id_t txn_id) {
    auto it = txn_prev_lsn_.find(txn_id);
    if (it == txn_prev_lsn_.end()) return INVALID_LSN;
    return it->second;
}
