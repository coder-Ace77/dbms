#pragma once

#include "storage_engine/common/common.h"
#include "concurrency/lock_manager.h"
#include <cstdint>
#include <vector>
#include <string>
#include <fstream>
#include <mutex>

// ============================================================================
// Write-Ahead Log (WAL)
//
// Log records for crash recovery. Each record has:
//   LSN (Log Sequence Number), TXN ID, type, page_id, and data images.
//
// Types:
//   BEGIN    — transaction start
//   COMMIT   — transaction committed
//   ABORT    — transaction aborted
//   INSERT   — record inserted (after image)
//   DELETE   — record deleted (before image)
//   UPDATE   — record updated (before + after images)
// ============================================================================

using lsn_t = int64_t;
static constexpr lsn_t INVALID_LSN = -1;

enum class LogRecordType : uint8_t {
    BEGIN   = 0,
    COMMIT  = 1,
    ABORT   = 2,
    INSERT  = 3,
    DELETE  = 4,
    UPDATE  = 5
};

struct LogRecord {
    lsn_t lsn;
    txn_id_t txn_id;
    lsn_t prev_lsn;        // Previous LSN for this transaction
    LogRecordType type;
    page_id_t page_id;      // Affected page (-1 for BEGIN/COMMIT/ABORT)
    uint16_t slot_id;       // Affected slot

    // Data images (only for INSERT/DELETE/UPDATE)
    std::vector<uint8_t> before_image;   // DELETE, UPDATE
    std::vector<uint8_t> after_image;    // INSERT, UPDATE

    // Serialization
    std::vector<uint8_t> Serialize() const;
    static LogRecord Deserialize(const uint8_t* data, size_t size, size_t& offset);
};

class WAL {
public:
    explicit WAL(const std::string& log_file_name = "wal.log");
    ~WAL();

    // Append a log record. Returns the assigned LSN.
    lsn_t AppendLogRecord(LogRecord& record);

    // Force flush all buffered log records to disk
    void Flush();

    // Read all log records from the log file (for recovery)
    std::vector<LogRecord> ReadAllRecords();

    // Get the current LSN
    lsn_t GetCurrentLSN() const { return next_lsn_; }

    // Get the previous LSN for a transaction
    lsn_t GetPrevLSN(txn_id_t txn_id);

private:
    std::string log_file_name_;
    std::ofstream log_file_;
    std::mutex latch_;
    lsn_t next_lsn_{0};

    // Track prev_lsn per transaction
    std::unordered_map<txn_id_t, lsn_t> txn_prev_lsn_;

    // In-memory buffer for log records
    std::vector<uint8_t> buffer_;
};
