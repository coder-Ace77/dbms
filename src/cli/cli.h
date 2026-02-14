#pragma once

#include "execution_engine/catalog/catalog.h"
#include "execution_engine/executor/executor.h"
#include "execution_engine/executor/seq_scan.h"
#include "execution_engine/executor/filter.h"
#include "execution_engine/executor/index_scan.h"
#include "storage_engine/buffer/buffer_pool.h"
#include "storage_engine/disk_manager/disk_manager.h"
#include "storage_engine/serializer/serializer.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "recovery/wal.h"
#include "recovery/recovery_manager.h"

#include <string>
#include <sstream>
#include <vector>
#include <memory>

// ============================================================================
// CLI — Interactive command shell for DocDB
//
// Supported commands (MongoDB-style):
//
//   show collections
//   use <collection>                    — set active collection (auto-creates)
//   db.insert({ "key": "value", ... }) — insert a document
//   db.find()                          — find all documents
//   db.find({ "key": "value" })        — find with filter
//   db.delete({ "key": "value" })      — delete matching documents
//   db.update({ "key": "old" }, { "key": "new" })
//   db.createIndex("field")            — create B+ Tree index on field
//   db.count()                         — count all documents
//   db.drop()                          — drop current collection
//   help                               — show help
//   exit / quit                        — exit the shell
// ============================================================================

class CLI {
public:
    CLI(const std::string& db_file = "docdb_data.db");
    ~CLI();

    // Run the interactive REPL
    void Run();

private:
    // ---- Command handlers ----
    void HandleShowCollections();
    void HandleUse(const std::string& collection_name);
    void HandleInsert(const std::string& json_str);
    void HandleFind(const std::string& filter_str);
    void HandleDelete(const std::string& filter_str);
    void HandleUpdate(const std::string& filter_str, const std::string& update_str);
    void HandleCreateIndex(const std::string& field_name);
    void HandleCount();
    void HandleDrop();
    void HandleHelp();

    // ---- JSON mini-parser ----
    // Parses a flat JSON object like { "name": "Alice", "age": 30 }
    BsonDocument ParseJSON(const std::string& json);

    // Print a BsonDocument as JSON
    void PrintDoc(const BsonDocument& doc);

    // Parse predicates from a JSON filter
    std::vector<Predicate> ParseFilter(const BsonDocument& filter_doc);

    // ---- Utility ----
    std::string Trim(const std::string& s);
    std::string ExtractBetween(const std::string& s, char open, char close);

    // ---- Engine components ----
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<Catalog> catalog_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> txn_manager_;

    std::string current_collection_;
    bool running_;
};
