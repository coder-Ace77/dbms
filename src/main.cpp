#include <iostream>
#include <cassert>
#include <string>
#include <vector>
#include <memory>
#include <cstdio>
#include <cstring>

// Storage Engine
#include "storage_engine/config/config.h"
#include "storage_engine/disk_manager/disk_manager.h"
#include "storage_engine/buffer/buffer_pool.h"
#include "storage_engine/serializer/serializer.h"
#include "storage_engine/page/slotted_page.h"
#include "storage_engine/page/free_space_map.h"
#include "storage_engine/common/bson_types.h"

// Data Organisation
#include "data_organisation/heap_file/heap_file.h"
#include "data_organisation/bptree/bptree.h"

// Execution Engine
#include "execution_engine/catalog/catalog.h"
#include "execution_engine/executor/executor.h"
#include "execution_engine/executor/seq_scan.h"
#include "execution_engine/executor/filter.h"
#include "execution_engine/executor/index_scan.h"

// Concurrency & Recovery
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "recovery/wal.h"
#include "recovery/recovery_manager.h"

// CLI
#include "cli/cli.h"

// Server
#include "server/server.h"

// ============================================================================
// Helper: print a BSON document
// ============================================================================
void PrintDoc(const BsonDocument& doc) {
    std::cout << "{ ";
    bool first = true;
    for (const auto& [key, val] : doc.elements) {
        if (!first) std::cout << ", ";
        first = false;
        std::cout << "\"" << key << "\": ";
        if (std::holds_alternative<std::string>(val)) {
            std::cout << "\"" << std::get<std::string>(val) << "\"";
        } else if (std::holds_alternative<int32_t>(val)) {
            std::cout << std::get<int32_t>(val);
        } else if (std::holds_alternative<int64_t>(val)) {
            std::cout << std::get<int64_t>(val) << "L";
        } else if (std::holds_alternative<double>(val)) {
            std::cout << std::get<double>(val);
        } else if (std::holds_alternative<bool>(val)) {
            std::cout << (std::get<bool>(val) ? "true" : "false");
        } else {
            std::cout << "<complex>";
        }
    }
    std::cout << " }" << std::endl;
}

// ============================================================================
// Main — Integration Test Driver
// ============================================================================
int run_tests() {
    // Clean up any previous test files
    std::remove("test_docdb.db");
    std::remove("test_wal.log");

    std::cout << "========================================" << std::endl;
    std::cout << "  DocDB Engine — Integration Test" << std::endl;
    std::cout << "========================================" << std::endl;

    // ---- 1. Initialize Storage Engine ----
    std::cout << "\n--- Phase 1: Storage Engine Init ---" << std::endl;

    DBConfigs config;
    config.db_file_name = "test_docdb.db";
    config.page_size = 4096;

    DiskManager disk_manager(config);
    BufferPoolManager bpm(64, &disk_manager);  // 64 page buffer pool

    std::cout << "✓ DiskManager + BufferPoolManager initialized" << std::endl;

    // ---- 2. Test BSON Serialization ----
    std::cout << "\n--- Phase 1: BSON Serialization ---" << std::endl;

    BsonDocument test_doc;
    test_doc.Add("name", std::string("Alice"));
    test_doc.Add("age", int32_t(30));
    test_doc.Add("score", 95.5);
    test_doc.Add("active", true);

    auto serialized = BsonSerializer::Serialize(test_doc);
    auto deserialized = BsonSerializer::Deserialize(serialized);
    assert(std::get<std::string>(deserialized.elements["name"]) == "Alice");
    assert(std::get<int32_t>(deserialized.elements["age"]) == 30);
    std::cout << "✓ BSON serialize/deserialize roundtrip passed" << std::endl;

    // ---- 3. Test Slotted Page ----
    std::cout << "\n--- Phase 1: Slotted Page ---" << std::endl;

    page_id_t test_page_id;
    Page* test_page = bpm.NewPage(&test_page_id);
    SlottedPage::Init(test_page->GetData());

    int16_t slot = SlottedPage::InsertRecord(test_page->GetData(), serialized.data(),
                                              static_cast<uint16_t>(serialized.size()));
    assert(slot >= 0);
    
    uint16_t rec_len;
    const uint8_t* rec_data = SlottedPage::GetRecord(test_page->GetData(), slot, &rec_len);
    assert(rec_data != nullptr);
    assert(rec_len == serialized.size());

    BsonDocument read_back = BsonSerializer::Deserialize(rec_data, rec_len);
    assert(std::get<std::string>(read_back.elements["name"]) == "Alice");
    std::cout << "✓ Slotted Page insert/get roundtrip passed" << std::endl;
    bpm.UnpinPage(test_page_id, true);

    // ---- 4. Test Catalog + Heap File ----
    std::cout << "\n--- Phase 2/3: Catalog + Heap File ---" << std::endl;

    Catalog catalog(&bpm);
    assert(catalog.CreateCollection("users"));
    assert(catalog.CreateCollection("products"));

    CollectionInfo* users = catalog.GetCollection("users");
    assert(users != nullptr);

    // Insert 20 documents
    std::vector<RecordID> inserted_rids;
    for (int i = 0; i < 20; i++) {
        BsonDocument doc;
        doc.Add("name", std::string("User_") + std::to_string(i));
        doc.Add("age", int32_t(20 + i));
        doc.Add("city", std::string(i < 10 ? "NYC" : "LA"));

        RecordID rid = users->heap_file->InsertRecord(doc);
        assert(rid.IsValid());
        inserted_rids.push_back(rid);
    }
    std::cout << "✓ Inserted 20 documents into 'users' collection" << std::endl;

    // Read back
    BsonDocument fetched = users->heap_file->GetRecord(inserted_rids[0]);
    assert(std::get<std::string>(fetched.elements["name"]) == "User_0");
    std::cout << "✓ Fetched first document: ";
    PrintDoc(fetched);

    // ---- 5. Test Sequential Scan ----
    std::cout << "\n--- Phase 3: Sequential Scan ---" << std::endl;

    SeqScanExecutor seq_scan(users->heap_file.get());
    seq_scan.Init();

    int count = 0;
    Tuple tuple;
    while (seq_scan.Next(&tuple)) {
        count++;
    }
    seq_scan.Close();
    assert(count == 20);
    std::cout << "✓ SeqScan found " << count << " records (expected 20)" << std::endl;

    // ---- 6. Test Filter ----
    std::cout << "\n--- Phase 3: Filter ---" << std::endl;

    auto child_scan = std::make_unique<SeqScanExecutor>(users->heap_file.get());
    Predicate pred;
    pred.field_name = "city";
    pred.op = CompareOp::EQ;
    pred.value = std::string("NYC");

    FilterExecutor filter(std::move(child_scan), {pred});
    filter.Init();

    count = 0;
    while (filter.Next(&tuple)) {
        assert(std::get<std::string>(tuple.doc.elements["city"]) == "NYC");
        count++;
    }
    filter.Close();
    assert(count == 10);
    std::cout << "✓ Filter(city=NYC) found " << count << " records (expected 10)" << std::endl;

    // ---- 7. Test B+ Tree Index + Index Scan ----
    std::cout << "\n--- Phase 2/3: B+ Tree Index + IndexScan ---" << std::endl;

    assert(catalog.CreateIndex("users", "name"));

    // Find the index
    IndexInfo* name_idx = nullptr;
    for (auto& idx : users->indexes) {
        if (idx.field_name == "name") {
            name_idx = &idx;
            break;
        }
    }
    assert(name_idx != nullptr);

    // Exact search via B+ Tree
    RecordID found = name_idx->btree->Search("User_5");
    assert(found.IsValid());
    BsonDocument found_doc = users->heap_file->GetRecord(found);
    assert(std::get<std::string>(found_doc.elements["name"]) == "User_5");
    std::cout << "✓ B+ Tree exact search for 'User_5': ";
    PrintDoc(found_doc);

    // Range scan via IndexScan executor
    IndexScanExecutor idx_scan(name_idx->btree.get(), users->heap_file.get(), "User_1", "User_3");
    idx_scan.Init();

    count = 0;
    std::cout << "  IndexScan [User_1, User_3]:" << std::endl;
    while (idx_scan.Next(&tuple)) {
        std::cout << "    ";
        PrintDoc(tuple.doc);
        count++;
    }
    idx_scan.Close();
    // User_1, User_10..User_19, User_2, User_3  (lexicographic order)
    std::cout << "✓ IndexScan found " << count << " records in range" << std::endl;

    // ---- 8. Test Delete ----
    std::cout << "\n--- Phase 2: Delete ---" << std::endl;

    bool deleted = users->heap_file->DeleteRecord(inserted_rids[0]);
    assert(deleted);
    
    // Verify it's gone via scan
    SeqScanExecutor verify_scan(users->heap_file.get());
    verify_scan.Init();
    count = 0;
    while (verify_scan.Next(&tuple)) { count++; }
    verify_scan.Close();
    assert(count == 19);
    std::cout << "✓ Deleted User_0, remaining records: " << count << " (expected 19)" << std::endl;

    // ---- 9. Test Transactions ----
    std::cout << "\n--- Phase 4: Transactions ---" << std::endl;

    LockManager lock_manager;
    TransactionManager txn_manager(&lock_manager);

    Transaction* txn1 = txn_manager.Begin();
    assert(txn1 != nullptr);
    assert(txn1->state == TransactionState::GROWING);

    // Simulate locking
    lock_manager.LockShared(txn1->txn_id, inserted_rids[1]);
    lock_manager.LockExclusive(txn1->txn_id, inserted_rids[2]);

    txn_manager.Commit(txn1);
    assert(txn1->state == TransactionState::COMMITTED);
    std::cout << "✓ Transaction lifecycle: BEGIN → LOCK → COMMIT" << std::endl;

    Transaction* txn2 = txn_manager.Begin();
    txn_manager.Abort(txn2);
    assert(txn2->state == TransactionState::ABORTED);
    std::cout << "✓ Transaction lifecycle: BEGIN → ABORT" << std::endl;

    // ---- 10. Test WAL ----
    std::cout << "\n--- Phase 4: WAL ---" << std::endl;

    WAL wal("test_wal.log");

    LogRecord log1;
    log1.txn_id = 100;
    log1.type = LogRecordType::BEGIN;
    log1.page_id = INVALID_PAGE_ID;
    log1.slot_id = 0;
    wal.AppendLogRecord(log1);

    LogRecord log2;
    log2.txn_id = 100;
    log2.type = LogRecordType::INSERT;
    log2.page_id = 5;
    log2.slot_id = 0;
    log2.after_image = {0x01, 0x02, 0x03};
    wal.AppendLogRecord(log2);

    LogRecord log3;
    log3.txn_id = 100;
    log3.type = LogRecordType::COMMIT;
    log3.page_id = INVALID_PAGE_ID;
    log3.slot_id = 0;
    wal.AppendLogRecord(log3);  // This triggers flush

    // Read back logs
    auto records = wal.ReadAllRecords();
    assert(records.size() == 3);
    assert(records[0].type == LogRecordType::BEGIN);
    assert(records[1].type == LogRecordType::INSERT);
    assert(records[2].type == LogRecordType::COMMIT);
    std::cout << "✓ WAL: wrote 3 log records, read back " << records.size() << std::endl;

    // ---- 11. Test Recovery ----
    std::cout << "\n--- Phase 4: Recovery Manager ---" << std::endl;

    RecoveryManager recovery(&wal, &bpm);
    recovery.Recover();
    std::cout << "✓ Recovery completed successfully" << std::endl;

    // ---- Summary ----
    std::cout << "\n========================================" << std::endl;
    std::cout << "  ALL TESTS PASSED ✓" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "\nComponents tested:" << std::endl;
    std::cout << "  Phase 1: DiskManager, BsonSerializer, SlottedPage, BufferPool" << std::endl;
    std::cout << "  Phase 2: FreeSpaceMap, HeapFile, B+Tree Index" << std::endl;
    std::cout << "  Phase 3: Catalog, SeqScan, Filter, IndexScan" << std::endl;
    std::cout << "  Phase 4: LockManager, TransactionManager, WAL, RecoveryManager" << std::endl;

    // Cleanup test files
    std::remove("test_docdb.db");
    std::remove("test_wal.log");

    return 0;
}

// ============================================================================
// Main — dispatch to CLI, server, or tests
// ============================================================================
int main(int argc, char* argv[]) {
    if (argc > 1 && std::strcmp(argv[1], "--test") == 0) {
        return run_tests();
    }

    if (argc > 1 && std::strcmp(argv[1], "--server") == 0) {
        int port = 6379;
        if (argc > 2) port = std::atoi(argv[2]);
        Server server("docdb_data.db", port);
        server.Start();
        return 0;
    }

    CLI cli;
    cli.Run();
    return 0;
}