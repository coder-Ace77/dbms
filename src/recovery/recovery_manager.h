#pragma once

#include "recovery/wal.h"
#include "storage_engine/buffer/buffer_pool.h"
#include "storage_engine/page/slotted_page.h"
#include <unordered_map>
#include <unordered_set>
#include <vector>

// ============================================================================
// Recovery Manager — ARIES-style crash recovery
//
// Three phases:
//   1. Analysis: Scan log to find active transactions and dirty pages
//   2. Redo:     Replay all logged actions to bring data to crash state
//   3. Undo:     Roll back all uncommitted transactions
// ============================================================================

class RecoveryManager {
public:
    RecoveryManager(WAL* wal, BufferPoolManager* bpm);

    // Run full ARIES recovery
    void Recover();

private:
    // Phase 1: Build the active transaction table and dirty page table
    void AnalysisPhase(const std::vector<LogRecord>& records,
                       std::unordered_set<txn_id_t>& active_txns,
                       std::unordered_map<page_id_t, lsn_t>& dirty_pages);

    // Phase 2: Redo all actions from the log
    void RedoPhase(const std::vector<LogRecord>& records,
                   const std::unordered_map<page_id_t, lsn_t>& dirty_pages);

    // Phase 3: Undo all uncommitted transactions
    void UndoPhase(const std::vector<LogRecord>& records,
                   const std::unordered_set<txn_id_t>& active_txns);

    WAL* wal_;
    BufferPoolManager* bpm_;
};
