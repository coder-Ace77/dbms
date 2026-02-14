#include "recovery_manager.h"
#include <iostream>
#include <algorithm>

// ============================================================================
// Constructor
// ============================================================================

RecoveryManager::RecoveryManager(WAL* wal, BufferPoolManager* bpm)
    : wal_(wal), bpm_(bpm) {}

// ============================================================================
// Recover — run all 3 ARIES phases
// ============================================================================

void RecoveryManager::Recover() {
    std::cout << "=== RECOVERY START ===" << std::endl;

    std::vector<LogRecord> records = wal_->ReadAllRecords();
    if (records.empty()) {
        std::cout << "No WAL records found. Clean start." << std::endl;
        return;
    }

    std::cout << "Read " << records.size() << " log records." << std::endl;

    // Phase 1: Analysis
    std::unordered_set<txn_id_t> active_txns;
    std::unordered_map<page_id_t, lsn_t> dirty_pages;
    AnalysisPhase(records, active_txns, dirty_pages);

    // Phase 2: Redo
    RedoPhase(records, dirty_pages);

    // Phase 3: Undo
    UndoPhase(records, active_txns);

    std::cout << "=== RECOVERY COMPLETE ===" << std::endl;
}

// ============================================================================
// Phase 1: Analysis
// ============================================================================

void RecoveryManager::AnalysisPhase(const std::vector<LogRecord>& records,
                                     std::unordered_set<txn_id_t>& active_txns,
                                     std::unordered_map<page_id_t, lsn_t>& dirty_pages) {
    std::cout << "[Analysis Phase]" << std::endl;

    for (const auto& record : records) {
        switch (record.type) {
            case LogRecordType::BEGIN:
                active_txns.insert(record.txn_id);
                break;

            case LogRecordType::COMMIT:
            case LogRecordType::ABORT:
                active_txns.erase(record.txn_id);
                break;

            case LogRecordType::INSERT:
            case LogRecordType::DELETE:
            case LogRecordType::UPDATE:
                active_txns.insert(record.txn_id);
                if (record.page_id != INVALID_PAGE_ID) {
                    if (dirty_pages.find(record.page_id) == dirty_pages.end()) {
                        dirty_pages[record.page_id] = record.lsn;
                    }
                }
                break;
        }
    }

    std::cout << "  Active transactions: " << active_txns.size() << std::endl;
    std::cout << "  Dirty pages: " << dirty_pages.size() << std::endl;
}

// ============================================================================
// Phase 2: Redo
// ============================================================================

void RecoveryManager::RedoPhase(const std::vector<LogRecord>& records,
                                 const std::unordered_map<page_id_t, lsn_t>& dirty_pages) {
    std::cout << "[Redo Phase]" << std::endl;

    int redo_count = 0;

    for (const auto& record : records) {
        if (record.type != LogRecordType::INSERT &&
            record.type != LogRecordType::DELETE &&
            record.type != LogRecordType::UPDATE) {
            continue;
        }

        if (record.page_id == INVALID_PAGE_ID) continue;

        // Check if page is in dirty page table and if record LSN >= first dirty LSN
        auto it = dirty_pages.find(record.page_id);
        if (it == dirty_pages.end()) continue;
        if (record.lsn < it->second) continue;

        // Redo the operation
        Page* page = bpm_->FetchPage(record.page_id);
        if (!page) continue;

        if (record.type == LogRecordType::INSERT) {
            // Re-insert the after image
            if (!record.after_image.empty()) {
                SlottedPage::InsertRecord(page->GetData(),
                    record.after_image.data(),
                    static_cast<uint16_t>(record.after_image.size()));
                redo_count++;
            }
        } else if (record.type == LogRecordType::DELETE) {
            SlottedPage::DeleteRecord(page->GetData(), record.slot_id);
            redo_count++;
        } else if (record.type == LogRecordType::UPDATE) {
            if (!record.after_image.empty()) {
                SlottedPage::UpdateRecord(page->GetData(), record.slot_id,
                    record.after_image.data(),
                    static_cast<uint16_t>(record.after_image.size()));
                redo_count++;
            }
        }

        bpm_->UnpinPage(record.page_id, true);
    }

    std::cout << "  Redone " << redo_count << " operations." << std::endl;
}

// ============================================================================
// Phase 3: Undo
// ============================================================================

void RecoveryManager::UndoPhase(const std::vector<LogRecord>& records,
                                 const std::unordered_set<txn_id_t>& active_txns) {
    std::cout << "[Undo Phase]" << std::endl;

    if (active_txns.empty()) {
        std::cout << "  No uncommitted transactions to undo." << std::endl;
        return;
    }

    int undo_count = 0;

    // Process log records in reverse order
    for (auto it = records.rbegin(); it != records.rend(); ++it) {
        const LogRecord& record = *it;

        if (active_txns.find(record.txn_id) == active_txns.end()) {
            continue;  // Not an active (uncommitted) transaction
        }

        if (record.page_id == INVALID_PAGE_ID) continue;

        Page* page = bpm_->FetchPage(record.page_id);
        if (!page) continue;

        if (record.type == LogRecordType::INSERT) {
            // Undo insert → delete
            SlottedPage::DeleteRecord(page->GetData(), record.slot_id);
            undo_count++;
        } else if (record.type == LogRecordType::DELETE) {
            // Undo delete → re-insert before image
            if (!record.before_image.empty()) {
                SlottedPage::InsertRecord(page->GetData(),
                    record.before_image.data(),
                    static_cast<uint16_t>(record.before_image.size()));
                undo_count++;
            }
        } else if (record.type == LogRecordType::UPDATE) {
            // Undo update → restore before image
            if (!record.before_image.empty()) {
                SlottedPage::UpdateRecord(page->GetData(), record.slot_id,
                    record.before_image.data(),
                    static_cast<uint16_t>(record.before_image.size()));
                undo_count++;
            }
        }

        bpm_->UnpinPage(record.page_id, true);
    }

    std::cout << "  Undone " << undo_count << " operations from "
              << active_txns.size() << " uncommitted transactions." << std::endl;
}
