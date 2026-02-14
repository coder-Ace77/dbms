#pragma once

#include "concurrency/lock_manager.h"
#include <atomic>
#include <unordered_set>
#include <mutex>
#include <vector>

// ============================================================================
// Transaction States
// ============================================================================
enum class TransactionState {
    GROWING,     // Acquiring locks (2PL growing phase)
    SHRINKING,   // Releasing locks (2PL shrinking phase — only at commit/abort)
    COMMITTED,
    ABORTED
};

// ============================================================================
// Transaction — represents a single transaction
// ============================================================================
struct Transaction {
    txn_id_t txn_id;
    TransactionState state;

    explicit Transaction(txn_id_t id)
        : txn_id(id), state(TransactionState::GROWING) {}
};

// ============================================================================
// Transaction Manager — coordinates Begin/Commit/Abort
// ============================================================================
class TransactionManager {
public:
    explicit TransactionManager(LockManager* lock_manager);

    // Begin a new transaction, returns the Transaction object
    Transaction* Begin();

    // Commit a transaction — release all locks
    void Commit(Transaction* txn);

    // Abort a transaction — release all locks
    void Abort(Transaction* txn);

    // Get a transaction by ID
    Transaction* GetTransaction(txn_id_t txn_id);

private:
    LockManager* lock_manager_;
    std::atomic<txn_id_t> next_txn_id_{0};
    std::mutex latch_;
    std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> txn_map_;
};
