#include "transaction.h"
#include <iostream>

// ============================================================================
// TransactionManager
// ============================================================================

TransactionManager::TransactionManager(LockManager* lock_manager)
    : lock_manager_(lock_manager) {}

Transaction* TransactionManager::Begin() {
    txn_id_t txn_id = next_txn_id_.fetch_add(1);
    auto txn = std::make_unique<Transaction>(txn_id);
    Transaction* ptr = txn.get();

    {
        std::lock_guard<std::mutex> guard(latch_);
        txn_map_[txn_id] = std::move(txn);
    }

    std::cout << "TXN [" << txn_id << "] BEGIN" << std::endl;
    return ptr;
}

void TransactionManager::Commit(Transaction* txn) {
    if (!txn) return;

    txn->state = TransactionState::SHRINKING;

    // Release all locks (2PL shrinking phase)
    lock_manager_->UnlockAll(txn->txn_id);

    txn->state = TransactionState::COMMITTED;
    std::cout << "TXN [" << txn->txn_id << "] COMMITTED" << std::endl;
}

void TransactionManager::Abort(Transaction* txn) {
    if (!txn) return;

    txn->state = TransactionState::SHRINKING;

    // Release all locks
    lock_manager_->UnlockAll(txn->txn_id);

    txn->state = TransactionState::ABORTED;
    std::cout << "TXN [" << txn->txn_id << "] ABORTED" << std::endl;
}

Transaction* TransactionManager::GetTransaction(txn_id_t txn_id) {
    std::lock_guard<std::mutex> guard(latch_);
    auto it = txn_map_.find(txn_id);
    if (it == txn_map_.end()) return nullptr;
    return it->second.get();
}
