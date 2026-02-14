#include "lock_manager.h"
#include <algorithm>
#include <iostream>

// ============================================================================
// CanGrantLock — check if a new lock request can be granted
// ============================================================================

bool LockManager::CanGrantLock(const LockRequestQueue& queue, LockMode mode) const {
    for (const auto& req : queue.queue) {
        if (!req.granted) continue;

        if (mode == LockMode::EXCLUSIVE) {
            // Exclusive conflicts with anything
            return false;
        }
        if (req.mode == LockMode::EXCLUSIVE) {
            // Shared conflicts with exclusive
            return false;
        }
        // Shared + Shared = OK
    }
    return true;
}

// ============================================================================
// LockShared
// ============================================================================

bool LockManager::LockShared(txn_id_t txn_id, const RecordID& rid) {
    LockKey key = MakeKey(rid);
    std::unique_lock<std::mutex> guard(latch_);

    auto& queue = lock_table_[key];

    // Check if we already hold a lock on this resource
    for (auto& req : queue.queue) {
        if (req.txn_id == txn_id && req.granted) {
            return true;  // Already have a lock (shared or exclusive)
        }
    }

    LockRequest request{txn_id, LockMode::SHARED, false};
    queue.queue.push_back(request);
    auto it = std::prev(queue.queue.end());

    // Wait until we can be granted
    while (!CanGrantLock(queue, LockMode::SHARED)) {
        queue.cv.wait(guard);
    }

    it->granted = true;
    txn_locks_[txn_id].insert(key);
    return true;
}

// ============================================================================
// LockExclusive
// ============================================================================

bool LockManager::LockExclusive(txn_id_t txn_id, const RecordID& rid) {
    LockKey key = MakeKey(rid);
    std::unique_lock<std::mutex> guard(latch_);

    auto& queue = lock_table_[key];

    // Check if we already hold an exclusive lock
    for (auto& req : queue.queue) {
        if (req.txn_id == txn_id && req.granted && req.mode == LockMode::EXCLUSIVE) {
            return true;  // Already have exclusive
        }
    }

    LockRequest request{txn_id, LockMode::EXCLUSIVE, false};
    queue.queue.push_back(request);
    auto it = std::prev(queue.queue.end());

    // Wait until we can be granted — check that no other txn holds any lock
    while (true) {
        bool can_grant = true;
        for (const auto& req : queue.queue) {
            if (req.txn_id != txn_id && req.granted) {
                can_grant = false;
                break;
            }
        }
        if (can_grant) break;
        queue.cv.wait(guard);
    }

    it->granted = true;
    txn_locks_[txn_id].insert(key);
    return true;
}

// ============================================================================
// LockUpgrade — shared → exclusive
// ============================================================================

bool LockManager::LockUpgrade(txn_id_t txn_id, const RecordID& rid) {
    LockKey key = MakeKey(rid);
    std::unique_lock<std::mutex> guard(latch_);

    auto table_it = lock_table_.find(key);
    if (table_it == lock_table_.end()) return false;

    auto& queue = table_it->second;

    // Find our shared lock
    auto it = std::find_if(queue.queue.begin(), queue.queue.end(),
        [&](const LockRequest& r) { return r.txn_id == txn_id && r.granted; });

    if (it == queue.queue.end()) return false;
    if (it->mode == LockMode::EXCLUSIVE) return true;  // Already exclusive

    // Wait until we're the only one holding a lock
    while (true) {
        bool can_upgrade = true;
        for (const auto& req : queue.queue) {
            if (req.txn_id != txn_id && req.granted) {
                can_upgrade = false;
                break;
            }
        }
        if (can_upgrade) break;
        queue.cv.wait(guard);
    }

    it->mode = LockMode::EXCLUSIVE;
    return true;
}

// ============================================================================
// UnlockAll — release all locks held by a transaction (called at commit/abort)
// ============================================================================

void LockManager::UnlockAll(txn_id_t txn_id) {
    std::unique_lock<std::mutex> guard(latch_);

    auto txn_it = txn_locks_.find(txn_id);
    if (txn_it == txn_locks_.end()) return;

    for (const auto& key : txn_it->second) {
        auto table_it = lock_table_.find(key);
        if (table_it == lock_table_.end()) continue;

        auto& queue = table_it->second;

        // Remove all lock requests from this txn
        queue.queue.remove_if([&](const LockRequest& r) {
            return r.txn_id == txn_id;
        });

        // Wake up waiters
        queue.cv.notify_all();

        // Clean up empty queues
        if (queue.queue.empty()) {
            lock_table_.erase(table_it);
        }
    }

    txn_locks_.erase(txn_it);
}
