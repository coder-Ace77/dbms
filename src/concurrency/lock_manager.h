#pragma once

#include "storage_engine/common/common.h"
#include "storage_engine/page/slotted_page.h"
#include <unordered_map>
#include <list>
#include <mutex>
#include <condition_variable>
#include <set>
#include <cstdint>

// ============================================================================
// Lock Manager — Strict Two-Phase Locking (2PL)
//
// Supports SHARED and EXCLUSIVE locks on resources identified by RecordID.
// Uses a lock request queue per resource. Locks are released only at
// transaction end (commit/abort) — no early unlock.
// ============================================================================

using txn_id_t = int64_t;
static constexpr txn_id_t INVALID_TXN_ID = -1;

enum class LockMode {
    SHARED,
    EXCLUSIVE
};

struct LockRequest {
    txn_id_t txn_id;
    LockMode mode;
    bool granted;
};

// A resource key: we lock at the page+slot level
struct LockKey {
    page_id_t page_id;
    uint16_t slot_id;

    bool operator==(const LockKey& other) const {
        return page_id == other.page_id && slot_id == other.slot_id;
    }
};

struct LockKeyHash {
    size_t operator()(const LockKey& k) const {
        return std::hash<int64_t>()(
            (static_cast<int64_t>(k.page_id) << 16) | k.slot_id
        );
    }
};

struct LockRequestQueue {
    std::list<LockRequest> queue;
    std::condition_variable cv;
};

class LockManager {
public:
    LockManager() = default;
    ~LockManager() = default;

    // Acquire a lock. Blocks if incompatible lock is held.
    // Returns false if deadlock would occur (simplified detection).
    bool LockShared(txn_id_t txn_id, const RecordID& rid);
    bool LockExclusive(txn_id_t txn_id, const RecordID& rid);

    // Upgrade from shared to exclusive
    bool LockUpgrade(txn_id_t txn_id, const RecordID& rid);

    // Release all locks held by a transaction
    void UnlockAll(txn_id_t txn_id);

private:
    LockKey MakeKey(const RecordID& rid) const {
        return {rid.page_id, rid.slot_id};
    }

    bool CanGrantLock(const LockRequestQueue& queue, LockMode mode) const;

    std::mutex latch_;
    std::unordered_map<LockKey, LockRequestQueue, LockKeyHash> lock_table_;
    // Track which locks each txn holds (for UnlockAll)
    std::unordered_map<txn_id_t, std::set<LockKey, std::less<>>> txn_locks_;
};

// Comparison operator for LockKey (used in std::set)
inline bool operator<(const LockKey& a, const LockKey& b) {
    if (a.page_id != b.page_id) return a.page_id < b.page_id;
    return a.slot_id < b.slot_id;
}
