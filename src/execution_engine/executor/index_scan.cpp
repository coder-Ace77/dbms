#include "index_scan.h"
#include <stdexcept>

// ============================================================================
// IndexScanExecutor
// ============================================================================

IndexScanExecutor::IndexScanExecutor(BPlusTree* index, HeapFile* heap_file,
                                     const std::string& lo_key, const std::string& hi_key)
    : index_(index), heap_file_(heap_file), lo_key_(lo_key), hi_key_(hi_key), current_idx_(0) {}

void IndexScanExecutor::Init() {
    results_ = index_->RangeScan(lo_key_, hi_key_);
    current_idx_ = 0;
}

bool IndexScanExecutor::Next(Tuple* tuple) {
    if (current_idx_ >= results_.size()) {
        return false;
    }

    const auto& [key, rid] = results_[current_idx_];
    tuple->rid = rid;
    tuple->doc = heap_file_->GetRecord(rid);
    current_idx_++;
    return true;
}

void IndexScanExecutor::Close() {
    results_.clear();
    current_idx_ = 0;
}
