#include "seq_scan.h"

// ============================================================================
// SeqScanExecutor
// ============================================================================

SeqScanExecutor::SeqScanExecutor(HeapFile* heap_file)
    : heap_file_(heap_file) {}

void SeqScanExecutor::Init() {
    auto it = heap_file_->Begin();
    iterator_ = std::make_unique<HeapFile::Iterator>(it);
}

bool SeqScanExecutor::Next(Tuple* tuple) {
    if (!iterator_) return false;
    return iterator_->Next(&tuple->rid, &tuple->doc);
}

void SeqScanExecutor::Close() {
    iterator_.reset();
}
