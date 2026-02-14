#pragma once

#include "executor.h"
#include "data_organisation/heap_file/heap_file.h"
#include <memory>

// ============================================================================
// SeqScan — sequential scan over all records in a heap file
// ============================================================================
class SeqScanExecutor : public Executor {
public:
    SeqScanExecutor(HeapFile* heap_file);

    void Init() override;
    bool Next(Tuple* tuple) override;
    void Close() override;

private:
    HeapFile* heap_file_;
    std::unique_ptr<HeapFile::Iterator> iterator_;
};
