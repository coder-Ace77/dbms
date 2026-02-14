#pragma once

#include "executor.h"
#include "data_organisation/bptree/bptree.h"
#include "data_organisation/heap_file/heap_file.h"
#include <vector>
#include <string>

// ============================================================================
// IndexScan — uses B+ Tree range scan to produce matching records
// ============================================================================
class IndexScanExecutor : public Executor {
public:
    IndexScanExecutor(BPlusTree* index, HeapFile* heap_file,
                      const std::string& lo_key, const std::string& hi_key);

    void Init() override;
    bool Next(Tuple* tuple) override;
    void Close() override;

private:
    BPlusTree* index_;
    HeapFile* heap_file_;
    std::string lo_key_;
    std::string hi_key_;
    std::vector<std::pair<std::string, RecordID>> results_;
    size_t current_idx_;
};
