#pragma once

#include "storage_engine/common/common.h"
#include "storage_engine/buffer/buffer_pool.h"
#include "storage_engine/page/free_space_map.h"
#include "data_organisation/heap_file/heap_file.h"
#include "data_organisation/bptree/bptree.h"
#include <string>
#include <unordered_map>
#include <memory>
#include <vector>

// ============================================================================
// Catalog — stores metadata about collections and their indexes
//
// CollectionInfo tracks:
//   - Collection name
//   - First heap page (root of the heap file)
//   - FSM page for the collection
//   - List of indexes (field_name → B+ Tree root page)
//   - Pointers to the HeapFile and FreeSpaceMap objects
// ============================================================================

struct IndexInfo {
    std::string field_name;
    page_id_t btree_root_page;
    std::unique_ptr<BPlusTree> btree;
};

struct CollectionInfo {
    std::string name;
    page_id_t first_heap_page;
    page_id_t fsm_page;
    std::unique_ptr<HeapFile> heap_file;
    std::unique_ptr<FreeSpaceMap> fsm;
    std::vector<IndexInfo> indexes;
};

class Catalog {
public:
    explicit Catalog(BufferPoolManager* bpm);

    // Create a new collection. Allocates heap + FSM pages.
    bool CreateCollection(const std::string& name);

    // Drop a collection.
    bool DropCollection(const std::string& name);

    // Get collection info (returns nullptr if not found)
    CollectionInfo* GetCollection(const std::string& name);

    // Create an index on a field for a collection
    bool CreateIndex(const std::string& collection_name, const std::string& field_name);

    // Get all collection names
    std::vector<std::string> ListCollections() const;

    // Persist catalog metadata to page 0 of the database file
    void SaveCatalog();

    // Load catalog metadata from page 0 (called on startup)
    void LoadCatalog();

private:
    BufferPoolManager* bpm_;
    std::unordered_map<std::string, std::unique_ptr<CollectionInfo>> collections_;
};
