#pragma once

#include "storage_engine/common/common.h"
#include "storage_engine/buffer/buffer_pool.h"
#include "storage_engine/page/slotted_page.h"  // For RecordID
#include <string>
#include <vector>
#include <cstring>
#include <optional>

// ============================================================================
// B+ Tree Index
//
// On-disk B+ Tree with variable-length string keys.
// Each node occupies one page.
//
// Node Layout (in page data):
//   [NodeHeader][Key1][Value1][Key2][Value2]...[KeyN][ValueN]
//
// NodeHeader:
//   is_leaf (1 byte) | num_keys (uint16) | next_leaf (page_id, leaf only)
//
// Key: [uint16_t length][char data...]
// Value (leaf):    RecordID (6 bytes: page_id + slot_id)
// Value (internal): page_id_t (4 bytes, child pointer)
//
// Internal node: keys[i] separates children[i] and children[i+1]
//   children[0] | key[0] | children[1] | key[1] | ... | children[n]
//   So there are num_keys keys and num_keys+1 children.
//   Stored as: [child0][key0][child1][key1]...[keyN-1][childN]
//
// Leaf node: keys[i] maps to rid[i], plus next_leaf pointer
// ============================================================================

static constexpr uint16_t BTREE_NODE_HEADER_SIZE = 1 + 2 + 4;  // is_leaf + num_keys + next_leaf

struct BTreeNodeHeader {
    uint8_t is_leaf;
    uint16_t num_keys;
    page_id_t next_leaf;  // Only used for leaf nodes (-1 if none)
};

class BPlusTree {
public:
    BPlusTree(BufferPoolManager* bpm, page_id_t root_page_id, uint16_t max_keys_per_node = 50);

    // Insert a key-RecordID pair
    void Insert(const std::string& key, const RecordID& rid);

    // Search for an exact key. Returns the RecordID or INVALID_RECORD_ID.
    RecordID Search(const std::string& key);

    // Delete a key
    bool Delete(const std::string& key);

    // Range scan: returns all RecordIDs where lo_key <= key <= hi_key
    std::vector<std::pair<std::string, RecordID>> RangeScan(const std::string& lo_key, const std::string& hi_key);

    // Get root page id
    page_id_t GetRootPageId() const { return root_page_id_; }

    // Set root page id (after split)
    void SetRootPageId(page_id_t pid) { root_page_id_ = pid; }

private:
    // ---- Node I/O helpers ----
    void WriteNodeHeader(char* page_data, const BTreeNodeHeader& header);
    BTreeNodeHeader ReadNodeHeader(const char* page_data);

    // Read all keys and children/rids from a page
    void ReadLeafNode(const char* page_data, std::vector<std::string>& keys, std::vector<RecordID>& rids);
    void WriteLeafNode(char* page_data, const std::vector<std::string>& keys, const std::vector<RecordID>& rids, page_id_t next_leaf);

    void ReadInternalNode(const char* page_data, std::vector<std::string>& keys, std::vector<page_id_t>& children);
    void WriteInternalNode(char* page_data, const std::vector<std::string>& keys, const std::vector<page_id_t>& children);

    // ---- Core operations ----
    // Returns (split_happened, new_key, new_page_id)
    struct InsertResult {
        bool did_split = false;
        std::string split_key;
        page_id_t new_page_id = INVALID_PAGE_ID;
    };

    InsertResult InsertInternal(page_id_t node_page_id, const std::string& key, const RecordID& rid);

    // Find the leaf page that should contain the key
    page_id_t FindLeaf(const std::string& key);

    BufferPoolManager* bpm_;
    page_id_t root_page_id_;
    uint16_t max_keys_;
};
