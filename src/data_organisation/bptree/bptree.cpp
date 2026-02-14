#include "bptree.h"
#include <algorithm>
#include <iostream>
#include <stdexcept>

// ============================================================================
// Node Header I/O
// ============================================================================

void BPlusTree::WriteNodeHeader(char* page_data, const BTreeNodeHeader& header) {
    size_t offset = 0;
    page_data[offset++] = header.is_leaf;
    std::memcpy(page_data + offset, &header.num_keys, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    std::memcpy(page_data + offset, &header.next_leaf, sizeof(page_id_t));
}

BTreeNodeHeader BPlusTree::ReadNodeHeader(const char* page_data) {
    BTreeNodeHeader header;
    size_t offset = 0;
    header.is_leaf = page_data[offset++];
    std::memcpy(&header.num_keys, page_data + offset, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    std::memcpy(&header.next_leaf, page_data + offset, sizeof(page_id_t));
    return header;
}

// ============================================================================
// Leaf Node I/O
// Format after header: [key_len(2)][key_data...][page_id(4)][slot_id(2)] ...
// ============================================================================

void BPlusTree::ReadLeafNode(const char* page_data, std::vector<std::string>& keys, std::vector<RecordID>& rids) {
    BTreeNodeHeader header = ReadNodeHeader(page_data);
    size_t offset = BTREE_NODE_HEADER_SIZE;

    for (uint16_t i = 0; i < header.num_keys; ++i) {
        // Read key
        uint16_t key_len;
        std::memcpy(&key_len, page_data + offset, sizeof(uint16_t));
        offset += sizeof(uint16_t);
        std::string key(page_data + offset, key_len);
        offset += key_len;

        // Read RecordID
        RecordID rid;
        std::memcpy(&rid.page_id, page_data + offset, sizeof(page_id_t));
        offset += sizeof(page_id_t);
        std::memcpy(&rid.slot_id, page_data + offset, sizeof(uint16_t));
        offset += sizeof(uint16_t);

        keys.push_back(key);
        rids.push_back(rid);
    }
}

void BPlusTree::WriteLeafNode(char* page_data, const std::vector<std::string>& keys, const std::vector<RecordID>& rids, page_id_t next_leaf) {
    BTreeNodeHeader header;
    header.is_leaf = 1;
    header.num_keys = static_cast<uint16_t>(keys.size());
    header.next_leaf = next_leaf;
    WriteNodeHeader(page_data, header);

    size_t offset = BTREE_NODE_HEADER_SIZE;
    for (size_t i = 0; i < keys.size(); ++i) {
        uint16_t key_len = static_cast<uint16_t>(keys[i].size());
        std::memcpy(page_data + offset, &key_len, sizeof(uint16_t));
        offset += sizeof(uint16_t);
        std::memcpy(page_data + offset, keys[i].data(), key_len);
        offset += key_len;
        std::memcpy(page_data + offset, &rids[i].page_id, sizeof(page_id_t));
        offset += sizeof(page_id_t);
        std::memcpy(page_data + offset, &rids[i].slot_id, sizeof(uint16_t));
        offset += sizeof(uint16_t);
    }
}

// ============================================================================
// Internal Node I/O
// Format after header: [child0(4)][key_len(2)][key_data...][child1(4)]...
// ============================================================================

void BPlusTree::ReadInternalNode(const char* page_data, std::vector<std::string>& keys, std::vector<page_id_t>& children) {
    BTreeNodeHeader header = ReadNodeHeader(page_data);
    size_t offset = BTREE_NODE_HEADER_SIZE;

    // Read first child
    page_id_t child;
    std::memcpy(&child, page_data + offset, sizeof(page_id_t));
    offset += sizeof(page_id_t);
    children.push_back(child);

    for (uint16_t i = 0; i < header.num_keys; ++i) {
        // Read key
        uint16_t key_len;
        std::memcpy(&key_len, page_data + offset, sizeof(uint16_t));
        offset += sizeof(uint16_t);
        std::string key(page_data + offset, key_len);
        offset += key_len;
        keys.push_back(key);

        // Read next child
        std::memcpy(&child, page_data + offset, sizeof(page_id_t));
        offset += sizeof(page_id_t);
        children.push_back(child);
    }
}

void BPlusTree::WriteInternalNode(char* page_data, const std::vector<std::string>& keys, const std::vector<page_id_t>& children) {
    BTreeNodeHeader header;
    header.is_leaf = 0;
    header.num_keys = static_cast<uint16_t>(keys.size());
    header.next_leaf = INVALID_PAGE_ID;
    WriteNodeHeader(page_data, header);

    size_t offset = BTREE_NODE_HEADER_SIZE;

    // Write first child
    std::memcpy(page_data + offset, &children[0], sizeof(page_id_t));
    offset += sizeof(page_id_t);

    for (size_t i = 0; i < keys.size(); ++i) {
        uint16_t key_len = static_cast<uint16_t>(keys[i].size());
        std::memcpy(page_data + offset, &key_len, sizeof(uint16_t));
        offset += sizeof(uint16_t);
        std::memcpy(page_data + offset, keys[i].data(), key_len);
        offset += key_len;
        std::memcpy(page_data + offset, &children[i + 1], sizeof(page_id_t));
        offset += sizeof(page_id_t);
    }
}

// ============================================================================
// Constructor — if root page doesn't exist, create an empty leaf
// ============================================================================

BPlusTree::BPlusTree(BufferPoolManager* bpm, page_id_t root_page_id, uint16_t max_keys)
    : bpm_(bpm), root_page_id_(root_page_id), max_keys_(max_keys) {}

// ============================================================================
// FindLeaf — traverse from root to the leaf that should contain key
// ============================================================================

page_id_t BPlusTree::FindLeaf(const std::string& key) {
    page_id_t current = root_page_id_;

    while (true) {
        Page* page = bpm_->FetchPage(current);
        if (!page) return INVALID_PAGE_ID;

        BTreeNodeHeader header = ReadNodeHeader(page->GetData());

        if (header.is_leaf) {
            bpm_->UnpinPage(current, false);
            return current;
        }

        // Internal node — find the correct child
        std::vector<std::string> keys;
        std::vector<page_id_t> children;
        ReadInternalNode(page->GetData(), keys, children);
        bpm_->UnpinPage(current, false);

        // Binary search for the correct child
        size_t idx = 0;
        while (idx < keys.size() && key >= keys[idx]) {
            idx++;
        }
        current = children[idx];
    }
}

// ============================================================================
// Search — find exact key
// ============================================================================

RecordID BPlusTree::Search(const std::string& key) {
    page_id_t leaf_page = FindLeaf(key);
    if (leaf_page == INVALID_PAGE_ID) return INVALID_RECORD_ID;

    Page* page = bpm_->FetchPage(leaf_page);
    if (!page) return INVALID_RECORD_ID;

    std::vector<std::string> keys;
    std::vector<RecordID> rids;
    ReadLeafNode(page->GetData(), keys, rids);
    bpm_->UnpinPage(leaf_page, false);

    for (size_t i = 0; i < keys.size(); ++i) {
        if (keys[i] == key) {
            return rids[i];
        }
    }

    return INVALID_RECORD_ID;
}

// ============================================================================
// Insert
// ============================================================================

void BPlusTree::Insert(const std::string& key, const RecordID& rid) {
    InsertResult result = InsertInternal(root_page_id_, key, rid);

    if (result.did_split) {
        // Root was split — create a new root
        page_id_t new_root_id;
        Page* new_root = bpm_->NewPage(&new_root_id);
        if (!new_root) {
            throw std::runtime_error("BPlusTree: Failed to allocate new root page");
        }

        std::memset(new_root->GetData(), 0, 4096);

        std::vector<std::string> keys = {result.split_key};
        std::vector<page_id_t> children = {root_page_id_, result.new_page_id};
        WriteInternalNode(new_root->GetData(), keys, children);

        bpm_->UnpinPage(new_root_id, true);
        root_page_id_ = new_root_id;
    }
}

BPlusTree::InsertResult BPlusTree::InsertInternal(page_id_t node_page_id, const std::string& key, const RecordID& rid) {
    Page* page = bpm_->FetchPage(node_page_id);
    if (!page) {
        throw std::runtime_error("BPlusTree: Failed to fetch page");
    }

    BTreeNodeHeader header = ReadNodeHeader(page->GetData());

    if (header.is_leaf) {
        // ---- LEAF INSERT ----
        std::vector<std::string> keys;
        std::vector<RecordID> rids;
        ReadLeafNode(page->GetData(), keys, rids);

        // Find insertion position (sorted order)
        auto it = std::lower_bound(keys.begin(), keys.end(), key);
        size_t pos = it - keys.begin();

        // Allow duplicate keys (non-unique indexes)
        // Skip to after existing entries with the same key
        while (it != keys.end() && *it == key) {
            ++it;
            ++pos;
        }

        keys.insert(it, key);
        rids.insert(rids.begin() + pos, rid);

        if (keys.size() <= max_keys_) {
            // No split needed
            std::memset(page->GetData(), 0, 4096);
            WriteLeafNode(page->GetData(), keys, rids, header.next_leaf);
            bpm_->UnpinPage(node_page_id, true);
            return {false, "", INVALID_PAGE_ID};
        }

        // ---- LEAF SPLIT ----
        size_t mid = keys.size() / 2;

        // Left half stays in current page
        std::vector<std::string> left_keys(keys.begin(), keys.begin() + mid);
        std::vector<RecordID> left_rids(rids.begin(), rids.begin() + mid);

        // Right half goes to new page
        std::vector<std::string> right_keys(keys.begin() + mid, keys.end());
        std::vector<RecordID> right_rids(rids.begin() + mid, rids.end());

        page_id_t new_page_id;
        Page* new_page = bpm_->NewPage(&new_page_id);
        if (!new_page) {
            throw std::runtime_error("BPlusTree: Failed to allocate new leaf page");
        }
        std::memset(new_page->GetData(), 0, 4096);

        // Link: current → new → old_next
        WriteLeafNode(new_page->GetData(), right_keys, right_rids, header.next_leaf);
        bpm_->UnpinPage(new_page_id, true);

        std::memset(page->GetData(), 0, 4096);
        WriteLeafNode(page->GetData(), left_keys, left_rids, new_page_id);
        bpm_->UnpinPage(node_page_id, true);

        return {true, right_keys[0], new_page_id};

    } else {
        // ---- INTERNAL NODE ----
        std::vector<std::string> keys;
        std::vector<page_id_t> children;
        ReadInternalNode(page->GetData(), keys, children);
        bpm_->UnpinPage(node_page_id, false);

        // Find which child to recurse into
        size_t idx = 0;
        while (idx < keys.size() && key >= keys[idx]) {
            idx++;
        }

        InsertResult child_result = InsertInternal(children[idx], key, rid);

        if (!child_result.did_split) {
            return {false, "", INVALID_PAGE_ID};
        }

        // Child split — insert the new key + child pointer
        keys.insert(keys.begin() + idx, child_result.split_key);
        children.insert(children.begin() + idx + 1, child_result.new_page_id);

        if (keys.size() <= max_keys_) {
            // No split needed at this level
            Page* p = bpm_->FetchPage(node_page_id);
            std::memset(p->GetData(), 0, 4096);
            WriteInternalNode(p->GetData(), keys, children);
            bpm_->UnpinPage(node_page_id, true);
            return {false, "", INVALID_PAGE_ID};
        }

        // ---- INTERNAL SPLIT ----
        size_t mid = keys.size() / 2;
        std::string push_up_key = keys[mid];

        std::vector<std::string> left_keys(keys.begin(), keys.begin() + mid);
        std::vector<page_id_t> left_children(children.begin(), children.begin() + mid + 1);

        std::vector<std::string> right_keys(keys.begin() + mid + 1, keys.end());
        std::vector<page_id_t> right_children(children.begin() + mid + 1, children.end());

        page_id_t new_page_id;
        Page* new_page = bpm_->NewPage(&new_page_id);
        if (!new_page) {
            throw std::runtime_error("BPlusTree: Failed to allocate new internal page");
        }
        std::memset(new_page->GetData(), 0, 4096);
        WriteInternalNode(new_page->GetData(), right_keys, right_children);
        bpm_->UnpinPage(new_page_id, true);

        Page* p = bpm_->FetchPage(node_page_id);
        std::memset(p->GetData(), 0, 4096);
        WriteInternalNode(p->GetData(), left_keys, left_children);
        bpm_->UnpinPage(node_page_id, true);

        return {true, push_up_key, new_page_id};
    }
}

// ============================================================================
// Delete — simplified: remove from leaf, no rebalancing
// ============================================================================

bool BPlusTree::Delete(const std::string& key) {
    page_id_t leaf_page = FindLeaf(key);
    if (leaf_page == INVALID_PAGE_ID) return false;

    Page* page = bpm_->FetchPage(leaf_page);
    if (!page) return false;

    std::vector<std::string> keys;
    std::vector<RecordID> rids;
    BTreeNodeHeader header = ReadNodeHeader(page->GetData());
    ReadLeafNode(page->GetData(), keys, rids);

    // Find and remove the key
    bool found = false;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (keys[i] == key) {
            keys.erase(keys.begin() + i);
            rids.erase(rids.begin() + i);
            found = true;
            break;
        }
    }

    if (found) {
        std::memset(page->GetData(), 0, 4096);
        WriteLeafNode(page->GetData(), keys, rids, header.next_leaf);
        bpm_->UnpinPage(leaf_page, true);
    } else {
        bpm_->UnpinPage(leaf_page, false);
    }

    return found;
}

// ============================================================================
// RangeScan — follow leaf chain
// ============================================================================

std::vector<std::pair<std::string, RecordID>> BPlusTree::RangeScan(const std::string& lo_key, const std::string& hi_key) {
    std::vector<std::pair<std::string, RecordID>> results;

    page_id_t leaf_page = FindLeaf(lo_key);
    if (leaf_page == INVALID_PAGE_ID) return results;

    while (leaf_page != INVALID_PAGE_ID) {
        Page* page = bpm_->FetchPage(leaf_page);
        if (!page) break;

        std::vector<std::string> keys;
        std::vector<RecordID> rids;
        BTreeNodeHeader header = ReadNodeHeader(page->GetData());
        ReadLeafNode(page->GetData(), keys, rids);
        bpm_->UnpinPage(leaf_page, false);

        for (size_t i = 0; i < keys.size(); ++i) {
            if (keys[i] >= lo_key && keys[i] <= hi_key) {
                results.emplace_back(keys[i], rids[i]);
            }
            if (keys[i] > hi_key) {
                return results;  // Done — past the range
            }
        }

        leaf_page = header.next_leaf;
    }

    return results;
}
