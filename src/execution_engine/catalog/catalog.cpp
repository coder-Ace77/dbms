#include "catalog.h"
#include "storage_engine/page/slotted_page.h"
#include <iostream>
#include <stdexcept>

// ============================================================================
// Constructor
// ============================================================================

Catalog::Catalog(BufferPoolManager* bpm) : bpm_(bpm) {}

// ============================================================================
// CreateCollection — allocate FSM page + first heap page
// ============================================================================

bool Catalog::CreateCollection(const std::string& name) {
    if (collections_.find(name) != collections_.end()) {
        std::cerr << "Catalog: Collection '" << name << "' already exists." << std::endl;
        return false;
    }

    auto info = std::make_unique<CollectionInfo>();
    info->name = name;

    // Allocate FSM page
    page_id_t fsm_page_id;
    Page* fsm_page = bpm_->NewPage(&fsm_page_id);
    if (!fsm_page) {
        throw std::runtime_error("Catalog: Failed to allocate FSM page");
    }
    std::memset(fsm_page->GetData(), 0, 4096);  // Zero out FSM page
    bpm_->UnpinPage(fsm_page_id, true);
    info->fsm_page = fsm_page_id;

    // Create FSM object
    info->fsm = std::make_unique<FreeSpaceMap>(bpm_, fsm_page_id);

    // Allocate first heap page
    page_id_t heap_page_id;
    Page* heap_page = bpm_->NewPage(&heap_page_id);
    if (!heap_page) {
        throw std::runtime_error("Catalog: Failed to allocate heap page");
    }
    SlottedPage::Init(heap_page->GetData());
    bpm_->UnpinPage(heap_page_id, true);
    info->first_heap_page = heap_page_id;

    // Register with FSM
    uint16_t free_space = 4096 - sizeof(PageHeader);  // Approximate
    info->fsm->RegisterNewPage(heap_page_id, free_space);

    // Create HeapFile object
    info->heap_file = std::make_unique<HeapFile>(bpm_, info->fsm.get(), heap_page_id);

    collections_[name] = std::move(info);

    std::cout << "Catalog: Created collection '" << name
              << "' (FSM page=" << fsm_page_id
              << ", Heap page=" << heap_page_id << ")" << std::endl;

    return true;
}

// ============================================================================
// DropCollection
// ============================================================================

bool Catalog::DropCollection(const std::string& name) {
    auto it = collections_.find(name);
    if (it == collections_.end()) {
        return false;
    }

    // In a full implementation, we'd deallocate all pages.
    // For now, just remove from catalog.
    collections_.erase(it);
    std::cout << "Catalog: Dropped collection '" << name << "'" << std::endl;
    return true;
}

// ============================================================================
// GetCollection
// ============================================================================

CollectionInfo* Catalog::GetCollection(const std::string& name) {
    auto it = collections_.find(name);
    if (it == collections_.end()) {
        return nullptr;
    }
    return it->second.get();
}

// ============================================================================
// CreateIndex — allocate a B+ Tree root page and build the index
// ============================================================================

bool Catalog::CreateIndex(const std::string& collection_name, const std::string& field_name) {
    CollectionInfo* coll = GetCollection(collection_name);
    if (!coll) {
        std::cerr << "Catalog: Collection '" << collection_name << "' not found." << std::endl;
        return false;
    }

    // Check if index already exists
    for (auto& idx : coll->indexes) {
        if (idx.field_name == field_name) {
            std::cerr << "Catalog: Index on '" << field_name << "' already exists." << std::endl;
            return false;
        }
    }

    // Allocate root page for B+ Tree
    page_id_t root_page_id;
    Page* root_page = bpm_->NewPage(&root_page_id);
    if (!root_page) {
        throw std::runtime_error("Catalog: Failed to allocate B+ Tree root page");
    }

    // Initialize as empty leaf node
    std::memset(root_page->GetData(), 0, 4096);
    // Write an empty leaf header
    char* data = root_page->GetData();
    data[0] = 1;  // is_leaf = true
    // num_keys = 0, next_leaf = -1 (already zeroed, just set next_leaf)
    page_id_t invalid = INVALID_PAGE_ID;
    std::memcpy(data + 3, &invalid, sizeof(page_id_t));

    bpm_->UnpinPage(root_page_id, true);

    // Create B+ Tree object
    IndexInfo idx_info;
    idx_info.field_name = field_name;
    idx_info.btree_root_page = root_page_id;
    idx_info.btree = std::make_unique<BPlusTree>(bpm_, root_page_id);

    // Build index by scanning existing records
    HeapFile::Iterator it = coll->heap_file->Begin();
    RecordID rid;
    BsonDocument doc;
    while (it.Next(&rid, &doc)) {
        auto field_it = doc.elements.find(field_name);
        if (field_it != doc.elements.end()) {
            // Extract the string key
            if (std::holds_alternative<std::string>(field_it->second)) {
                idx_info.btree->Insert(std::get<std::string>(field_it->second), rid);
            } else if (std::holds_alternative<int32_t>(field_it->second)) {
                idx_info.btree->Insert(std::to_string(std::get<int32_t>(field_it->second)), rid);
            }
        }
    }

    coll->indexes.push_back(std::move(idx_info));

    std::cout << "Catalog: Created index on '" << collection_name << "." << field_name
              << "' (root page=" << root_page_id << ")" << std::endl;

    return true;
}

// ============================================================================
// ListCollections
// ============================================================================

std::vector<std::string> Catalog::ListCollections() const {
    std::vector<std::string> names;
    for (auto& [name, _] : collections_) {
        names.push_back(name);
    }
    return names;
}

// ============================================================================
// SaveCatalog — persist metadata to page 0
//
// Format on page 0:
//   [4 bytes] num_collections
//   For each collection:
//     [4 bytes] name_len
//     [name_len bytes] name
//     [4 bytes] fsm_page
//     [4 bytes] first_heap_page
//     [4 bytes] num_indexes
//     For each index:
//       [4 bytes] field_name_len
//       [field_name_len bytes] field_name
//       [4 bytes] btree_root_page
// ============================================================================

void Catalog::SaveCatalog() {
    // Ensure page 0 exists — try to fetch, create if needed
    Page* page = bpm_->FetchPage(0);
    if (!page) {
        page_id_t pid;
        page = bpm_->NewPage(&pid);
        if (!page) {
            std::cerr << "Catalog: Failed to allocate catalog page!" << std::endl;
            return;
        }
    }

    char* data = page->GetData();
    std::memset(data, 0, 4096);
    size_t offset = 0;

    uint32_t num_collections = static_cast<uint32_t>(collections_.size());
    std::memcpy(data + offset, &num_collections, 4); offset += 4;

    for (auto& [name, info] : collections_) {
        // Name
        uint32_t name_len = static_cast<uint32_t>(name.size());
        std::memcpy(data + offset, &name_len, 4); offset += 4;
        std::memcpy(data + offset, name.c_str(), name_len); offset += name_len;

        // Pages
        std::memcpy(data + offset, &info->fsm_page, 4); offset += 4;
        std::memcpy(data + offset, &info->first_heap_page, 4); offset += 4;

        // Indexes
        uint32_t num_indexes = static_cast<uint32_t>(info->indexes.size());
        std::memcpy(data + offset, &num_indexes, 4); offset += 4;

        for (auto& idx : info->indexes) {
            uint32_t field_len = static_cast<uint32_t>(idx.field_name.size());
            std::memcpy(data + offset, &field_len, 4); offset += 4;
            std::memcpy(data + offset, idx.field_name.c_str(), field_len); offset += field_len;
            std::memcpy(data + offset, &idx.btree_root_page, 4); offset += 4;
        }

        if (offset >= 4000) {
            std::cerr << "Catalog: Warning — catalog metadata approaching page limit!" << std::endl;
            break;
        }
    }

    bpm_->UnpinPage(0, true);
    bpm_->FlushAllPages();
}

// ============================================================================
// LoadCatalog — restore metadata from page 0
// ============================================================================

void Catalog::LoadCatalog() {
    Page* page = bpm_->FetchPage(0);
    if (!page) return;

    char* data = page->GetData();
    size_t offset = 0;

    uint32_t num_collections;
    std::memcpy(&num_collections, data + offset, 4); offset += 4;

    if (num_collections == 0 || num_collections > 1000) {
        // No saved catalog or corrupt data
        bpm_->UnpinPage(0, false);
        return;
    }

    for (uint32_t c = 0; c < num_collections; c++) {
        auto info = std::make_unique<CollectionInfo>();

        // Name
        uint32_t name_len;
        std::memcpy(&name_len, data + offset, 4); offset += 4;
        if (name_len == 0 || name_len > 255 || offset + name_len > 4096) break;
        info->name = std::string(data + offset, name_len); offset += name_len;

        // Pages
        std::memcpy(&info->fsm_page, data + offset, 4); offset += 4;
        std::memcpy(&info->first_heap_page, data + offset, 4); offset += 4;

        // Reconstruct FSM and HeapFile
        info->fsm = std::make_unique<FreeSpaceMap>(bpm_, info->fsm_page);
        info->heap_file = std::make_unique<HeapFile>(bpm_, info->fsm.get(), info->first_heap_page);

        // Indexes
        uint32_t num_indexes;
        std::memcpy(&num_indexes, data + offset, 4); offset += 4;

        for (uint32_t i = 0; i < num_indexes; i++) {
            IndexInfo idx_info;

            uint32_t field_len;
            std::memcpy(&field_len, data + offset, 4); offset += 4;
            if (field_len == 0 || field_len > 255 || offset + field_len > 4096) break;
            idx_info.field_name = std::string(data + offset, field_len); offset += field_len;

            std::memcpy(&idx_info.btree_root_page, data + offset, 4); offset += 4;

            idx_info.btree = std::make_unique<BPlusTree>(bpm_, idx_info.btree_root_page);
            info->indexes.push_back(std::move(idx_info));
        }

        std::string coll_name = info->name;
        collections_[coll_name] = std::move(info);
    }

    bpm_->UnpinPage(0, false);

    if (!collections_.empty()) {
        std::cout << "Catalog: Loaded " << collections_.size() << " collection(s) from disk." << std::endl;
    }
}
