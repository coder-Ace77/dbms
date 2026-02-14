#pragma once

#include "storage_engine/common/common.h"
#include "storage_engine/common/bson_types.h"
#include "storage_engine/buffer/buffer_pool.h"
#include "storage_engine/page/slotted_page.h"
#include "storage_engine/page/free_space_map.h"
#include "storage_engine/serializer/serializer.h"
#include <vector>
#include <functional>

// ============================================================================
// Heap File
//
// Manages an unordered collection of BSON documents stored across pages.
// Uses:
//   - FreeSpaceMap to find pages with available space
//   - BufferPoolManager to fetch/create pages  
//   - SlottedPage to manage records within a page
//   - BsonSerializer to convert documents to/from bytes
// ============================================================================

class HeapFile {
public:
    // first_page_id: the first data page of this heap
    // fsm: the free space map tracking this heap's pages
    HeapFile(BufferPoolManager* bpm, FreeSpaceMap* fsm, page_id_t first_page_id);

    // Insert a BSON document. Returns a RecordID.
    RecordID InsertRecord(const BsonDocument& doc);

    // Delete a record by RecordID. Returns true on success.
    bool DeleteRecord(const RecordID& rid);

    // Get a record by RecordID. Returns the deserialized BSON document.
    // Throws if record not found.
    BsonDocument GetRecord(const RecordID& rid);

    // Update a record. If it doesn't fit in place, delete + re-insert.
    RecordID UpdateRecord(const RecordID& rid, const BsonDocument& doc);

    // Get the first data page id
    page_id_t GetFirstPageId() const { return first_page_id_; }

    // Set the maximum page id (used to know how many pages to scan)
    void SetMaxPageId(page_id_t max_id) { max_page_id_ = max_id; }
    page_id_t GetMaxPageId() const { return max_page_id_; }

    // =========================================================================
    // Heap Iterator — sequential scan over all live records
    // =========================================================================
    class Iterator {
    public:
        Iterator(HeapFile* heap, page_id_t start_page, page_id_t max_page);

        // Advance to the next record. Returns false when done.
        bool Next(RecordID* out_rid, BsonDocument* out_doc);

        // Reset to beginning
        void Reset();

    private:
        HeapFile* heap_;
        page_id_t current_page_;
        page_id_t start_page_;
        page_id_t max_page_;
        uint16_t current_slot_;
    };

    // Get an iterator over all records
    Iterator Begin();

private:
    // Allocate a new data page, init it, register with FSM
    page_id_t AllocateNewPage();

    BufferPoolManager* bpm_;
    FreeSpaceMap* fsm_;
    page_id_t first_page_id_;
    page_id_t max_page_id_;  // Tracks highest allocated page
};
