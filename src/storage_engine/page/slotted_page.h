#pragma once

#include "storage_engine/common/common.h"
#include <cstdint>
#include <cstring>
#include <utility>
#include <stdexcept>

// ============================================================================
// Record ID — unique identifier for a record (page_id + slot_id)
// ============================================================================
struct RecordID {
    page_id_t page_id = INVALID_PAGE_ID;
    uint16_t slot_id = 0;

    bool operator==(const RecordID& other) const {
        return page_id == other.page_id && slot_id == other.slot_id;
    }
    bool operator!=(const RecordID& other) const { return !(*this == other); }
    bool IsValid() const { return page_id != INVALID_PAGE_ID; }
};

const RecordID INVALID_RECORD_ID = {INVALID_PAGE_ID, 0};

// ============================================================================
// Slotted Page Layout
//
//  +---------------------------------------------------+
//  | PageHeader (8 bytes)                               |
//  +---------------------------------------------------+
//  | Slot Directory (grows downward →)                  |
//  |   SlotEntry[0], SlotEntry[1], ...                  |
//  +---------------------------------------------------+
//  |              Free Space                             |
//  +---------------------------------------------------+
//  |              Records (grow upward ←)               |
//  +---------------------------------------------------+
//
// Records grow from the END of the page backwards.
// Slot directory grows from AFTER the header forwards.
// ============================================================================

struct SlotEntry {
    uint16_t offset;   // Offset from start of page to the record data
    uint16_t length;   // Length of the record (0 = deleted/empty)
};

struct PageHeader {
    uint16_t num_slots;         // Total slots (including deleted)
    uint16_t free_space_begin;  // Offset where slot directory ends (first free byte after slots)
    uint16_t free_space_end;    // Offset where record data begins (first used byte from the end)
    uint16_t reserved;          // For alignment / future use
};

static_assert(sizeof(PageHeader) == 8, "PageHeader must be 8 bytes");
static_assert(sizeof(SlotEntry) == 4, "SlotEntry must be 4 bytes");

// ============================================================================
// SlottedPage — operates on a raw char* page buffer (owned by BufferPool Page)
// ============================================================================
class SlottedPage {
public:
    // Initialize a fresh page (call once on newly allocated pages)
    static void Init(char* page_data, uint16_t page_size = 4096);

    // Insert a record. Returns the slot_id, or -1 if not enough space.
    static int16_t InsertRecord(char* page_data, const uint8_t* record, uint16_t record_len);

    // Delete a record by slot_id. Returns true on success.
    static bool DeleteRecord(char* page_data, uint16_t slot_id);

    // Get a record by slot_id. Returns pointer to data and sets out_len.
    // Returns nullptr if slot is deleted or invalid.
    static const uint8_t* GetRecord(const char* page_data, uint16_t slot_id, uint16_t* out_len);

    // Update a record in place (only if new data fits in the same slot)
    static bool UpdateRecord(char* page_data, uint16_t slot_id, const uint8_t* record, uint16_t record_len);

    // Get available free space in bytes
    static uint16_t GetFreeSpace(const char* page_data);

    // Get number of slots (including deleted)
    static uint16_t GetNumSlots(const char* page_data);

    // Check if a slot is occupied (not deleted)
    static bool IsSlotOccupied(const char* page_data, uint16_t slot_id);

private:
    static PageHeader* GetHeader(char* page_data);
    static const PageHeader* GetHeader(const char* page_data);
    static SlotEntry* GetSlotEntry(char* page_data, uint16_t slot_id);
    static const SlotEntry* GetSlotEntry(const char* page_data, uint16_t slot_id);
};
