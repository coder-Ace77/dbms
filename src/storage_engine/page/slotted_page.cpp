#include "slotted_page.h"
#include <iostream>
#include <algorithm>

// ============================================================================
// Helper accessors
// ============================================================================

PageHeader* SlottedPage::GetHeader(char* page_data) {
    return reinterpret_cast<PageHeader*>(page_data);
}

const PageHeader* SlottedPage::GetHeader(const char* page_data) {
    return reinterpret_cast<const PageHeader*>(page_data);
}

SlotEntry* SlottedPage::GetSlotEntry(char* page_data, uint16_t slot_id) {
    return reinterpret_cast<SlotEntry*>(page_data + sizeof(PageHeader) + slot_id * sizeof(SlotEntry));
}

const SlotEntry* SlottedPage::GetSlotEntry(const char* page_data, uint16_t slot_id) {
    return reinterpret_cast<const SlotEntry*>(page_data + sizeof(PageHeader) + slot_id * sizeof(SlotEntry));
}

// ============================================================================
// Init — prepare a fresh page
// ============================================================================

void SlottedPage::Init(char* page_data, uint16_t page_size) {
    std::memset(page_data, 0, page_size);
    PageHeader* header = GetHeader(page_data);
    header->num_slots = 0;
    header->free_space_begin = sizeof(PageHeader);  // Right after header
    header->free_space_end = page_size;              // End of page
    header->reserved = 0;
}

// ============================================================================
// InsertRecord — allocate a slot and write record data from the end
// ============================================================================

int16_t SlottedPage::InsertRecord(char* page_data, const uint8_t* record, uint16_t record_len) {
    PageHeader* header = GetHeader(page_data);

    // Check if we need a new slot or can reuse a deleted one
    int16_t target_slot = -1;
    for (uint16_t i = 0; i < header->num_slots; ++i) {
        SlotEntry* slot = GetSlotEntry(page_data, i);
        if (slot->length == 0) {
            target_slot = i;
            break;
        }
    }

    uint16_t space_needed = record_len;
    if (target_slot == -1) {
        // Need a new slot entry too
        space_needed += sizeof(SlotEntry);
    }

    // Check free space
    if (header->free_space_end - header->free_space_begin < space_needed) {
        return -1;  // Not enough space
    }

    // Write record data from end of page backwards
    header->free_space_end -= record_len;
    std::memcpy(page_data + header->free_space_end, record, record_len);

    if (target_slot == -1) {
        // Allocate new slot at end of slot directory
        target_slot = header->num_slots;
        header->num_slots++;
        header->free_space_begin += sizeof(SlotEntry);
    }

    // Update slot entry
    SlotEntry* slot = GetSlotEntry(page_data, target_slot);
    slot->offset = header->free_space_end;
    slot->length = record_len;

    return target_slot;
}

// ============================================================================
// DeleteRecord — mark a slot as deleted (length = 0)
// ============================================================================

bool SlottedPage::DeleteRecord(char* page_data, uint16_t slot_id) {
    PageHeader* header = GetHeader(page_data);

    if (slot_id >= header->num_slots) {
        return false;
    }

    SlotEntry* slot = GetSlotEntry(page_data, slot_id);
    if (slot->length == 0) {
        return false;  // Already deleted
    }

    // Mark as deleted (we don't compact here — space is reclaimed lazily)
    slot->length = 0;
    slot->offset = 0;

    return true;
}

// ============================================================================
// GetRecord — retrieve record data by slot_id
// ============================================================================

const uint8_t* SlottedPage::GetRecord(const char* page_data, uint16_t slot_id, uint16_t* out_len) {
    const PageHeader* header = GetHeader(page_data);

    if (slot_id >= header->num_slots) {
        *out_len = 0;
        return nullptr;
    }

    const SlotEntry* slot = GetSlotEntry(page_data, slot_id);
    if (slot->length == 0) {
        *out_len = 0;
        return nullptr;  // Deleted
    }

    *out_len = slot->length;
    return reinterpret_cast<const uint8_t*>(page_data + slot->offset);
}

// ============================================================================
// UpdateRecord — in-place update (only if new data fits)
// ============================================================================

bool SlottedPage::UpdateRecord(char* page_data, uint16_t slot_id, const uint8_t* record, uint16_t record_len) {
    PageHeader* header = GetHeader(page_data);

    if (slot_id >= header->num_slots) {
        return false;
    }

    SlotEntry* slot = GetSlotEntry(page_data, slot_id);
    if (slot->length == 0) {
        return false;  // Deleted slot
    }

    if (record_len <= slot->length) {
        // Fits in existing space — overwrite in place
        std::memcpy(page_data + slot->offset, record, record_len);
        slot->length = record_len;
        return true;
    }

    // Doesn't fit — caller should delete + re-insert
    return false;
}

// ============================================================================
// GetFreeSpace — bytes available for new records + slot entries
// ============================================================================

uint16_t SlottedPage::GetFreeSpace(const char* page_data) {
    const PageHeader* header = GetHeader(page_data);
    if (header->free_space_end <= header->free_space_begin) {
        return 0;
    }
    return header->free_space_end - header->free_space_begin;
}

// ============================================================================
// GetNumSlots
// ============================================================================

uint16_t SlottedPage::GetNumSlots(const char* page_data) {
    return GetHeader(page_data)->num_slots;
}

// ============================================================================
// IsSlotOccupied
// ============================================================================

bool SlottedPage::IsSlotOccupied(const char* page_data, uint16_t slot_id) {
    const PageHeader* header = GetHeader(page_data);
    if (slot_id >= header->num_slots) {
        return false;
    }
    return GetSlotEntry(page_data, slot_id)->length > 0;
}
