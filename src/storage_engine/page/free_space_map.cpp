#include "free_space_map.h"
#include <algorithm>

// ============================================================================
// Constructor
// ============================================================================

FreeSpaceMap::FreeSpaceMap(BufferPoolManager* bpm, page_id_t fsm_start_page, uint16_t page_size)
    : bpm_(bpm), fsm_start_page_(fsm_start_page), page_size_(page_size) {
    entries_per_page_ = page_size_;  // 1 byte per entry, so one FSM page tracks page_size_ heap pages
}

// ============================================================================
// BytesToCategory / CategoryToBytes — quantize free space
// ============================================================================

uint8_t FreeSpaceMap::BytesToCategory(uint16_t free_bytes) const {
    uint16_t cat = free_bytes / GRANULARITY;
    return static_cast<uint8_t>(std::min<uint16_t>(cat, 255));
}

uint16_t FreeSpaceMap::CategoryToBytes(uint8_t category) const {
    return static_cast<uint16_t>(category) * GRANULARITY;
}

// ============================================================================
// GetFSMLocation — map heap_page_id → (fsm_page_id, offset)
// ============================================================================

void FreeSpaceMap::GetFSMLocation(page_id_t heap_page_id, page_id_t* fsm_page_id, uint16_t* offset) const {
    *fsm_page_id = fsm_start_page_ + (heap_page_id / entries_per_page_);
    *offset = static_cast<uint16_t>(heap_page_id % entries_per_page_);
}

// ============================================================================
// FindPageWithSpace — linear scan of FSM entries
// ============================================================================

page_id_t FreeSpaceMap::FindPageWithSpace(uint16_t needed_bytes) {
    uint8_t needed_cat = BytesToCategory(needed_bytes);

    // Scan FSM pages starting from the first
    // We'll scan up to a reasonable limit (first FSM page should cover most cases)
    Page* fsm_page = bpm_->FetchPage(fsm_start_page_);
    if (!fsm_page) return INVALID_PAGE_ID;

    const uint8_t* data = reinterpret_cast<const uint8_t*>(fsm_page->GetData());

    for (uint16_t i = 0; i < entries_per_page_; ++i) {
        if (data[i] >= needed_cat && data[i] > 0) {
            bpm_->UnpinPage(fsm_start_page_, false);
            return static_cast<page_id_t>(i);
        }
    }

    bpm_->UnpinPage(fsm_start_page_, false);
    return INVALID_PAGE_ID;  // No page with enough space
}

// ============================================================================
// UpdateFreeSpace — write the category for a heap page
// ============================================================================

void FreeSpaceMap::UpdateFreeSpace(page_id_t heap_page_id, uint16_t free_bytes) {
    page_id_t fsm_page_id;
    uint16_t offset;
    GetFSMLocation(heap_page_id, &fsm_page_id, &offset);

    Page* fsm_page = bpm_->FetchPage(fsm_page_id);
    if (!fsm_page) return;

    uint8_t* data = reinterpret_cast<uint8_t*>(fsm_page->GetData());
    data[offset] = BytesToCategory(free_bytes);

    bpm_->UnpinPage(fsm_page_id, true);  // Dirty
}

// ============================================================================
// RegisterNewPage — same as UpdateFreeSpace but semantically distinct
// ============================================================================

void FreeSpaceMap::RegisterNewPage(page_id_t heap_page_id, uint16_t free_bytes) {
    UpdateFreeSpace(heap_page_id, free_bytes);
}
