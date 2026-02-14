#pragma once

#include "storage_engine/common/common.h"
#include "storage_engine/buffer/buffer_pool.h"
#include <cstdint>
#include <vector>

// ============================================================================
// Free Space Map (FSM)
//
// Stores one byte per heap page indicating approximate free space.
// Value 0-255 maps to 0–4080 bytes free (in 16-byte granularity).
// FSM data is stored in dedicated page(s) managed by the buffer pool.
//
// Layout of an FSM page:
//   Byte 0: free-space category for heap page 0
//   Byte 1: free-space category for heap page 1
//   ...
//   One FSM page can track up to PAGE_SIZE heap pages.
// ============================================================================

class FreeSpaceMap {
public:
    // fsm_start_page: the page_id of the first FSM page in the file
    FreeSpaceMap(BufferPoolManager* bpm, page_id_t fsm_start_page, uint16_t page_size = 4096);

    // Find a page with at least `needed_bytes` free. Returns INVALID_PAGE_ID if none found.
    page_id_t FindPageWithSpace(uint16_t needed_bytes);

    // Update the free space record for a given heap page.
    void UpdateFreeSpace(page_id_t heap_page_id, uint16_t free_bytes);

    // Register a newly allocated heap page in the FSM.
    void RegisterNewPage(page_id_t heap_page_id, uint16_t free_bytes);

    // Get the FSM start page
    page_id_t GetStartPage() const { return fsm_start_page_; }

private:
    // Convert free bytes to a category byte (0-255)
    uint8_t BytesToCategory(uint16_t free_bytes) const;

    // Convert category byte back to minimum free bytes
    uint16_t CategoryToBytes(uint8_t category) const;

    // Get which FSM page and offset within it for a given heap_page_id
    void GetFSMLocation(page_id_t heap_page_id, page_id_t* fsm_page_id, uint16_t* offset) const;

    BufferPoolManager* bpm_;
    page_id_t fsm_start_page_;
    uint16_t page_size_;
    uint16_t entries_per_page_;  // How many heap pages one FSM page tracks
    static constexpr uint16_t GRANULARITY = 16;  // 16-byte granularity
};
