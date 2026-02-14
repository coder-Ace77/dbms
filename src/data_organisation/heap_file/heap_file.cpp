#include "heap_file.h"
#include <iostream>
#include <stdexcept>

// ============================================================================
// Constructor
// ============================================================================

HeapFile::HeapFile(BufferPoolManager* bpm, FreeSpaceMap* fsm, page_id_t first_page_id)
    : bpm_(bpm), fsm_(fsm), first_page_id_(first_page_id), max_page_id_(first_page_id) {}

// ============================================================================
// AllocateNewPage — create a new data page, register with FSM
// ============================================================================

page_id_t HeapFile::AllocateNewPage() {
    page_id_t new_page_id;
    Page* page = bpm_->NewPage(&new_page_id);
    if (!page) {
        throw std::runtime_error("HeapFile: Failed to allocate new page");
    }

    // Initialize as a slotted page
    SlottedPage::Init(page->GetData());

    // Register with FSM — full page free space (minus header)
    uint16_t free_space = SlottedPage::GetFreeSpace(page->GetData());
    fsm_->RegisterNewPage(new_page_id, free_space);

    bpm_->UnpinPage(new_page_id, true);  // Dirty — we initialized it

    if (new_page_id > max_page_id_) {
        max_page_id_ = new_page_id;
    }

    return new_page_id;
}

// ============================================================================
// InsertRecord
// ============================================================================

RecordID HeapFile::InsertRecord(const BsonDocument& doc) {
    // Serialize the document
    std::vector<uint8_t> data = BsonSerializer::Serialize(doc);
    uint16_t record_len = static_cast<uint16_t>(data.size());

    // Need space for the record + a slot entry (4 bytes)
    uint16_t total_needed = record_len + sizeof(SlotEntry);

    // Ask FSM for a page with enough space
    page_id_t target_page = fsm_->FindPageWithSpace(total_needed);

    if (target_page == INVALID_PAGE_ID) {
        // No existing page has space — allocate a new one
        target_page = AllocateNewPage();
    }

    // Fetch the page
    Page* page = bpm_->FetchPage(target_page);
    if (!page) {
        throw std::runtime_error("HeapFile: Failed to fetch page " + std::to_string(target_page));
    }

    // Insert into slotted page
    int16_t slot_id = SlottedPage::InsertRecord(page->GetData(), data.data(), record_len);

    if (slot_id == -1) {
        // Page didn't have enough space (FSM was stale) — try a new page
        bpm_->UnpinPage(target_page, false);
        target_page = AllocateNewPage();
        page = bpm_->FetchPage(target_page);
        if (!page) {
            throw std::runtime_error("HeapFile: Failed to fetch new page");
        }
        slot_id = SlottedPage::InsertRecord(page->GetData(), data.data(), record_len);
        if (slot_id == -1) {
            bpm_->UnpinPage(target_page, false);
            throw std::runtime_error("HeapFile: Record too large for a single page");
        }
    }

    // Update FSM with remaining free space
    uint16_t remaining = SlottedPage::GetFreeSpace(page->GetData());
    fsm_->UpdateFreeSpace(target_page, remaining);

    bpm_->UnpinPage(target_page, true);  // Dirty

    RecordID rid;
    rid.page_id = target_page;
    rid.slot_id = static_cast<uint16_t>(slot_id);
    return rid;
}

// ============================================================================
// DeleteRecord
// ============================================================================

bool HeapFile::DeleteRecord(const RecordID& rid) {
    Page* page = bpm_->FetchPage(rid.page_id);
    if (!page) return false;

    bool ok = SlottedPage::DeleteRecord(page->GetData(), rid.slot_id);

    if (ok) {
        // Update FSM
        uint16_t remaining = SlottedPage::GetFreeSpace(page->GetData());
        fsm_->UpdateFreeSpace(rid.page_id, remaining);
    }

    bpm_->UnpinPage(rid.page_id, ok);  // Dirty if deleted
    return ok;
}

// ============================================================================
// GetRecord
// ============================================================================

BsonDocument HeapFile::GetRecord(const RecordID& rid) {
    Page* page = bpm_->FetchPage(rid.page_id);
    if (!page) {
        throw std::runtime_error("HeapFile: Failed to fetch page " + std::to_string(rid.page_id));
    }

    uint16_t len;
    const uint8_t* data = SlottedPage::GetRecord(page->GetData(), rid.slot_id, &len);

    if (!data || len == 0) {
        bpm_->UnpinPage(rid.page_id, false);
        throw std::runtime_error("HeapFile: Record not found at " +
            std::to_string(rid.page_id) + ":" + std::to_string(rid.slot_id));
    }

    BsonDocument doc = BsonSerializer::Deserialize(data, len);
    bpm_->UnpinPage(rid.page_id, false);
    return doc;
}

// ============================================================================
// UpdateRecord — try in-place, else delete + re-insert
// ============================================================================

RecordID HeapFile::UpdateRecord(const RecordID& rid, const BsonDocument& doc) {
    std::vector<uint8_t> data = BsonSerializer::Serialize(doc);
    uint16_t record_len = static_cast<uint16_t>(data.size());

    Page* page = bpm_->FetchPage(rid.page_id);
    if (!page) {
        throw std::runtime_error("HeapFile: Failed to fetch page for update");
    }

    bool ok = SlottedPage::UpdateRecord(page->GetData(), rid.slot_id, data.data(), record_len);

    if (ok) {
        bpm_->UnpinPage(rid.page_id, true);
        return rid;
    }

    // Didn't fit — delete old, insert new
    bpm_->UnpinPage(rid.page_id, false);
    DeleteRecord(rid);
    return InsertRecord(doc);
}

// ============================================================================
// Iterator
// ============================================================================

HeapFile::Iterator::Iterator(HeapFile* heap, page_id_t start_page, page_id_t max_page)
    : heap_(heap), current_page_(start_page), start_page_(start_page),
      max_page_(max_page), current_slot_(0) {}

bool HeapFile::Iterator::Next(RecordID* out_rid, BsonDocument* out_doc) {
    while (current_page_ <= max_page_) {
        Page* page = heap_->bpm_->FetchPage(current_page_);
        if (!page) {
            current_page_++;
            current_slot_ = 0;
            continue;
        }

        uint16_t num_slots = SlottedPage::GetNumSlots(page->GetData());

        while (current_slot_ < num_slots) {
            if (SlottedPage::IsSlotOccupied(page->GetData(), current_slot_)) {
                uint16_t len;
                const uint8_t* data = SlottedPage::GetRecord(page->GetData(), current_slot_, &len);

                if (data && len > 0) {
                    out_rid->page_id = current_page_;
                    out_rid->slot_id = current_slot_;
                    *out_doc = BsonSerializer::Deserialize(data, len);
                    
                    current_slot_++;
                    heap_->bpm_->UnpinPage(current_page_, false);
                    return true;
                }
            }
            current_slot_++;
        }

        heap_->bpm_->UnpinPage(current_page_, false);
        current_page_++;
        current_slot_ = 0;
    }

    return false;  // No more records
}

void HeapFile::Iterator::Reset() {
    current_page_ = start_page_;
    current_slot_ = 0;
}

HeapFile::Iterator HeapFile::Begin() {
    return Iterator(this, first_page_id_, max_page_id_);
}
