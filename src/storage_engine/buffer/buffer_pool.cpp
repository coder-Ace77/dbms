#include "buffer_pool.h" 
#include <iostream>

// ==============================================================================
// LRUReplacer Implementation
// ==============================================================================

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = lru_map_.find(frame_id);
    if (it != lru_map_.end()) {
        lru_list_.erase(it->second);
        lru_map_.erase(it);
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (lru_map_.find(frame_id) == lru_map_.end()) {
        lru_list_.push_front(frame_id);
        lru_map_[frame_id] = lru_list_.begin();
    }
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (lru_list_.empty()) {
        return false;
    }
    *frame_id = lru_list_.back();
    lru_map_.erase(*frame_id);
    lru_list_.pop_back();
    return true;
}

size_t LRUReplacer::Size() {
    std::lock_guard<std::mutex> lock(mutex_);
    return lru_list_.size();
}

// ==============================================================================
// BufferPoolManager Implementation
// ==============================================================================

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
    
    // FIX 1: Use 'new' instead of 'resize'
    pages_ = new Page[pool_size_];
    
    replacer_ = std::make_unique<LRUReplacer>(pool_size);

    for (size_t i = 0; i < pool_size_; ++i) {
        free_list_.emplace_back(static_cast<frame_id_t>(i));
    }
}

BufferPoolManager::~BufferPoolManager() {
    // Flush all dirty pages directly (no lock — we're in destructor, single-threaded)
    for (size_t i = 0; i < pool_size_; ++i) {
        if (pages_[i].IsDirty() && pages_[i].GetPageId() != INVALID_PAGE_ID) {
            disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
            pages_[i].is_dirty_ = false;
        }
    }
    disk_manager_->Sync();
    delete[] pages_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    if (page_table_.find(page_id) != page_table_.end()) {
        frame_id_t frame_id = page_table_[page_id];
        replacer_->Pin(frame_id);
        pages_[frame_id].pin_count_++;
        return &pages_[frame_id];
    }

    frame_id_t frame_id;
    if (!FindFreeFrame(&frame_id)) {
        return nullptr;
    }

    Page *page = &pages_[frame_id];

    if (page->IsDirty()) {
        disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }

    page_table_.erase(page->GetPageId());
    page_table_[page_id] = frame_id;

    page->page_id_ = page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    page->ResetMemory(); 

    disk_manager_->ReadPage(page_id, page->GetData());
    replacer_->Pin(frame_id);

    return page;
}

Page *BufferPoolManager::NewPage(page_id_t *page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    frame_id_t frame_id;
    if (!FindFreeFrame(&frame_id)) {
        return nullptr;
    }

    *page_id = disk_manager_->AllocatePage();
    Page *page = &pages_[frame_id];
    
    if (page->IsDirty()) {
        disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }

    page_table_.erase(page->GetPageId());
    page_table_[*page_id] = frame_id;

    page->page_id_ = *page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    page->ResetMemory(); 

    replacer_->Pin(frame_id);

    return page;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    std::lock_guard<std::mutex> lock(latch_);

    if (page_table_.find(page_id) == page_table_.end()) {
        return false;
    }

    frame_id_t frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];

    if (is_dirty) {
        page->is_dirty_ = true;
    }

    if (page->pin_count_ <= 0) {
        return false;
    }

    page->pin_count_--;

    if (page->pin_count_ == 0) {
        replacer_->Unpin(frame_id);
    }

    return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);
    
    if (page_table_.find(page_id) == page_table_.end()) {
        return false;
    }
    
    frame_id_t frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];
    
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
    return true;
}

void BufferPoolManager::FlushAllPages() {
    std::lock_guard<std::mutex> lock(latch_);
    for (auto& [pid, fid] : page_table_) {
        Page* page = &pages_[fid];
        if (page->IsDirty() && page->GetPageId() != INVALID_PAGE_ID) {
            disk_manager_->WritePage(page->GetPageId(), page->GetData());
            page->is_dirty_ = false;
        }
    }
    disk_manager_->Sync();
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    if (page_table_.find(page_id) == page_table_.end()) {
        disk_manager_->DeallocatePage(page_id);
        return true;
    }

    frame_id_t frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];

    if (page->pin_count_ > 0) {
        return false;
    }

    disk_manager_->DeallocatePage(page_id);
    page_table_.erase(page_id);

    page->page_id_ = INVALID_PAGE_ID;
    page->is_dirty_ = false;
    page->pin_count_ = 0;
    page->ResetMemory();

    free_list_.push_back(frame_id);
    return true;
}

bool BufferPoolManager::FindFreeFrame(frame_id_t *out_frame_id) {
    if (!free_list_.empty()) {
        *out_frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }
    return replacer_->Victim(out_frame_id);
}