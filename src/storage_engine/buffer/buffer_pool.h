#pragma once

#include <vector>
#include <list>
#include <mutex>
#include <memory>
#include <unordered_map>
#include "storage_engine/common/common.h"
#include "storage_engine/disk_manager/disk_manager.h"
#include "storage_engine/config/config.h"
#include <cstring>
#include <shared_mutex>

using frame_id_t = int32_t;
constexpr frame_id_t INVALID_FRAME_ID = -1;
const int32_t PAGE_SIZE = 4096;

/**
 * Page Class
 * Wrapper around the 4KB data array with metadata.
 */
class Page {
    friend class BufferPoolManager; 

public:
    Page() { ResetMemory(); }
    ~Page() = default;

    inline char *GetData() { return data_; }
    
    inline page_id_t GetPageId() { return page_id_; }
    
    inline int GetPinCount() { return pin_count_; }

    inline bool IsDirty() { return is_dirty_; }

    inline void WLatch() { rwlatch_.lock(); }
    inline void WUnlatch() { rwlatch_.unlock(); }
    inline void RLatch() { rwlatch_.lock_shared(); }
    inline void RUnlatch() { rwlatch_.unlock_shared(); }

private:
    void ResetMemory() { memset(data_, 0, PAGE_SIZE); }

    char data_[PAGE_SIZE]{}; 
    page_id_t page_id_ = INVALID_PAGE_ID;
    
    int pin_count_ = 0;   
    bool is_dirty_ = false; 
    std::shared_mutex rwlatch_; 
};

/**
 * LRU Replacer
 * Keeps track of unpinned frames to decide which one to evict.
 */
class LRUReplacer {
public:
    explicit LRUReplacer(size_t num_pages) {}
    ~LRUReplacer() = default;

    // Remove the object that was accessed the least recently (victim)
    bool Victim(frame_id_t *frame_id);

    // Pin: Called when a page is pinned (removed from LRU candidates)
    void Pin(frame_id_t frame_id);

    // Unpin: Called when a page is unpinned (added to LRU candidates)
    void Unpin(frame_id_t frame_id);

    size_t Size();

private:
    std::mutex mutex_;
    std::list<frame_id_t> lru_list_; // Front = Least recently used
    std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> lru_map_;
};

/**
 * Buffer Pool Manager
 */
class BufferPoolManager {
public:
    BufferPoolManager(size_t pool_size, DiskManager *disk_manager);
    ~BufferPoolManager();

    // 1. Fetch a page from disk or memory. Returns nullptr if no frames free.
    Page *FetchPage(page_id_t page_id);

    // 2. Unpin a page. Set is_dirty to true if you modified it.
    bool UnpinPage(page_id_t page_id, bool is_dirty);

    // 3. Create a new page on disk and bring it into memory.
    Page *NewPage(page_id_t *page_id);

    // 4. Delete a page from disk and memory.
    bool DeletePage(page_id_t page_id);

    // 5. Force flush a specific page to disk.
    bool FlushPage(page_id_t page_id);

    // 6. Flush ALL dirty pages to disk.
    void FlushAllPages();

private:
    // Helper to find a free frame or evict a victim
    bool FindFreeFrame(frame_id_t *out_frame_id);

    size_t pool_size_;
    Page* pages_; // The actual memory pool (array of Pages)
    
    DiskManager *disk_manager_;
    std::unique_ptr<LRUReplacer> replacer_;

    // Map PageID -> FrameID
    std::unordered_map<page_id_t, frame_id_t> page_table_;
    
    // List of free frames (indices in pages_ vector)
    std::list<frame_id_t> free_list_;
    
    std::mutex latch_; // Global latch for BPM structure safety
};