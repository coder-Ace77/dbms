#pragma once
#include <atomic>
#include <string>
#include <vector>
#include "storage_engine/common/common.h"
#include "storage_engine/config/config.h"

class DiskManager {
public:
    explicit DiskManager(const DBConfigs& config);
    ~DiskManager();

    // Disable copy/move to prevent managing the same fd twice
    DiskManager(const DiskManager&) = delete;
    DiskManager& operator=(const DiskManager&) = delete;

    // Core I/O - No Mutex needed!
    void WritePage(page_id_t page_id, const char* data);
    void ReadPage(page_id_t page_id, char* data);

    // Management
    page_id_t AllocatePage();
    void DeallocatePage(page_id_t page_id);
    int GetFileSize();
    
    // Force OS to flush data to physical disk (Critical for WAL later)
    void Sync(); 

private:
    int fd_; // The Linux File Descriptor
    page_size_t page_size_;
    std::string file_name_;
    std::atomic<page_id_t> next_page_id_; // Atomic counter for lock-free allocation
};