#include "disk_manager.h"
#include <fcntl.h>
#include <unistd.h>     
#include <sys/stat.h>   
#include <stdexcept>
#include <iostream>
#include <cstring>

DiskManager::DiskManager(const DBConfigs& config) : file_name_(config.db_file_name) , page_size_(config.page_size){

    fd_ = open(file_name_.c_str(), O_RDWR | O_CREAT, 0664);
    if (fd_ == -1) {
        throw std::runtime_error("Error opening database file: " + std::string(strerror(errno)));
    }

    off_t file_size = lseek(fd_, 0, SEEK_END);
    
    if (file_size == -1) {
        throw std::runtime_error("Error seeking file end: " + std::string(strerror(errno)));
    }

    if(file_size > 0){
        next_page_id_ = file_size / config.page_size;
    } else {
        next_page_id_ = 0;
    }
}

DiskManager::~DiskManager() {
    if (fd_ != -1) {
        close(fd_);
    }
}

void DiskManager::WritePage(page_id_t page_id, const char* data){

    off_t offset = static_cast<off_t>(page_id)*page_size_; 

    ssize_t bytes_written = pwrite(fd_, data, page_size_, offset);

    if (bytes_written == -1) {
        throw std::runtime_error("I/O error writing page " + std::to_string(page_id) + ": " + strerror(errno));
    }
    if(bytes_written != page_size_){
        std::cerr << "Warning: Partial write to page " << page_id << std::endl;
    }
}

void DiskManager::ReadPage(page_id_t page_id, char* data){
    off_t offset = static_cast<off_t>(page_id) *page_size_;

    ssize_t bytes_read = pread(fd_, data, page_size_, offset);

    if (bytes_read == -1) {
        throw std::runtime_error("I/O error reading page " + std::to_string(page_id) + ": " + strerror(errno));
    }
    
    if (bytes_read < page_size_) {
        std::memset(data + bytes_read, 0, page_size_ - bytes_read);
    }
}

page_id_t DiskManager::AllocatePage() {
    return next_page_id_.fetch_add(1);
}

void DiskManager::DeallocatePage(page_id_t page_id){
}

page_size_t DiskManager::GetFileSize(){
    struct stat st;
    if(fstat(fd_, &st) == -1){
        return -1;
    }
    return st.st_size;
}

void DiskManager::Sync() {
    if (fsync(fd_) == -1) {
        throw std::runtime_error("fsync failed: " + std::string(strerror(errno)));
    }
}