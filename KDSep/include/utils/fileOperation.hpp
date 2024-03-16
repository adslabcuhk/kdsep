#pragma once

#include "rocksdb/options.h"
#include "utils/debug.hpp"
#include <bits/stdc++.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace std;

namespace KDSEP_NAMESPACE {

enum fileOperationType { kFstream = 0,
    kDirectIO = 1,
    kAlignLinuxIO = 2, 
    kPreadWrite = 3};

typedef struct FileOpStatus {
    bool success_;
    
    // the physical size added to the file
    uint64_t physicalSize_;
    // the logical size added to the file in the disk (not including the buffer)
    uint64_t logicalSize_;
    // the remaining data size in the buffer 
    uint64_t bufferedSize_;
    FileOpStatus(bool success,
        uint64_t physicalSize,
        uint64_t logicalSize,
        uint64_t bufferedSize)
    {
        success_ = success;
        physicalSize_ = physicalSize;
        logicalSize_ = logicalSize;
        bufferedSize_ = bufferedSize;
    };
    FileOpStatus() {};
} FileOpStatus;

class FileOperation {
public:
    FileOperation(fileOperationType operationType);
    FileOperation(fileOperationType operationType, uint64_t fileSize, uint64_t bufferSize);
    FileOperation(fileOperationType operationType, uint64_t fileSize, 
        uint64_t bufferSize, int existing_fd, uint64_t existing_offset);
    ~FileOperation();
    bool canWriteFile(uint64_t size);
    FileOpStatus writeFile(char* write_buf, uint64_t size);
    FileOpStatus writeAndFlushFile(char* write_buf, uint64_t size);
    FileOpStatus readFile(char* read_buf, uint64_t size);
    FileOpStatus positionedReadFile(char* read_buf, uint64_t offset, uint64_t size);
    FileOpStatus flushFile();

    bool openFile(string path);
    bool createFile(string path);
    bool createThenOpenFile(string path);
    bool closeFile();
    bool isFileOpen();
    bool removeAndReopen();
    uint64_t getFileSize();
    uint64_t getCachedFileDataSize();
    uint64_t getCachedFileSize();
    uint64_t getFilePhysicalSize(string path);
    uint64_t getFileBufferedSize();
    uint64_t getStartOffset();
    void markDirectDataAddress(uint64_t data);

    // for recovery
    bool tryOpenAndReadFile(string path, char*& read_buf, uint64_t& data_size, 
	    bool save_page_data_sizes);
    bool retrieveFilePiece(char*& read_buf, uint64_t& data_size, 
	    bool save_page_data_sizes, 
            uint64_t physical_size = ~0ull);
    bool rollbackFile(char* read_buf, uint64_t rollback_offset);

    bool setStartOffset(uint64_t start_offset);
    bool reuseLargeFile(uint64_t start_offset);
    bool reuseLargeFileRecovery(uint64_t start_offset); 

    bool cleanFile();

private:
    fileOperationType operationType_;
    fstream fileStream_;
    int fd_;
    uint64_t page_size_ = sysconf(_SC_PAGESIZE);
    uint64_t page_size_m4_ = sysconf(_SC_PAGESIZE) - sizeof(uint32_t);
    uint64_t disk_size_ = 0;
    uint64_t data_size_ = 0;
    uint64_t is_newly_created_ = false;
    uint64_t max_size_ = 0;
    char* write_buf_ = nullptr;
    uint64_t buf_used_size_ = 0;
    uint64_t buf_size_ = 0;

    string path_;

    uint64_t mark_data_ = 0;
    uint64_t mark_disk_ = 0;
    uint64_t mark_in_page_offset_ = 0;

    // operate an existing file
    bool use_existing_ = false; 
    uint64_t start_offset_ = 0;

    bool recovery_state_ = false;
    bool closed_before_ = false;
    bool debug_flag_ = false;
    std::vector<uint32_t> page_data_sizes_;
};

} // namespace KDSEP_NAMESPACE
