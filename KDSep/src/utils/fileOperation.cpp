#include "utils/fileOperation.hpp"
#include "utils/statsRecorder.hh"

using namespace std;

namespace KDSEP_NAMESPACE {

FileOperation::FileOperation(fileOperationType operationType)
{
    operationType_ = operationType;
    fd_ = -1;
    max_size_ = 256 * 1024;
    buf_size_ = 0; // page_size_ - sizeof(uint32_t);
    write_buf_ = (buf_size_ > 0) ? new char[buf_size_] : nullptr;
    buf_used_size_ = 0;
}

FileOperation::FileOperation(fileOperationType operationType, uint64_t
        fileSize, uint64_t bufferSize) {
    operationType_ = operationType;
    fd_ = -1;
    max_size_ = fileSize;
    write_buf_ = (bufferSize > 0) ? new char[bufferSize] : nullptr;
    buf_size_ = bufferSize;
    buf_used_size_ = 0;
}

// open an existing file
FileOperation::FileOperation(fileOperationType operationType, 
    uint64_t fileSize, uint64_t bufferSize, int existing_fd, uint64_t offset)
{
    operationType_ = operationType;
    fd_ = existing_fd; //-1;
    max_size_ = fileSize;
    write_buf_ = (bufferSize > 0) ? new char[bufferSize] : nullptr;
    buf_size_ = bufferSize;
    buf_used_size_ = 0;

    use_existing_ = true;
    start_offset_ = offset;
}

FileOperation::~FileOperation()
{
    if (write_buf_ != nullptr) { 
        delete[] write_buf_;
    }
}

bool FileOperation::createFile(string path)
{
    path_ = path;
    if (operationType_ == kDirectIO || 
            operationType_ == kAlignLinuxIO ||
            operationType_ == kPreadWrite) {
        fd_ = open(path.c_str(), O_CREAT, 0644);
        if (fd_ == -1) {
            debug_error("[ERROR] File descriptor (create) = %d, err = %s\n", fd_, strerror(errno));
            return false;
        } else {
            closed_before_ = false;
            is_newly_created_ = true;
            return true;
        }
    } 
    return false;
}

bool FileOperation::openFile(string path)
{
    path_ = path;
    if (operationType_ == kDirectIO ||
            operationType_ == kAlignLinuxIO || 
            operationType_ == kPreadWrite) {
        auto flag = O_RDWR | (operationType_ == kDirectIO ? O_DIRECT : 0);
        fd_ = open(path.c_str(), flag, 0644);
        if (fd_ == -1) {
            debug_error("[ERROR] File descriptor (open) = %d, err = %s\n", fd_, strerror(errno));
            return false;
        } else {
            closed_before_ = false;
            if (is_newly_created_ == true) {
                disk_size_ = 0;
                data_size_ = 0;
                debug_info("Open new file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fd_, disk_size_, data_size_);
            } else {
                disk_size_ = getFilePhysicalSize(path);
                data_size_ = getFileSize();
                debug_info("Open old file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fd_, disk_size_, data_size_);
            }
            return true;
        }
    } else {
        return false;
    }
}

bool FileOperation::tryOpenAndReadFile(string path, char*& read_buf, 
        uint64_t& data_size, bool save_page_data_sizes)
{
    path_ = path;
    if (operationType_ == kDirectIO ||
            operationType_ == kAlignLinuxIO) {
        auto flag = O_RDWR | (operationType_ == kDirectIO ? O_DIRECT : 0);
        fd_ = open(path.c_str(), flag, 0644);
        read_buf = nullptr;
        if (fd_ == -1) {
            if (errno != 2) {
                // Not "No such file or directory"
                debug_error("[ERROR] err = %s, path %s\n",
                    strerror(errno), path.c_str());
            }
            return false;
        } 
        closed_before_ = false;

        if (is_newly_created_ == true) {
            disk_size_ = 0;
            data_size_ = 0;
            debug_info("Open new file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fd_, disk_size_, data_size_);
        } 
        
        // here different from openFile()
        disk_size_ = getFilePhysicalSize(path);
        data_size_ = 0;

        uint64_t req_page_num = (disk_size_ + page_size_ - 1) / page_size_;
        // align mem
        char* readBuffer;
        auto readBufferSize = page_size_ * req_page_num;
        auto ret = posix_memalign((void**)&readBuffer, page_size_, readBufferSize);
//        debug_error("rss %lu disk_size_ %lu readBufferSize %lu req_page_num "
//            "%lu\n", getRss(), disk_size_, readBufferSize, req_page_num);
        read_buf = new char[readBufferSize];

        if (ret) {
            debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
            delete[] read_buf;
            read_buf = nullptr;
            return false;
        }


        uint64_t left = readBufferSize, p = 0;
        while (left > 0) {
            if (debug_flag_) {
                fprintf(stdout, "[%d %s] %p pread offset %lu left %lu\n",
                        __LINE__, __func__, this, start_offset_ + p, left);
            }
            auto rReturn = pread(fd_, readBuffer + p, left, start_offset_ + p);
            if (rReturn > left || rReturn == 0 || rReturn < 0) {
                debug_error("rReturn %ld %lx err %s left %lu\n", 
                        rReturn, rReturn, strerror(errno), left);
                delete[] read_buf;
                read_buf = nullptr;
                return false;
            }
            p += rReturn;
            left -= rReturn;
        }

        if (p != readBufferSize) {
            free(readBuffer);
            delete[] read_buf;
            read_buf = nullptr;
            debug_error("[ERROR] Read return value = %lu, err = %s,"
                    " req_page_num = %lu, readBuffer size = %lu, disk_size_ ="
                    " %lu\n", p, strerror(errno), req_page_num, readBufferSize,
                    disk_size_);
            return false;
        }

        data_size = 0;
        disk_size_ = 0;

        if (save_page_data_sizes) {
            page_data_sizes_.clear();
        }
        for (auto page_i = 0; page_i < req_page_num; page_i++) {
            uint32_t page_data_size = 0;
            memcpy(&page_data_size, readBuffer + page_i * page_size_, sizeof(uint32_t));

            if (page_data_size == 0) {
                break;
            }

            if (save_page_data_sizes) {
                page_data_sizes_.push_back(page_data_size);
            }

            disk_size_ += page_size_;
            memcpy(read_buf + data_size, 
                    readBuffer + page_i * page_size_ + sizeof(uint32_t), 
                    page_data_size);
            data_size += page_data_size;
        }
        data_size_ = data_size;
        free(readBuffer);
        debug_info("Open old file at path = %s, file fd = %d, current "
                " physical file size = %lu, actual file size = %lu\n", 
                path.c_str(), fd_, disk_size_, data_size_);

        if (save_page_data_sizes) {
            recovery_state_ = true;
        }
        return true;
    } else {
        return false;
    }
}

// for recovery. Don't know the file size
bool FileOperation::retrieveFilePiece(char*& read_buf, 
        uint64_t& data_size, bool save_page_data_sizes, 
        uint64_t physical_size) {
    if (operationType_ != kDirectIO && operationType_ != kAlignLinuxIO) {
        debug_error("not direct IO%s\n", "");
        exit(1);
    }

    if (!use_existing_) {
        debug_error("[ERROR] open a non-existing file %s\n", "");
        exit(1);
    }

    disk_size_ = max_size_;
    data_size_ = 0;

    // check whether the file is too small
    if (physical_size < start_offset_) {
        debug_error("[ERROR] file too small to retrieve: %lu %lu\n", 
                physical_size, start_offset_);
        return false;
    } else if (physical_size < start_offset_ + max_size_) {
        disk_size_ = physical_size - start_offset_;
    }

    if (disk_size_ == 0) {
        read_buf = nullptr;
        data_size = 0;
        return true;
    }

    uint64_t req_page_num = disk_size_ / page_size_;
    // align mem
    char* cur_read_buf;
    auto read_buf_sz = page_size_ * req_page_num;
    auto ret = posix_memalign((void**)&cur_read_buf, page_size_, read_buf_sz);
    read_buf = new char[read_buf_sz];

    if (ret) {
        debug_error("[ERROR] posix_memalign failed: %d %s\n", errno,
                strerror(errno));
        return false;
    }

    if (debug_flag_) {
        fprintf(stdout, "[%d %s] %p pread offset %lu left %lu\n",
                __LINE__, __func__, this, start_offset_, read_buf_sz);
    }
    auto rReturn = pread(fd_, cur_read_buf, read_buf_sz, start_offset_);
    if (rReturn != read_buf_sz) {
        free(cur_read_buf);
        debug_error("[ERROR] Read return value = %lu, err = %s,"
                " req_page_num = %lu, cur_read_buf size = %lu, disk_size_ ="
                " %lu, start_offset_ %lu physical %lu\n",
                rReturn, strerror(errno), req_page_num, read_buf_sz,
                disk_size_, start_offset_, physical_size);
        return false;
    }

    data_size = 0;
    disk_size_ = 0;

    if (save_page_data_sizes) {
        page_data_sizes_.clear();
    }
    for (auto page_i = 0; page_i < req_page_num; page_i++) {
        uint32_t page_data_size = 0;
        memcpy(&page_data_size, cur_read_buf + page_i * page_size_,
                sizeof(uint32_t));

        if (page_data_size == 0) {
            break;
        }

        if (save_page_data_sizes) {
            page_data_sizes_.push_back(page_data_size);
        }

        disk_size_ += page_size_;
        memcpy(read_buf + data_size, 
                cur_read_buf + page_i * page_size_ + sizeof(uint32_t), 
                page_data_size);
        data_size += page_data_size;
    }
    data_size_ = data_size;
    free(cur_read_buf);
    debug_info("Open old fd = %d, current "
            " physical file size = %lu, actual file size = %lu\n",
            fd_, disk_size_, data_size_);

    if (save_page_data_sizes) {
        recovery_state_ = true;
    }
    return true;
}

bool FileOperation::cleanFile() {
    if (!use_existing_) {
        debug_error("[ERROR] clean a non-existing file %s\n", "");
        exit(1);
    }

    // generate a buffer filled with zeros, and size is max_size_
    char* zeroBuffer;
    auto ret = posix_memalign((void**)&zeroBuffer, page_size_, max_size_);
    if (ret) {
        debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
        return false;
    }
    memset(zeroBuffer, 0, max_size_);

    if (debug_flag_) {
        fprintf(stdout, "[%d %s] %p pwrite offset %lu left %lu\n",
                __LINE__, __func__, this, 
                start_offset_, max_size_);
    }
    ret = pwrite(fd_, zeroBuffer, max_size_, start_offset_);
    if (ret != max_size_) {
        debug_error("[ERROR] pwrite failed: %d %s ret %d\n", errno,
                strerror(errno), ret);
        throw std::runtime_error("exception");
    }

    free(zeroBuffer);
    buf_used_size_ = 0;
    disk_size_ = 0;
    data_size_ = 0;
    return true;
}

bool FileOperation::rollbackFile(char* read_buf, uint64_t rollback_offset)
{
    if (rollback_offset > data_size_) {
        debug_error("[ERROR] roll back offset too large: %lu > %lu\n",
            rollback_offset, data_size_);
        return false;
    }

    if (recovery_state_ == false) {
        debug_error("[ERROR] not in recovery but rollback: %lu %lu\n",
            data_size_, rollback_offset);
        return false;
    }

    uint64_t data_i_on_disk = 0;
    for (auto i = 0; i < page_data_sizes_.size(); i++) {
        if (data_i_on_disk + page_data_sizes_[i] > rollback_offset) {
            debug_info("roll back to page %d\n", i);

            data_size_ = data_i_on_disk;
            disk_size_ = i * page_size_;
            buf_used_size_ = rollback_offset - data_i_on_disk;
//            debug_error("roll back: %s new data size %lu new disk size %lu"
//                        " roll offset %lu buf size %lu\n",
//                path_.c_str(),
//                data_size_, disk_size_, rollback_offset,
//                buf_used_size_);

            if (buf_used_size_ > 0) {
                memcpy(write_buf_, read_buf + data_i_on_disk,
                    buf_used_size_);
            }
            break;
        }
        data_i_on_disk += page_data_sizes_[i];
    }

    return true;
}

bool FileOperation::setStartOffset(uint64_t start_offset) {
    start_offset_ = start_offset;
    if (!use_existing_) {
        debug_error("[ERROR] a non-existing file %s\n", "");
        exit(1);
    }
    return true;
}

bool FileOperation::reuseLargeFileRecovery(uint64_t start_offset) {
    if (!use_existing_) {
        debug_error("[ERROR] reuse a non-existing file %s\n", "");
        exit(1);
    }
    if (debug_flag_) {
        fprintf(stdout, "[%d %s] %p reuseLargeFile old_offset %lu "
                "start_offset %lu\n",
                __LINE__, __func__, this, start_offset_, start_offset);
    }
    start_offset_ = start_offset;
    return true;
}

bool FileOperation::reuseLargeFile(uint64_t start_offset) {
    if (!use_existing_) {
        debug_error("[ERROR] reuse a non-existing file %s\n", "");
        exit(1);
    }
    if (debug_flag_) {
        fprintf(stdout, "[%d %s] %p reuseLargeFile old_offset %lu "
                "start_offset %lu\n",
                __LINE__, __func__, this, start_offset_, start_offset);
    }
    start_offset_ = start_offset;
    cleanFile();
    return true;
}

uint64_t FileOperation::getStartOffset() {
    return start_offset_;
} 

bool FileOperation::createThenOpenFile(string path)
{
    path_ = path;
    switch (operationType_) {
    case kDirectIO:
    case kAlignLinuxIO:
    case kPreadWrite: 
    {
        if (use_existing_) {
            // ignore the variable path
            cleanFile();
            return true;
        } else {
            auto flag = O_CREAT | O_RDWR | 
                (operationType_ == kDirectIO ? O_DIRECT : 0);
            fd_ = open(path.c_str(), flag, 0644);
            if (fd_ == -1) {
                debug_error("[ERROR] File descriptor (open) = %d, err = %s,"
                        " operationType %d path %s\n", fd_, strerror(errno),
                        operationType_, path.c_str());
                exit(1);
                return false;
//            } else {
//                int allocateStatus = fallocate(fd_, 0, 0, max_size_);
//                if (allocateStatus != 0) {
//                    debug_error("[WARN] Could not pre-allocate space for current file: %s", path.c_str());
//                }
            }
            disk_size_ = 0;
            data_size_ = 0;
            debug_info("Open new file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fd_, disk_size_, data_size_);
            return true;
        }
        break;
    }
    default:
        return false;
        break;
    }
    return false;
}

bool FileOperation::closeFile()
{
    if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO ||
            operationType_ == kPreadWrite) {
        buf_used_size_ = 0;
        if (!use_existing_) {
            debug_info("Close file fd = %d\n", fd_);
            int status = close(fd_);
            if (status == 0) {
                debug_info("Close file success, current file fd = %d\n", fd_);
                closed_before_ = true;
                fd_ = -1;
                return true;
            } else {
                debug_error("[ERROR] File descriptor (close) = %d, err = %s\n", fd_, strerror(errno));
                return false;
            }
        } else {
            debug_error("Closing an existing file %s\n", "");
            return false;
        }
    } else {
        return false;
    }
}

bool FileOperation::isFileOpen()
{
    if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO ||
            operationType_ == kPreadWrite) {
        return (fd_ != -1);
    } else {
        return false;
    }
}

uint64_t FileOperation::getFileBufferedSize()
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    return buf_used_size_;
}

void FileOperation::markDirectDataAddress(uint64_t data) {
    mark_data_ = data;
    mark_disk_ = disk_size_;
    mark_in_page_offset_ = (buf_used_size_ == 0) ? 0 : 1;

    if (recovery_state_) {
        uint64_t data_i_on_disk = 0;
        for (auto i = 0; i < page_data_sizes_.size(); i++) {
            if (data_i_on_disk + page_data_sizes_[i] == data) {
                mark_disk_ = i * page_size_;
            }
        }
        mark_in_page_offset_ = 0; // must be in recovery state
    }
}

// A simple version of write file
bool FileOperation::canWriteFile(uint64_t write_size) {
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (buf_size_ > 0) {
            // always assume that it needs flush
            uint64_t req_page_num = (write_size + buf_used_size_ +
                    page_size_m4_ - 1) / page_size_m4_;
            uint64_t written_size = 0;
            auto writeBufferSize = page_size_ * req_page_num;
            uint64_t page_i = 0;

            return writeBufferSize + disk_size_ <= max_size_;
        } else {
            uint64_t req_page_num = (write_size + page_size_m4_ - 1) /
                page_size_m4_;
            auto writeBufferSize = page_size_ * req_page_num;
            return writeBufferSize + disk_size_ <= max_size_;
        }
    } else if (operationType_ == kPreadWrite) {
        if (write_size + buf_used_size_ <= buf_size_) {
            return true;
        } else if (buf_size_ > 0) {
            // need to flush
            debug_e("Not implemented");
            return false;
        } else {
            return write_size + disk_size_ <= max_size_;
        }
    } else {
        return false;
    }
}

FileOpStatus FileOperation::writeFile(char* contentBuffer, uint64_t write_size)
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (write_size + buf_used_size_ <= buf_size_) {
            memcpy(write_buf_ + buf_used_size_, contentBuffer, write_size);
            buf_used_size_ += write_size;
            FileOpStatus ret(true, 0, 0, write_size);
            return ret;
        } else if (buf_size_ > 0) {
            // need to flush
            uint64_t req_page_num = (write_size + buf_used_size_ +
                    page_size_m4_ - 1) / page_size_m4_;
            uint64_t written_size = 0;
            // align mem
            char* write_buf_dio;
            auto writeBufferSize = page_size_ * req_page_num;
            auto ret = posix_memalign((void**)&write_buf_dio, page_size_, writeBufferSize);
            if (ret) {
                debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            } else {
                memset(write_buf_dio, 0, writeBufferSize);
            }
            uint64_t page_i = 0;
            uint64_t targetWriteSize = buf_used_size_ + write_size;

            uint64_t previousBufferUsedSize = buf_used_size_;
            char exist_content[targetWriteSize];
            memcpy(exist_content, write_buf_, previousBufferUsedSize);
            memcpy(exist_content + previousBufferUsedSize, contentBuffer, write_size);
            int actual_disk_write_size = 0;
            uint32_t currentPageWriteSize = 0;
            while (written_size != targetWriteSize) {
                if ((targetWriteSize - written_size) >= page_size_m4_) {
                    currentPageWriteSize = page_size_m4_;
                    memcpy(write_buf_dio + page_i * page_size_, &currentPageWriteSize, sizeof(uint32_t));
                    memcpy(write_buf_dio + page_i * page_size_ + sizeof(uint32_t), exist_content + written_size, currentPageWriteSize);
                    written_size += currentPageWriteSize;
                    actual_disk_write_size += page_size_;
                    page_i++;
                } else {
                    currentPageWriteSize = targetWriteSize - written_size;
                    memcpy(write_buf_, exist_content + written_size, currentPageWriteSize);
                    buf_used_size_ = currentPageWriteSize;
                    written_size += currentPageWriteSize;
                    page_i++;
                }
            }
            if (currentPageWriteSize == page_size_m4_) {
                buf_used_size_ = 0;
            }
            struct timeval tv;
            gettimeofday(&tv, 0);
            // write to the offset start_offset_ + disk_size_
            if (actual_disk_write_size + disk_size_ > max_size_ ||
                    start_offset_ % max_size_ > 0) {
                debug_error("write too much: %d + %lu > %lu\n",
                        actual_disk_write_size, disk_size_,
                        max_size_);
                throw std::runtime_error("exception");
            }
            if (debug_flag_) {
                fprintf(stdout, "[%d %s] %p pwrite offset %lu left %d\n",
                        __LINE__, __func__, this, 
                        start_offset_ + disk_size_, actual_disk_write_size);
            }
            auto wReturn = pwrite(fd_, write_buf_dio, actual_disk_write_size,
                start_offset_ + disk_size_);
            StatsRecorder::getInstance()->timeProcess(
                StatsType::DS_FILE_FUNC_REAL_WRITE, tv);
            if (wReturn != actual_disk_write_size) {
                free(write_buf_dio);
                debug_error("[ERROR] Write return value = %ld, file fd = %d, err = %s\n", wReturn, fd_, strerror(errno));
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            } else {
                free(write_buf_dio);
                disk_size_ += actual_disk_write_size;
                // data size already on disk
                data_size_ += (targetWriteSize - buf_used_size_);
                FileOpStatus ret(true, actual_disk_write_size,
                    targetWriteSize - buf_used_size_, buf_used_size_);
                return ret;
            }
        } else {
            uint64_t req_page_num = ceil((double)write_size / (double)page_size_m4_);
            uint64_t written_size = 0;
            // align mem
            char* write_buf_dio;
            auto writeBufferSize = page_size_ * req_page_num;
            auto ret = posix_memalign((void**)&write_buf_dio, page_size_, writeBufferSize);
            if (ret) {
                debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            } else {
                memset(write_buf_dio, 0, writeBufferSize);
            }
            uint64_t processedPageNumber = 0;
            uint64_t targetWriteSize = write_size;

            int actual_disk_write_size = 0;
            uint32_t currentPageWriteSize = 0;
            while (written_size != targetWriteSize) {
                currentPageWriteSize = min(page_size_m4_, targetWriteSize - written_size);
                memcpy(write_buf_dio + processedPageNumber * page_size_, &currentPageWriteSize, sizeof(uint32_t));
                memcpy(write_buf_dio + processedPageNumber * page_size_ + sizeof(uint32_t), contentBuffer + written_size, currentPageWriteSize);
                written_size += currentPageWriteSize;
                actual_disk_write_size += page_size_;
                processedPageNumber++;
            }
            struct timeval tv;
            gettimeofday(&tv, 0);
            if (actual_disk_write_size + disk_size_ > max_size_ ||
                    start_offset_ % max_size_ > 0) {
                debug_error("write too much: %d + %lu > %lu, %s\n",
                        actual_disk_write_size, disk_size_,
                        max_size_, path_.c_str());
                throw std::runtime_error("exception");
            }
            if (debug_flag_) {
                fprintf(stdout, "[%d %s] %p pwrite offset %lu left %d\n",
                        __LINE__, __func__, this, 
                        start_offset_ + disk_size_, actual_disk_write_size);
            }
            auto wReturn = pwrite(fd_, write_buf_dio, actual_disk_write_size,
                start_offset_ + disk_size_);
            StatsRecorder::getInstance()->timeProcess(StatsType::DS_FILE_FUNC_REAL_WRITE, tv);
            if (wReturn != actual_disk_write_size) {
                free(write_buf_dio);
                debug_error("[ERROR] Write return value = %ld, file fd = %d, err = %s\n", wReturn, fd_, strerror(errno));
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            } else {
                free(write_buf_dio);
                disk_size_ += actual_disk_write_size;
                data_size_ += targetWriteSize;
                FileOpStatus ret(true, actual_disk_write_size, targetWriteSize, 0);
                return ret;
            }
        }
    } else if (operationType_ == kPreadWrite) {
        if (write_size + buf_used_size_ <= buf_size_) {
            memcpy(write_buf_ + buf_used_size_, contentBuffer, write_size);
            buf_used_size_ += write_size;
            FileOpStatus ret(true, 0, 0, write_size);
            return ret;
        } else if (buf_size_ > 0) {
            debug_error("Not complete! buf_size_ %lu\n", buf_size_);
            if (write_size + buf_used_size_ - buf_size_ < buf_size_) {
                memcpy(write_buf_ + buf_used_size_, contentBuffer, buf_size_ - buf_used_size_);
                struct timeval tv;
                gettimeofday(&tv, 0);
                auto wReturn = pwrite(fd_, write_buf_, buf_size_,
                        start_offset_ + disk_size_);
                StatsRecorder::getInstance()->timeProcess(StatsType::DS_FILE_FUNC_REAL_WRITE, tv);
                if (wReturn != buf_size_) {
                    debug_error("[ERROR] Write return value = %ld, file fd = %d, err = %s\n", wReturn, fd_, strerror(errno));
                    FileOpStatus ret(false, 0, 0, 0);
                    return ret;
                } 

                buf_used_size_ = write_size - (buf_size_ - buf_used_size_);
                disk_size_ += buf_size_;
                data_size_ += write_size - buf_used_size_;
                memcpy(write_buf_, contentBuffer + write_size - buf_used_size_, buf_used_size_);
                FileOpStatus ret(true, buf_size_, write_size - buf_used_size_, buf_used_size_);
                return ret;
            } else {
                // Not complete
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            }
        } else {
            struct timeval tv;
            gettimeofday(&tv, 0);
            auto wReturn = pwrite(fd_, contentBuffer, write_size,
                    start_offset_ + disk_size_);
            StatsRecorder::getInstance()->timeProcess(StatsType::DS_FILE_FUNC_REAL_WRITE, tv);
            if (wReturn != write_size) {
                debug_error("[ERROR] Write return value = %ld, "
                        "file fd = %d, err = %s\n", wReturn, fd_, strerror(errno));
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            } else {
                disk_size_ += write_size;
                data_size_ += write_size;
                FileOpStatus ret(true, write_size, write_size, 0);
                return ret;
            }
        }
    } else {
        FileOpStatus ret(false, 0, 0, 0);
        return ret;
    }
}

FileOpStatus FileOperation::writeAndFlushFile(char* contentBuffer, uint64_t contentSize)
{
    static bool printed_commit_log = false;
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (buf_size_ > 0) {
            uint64_t targetWriteSize = buf_used_size_ + contentSize;
            uint64_t req_page_num = (targetWriteSize + page_size_m4_ - 1) /
                page_size_m4_;
            uint64_t written_size = 0;
            // align mem
            char* write_buf_dio;
            auto writeBufferSize = page_size_ * req_page_num;
            auto ret = posix_memalign((void**)&write_buf_dio, page_size_, writeBufferSize);
            if (ret) {
                debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            } else {
                memset(write_buf_dio, 0, writeBufferSize);
            }
            uint64_t page_i = 0;

            char exist_content[targetWriteSize];
            memcpy(exist_content, write_buf_, buf_used_size_);
            memcpy(exist_content + buf_used_size_, contentBuffer, contentSize);
            buf_used_size_ = 0;

            int actual_disk_write_size = 0;
            uint32_t currentPageWriteSize = 0;
            while (written_size != targetWriteSize) {
                currentPageWriteSize = min(targetWriteSize - written_size, page_size_m4_);
                memcpy(write_buf_dio + page_i * page_size_, &currentPageWriteSize, sizeof(uint32_t));
                memcpy(write_buf_dio + page_i * page_size_ + sizeof(uint32_t), exist_content + written_size, currentPageWriteSize);
                written_size += currentPageWriteSize;
                actual_disk_write_size += page_size_;
                page_i++;
            }
            struct timeval tv;
            gettimeofday(&tv, 0);
            if (!printed_commit_log && disk_size_ > 128 * 1024 * 1024 &&
                    path_.find("commit") != std::string::npos) {
                debug_error("write %s disk_size_ %lu max_size_ %lu\n",
                        path_.c_str(), disk_size_, max_size_);
                printed_commit_log = true;
            }
            if (actual_disk_write_size + disk_size_ > max_size_ || 
                    start_offset_ % max_size_ > 0) {
                debug_error("write too much: %d + %lu > %lu\n",
                        actual_disk_write_size, disk_size_,
                        max_size_);
                throw std::runtime_error("exception");
            }
            if (debug_flag_) {
                fprintf(stdout, "[%d %s] %p pwrite offset %lu left %d\n",
                        __LINE__, __func__, this, 
                        start_offset_ + disk_size_, actual_disk_write_size);
            }
            auto wReturn = pwrite(fd_, write_buf_dio, actual_disk_write_size,
                    start_offset_ + disk_size_);
            StatsRecorder::getInstance()->timeProcess(StatsType::DS_FILE_FUNC_REAL_WRITE, tv);

            // stop recovery
            recovery_state_ = false;

            if (wReturn != actual_disk_write_size) {
                free(write_buf_dio);
                debug_error("[ERROR] Write return value = %ld, file fd = %d, err = %s\n", wReturn, fd_, strerror(errno));
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            } else {
                free(write_buf_dio);
                disk_size_ += actual_disk_write_size;
                data_size_ += targetWriteSize;
                FileOpStatus ret(true, actual_disk_write_size, targetWriteSize,
                        buf_used_size_);
                return ret;
            }
        } else {
            return writeFile(contentBuffer, contentSize); 
        }
    } else if (operationType_ == kPreadWrite) {
        if (buf_size_ > 0) {
            debug_error("Not complete! buf_size_ %lu\n", buf_size_);
            if (contentSize + buf_used_size_ - buf_size_ < buf_size_) {
                memcpy(write_buf_ + buf_used_size_, contentBuffer, buf_size_ - buf_used_size_);
                struct timeval tv;
                gettimeofday(&tv, 0);
                auto wReturn = pwrite(fd_, write_buf_, buf_size_, disk_size_);
                StatsRecorder::getInstance()->timeProcess(StatsType::DS_FILE_FUNC_REAL_WRITE, tv);
                if (wReturn != buf_size_) {
                    debug_error("[ERROR] Write return value = %ld, file fd = %d, err = %s\n", wReturn, fd_, strerror(errno));
                    FileOpStatus ret(false, 0, 0, 0);
                    return ret;
                } 

                buf_used_size_ = contentSize - (buf_size_ - buf_used_size_);
                disk_size_ += buf_size_;
                data_size_ += contentSize - buf_used_size_;
                memcpy(write_buf_, contentBuffer + contentSize - buf_used_size_, buf_used_size_);
                FileOpStatus ret(true, buf_size_, contentSize - buf_used_size_, buf_used_size_);
                return ret;
            } else {
                // Not complete
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            }
        } else {
            return writeFile(contentBuffer, contentSize);
        }
    } else {
        FileOpStatus ret(false, 0, 0, 0);
        return ret;
    }
}

FileOpStatus FileOperation::readFile(char* contentBuffer, uint64_t contentSize)
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (contentSize <= max_size_ && contentSize != data_size_ +
                buf_used_size_) {
            debug_error("[ERROR] Read size mismatch, request size = %lu,"
                    " DirectIO current writed physical size = %lu, actual size"
                    " = %lu, buffered size = %lu\n", 
                    contentSize, disk_size_, data_size_,
                    buf_used_size_);
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }
        uint64_t req_page_num = ceil((double)disk_size_ / (double)page_size_);
        // align mem
        char* readBuffer;
        auto readBufferSize = page_size_ * req_page_num;
        auto ret = posix_memalign((void**)&readBuffer, page_size_, readBufferSize);
        if (ret) {
            debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }
        if (debug_flag_) {
            fprintf(stdout, "[%d %s] %p pread offset %lu left %lu\n",
                    __LINE__, __func__, this, start_offset_, readBufferSize);
        }
        auto rReturn = pread(fd_, readBuffer, readBufferSize, start_offset_);
        if (rReturn != readBufferSize) {
            free(readBuffer);
            debug_error("[ERROR] Read return value = %lu, file fd = %d, "
                    "(closed %d) "
                    "err = %s, req_page_num = %lu, readBuffer size = %lu,"
                    " disk_size_ = %lu\n", rReturn, fd_, closed_before_,
                    strerror(errno), req_page_num, readBufferSize, disk_size_);
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }
        uint64_t currentReadDoneSize = 0;
        vector<uint64_t> vec;
        for (auto processedPageNumber = 0; processedPageNumber < req_page_num; processedPageNumber++) {
            uint32_t page_data_size = 0;
            memcpy(&page_data_size, readBuffer + processedPageNumber * page_size_, sizeof(uint32_t));
            memcpy(contentBuffer + currentReadDoneSize, readBuffer + processedPageNumber * page_size_ + sizeof(uint32_t), page_data_size);
            currentReadDoneSize += page_data_size;
            vec.push_back(page_data_size);
        }
        if (buf_used_size_ != 0) {
            memcpy(contentBuffer + currentReadDoneSize, write_buf_, buf_used_size_);
            currentReadDoneSize += buf_used_size_;
        }
        if (contentSize <= max_size_ && currentReadDoneSize !=
            contentSize) {
//            free(readBuffer);
            debug_error("[ERROR] Read size mismatch, read size = %lu, request size = %lu, DirectIO current page number = %lu, DirectIO current read physical size = %lu, actual size = %lu, buffered size = %lu\n", currentReadDoneSize, contentSize, req_page_num, disk_size_, data_size_, buf_used_size_);
            for (int i = 0; i < (int)vec.size(); i++) {
                debug_error("vec[%d] %lu (start %lu)\n", i, vec[i], 
                        start_offset_ / max_size_);
            }
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        } else {
            free(readBuffer);
            FileOpStatus ret(true, disk_size_, contentSize, 0);
            return ret;
        }
    } else if (operationType_ == kPreadWrite) {
        if (contentSize != data_size_ + buf_used_size_) {
            debug_error("[ERROR] Read size mismatch, request size = %lu,"
                    " DirectIO current writed physical size = %lu, actual size"
                    " = %lu, buffered size = %lu\n", 
                    contentSize, disk_size_, data_size_,
                    buf_used_size_);
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }
        if (debug_flag_) {
            fprintf(stdout, "[%d %s] %p pread offset %lu left %lu\n",
                    __LINE__, __func__, this, start_offset_, disk_size_);
        }
        auto rReturn = pread(fd_, contentBuffer, disk_size_, start_offset_);
        if (rReturn != data_size_) {
            debug_error("[ERROR] Read return value = %lu, file fd = %d, "
                    "err = %s, disk_size_ = %lu\n", 
                    rReturn, fd_, strerror(errno), disk_size_);
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }
        vector<uint64_t> vec;
        if (buf_used_size_ != 0) {
            memcpy(contentBuffer + disk_size_, write_buf_, buf_used_size_);
        }

        FileOpStatus ret(true, disk_size_, contentSize, 0);
        return ret;
    } else {
        FileOpStatus ret(false, 0, 0, 0);
        return ret;
    }
}

FileOpStatus FileOperation::positionedReadFile(char* read_buf, 
        uint64_t offset, uint64_t read_buf_size)
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (read_buf_size == 0) {
            debug_error("read size cannot be zero!%s\n", "");
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }
        if (offset + read_buf_size > data_size_ + buf_used_size_) {
            debug_error("[ERROR] Read size mismatch, request size = %lu,"
                    " DirectIO current writed physical size %lu, actual size"
                    " %lu, buffered size %lu\n", read_buf_size,
                    disk_size_, data_size_,
                    buf_used_size_);
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }

        // if both are 0, don't need to read from the buffer
        uint64_t disk_start_page_id; 
        uint64_t disk_end_page_id;

        // buf_end_offset: The offset for read in the buffer
        // if it is 0, don't need to read from the buffer

        // req_disk_data_size: The data size on disk
        // req_buf_data_size: The data size in buffer 
        // req_disk_data_size + req_buf_data_size == read_buf_size
        uint64_t req_disk_data_size;
        uint64_t req_buf_data_size;

        // page_index: Where the page starts to read from tmp_read_buf to. It
        // is the index within the page.
        uint64_t page_index = 0;

        if (offset >= data_size_) {
            // The whole data in buffer
            memcpy(read_buf, 
                    write_buf_ + offset - data_size_,
                    read_buf_size); 
            return FileOpStatus(true, read_buf_size, read_buf_size, 0);
        } 

        if (offset == mark_data_ && write_buf_ == 0) {
            // Reading the unsorted part. Read from the marked address
            if (offset + read_buf_size != data_size_) {
                debug_error("[ERROR] Not reading to the end! "
                        "%lu + %lu v.s. %lu\n", 
                        offset, read_buf_size, data_size_);
                exit(1);
            }
            req_buf_data_size = 0;
            req_disk_data_size = data_size_ - mark_data_;
            // not mark_data_
            disk_start_page_id = mark_disk_ / page_size_;  
            // not page_size_m4_ 
            disk_end_page_id = disk_size_  / page_size_;
            page_index = 0;
        } else if (offset == mark_data_) {
            // Reading the unsorted part. Read from the marked address, using buffer
            if (offset + read_buf_size != data_size_ + buf_used_size_) {
                debug_error("[ERROR] Not reading to the end! "
                        "%lu + %lu v.s. %lu\n", 
                        offset, read_buf_size, data_size_);
                exit(1);
            }
            req_buf_data_size = buf_used_size_;
            req_disk_data_size = data_size_ - offset;
            disk_start_page_id = mark_disk_ / page_size_;  
            // not page_size_m4_ 
            disk_end_page_id = disk_size_  / page_size_;
            page_index = (mark_in_page_offset_ == 0) ? 0 : 
                offset - disk_start_page_id * page_size_m4_;
//            debug_error("mark_data_ %lu"
//                    " offset %lu data size %lu start %lu end %lu index %lu\n",
//                    mark_data_, offset, data_size_, 
//                    disk_start_page_id, disk_end_page_id, page_index);
        } else if (offset + read_buf_size > data_size_) {
            // Half data on disk, half data in buffer 
            req_buf_data_size = offset + read_buf_size - data_size_;
            req_disk_data_size = read_buf_size - req_buf_data_size;
            disk_start_page_id = offset / page_size_m4_; 
            // Read to the end of the disk part 
            disk_end_page_id = disk_size_ / page_size_m4_;  
            page_index = offset - disk_start_page_id * page_size_m4_;
        } else {
            // All data on disk
            req_disk_data_size = read_buf_size;
            req_buf_data_size = 0;
            disk_start_page_id = offset / page_size_m4_; 
            disk_end_page_id = (offset + read_buf_size + page_size_m4_ - 1) /
                page_size_m4_;
            // Read to the end of the disk part
            if (offset + read_buf_size == data_size_) {
                disk_end_page_id = disk_size_ / page_size_m4_;
            }
            page_index = offset - disk_start_page_id * page_size_m4_;
        }

        uint64_t req_page_num = disk_end_page_id - disk_start_page_id;

        // align mem
        char* tmp_read_buf;
        auto readBufferSize = req_page_num * page_size_;

        if (req_page_num == 0) {
            debug_error("[ERROR] req page number should not be 0, "
                    "offset %lu size %lu direct io size %lu "
                    "req disk %lu req buf %lu\n", 
                    offset, read_buf_size, data_size_,
                    req_disk_data_size, req_buf_data_size);
            exit(1);
        }

        auto ret = posix_memalign((void**)&tmp_read_buf, page_size_, readBufferSize);
        if (ret) {
            debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }
        if (debug_flag_) {
            fprintf(stdout, "[%d %s] %p pread offset %lu left %lu\n",
                    __LINE__, __func__, this,
                    start_offset_ + disk_start_page_id * page_size_,
                    readBufferSize);
        }
        auto rReturn = pread(fd_, tmp_read_buf, readBufferSize,
                start_offset_ + disk_start_page_id * page_size_);
        if (rReturn != readBufferSize) {
            free(tmp_read_buf);
            debug_error("[ERROR] Read return value = %lu, file fd = %d, err = %s, req_page_num = %lu, tmp_read_buf size = %lu, disk_size_ = %lu\n", rReturn, fd_, strerror(errno), req_page_num, readBufferSize, disk_size_);
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }

        uint64_t left_size = req_disk_data_size; 
        uint64_t read_done_size = 0;

        for (auto page_id = 0; page_id < req_page_num; page_id++) {
            uint32_t page_data_size = 0;

            memcpy(&page_data_size, 
                    tmp_read_buf + page_id * page_size_, sizeof(uint32_t));

            // Read size in this page
            uint64_t read_size = min(page_data_size - page_index, left_size); 

//            debug_error("read_size %lu page_data_size %u done size %lu"
//                    " page_index %lu\n", 
//                    read_size, page_data_size, read_done_size,
//                    page_index);

            if (read_size == 0) {
                page_index = 0;
                continue;
            } 

            memcpy(read_buf + read_done_size, 
                    tmp_read_buf + page_id * page_size_ + sizeof(uint32_t) + page_index, 
                    read_size);

            read_done_size += read_size;
            left_size -= read_size;
            page_index = 0;
        }

        if (read_done_size != req_disk_data_size || left_size != 0) {
            debug_error("[ERROR] Read size mismatch: %lu v.s. %lu %lu"
                    " req_page_num %lu mark_data_ %lu"
                    " offset %lu data size %lu\n",
                    read_done_size, req_disk_data_size, left_size,
                    req_page_num, mark_data_, 
                    offset, data_size_);
            return FileOpStatus(false, 0, 0, 0);
        }

        if (req_buf_data_size != 0) {
            memcpy(read_buf + read_done_size, 
                    write_buf_, req_buf_data_size);
            read_done_size += req_buf_data_size;
        }

        free(tmp_read_buf);
        if (read_done_size != read_buf_size) {
            debug_error("[ERROR] Read size mismatch, read size = %lu, request size = %lu, DirectIO current page number = %lu, DirectIO current read physical size = %lu, actual size = %lu, buffered size = %lu\n", read_done_size, read_buf_size, req_page_num, disk_size_, data_size_, buf_used_size_);
            return FileOpStatus(false, 0, 0, 0);
        } else {
            return FileOpStatus(true, 
                    req_buf_data_size + req_page_num * page_size_,
                    read_buf_size, 0);
        }
    } else if (operationType_ == kPreadWrite) {
        if (read_buf_size == 0) {
            debug_error("read size cannot be zero!%s\n", "");
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }
        if (offset + read_buf_size > data_size_ + buf_used_size_) {
            debug_error("[ERROR] Read size mismatch, request size = %lu,"
                    " DirectIO current writed physical size %lu, actual size"
                    " %lu, buffered size %lu\n", read_buf_size,
                    disk_size_, data_size_,
                    buf_used_size_);
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }

        // buf_end_offset: The offset for read in the buffer
        // if it is 0, don't need to read from the buffer

        // req_disk_data_size: The data size on disk
        // req_buf_data_size: The data size in buffer 
        // req_disk_data_size + req_buf_data_size == read_buf_size
        uint64_t req_disk_data_size;
        uint64_t req_buf_data_size;

        if (offset >= data_size_) {
            // The whole data in buffer
            memcpy(read_buf, 
                    write_buf_ + offset - data_size_,
                    read_buf_size); 
            return FileOpStatus(true, read_buf_size, read_buf_size, 0);
        } 

        if (offset + read_buf_size > data_size_) {
            // Half data on disk, half data in buffer 
            req_buf_data_size = offset + read_buf_size - data_size_;
            req_disk_data_size = read_buf_size - req_buf_data_size;
        } else {
            // All data on disk. did nothing
            req_disk_data_size = read_buf_size;
            req_buf_data_size = 0;
        }

        if (debug_flag_) {
            fprintf(stdout, "[%d %s] %p pread offset %lu left %lu\n",
                    __LINE__, __func__, this, 
                    start_offset_ + offset, req_disk_data_size);
        }
        auto rReturn = pread(fd_, read_buf, req_disk_data_size, 
                start_offset_ + offset);
        if (rReturn != req_disk_data_size) {
            debug_error("[ERROR] Read return value = %lu, file fd = %d,"
                    " err = %s, disk_size_ = %lu\n", 
                    rReturn, fd_, strerror(errno), 
                    disk_size_);
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }

        if (req_buf_data_size != 0) {
            memcpy(read_buf + req_disk_data_size, 
                    write_buf_, req_buf_data_size);
        }

        return FileOpStatus(true, req_disk_data_size, read_buf_size, 0);
    } else {
        FileOpStatus ret(false, 0, 0, 0);
        return ret;
    }
}

FileOpStatus FileOperation::flushFile()
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (buf_used_size_ > 0) {
            uint64_t req_page_num = 
                (buf_used_size_ + page_size_m4_ - 1) / page_size_m4_;
            // align mem
            char* writeBuffer;
            auto writeBufferSize = page_size_ * req_page_num;
            auto ret = posix_memalign((void**)&writeBuffer, page_size_, writeBufferSize);
            if (ret) {
                debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            } else {
                memset(writeBuffer, 0, writeBufferSize);
            }
            uint64_t page_i = 0;
            uint64_t targetWriteSize = buf_used_size_;
            uint64_t written_size = 0;
            while (written_size != targetWriteSize) {
                uint32_t curr_sz;
                curr_sz = min(targetWriteSize - written_size, page_size_m4_);
                memcpy(writeBuffer + page_i * page_size_, 
                        &curr_sz, sizeof(uint32_t));
                memcpy(writeBuffer + page_i * page_size_ + sizeof(uint32_t),
                        write_buf_ + written_size, curr_sz);
                written_size += curr_sz;
                page_i++;
            }
            struct timeval tv;
            gettimeofday(&tv, 0);
            if (writeBufferSize + disk_size_ > max_size_ || start_offset_ %
                    max_size_ > 0) {
                debug_error("write too much: %lu + %lu > %lu\n",
                        writeBufferSize, disk_size_,
                        max_size_);
                throw std::runtime_error("exception");
            }
            if (debug_flag_) {
                fprintf(stdout, "[%d %s] %p pwrite offset %lu left %lu\n",
                        __LINE__, __func__, this, 
                        start_offset_ + disk_size_, writeBufferSize);
            }
            auto wReturn = pwrite(fd_, writeBuffer, writeBufferSize, 
                    start_offset_ + disk_size_);
            StatsRecorder::getInstance()->timeProcess(StatsType::DS_FILE_FUNC_REAL_FLUSH, tv);
            if (wReturn != writeBufferSize) {
                free(writeBuffer);
                debug_error("[ERROR] Write return value = %ld, file fd = %d, err = %s\n", wReturn, fd_, strerror(errno));
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            } else {
                free(writeBuffer);
                disk_size_ += writeBufferSize;
                data_size_ += buf_used_size_;
                uint64_t flushedSize = buf_used_size_;
                buf_used_size_ = 0;
                memset(write_buf_, 0, buf_size_);
                FileOpStatus ret(true, writeBufferSize, flushedSize, 0);
                return ret;
            }
        }
        FileOpStatus ret(true, 0, 0, 0);
        return ret;
    } else if (operationType_ == kPreadWrite) {
        if (buf_used_size_ != 0) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            auto wReturn = pwrite(fd_, write_buf_, buf_used_size_,
                    start_offset_ + disk_size_);
            StatsRecorder::getInstance()->timeProcess(StatsType::DS_FILE_FUNC_REAL_FLUSH, tv);
            if (wReturn != buf_used_size_) {
                debug_error("[ERROR] Write return value = %ld, file fd = %d, err = %s\n", wReturn, fd_, strerror(errno));
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            } else {
                disk_size_ += buf_used_size_;
                data_size_ += buf_used_size_;
                uint64_t flushedSize = buf_used_size_;
                buf_used_size_ = 0;
                FileOpStatus ret(true, flushedSize, flushedSize, 0);
                return ret;
            }
        }
        FileOpStatus ret(true, 0, 0, 0);
        return ret;
    } else {
        FileOpStatus ret(false, 0, 0, 0);
        return ret;
    }
}

uint64_t FileOperation::getFileSize()
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        uint64_t fileRealSizeWithoutPadding = 0;
        uint64_t req_page_num = ceil((double)disk_size_ / (double)page_size_);
        // align mem
        char* readBuffer;
        auto readBufferSize = page_size_ * req_page_num;
        auto ret = posix_memalign((void**)&readBuffer, page_size_, readBufferSize);
        if (ret) {
            debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
            return false;
        }
        if (debug_flag_) {
            fprintf(stdout, "[%d %s] %p pread offset %lu left %lu\n",
                    __LINE__, __func__, this,
                    start_offset_, readBufferSize);
        }
        auto rReturn = pread(fd_, readBuffer, readBufferSize, start_offset_);
        if (rReturn != readBufferSize) {
            free(readBuffer);
            debug_error("[ERROR] Read return value = %lu, err = %s, req_page_num = %lu, readBuffer size = %lu, disk_size_ = %lu\n", rReturn, strerror(errno), req_page_num, readBufferSize, disk_size_);
            free(readBuffer);
            return false;
        }
        for (auto processedPageNumber = 0; processedPageNumber < req_page_num; processedPageNumber++) {
            uint32_t page_data_size = 0;
            memcpy(&page_data_size, readBuffer + processedPageNumber * page_size_, sizeof(uint32_t));
            fileRealSizeWithoutPadding += page_data_size;
        }
        free(readBuffer);
        return fileRealSizeWithoutPadding;
    } else if (operationType_ == kPreadWrite) {
        return disk_size_;
    } else {
        return 0;
    }
}

bool FileOperation::removeAndReopen() {
    if (isFileOpen()) {
        closeFile();
    }
    auto ret = remove(path_.c_str());
    if (ret == -1) {
        debug_error("[ERROR] could not delete %s\n", path_.c_str());
        return false;
    }
    createThenOpenFile(path_);
    return true;
}

uint64_t FileOperation::getCachedFileSize() {
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO ||
            operationType_ == kPreadWrite) {
        return disk_size_;
    } else {
        return 0;
    }
}

uint64_t FileOperation::getCachedFileDataSize() {
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO ||
            operationType_ == kPreadWrite) {
        return data_size_;
    } else {
        return 0;
    }
}

uint64_t FileOperation::getFilePhysicalSize(string path)
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    struct stat statbuf;
    stat(path.c_str(), &statbuf);
    uint64_t physicalFileSize = statbuf.st_size;
    return physicalFileSize;
}

} // namespace KDSEP_NAMESPACE
