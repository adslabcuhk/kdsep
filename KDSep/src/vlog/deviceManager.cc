#include "vlog/deviceManager.hh"
#include "utils/debug.hpp"
#include "utils/statsRecorder.hh"
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;

namespace KDSEP_NAMESPACE {

DeviceManager::DeviceManager(const vector<DiskInfo> v_diskInfo, bool isSlave)
{

    std::set<disk_id_t> diskIdSet;

    for (DiskInfo diskInfo : v_diskInfo) {
        assert(!_diskInfo.count(diskInfo.diskId));
        assert(!_diskMutex.count(diskInfo.diskId));

        _diskInfo[diskInfo.diskId] = diskInfo;
        _diskMutex[diskInfo.diskId] = new mutex();
        _diskStatus[diskInfo.diskId] = true;
        diskIdSet.insert(diskInfo.diskId);
    }

    _numDisks = v_diskInfo.size();

    int numThread = ConfigManager::getInstance().getNumIOThread();
    _stp = new boost::asio::thread_pool(numThread);

    _diskIdVector.insert(_diskIdVector.begin(), diskIdSet.begin(), diskIdSet.end());

    _isSlave = isSlave;

    // buffer for direct I/O
    _pageSize = sysconf(_SC_PAGE_SIZE);
    _directIO = ConfigManager::getInstance().useDirectIO();

#ifdef DISKLBA_OUT
    fp = fopen("disklba.out", "w");
#endif /* DISKLBA_OUT */
}

DeviceManager::~DeviceManager()
{
    for (auto diskInfo : _diskInfo) {
        delete _diskMutex[diskInfo.first];
    }
}

void* DeviceManager::checkBufferSize(len_t neededSize, offset_t offset, len_t& bufsize)
{
    assert(neededSize != 0);

    void* buf;

    if (offset == INVALID_OFFSET) {
        neededSize = (neededSize + _pageSize - 1) / _pageSize * _pageSize;
    } else {
        offset_t end = offset + neededSize;
        offset = offset / _pageSize * _pageSize;
        end = (end + _pageSize - 1) / _pageSize * _pageSize;
        neededSize = end - offset;
    }

    int ret = posix_memalign(&buf, _pageSize, neededSize);
    if (ret != 0) {
        debug_error("[ERROR] check buffer size error, ret = %d, page size = %d, need size = %lu, err %s\n", ret, _pageSize, neededSize, strerror(errno));
    }
    bufsize = neededSize;

    return buf;
}

#ifdef DIRECT_LBA_SEGMENT_MAPPING
disk_id_t DeviceManager::getDiskBySegmentId(segment_id_t segmentId)
{
    return (segmentId == INVALID_SEGMENT) ? INVALID_DISK : segmentId % _numDisks;
}

offset_t DeviceManager::getOffsetBySegmentId(segment_id_t segmentId)
{
    ConfigManager& cm = ConfigManager::getInstance();

    segment_len_t mainSegmentSize = cm.getMainSegmentSize();
    segment_len_t logSegmentSize = cm.getLogSegmentSize();
    segment_id_t numMainSegments = cm.getNumMainSegment();

    if (segmentId == INVALID_SEGMENT) {
        return INVALID_LBA;
    } else if (segmentId < numMainSegments) {
        // main segments
        return segmentId / _numDisks * mainSegmentSize;
    } else {
        // log segments (start after the main segments)
        return (numMainSegments + _numDisks - 1) / _numDisks * mainSegmentSize + (segmentId - numMainSegments) / _numDisks * logSegmentSize;
    }
    return INVALID_OFFSET;
}

// len_t DeviceManager::getDiskSize(disk_id_t diskId) {
//     if (buf == 0 && !isDelete) { // check log size
//         struct stat logStat;
//         std::string fname (_diskInfo.at(diskId).diskPath);
//         fname.append("/c");
//         fname.append(std::to_string(segmentId));
//         if (stat(fname.c_str(), &logStat) != 0) {
//             debug_warn("%s log file not found\n", isUpdate? "Update" : "GC");
//             return 0;
//         }
//         return logStat.st_size;
//     }
// }

segment_id_t DeviceManager::getSegmentIdByOffset(disk_id_t diskId, offset_t ofs)
{
    assert(_diskInfo.count(diskId) > 0);

    ConfigManager& cm = ConfigManager::getInstance();

    segment_len_t mainSegmentSize = cm.getMainSegmentSize();
    segment_len_t logSegmentSize = cm.getLogSegmentSize();
    unsigned long long numMainSegments = cm.getNumMainSegment();
    unsigned long long numLogSegments = cm.getNumLogSegment();
    offset_t mainSegmentAreaEnd = (numMainSegments + _numDisks - 1) / _numDisks * mainSegmentSize;
    offset_t logSegmentAreaEnd = (numMainSegments + _numDisks - 1) / _numDisks * mainSegmentSize + (numLogSegments + _numDisks - 1) / _numDisks * logSegmentSize;
    ;
    if (ofs >= mainSegmentAreaEnd) {
        // log segments
        return (ofs - mainSegmentAreaEnd) / logSegmentSize * _numDisks + diskId + numMainSegments;
    } else if (ofs < logSegmentAreaEnd) {
        // main segments
        return ofs / mainSegmentSize * _numDisks + diskId;
    }
    return INVALID_SEGMENT;
}

#endif // ifdef DIRECT_LBA_SEGMENT_MAPPING

offset_t DeviceManager::writeSegment(segment_id_t segmentId, unsigned char* buf, segment_offset_t startingOffset)
{
    segment_len_t segmentSize = ConfigManager::getInstance().getSegmentSize(isLogSegment(segmentId));
    assert(startingOffset < segmentSize);
    return accessDataOnDisk(segmentId, startingOffset, segmentSize - startingOffset, buf, true);
}

bool DeviceManager::isLogSegment(segment_id_t segmentId)
{
    return segmentId > ConfigManager::getInstance().getNumMainSegment();
}

len_t DeviceManager::accessDisk(disk_id_t diskId, unsigned char* buf, offset_t diskOffset, len_t length, bool isWrite)
{
    debug_trace("accessDisk offset %lu length %lu write %d [%x%x]\n", diskOffset, length, (int)isWrite,
        (isWrite) ? buf[0] : ' ', (isWrite) ? buf[1] : ' ');

    // check if disk id is valid
    if (diskId == INVALID_DISK || _diskInfo.count(diskId) < 0) {
        return INVALID_LEN;
    }

    ConfigManager& cm = ConfigManager::getInstance();
    bool useFS = ConfigManager::getInstance().segmentAsFile();
    int fd = useFS ? accessFileFd(0) : 0;
    len_t delayUs = cm.getTestIODelayUs();

    if (delayUs > 0) {
        usleep(delayUs);
    }

    char* tempBuf = (char*)buf;
    offset_t originalDiskOffset = diskOffset;
    len_t originalLength = length;

    void* directbuf = nullptr;
    len_t bufsize;

    if (_directIO) {
        directbuf = checkBufferSize(length, diskOffset, bufsize);
        memset(directbuf, 0, bufsize);
        if (isWrite) {
            memcpy((char*)directbuf + diskOffset % _pageSize, buf, length);
        }
        length = ((diskOffset + length + _pageSize - 1) / _pageSize - diskOffset / _pageSize) * _pageSize;
        diskOffset = diskOffset - diskOffset % _pageSize;
        tempBuf = (char*)directbuf;
    }

    struct timeval tv;
    gettimeofday(&tv, 0);

    if (cm.enabledVLogMode() || _isSlave) {
        // get the capacity, allow wrap-around write/read
        len_t capacity = _isSlave ? cm.getColdStorageCapacity() : cm.getSystemEffectiveCapacity();
        offset_t runningDiskOffset = diskOffset;
        for (len_t remains = length, len = 0; remains > 0; remains -= len, runningDiskOffset = (runningDiskOffset + len) % capacity) {
            len = std::min(capacity - runningDiskOffset, remains);
            if (len == 0) {
                len = remains;
            }
            offset_t inOffset = length - remains;
            ssize_t ret = 0;
            if (isWrite) {
                if (useFS) {
                    //lock_guard<mutex> lk(*_diskMutex.at(diskId));
                    //fseek(fd, runningDiskOffset + _diskInfo.at(diskId).skipOffset, SEEK_SET);
                    //ret = fwrite(buf + inOffset, sizeof(unsigned char), len, fd);
                    ret = pwrite(fd, tempBuf + inOffset, len, runningDiskOffset + _diskInfo.at(diskId).skipOffset);
                } else {
                    ret = pwrite(_diskInfo.at(diskId).fd, tempBuf + inOffset, len, runningDiskOffset + _diskInfo.at(diskId).skipOffset);
                }
            } else {
                if (useFS) {
                    //lock_guard<mutex> lk(*_diskMutex.at(diskId));
                    //fseek(fd, runningDiskOffset + _diskInfo.at(diskId).skipOffset, SEEK_SET);
                    //ret = fread(buf + inOffset, sizeof(unsigned char), len, fd);
                    ret = pread(fd, tempBuf + inOffset, len, runningDiskOffset + _diskInfo.at(diskId).skipOffset);
                } else {
                    ret = pread(_diskInfo.at(diskId).fd, tempBuf + inOffset, len, runningDiskOffset + _diskInfo.at(diskId).skipOffset);
                }
            }
            if (ret < 0 || (size_t)ret != len) {
                debug_error("Error on p%s buf=%p to disk %d (fd=%d) at %lu length %lu (ret %lu): %s\n", (isWrite ? "write" : "read"), buf + inOffset, diskId, _diskInfo.at(diskId).fd, runningDiskOffset + _diskInfo.at(diskId).skipOffset, len, ret, strerror(errno));
                assert(0);
                exit(-1);
            }
        }
    } else {
        ssize_t ret = 0;
        if (isWrite) {
            if (useFS) {
                //lock_guard<mutex> lk(*_diskMutex.at(diskId));
                //fseek(fd, diskOffset, SEEK_SET);
                //ret = fwrite(buf, sizeof(unsigned char), length, fd);
                ret = pwrite(fd, tempBuf, length, diskOffset);
            } else {
                ret = pwrite(_diskInfo.at(diskId).fd, tempBuf, length, diskOffset);
            }
        } else {
            if (useFS) {
                //lock_guard<mutex> lk(*_diskMutex.at(diskId));
                //                fseek(fd, diskOffset, SEEK_SET);
                //                ret = fread(buf, sizeof(unsigned char), length, fd);
                ret = pread(fd, tempBuf, length, diskOffset);
            } else {
                ret = pread(_diskInfo.at(diskId).fd, tempBuf, length, diskOffset);
            }
        }
        if (ret < 0 || (size_t)ret != length) {
            debug_error("Error on p%s buf=%p to disk %d (fd=%d) at %lu length %lu: %s\n", (isWrite ? "write" : "read"), tempBuf, diskId, _diskInfo.at(diskId).fd, diskOffset, length, strerror(errno));
            assert(0);
            exit(-1);
        }
    }

    // mark disk as dirty if needed, and update stats
    if (isWrite) {
        StatsRecorder::getInstance()->IOBytesWrite(length, diskId);
	StatsRecorder::getInstance()->timeProcess(StatsType::DEVICE_WRITE, tv);
        if (!_directIO) {
            if (useFS) {
                //            fflush(fd);
            } else {
                _diskInfo.at(diskId).dirty = true;
            }
        }
    } else {
        StatsRecorder::getInstance()->IOBytesRead(length, diskId);
	StatsRecorder::getInstance()->timeProcess(StatsType::DEVICE_READ, tv);
    }

    if (!isWrite && _directIO) {
        // copy back
        memcpy(buf, (char*)directbuf + originalDiskOffset % _pageSize, originalLength);
    }

    if (_directIO) {
        free(directbuf);
    }

    return length;
}

offset_t DeviceManager::accessDataOnDisk(segment_id_t segmentId, segment_offset_t startingOffset, segment_len_t accessLength, unsigned char* buf, bool isWrite)
{
    disk_id_t diskId = getDiskBySegmentId(segmentId);

    assert(_diskInfo.count(diskId));
    assert(_diskMutex.count(diskId));

    if (accessLength <= 0 || diskId == INVALID_DISK || _diskInfo.count(diskId) < 0 || startingOffset + accessLength > _diskInfo.at(diskId).capacity) {
        return INVALID_LBA;
    }

    offset_t diskOffset = getOffsetBySegmentId(segmentId) + startingOffset;
    debug_warn("%s segment %lu to disk %d at off %lu len %lu)\n", isWrite ? "write" : "read", segmentId, diskId, diskOffset, accessLength);

    offset_t ret = INVALID_LBA;
#ifdef ACTUAL_DISK_IO
    bool sepSegmentFiles = (ConfigManager::getInstance().segmentAsFile() && ConfigManager::getInstance().segmentAsSeparateFile());
    if (sepSegmentFiles) {
        accessSegmentFile(segmentId, buf, startingOffset, accessLength, isWrite);
    } else {
        if (accessDisk(diskId, buf, diskOffset, accessLength, isWrite) == accessLength) {
            ret = diskOffset;
            debug_info("%s segment %lu to disk %d at %lu length %lu\n", (isWrite ? "Write" : "Read"), segmentId, diskId, diskOffset, accessLength);
        }
    }
#endif
#ifdef DISKLBA_OUT
    fprintf(fp, "%lld %d %lld %d %d\n", ts * 1000, diskId, diskOffset / DISK_BLOCK_SIZE, (accessLength + DISK_BLOCK_SIZE - 1) / DISK_BLOCK_SIZE, 0);
    ret = diskOffset;
#endif

    return ret;
}

int DeviceManager::accessFileFd(segment_id_t segmentId)
{
    int fd = 0;
    auto existfd = _segmentFiles.fds.find(segmentId);
    if (existfd == _segmentFiles.fds.end()) {
        // open the file
        disk_id_t diskId = getDiskBySegmentId(segmentId);
        std::string fname(_diskInfo.at(diskId).diskPath);
        fname.append("/c");
        fname.append(std::to_string(segmentId));
        if (ConfigManager::getInstance().useDirectIO()) {
            if (ConfigManager::getInstance().testDirectIOCorrectness()) {
                fd = open(fname.c_str(), O_RDWR | O_CREAT, 0755);
            } else {
                fd = open(fname.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0755);
            }
        } else {
            fd = open(fname.c_str(), O_RDWR | O_CREAT, 0755);
        }
        //        fd = open(fname.c_str(), "r+b");
        //        if (fd == 0) {
        //            // create if file not exists
        //            fd = fopen(fname.c_str(), "w+b");
        //        }
        assert(fd != 0);
        // keep track of its fd
        _segmentFiles.fds[segmentId] = fd;
    } else {
        fd = existfd->second;
    }
    return fd;
}

len_t DeviceManager::accessLogFile(bool isUpdate, unsigned char* buf, len_t logSize, bool isWrite, bool isDelete)
{

    std::string fname(_diskInfo.at(0).diskPath);
    fname.append("/log_");
    fname.append(isUpdate ? "update" : "gc");

    if (buf == 0 && !isDelete) { // check log size
        struct stat logStat;
        if (stat(fname.c_str(), &logStat) != 0) {
            debug_warn("%s log file not found\n", isUpdate ? "Update" : "GC");
            return 0;
        }
        return logStat.st_size;
    }

    if (isDelete) {
        assert(buf == 0);
        return remove(fname.c_str()) == 0;
    }

    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0755);

    if (!ConfigManager::getInstance().enabledVLogMode()) {
        if (fd != 0 && isWrite) {
            debug_error("Ack and remove log file before next write (size = %lu)!", logSize);
            assert(0);
            return 0;
        } else if (fd == 0 && !isWrite) {
            debug_error("Log file not found for read (isUpdate = %d)!", isUpdate);
            assert(0);
            return 0;
        } else if (isWrite) {
            debug_error("Log file requires writes but open failed (fname = %s)", fname.c_str());
            // create if file not exists
            //        fd = fopen(fname.c_str(), "wb");
        }
    }
    assert(fd != 0);

    len_t writeSize = accessFile(fd, buf, /* startingOffset = */ 0, logSize, isWrite, /* isCircular = */ false, /* isLog */ true);

    //    fclose(fd);
    close(fd);

    return writeSize;
}

len_t DeviceManager::accessSegmentFile(segment_id_t segmentId, unsigned char* buf, segment_offset_t startingOffset, segment_len_t length, bool isWrite)
{
    ConfigManager& cm = ConfigManager::getInstance();

    disk_id_t diskId = getDiskBySegmentId(segmentId);
    std::string fname(_diskInfo.at(diskId).diskPath);
    fname.append("/c");
    fname.append(std::to_string(segmentId));
    int fd; //= fopen(fname.c_str(), "r+b");
    //    if (fd == 0) {
    //        // create if file not exists
    //        fd = fopen(fname.c_str(), "w+b");
    //    }
    if (_directIO) {
        fd = open(fname.c_str(), O_RDWR | O_DIRECT | O_CREAT, 0755);
    } else {
        fd = open(fname.c_str(), O_RDWR | O_DIRECT, 0755);
    }
    assert(fd != 0);

    len_t writeSize = accessFile(fd, buf, startingOffset, length, isWrite, /* isCircular = */ cm.enabledVLogMode() || _isSlave);

    //    fclose(fd);
    close(fd);

    return writeSize;
}

len_t DeviceManager::accessFile(int fd, unsigned char* buf, segment_offset_t diskOffset, segment_len_t length, bool isWrite, bool isCircular, bool isLog)
{

    ConfigManager& cm = ConfigManager::getInstance();

    len_t accessLength = 0;

    char* tempBuf = (char*)buf;
    offset_t originalDiskOffset = diskOffset;
    len_t originalLength = length;

    void* directbuf = nullptr;
    len_t bufsize;

    if (_directIO && !isLog) {
        if (diskOffset % _pageSize) {
            debug_error("diskOffset not aligned: %lu\n", diskOffset);
        }
        directbuf = (void*)checkBufferSize(length, diskOffset, bufsize);
        memset(directbuf, 0, bufsize);
        if (isWrite) {
            memcpy((char*)directbuf + diskOffset % _pageSize, buf, length);
            length = ((diskOffset + length + _pageSize - 1) / _pageSize - diskOffset / _pageSize) * _pageSize;
            diskOffset = diskOffset - diskOffset % _pageSize;
        }
        tempBuf = (char*)directbuf;
    }

    struct timeval tv;
    gettimeofday(&tv, 0);

    if (isCircular) {
        // get the capacity, allow wrap-around write/read
        len_t capacity = _isSlave ? cm.getColdStorageCapacity() : cm.getSystemEffectiveCapacity();
        offset_t runningDiskOffset = diskOffset;
        for (len_t remains = length, len = 0; remains > 0; remains -= len, runningDiskOffset = (runningDiskOffset + len) % capacity) {
            len = std::min(capacity - runningDiskOffset, remains);
            if (len == 0) {
                len = remains;
            }
            offset_t inOffset = length - remains;
            ssize_t ret = 0;
            //            fseek(fd, runningDiskOffset, SEEK_SET);
            if (isWrite) {
                ret = pwrite(fd, tempBuf + inOffset, len, runningDiskOffset);
                //                ret = fwrite(buf + inOffset, sizeof(unsigned char), len, fd);
            } else {
                ret = pread(fd, tempBuf + inOffset, len, runningDiskOffset);
                //                ret = fread(buf + inOffset, sizeof(unsigned char), len, fd);
            }
            if (ret < 0 || (size_t)ret != len) {
                debug_error("Error on f%s buf=%p to file (fd=%d) at %lu length %lu: %s\n", (isWrite ? "write" : "read"), tempBuf + inOffset, fd, runningDiskOffset, len, strerror(errno));
                assert(0);
                exit(-1);
            }
            accessLength += ret;
        }
    } else {
        //        fseek(fd, diskOffset, SEEK_SET);
        if (isWrite) {
            accessLength = pwrite(fd, tempBuf, length, diskOffset);
            //            accessLength = fwrite(buf, sizeof(unsigned char), length, fd);
        } else {
            accessLength = pread(fd, tempBuf, length, diskOffset);
            //            accessLength = fread(buf, sizeof(unsigned char), length, fd);
        }
    }

    // mark disk as dirty if needed, and update stats
    if (isWrite) {
        StatsRecorder::getInstance()->IOBytesWrite(accessLength, 0);
	StatsRecorder::getInstance()->timeProcess(StatsType::DEVICE_WRITE, tv);
    } else {
        StatsRecorder::getInstance()->IOBytesRead(accessLength, 0);
	StatsRecorder::getInstance()->timeProcess(StatsType::DEVICE_READ, tv);
    }

    if (!isWrite && _directIO && !isLog) {
        // copy back
        memcpy(buf, (char*)directbuf + originalDiskOffset % _pageSize, originalLength);
    }

    if (directbuf != nullptr) {
        free(directbuf);
    }

    return accessLength;
}

unsigned char* DeviceManager::readMmap(segment_id_t segmentId, segment_offset_t offset, segment_len_t length, unsigned char* buf)
{
    ConfigManager& cm = ConfigManager::getInstance();
    bool useFS = cm.segmentAsFile();
    if (!useFS || cm.segmentAsSeparateFile())
        return 0;
    int fd = accessFileFd(0); // fileno(accessFileFd(0));

    offset_t foffset = getOffsetBySegmentId(segmentId) + offset;
    offset_t adjfoffset = foffset - (foffset % _pageSize);
    segment_len_t adjlength = length + (foffset % _pageSize);

    unsigned char* target = (unsigned char*)mmap(buf, adjlength, PROT_NONE | PROT_READ, MAP_PRIVATE, fd, adjfoffset);
    // printf("mmap %p (%p) offset %lu (%lu) length %lu (%lu) \n", buf, target, foffset, adjfoffset, length, adjlength);
    if ((void*)target == MAP_FAILED) {
        debug_error("mmap failed (%s)\n", strerror(errno));
        assert(0);
    }
    return target + (foffset % _pageSize);
}

bool DeviceManager::readUmmap(segment_id_t segmentId, segment_offset_t offset, segment_len_t length, unsigned char* buf)
{
    ConfigManager& cm = ConfigManager::getInstance();
    bool useFS = cm.segmentAsFile();
    if (!useFS || cm.segmentAsSeparateFile())
        return false;
    // printf("munmap %p length %lu\n", buf, length);

    offset_t foffset = getOffsetBySegmentId(segmentId) + offset;

    return (munmap(buf - (foffset % _pageSize), length + (foffset % _pageSize)) == 0);
}

bool DeviceManager::readAhead(segment_id_t segmentId, segment_offset_t offset, segment_len_t length)
{
    // Todo support vlog readahead
    if (_isSlave)
        return false;
    if (ConfigManager::getInstance().enabledVLogMode()) {
        segmentId = 0;
    }

    disk_id_t diskId = getDiskBySegmentId(segmentId);
    assert(_diskInfo.count(diskId));
    assert(_diskMutex.count(diskId));

    int fd = 0;
    offset_t foffset = 0;

    bool useFS = ConfigManager::getInstance().segmentAsFile();
    if (useFS) {
        if (ConfigManager::getInstance().segmentAsSeparateFile()) {
            assert(0);
            return false;
        }
        fd = accessFileFd(0); // fileno(accessFileFd(0));
    } else {
        fd = _diskInfo.at(diskId).fd;
        if (length <= 0 || diskId == INVALID_DISK || _diskInfo.count(diskId) < 0 || foffset + length > _diskInfo.at(diskId).capacity) {
            return INVALID_LBA;
        }
    }
    foffset = getOffsetBySegmentId(segmentId) + offset;
    lock_guard<mutex> lk(*_diskMutex.at(diskId));
    return posix_fadvise(fd, foffset, length, POSIX_FADV_RANDOM | POSIX_FADV_WILLNEED) == 0;
}

void DeviceManager::writePartialSegmentMt(segment_id_t segmentId, segment_offset_t startingOffset, segment_len_t length, unsigned char* buf, offset_t& ret, std::atomic_int& count)
{
    ret = writePartialSegment(segmentId, startingOffset, length, buf);
    count--;
}

offset_t DeviceManager::writePartialSegment(segment_id_t segmentId, segment_offset_t startingOffset, segment_len_t length, unsigned char* buf)
{
    return accessDataOnDisk(segmentId, startingOffset, length, buf, true);
}

bool DeviceManager::readSegment(segment_id_t segmentId, unsigned char* buf, segment_offset_t startingOffset)
{
    segment_len_t segmentSize = ConfigManager::getInstance().getSegmentSize(isLogSegment(segmentId));
    assert(startingOffset < segmentSize);

    return readPartialSegment(segmentId, startingOffset, segmentSize - startingOffset, buf);
}

len_t DeviceManager::writeDisk(disk_id_t diskId, unsigned char* buf, offset_t diskOffset, len_t length)
{
    if (ConfigManager::getInstance().segmentAsFile() && ConfigManager::getInstance().segmentAsSeparateFile()) {
        return accessSegmentFile(diskId, buf, diskOffset, length, /* isWrite = */ true);
    } else {
        return accessDisk(diskId, buf, diskOffset, length, /* isWrite = */ true);
    }
}

offset_t DeviceManager::writeUpdateLog(unsigned char* buf, len_t logSize)
{
    return accessLogFile(/* isUpdate = */ true, buf, logSize, /* isWrite = */ true);
}

offset_t DeviceManager::writeGCLog(unsigned char* buf, len_t logSize)
{
    return accessLogFile(/* isUpdate = */ false, buf, logSize, /* isWrite = */ true);
}

// For vLog, not the write journal and GC journal in HashKV
offset_t DeviceManager::writeLogHeadTail(unsigned char* buf, len_t logSize)
{
    return accessLogFile(/* isUpdate = */ true, buf, logSize, /* isWrite = */ true);
}

bool DeviceManager::readUpdateLog(unsigned char* buf, len_t logSize)
{
    return accessLogFile(/* isUpdate = */ true, buf, logSize, /* isWrite = */ false) == logSize;
}

bool DeviceManager::readGCLog(unsigned char* buf, len_t logSize)
{
    return accessLogFile(/* isUpdate = */ false, buf, logSize, /* isWrite = */ false) == logSize;
}

bool DeviceManager::readLogHeadTail(unsigned char* buf, len_t logSize)
{
    return accessLogFile(/* isUpdate = */ true, buf, logSize, /* isWrite */ false) == logSize;
}

len_t DeviceManager::getUpdateLogSize()
{
    return accessLogFile(/* isUpdate = */ true, /* buf = */ 0, /* logSize = */ 0, /* isWrite = */ false);
}

len_t DeviceManager::getGCLogSize()
{
    return accessLogFile(/* isUpdate = */ false, /* buf = */ 0, /* logSize = */ 0, /* isWrite = */ false);
}

bool DeviceManager::removeUpdateLog()
{
    return accessLogFile(/* isUpdate = */ true, 0, 0, /* isWrite = */ false, /* isDelete = */ true);
}

bool DeviceManager::removeGCLog()
{
    return accessLogFile(/* isUpdate = */ false, 0, 0, /* isWrite = */ false, /* isDelete = */ true);
}

void DeviceManager::readSegmentMt(segment_id_t segmentId, unsigned char* buf, std::atomic_int& count, segment_offset_t startingOffset)
{
    readSegment(segmentId, buf, startingOffset);
    count--;
}

bool DeviceManager::readPartialSegment(segment_id_t segmentId, segment_offset_t startingOffset, segment_len_t length, unsigned char* buf)
{
    return accessDataOnDisk(segmentId, startingOffset, length, buf, false) != INVALID_OFFSET;
}

void DeviceManager::readPartialSegmentMt(segment_id_t segmentId, segment_offset_t startingOffset, segment_len_t length, unsigned char* buf, std::atomic_int& count)
{
    readPartialSegment(segmentId, startingOffset, length, buf);
    count--;
}

void DeviceManager::readPartialSegmentMtD(segment_id_t segmentId, segment_offset_t startingOffset, segment_len_t length, unsigned char* buf, uint8_t& done)
{
    readPartialSegment(segmentId, startingOffset, length, buf);
    done = 1;
}

len_t DeviceManager::readDisk(disk_id_t diskId, unsigned char* buf, offset_t diskOffset, len_t length)
{
    if (ConfigManager::getInstance().segmentAsFile() && ConfigManager::getInstance().segmentAsSeparateFile()) {
        return accessSegmentFile(diskId, buf, diskOffset, length, /* isWrite = */ false);
    } else {
        return accessDisk(diskId, buf, diskOffset, length, /* isWrite = */ false);
    }
}

void DeviceManager::syncDevice(disk_id_t diskId, std::atomic_int& waitSync, bool needsUnlock)
{
    fsync(_diskInfo.at(diskId).fd);
    if (needsUnlock)
        _diskMutex.at(diskId)->unlock();
    waitSync--;
}

void DeviceManager::syncDevices()
{
    std::atomic_int waitSync;
    waitSync = 0;
    for (auto& disk : _diskInfo) {
        if (disk.second.dirty) {
            _diskMutex.at(disk.first)->lock();
            disk.second.dirty = false;
            if (_numDisks > 1) {
                waitSync++;
                boost::asio::post(*_stp, boost::bind(&DeviceManager::syncDevice, this, disk.first, boost::ref(waitSync), true));
            } else {
                fsync(disk.second.fd);
                _diskMutex.at(disk.first)->unlock();
            }
        }
    }
    while (_numDisks > 1 && waitSync > 0)
        ;
}

size_t DeviceManager::getDiskNum()
{
    return _diskInfo.size();
}

std::vector<DiskInfo> DeviceManager::getDisks(bool alive)
{
    std::vector<DiskInfo> disks;
    for (auto& disk : _diskIdVector) {
        if (_diskInfo.at(disk).alive == alive) {
            disks.push_back(_diskInfo.at(disk));
        }
    }
    return disks;
}

size_t DeviceManager::setDisksStatus(std::vector<disk_id_t>& diskIds, bool status)
{
    size_t ret = 0;
    for (auto id : diskIds) {
        if (_diskInfo.count(id) <= 0)
            continue;
        ret++;
        _diskInfo.at(id).setDisk(status);
    }
    return ret;
}

}
