#ifndef __INDEXSTOREDEVICE_HH__
#define __INDEXSTOREDEVICE_HH__

#include "common/dataStructure.hpp"
#include "common/indexStorePreDefines.hpp"
#include "ds/bitmap.hh"
#include "ds/diskinfo.hh"
#include <atomic>
#include <boost/asio.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/bind/bind.hpp>
#include <boost/thread.hpp>
#include <boost/thread/thread.hpp>
//#include "threadpool/threadpool.hpp"
#include <unordered_map>
#include <vector>

namespace KDSEP_NAMESPACE {

class DeviceManager {
public:
    DeviceManager()
    {
    }
    DeviceManager(std::vector<DiskInfo> disks, bool isSlave = false);
    ~DeviceManager();

    void* checkBufferSize(len_t neededSize, offset_t offset, len_t& bufsize);

    // mapping between segment and disks
    disk_id_t getDiskBySegmentId(segment_id_t segmentId);
    offset_t getOffsetBySegmentId(segment_id_t segmentId);
    segment_id_t getSegmentIdByOffset(disk_id_t diskId, offset_t ofs);

    // write
    offset_t writeSegment(segment_id_t segmentId, unsigned char* buf, segment_offset_t startingOffset = 0);
    offset_t writePartialSegment(segment_id_t segmentId, segment_offset_t startingOffset, segment_len_t length, unsigned char* buf);
    void writePartialSegmentMt(segment_id_t segmentId, segment_offset_t startingOffset, segment_len_t length, unsigned char* buf, offset_t& ret, std::atomic_int& count);
    len_t writeDisk(disk_id_t diskId, unsigned char* buf, offset_t diskOffset, len_t length);

    offset_t writeUpdateLog(unsigned char* buf, len_t logSize);
    offset_t writeGCLog(unsigned char* buf, len_t logSize);
    offset_t writeLogHeadTail(unsigned char* buf, len_t logSize);
    bool readLogHeadTail(unsigned char* buf, len_t logSize);

    bool removeUpdateLog();
    bool removeGCLog();

    // read
    bool readSegment(segment_id_t segmentId, unsigned char* buf, segment_offset_t startingOffset = 0);
    void readSegmentMt(segment_id_t segmentId, unsigned char* buf, std::atomic_int& count, segment_offset_t startingOffset = 0);
    bool readPartialSegment(segment_id_t segmentId, segment_offset_t startingOffset, segment_len_t length, unsigned char* buf);
    void readPartialSegmentMt(segment_id_t segmentId, segment_offset_t startingOffset, segment_len_t length, unsigned char* buf, std::atomic_int& count);
    void readPartialSegmentMtD(segment_id_t segmentId, segment_offset_t startingOffset, segment_len_t length, unsigned char* buf, uint8_t& done);
    len_t readDisk(disk_id_t diskId, unsigned char* buf, offset_t diskOffset, len_t length);

    bool readAhead(segment_id_t segmentId, segment_offset_t offset, segment_len_t length);

    unsigned char* readMmap(segment_id_t segmentId, segment_offset_t offset, segment_len_t length, unsigned char* buf);
    bool readUmmap(segment_id_t segmentId, segment_offset_t offset, segment_len_t length, unsigned char* buf);

    bool readUpdateLog(unsigned char* buf, len_t logSize);
    bool readGCLog(unsigned char* buf, len_t logSize);

    len_t getUpdateLogSize();
    len_t getGCLogSize();

    // sync disks
    void syncDevice(disk_id_t diskId, std::atomic_int& waitSync, bool needsUnlock = false);
    void syncDevices();

    size_t getDiskNum();
    std::vector<DiskInfo> getDisks(bool alive = true);

private:
    /** the mapping of disk id and disk infomation */
    std::unordered_map<disk_id_t, DiskInfo> _diskInfo;

    /** orderred list of data disk id **/
    std::vector<disk_id_t> _diskIdVector;

    /** the mapping of disk id and disk lock */
    std::unordered_map<disk_id_t, std::mutex*> _diskMutex;

    /** the mapping of disk id and disk status */
    std::unordered_map<disk_id_t, bool> _diskStatus;
    /** avoid race conditions for failure simulations **/
    std::mutex _diskStatusMutex;

    int _numDisks; // the number of all disks
    int _pageSize;
    bool _directIO;

    bool _isSlave;

    boost::asio::thread_pool* _stp;

#ifdef DISKOffset_OUT
    FILE* fp; // the file pointer for Offset printing
#endif

    struct {
        std::map<segment_id_t, int> fds;
    } _segmentFiles;

    len_t accessDisk(disk_id_t diskId, unsigned char* buf, offset_t diskOffset, len_t length, bool isWrite);
    offset_t accessDataOnDisk(segment_id_t segmentId, segment_offset_t startingOffset, segment_len_t writeLength, unsigned char* buf, bool isWrite);

    len_t accessSegmentFile(segment_id_t segmentId, unsigned char* buf, segment_offset_t startingOffset, segment_len_t writeLength, bool isWrite);
    len_t accessLogFile(bool isUpdate, unsigned char* buf, len_t logSize, bool isWrite, bool isDelete = false);
    len_t accessFile(int fd, unsigned char* buf, segment_offset_t startingOffset, segment_len_t writeLength, bool isWrite, bool isCicular, bool isLog = false);

    int accessFileFd(segment_id_t segmentId);

    size_t setDisksStatus(std::vector<disk_id_t>& diskIds, bool alive);

    bool isLogSegment(segment_id_t segmentId);
};

}

#endif // __INDEXSTOREDEVICE_HH__
