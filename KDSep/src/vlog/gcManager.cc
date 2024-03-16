#include "vlog/gcManager.hh"
#include "vlog/configManager.hh"
#include "utils/statsRecorder.hh"
#include <float.h>
#include <unordered_set>
#include <vector>

#define TAG_MASK (1000 * 1000)
#define RECORD_SIZE ((valueSize == INVALID_LEN ? 0 : valueSize) + (LL)sizeof(len_t) + KEY_REC_SIZE)

namespace KDSEP_NAMESPACE {

GCManager::GCManager(KeyManager* keyManager, ValueManager* valueManager, DeviceManager* deviceManager, SegmentGroupManager* segmentGroupManager, bool isSlave)
    : _keyManager(keyManager)
    , _valueManager(valueManager)
    , _deviceManager(deviceManager)
    , _segmentGroupManager(segmentGroupManager)
{
    _maxGC = ConfigManager::getInstance().getGreedyGCSize();
    _useMmap = ConfigManager::getInstance().useMmap();
    if ((ConfigManager::getInstance().enabledVLogMode() && !_useMmap) || isSlave) {
        Segment::init(_gcSegment.read, INVALID_SEGMENT, ConfigManager::getInstance().getVLogGCSize() * 2);
    }
    Segment::init(_gcSegment.write, INVALID_SEGMENT, ConfigManager::getInstance().getVLogGCSize());
    _isSlave = isSlave;
    _gcCount.ops = 0;
    _gcCount.groups = 0;
    _gcCount.scanBytes = 0;
    _gcWriteBackBytes = 0;
    _gcCompactedBytes = 0;

    //    _gcReadthreads.size_controller().resize(ConfigManager::getInstance().getNumGCReadThread());
}

GCManager::~GCManager()
{
    // for debug and measurement
    printStats();
}

void GCManager::printStats(FILE* out)
{
    fprintf(out, "_gcBytes writeBack = %lu compacted = %lu scan = %lu\n", _gcWriteBackBytes, _gcCompactedBytes, _gcCount.scanBytes);
    fprintf(out, "Mode counts (total ops = %lu groups = %lu):\n", _gcCount.ops, _gcCount.groups);
    for (auto it : _modeCount) {
        fprintf(out, "[%d] = %lu\n", it.first, it.second);
    }
}

size_t GCManager::gcVLog()
{

    _gcCount.ops++;
    //_valueManager->_GCLock.lock();

    size_t gcBytes = 0, gcScanBytes = 0, gcCompactedBytes = 0;
    size_t pageSize = sysconf(_SC_PAGE_SIZE);
    len_t gcSize = ConfigManager::getInstance().getVLogGCSize();
    unsigned char* readPool = _useMmap ? 0 : Segment::getData(_gcSegment.read);
    unsigned char* writePool = Segment::getData(_gcSegment.write);
    len_t logTail = _segmentGroupManager->getLogWriteOffset();

    if (ConfigManager::getInstance().useDirectIO() && gcSize % pageSize) {
        debug_error("use direct IO but gcSize %lu not aligned; stop\n", gcSize);
        return 0;
    }

    std::vector<char*> keys;
    std::vector<ValueLocation> values;
    ValueLocation valueLoc;
    valueLoc.segmentId = 0;

    len_t capacity = ConfigManager::getInstance().getSystemEffectiveCapacity();
    len_t len = INVALID_LEN;
    offset_t logOffset = INVALID_OFFSET, gcFront = INVALID_OFFSET, flushFront = 0;

    size_t remains = 0;
    bool ret = false;
    const segment_id_t maxSegment = ConfigManager::getInstance().getNumSegment();
    key_len_t keySize;
    len_t valueSize;
    offset_t keySizeOffset;

    // For scanKeyValue()
    char *key, *value;
    len_t compactedBytes = 0;

    // Statistics
    len_t rewrittenKeys = 0, cleanedKeys = 0;

    _keyManager->persistMeta();

    gcFront = _segmentGroupManager->getLogGCOffset();
//    debug_error("logTail %lu gcFront %lu minus %lu -minus %lu\n", logTail,
//            gcFront, logTail - gcFront, gcFront - logTail);

    struct timeval gcStartTime;
    gettimeofday(&gcStartTime, 0);
    // scan until the designated reclaim size is reached
    while (gcBytes < gcSize) {
        // see if there is anything left over in last scan
        flushFront = Segment::getFlushFront(_gcSegment.read);
        if (flushFront == gcSize) {
            debug_error("space not enough or have bugs: remains %lu gcBytes %lu flushFront %lu logHead %lu logTail %lu\n", remains, gcBytes, flushFront, gcFront, logTail);
        }
        assert(flushFront != gcSize);
        // read and fit up only the available part of buffer
        gcFront = _segmentGroupManager->getLogGCOffset();

        if ((gcFront < logTail && logTail - gcFront < gcSize * 2) 
                || (gcFront > logTail && logTail + capacity - gcFront < gcSize * 2)) {
            debug_info("GC stop: go through whole log but gcBytes not enough: gcBytes %lu gcSize %lu logHead %lu logTail %lu\n",
                gcBytes, gcSize, gcFront, logTail);
            break;
        }

        if (_useMmap) {
            readPool = _deviceManager->readMmap(0, gcFront - flushFront, gcSize, 0);
            Segment::init(_gcSegment.read, 0, readPool, gcSize);
        } else {
            STAT_PROCESS(_deviceManager->readAhead(/* diskId = */ 0, gcFront + flushFront, gcSize - flushFront), StatsType::GC_READ_AHEAD);
            STAT_PROCESS(_deviceManager->readDisk(/* diskId = */ 0, readPool + flushFront, gcFront + flushFront, gcSize - flushFront), StatsType::GC_READ);
        }

        // mark the amount of data in buffer
        Segment::setWriteFront(_gcSegment.read, gcSize);
        for (remains = gcSize; remains > 0;) {
            debug_trace("remains %lu gcBytes %lu flushFront %lu logHead %lu logTail %lu\n", remains, gcBytes, flushFront, gcFront, logTail);
            keySizeOffset = gcSize - remains;

            // change key, keySize, value, valueSize, remains, compactedBytes
            // Input
            //      gcFront:  starting offset in the disk
            //      gcSize: The size of buffer
            // Input and Output
            //      keySizeOffset: Buffer offset where scan starts
            //      remains: The remaining buffer bytes after scanning
            // Output:
            //      key, value: The pointere to the real key and value
            //      keySize, valueSize
            //      compactedBytes: The buffer of bytes with empty bytes
            //          (For direct I/O, zero bytes)
            bool ret = scanKeyValue((char*)readPool, gcFront, gcSize,
                    keySizeOffset, key, keySize, value, valueSize, remains,
                    compactedBytes, pageSize);

            // value may have the sequence number

            if (!ret) {
                break;
            }

            // check if we can read the value. Keep it to the next buffer.
            STAT_PROCESS(valueLoc = _keyManager->getKey(key),
                    StatsType::GC_KEY_LOOKUP);
            debug_trace("read LSM: key [%.*s] offset %lu\n", 
                    (int)keySize, KEY_OFFSET(key), valueLoc.offset);
            // check if pair is valid, avoid underflow by adding capacity, and
            // overflow by modulation
            if (valueLoc.segmentId == (_isSlave ? maxSegment : 0) &&
                    valueLoc.offset == (gcFront + keySizeOffset + capacity) %
                    capacity) {
                rewrittenKeys++;
                // buffer full, flush before write
                if (!Segment::canFit(_gcSegment.write, RECORD_SIZE)) {
                    // Flush to Vlog
                    STAT_PROCESS(tie(logOffset, len) =
                            _valueManager->flushSegmentToWriteFront(_gcSegment.write,
                                /* isGC = */ true), StatsType::GC_FLUSH);
                    assert(len > 0);
                    // update metadata
                    for (auto& v : values) {
                        // buffer (relative) offset to disk (absolute) offset
                        v.offset = (v.offset + logOffset) % capacity;
                    }
//                    debug_error("logOffset %lu len %lu total %lu\n", logOffset, len,
//                            logOffset + len);
                    STAT_PROCESS(ret = _keyManager->mergeKeyBatch(keys,
                                values), StatsType::UPDATE_KEY_WRITE_LSM_GC);
                    if (!ret) {
                        debug_error("Failed to update %lu keys to LSM\n", keys.size());
                        assert(0);
                        exit(-1);
                    }
                    // reset keys and value holders, buffers
                    keys.clear();
                    values.clear();
                    Segment::resetFronts(_gcSegment.write);
                }
                // mark and copy the key-value pair to tbe buffer
                offset_t writeKeyOffset = Segment::getWriteFront(_gcSegment.write);
                Segment::appendData(_gcSegment.write, readPool + keySizeOffset, RECORD_SIZE);
                keys.push_back((char*)writePool + writeKeyOffset);
                valueLoc.offset = writeKeyOffset;
                valueLoc.length = valueSize;
                values.push_back(valueLoc);
                _gcWriteBackBytes += RECORD_SIZE;
            } else {
                cleanedKeys++;
                // space is reclaimed directly
                gcBytes += RECORD_SIZE;
                // cold storage invalid bytes cleaned
                if (_isSlave) {
                    _valueManager->_slave.writtenBytes -= RECORD_SIZE;
                }
            }

            gcCompactedBytes += compactedBytes;
            gcBytes += compactedBytes;
        }
        if (remains > 0) {
            Segment::setFlushFront(_gcSegment.read, remains);
            if (!_useMmap) {
                // if some data left without scanning, move them to the front of buffer for next scan
                memmove(readPool, readPool + gcSize - remains, remains);
            }
        } else {
            // reset buffer
            Segment::resetFronts(_gcSegment.read);
        }
        if (_useMmap) {
            _deviceManager->readUmmap(0, gcFront, gcSize, readPool);
        }

        gcScanBytes += gcSize - remains;
        _segmentGroupManager->getAndIncrementVLogGCOffset(gcSize - remains);
        debug_info("one loop -- logHead %lu gcScanBytes %lu (%lu keys) gcBytes %lu (%lu keys)\n", _segmentGroupManager->getLogGCOffset(), gcScanBytes, rewrittenKeys + cleanedKeys, gcBytes, cleanedKeys);
    }

    // final check on remaining data to flush
    if (!keys.empty()) {

        STAT_PROCESS(tie(logOffset, len) = _valueManager->flushSegmentToWriteFront(_gcSegment.write, /* isGC */ true), StatsType::GC_FLUSH);
        for (auto& v : values) {
            v.offset = (v.offset + logOffset) % capacity;
        }
//        debug_error("logOffset %lu len %lu total %lu\n", logOffset, len,
//                logOffset + len);
        // update metadata of flushed data
        STAT_PROCESS(ret = _keyManager->mergeKeyBatch(keys, values), StatsType::UPDATE_KEY_WRITE_LSM_GC);
        if (!ret) {
            debug_error("Failed to update %lu keys to LSM\n", keys.size());
            assert(0);
            exit(-1);
        }

        if (ConfigManager::getInstance().persistLogMeta()) {
            std::string value;
            value.append(to_string(logOffset + len));
            _keyManager->writeMeta(SegmentGroupManager::LogTailString, strlen(SegmentGroupManager::LogTailString), value);
            _keyManager->persistMeta();
        }
    }

    // printf("gcBytes %lu\n", gcBytes);
    StatsRecorder::getInstance()->totalProcess(StatsType::GC_SCAN_BYTES, gcScanBytes);
    StatsRecorder::getInstance()->totalProcess(StatsType::GC_WRITE_BYTES, gcScanBytes - gcBytes);
    // reset write buffer
    Segment::resetFronts(_gcSegment.write);

    _gcCompactedBytes += gcCompactedBytes;

    debug_info("after GC, logHead %lu gcScanBytes %lu cleaned bytes %lu\n", _segmentGroupManager->getLogGCOffset(), gcScanBytes, gcBytes);

    if (ConfigManager::getInstance().persistLogMeta()) {
        std::string value;
        value.append(to_string(gcFront + gcSize - remains));
        _keyManager->writeMeta(SegmentGroupManager::LogHeadString, strlen(SegmentGroupManager::LogHeadString), value);
    }
    Segment::resetFronts(_gcSegment.read);
    StatsRecorder::getInstance()->timeProcess(StatsType::GC_TOTAL, gcStartTime);

    _gcCount.scanBytes += gcScanBytes;
    //_valueManager->_GCLock.unlock();
    return gcBytes;
}

inline int GCManager::getHotness(group_id_t groupId, int updateCount)
{
    return (updateCount > 1) ? ConfigManager::getInstance().getHotnessLevel() / 2 : 0;
}

GCMode GCManager::getGCMode(group_id_t groupId, len_t reservedBytes)
{
    GCMode gcMode = ConfigManager::getInstance().getGCMode();
    // check if the triggering condition(s) matches,
    // swtich to all if
    // (1) log reclaim is less than reclaim min threshold
    // (2) write back ratio is smaller for all
    double ratioAll = _segmentGroupManager->getGroupWriteBackRatio(groupId, /* type = */ 0, /* isGC = */ true);
    double ratioLogOnly = _segmentGroupManager->getGroupWriteBackRatio(groupId, /* type = */ 1, /* isGC = */ true);
    if (gcMode == LOG_ONLY && ratioAll <= ratioLogOnly) {
        gcMode = ALL;
    }
    return gcMode;
}

}

#undef TAG_MASK
