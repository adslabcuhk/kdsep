#include "vlog/valueManager.hh"
#include "utils/timer.hpp"

#define RECORD_SIZE ((valueSize == INVALID_LEN ? 0 : valueSize) + (LL)sizeof(len_t) + KEY_REC_SIZE)
#define RECOVERY_MULTI_THREAD_ENCODE
#define NUM_RESERVED_GC_SEGMENT (2)

namespace KDSEP_NAMESPACE {

// always reserve (at least) one spare for flush of reserved space
int ValueManager::spare = 1;

ValueManager::ValueManager(DeviceManager *deviceManager, SegmentGroupManager *segmentGroupManager, KeyManager *keyManager, LogManager *logManager, bool isSlave)
{
    // init the connections to different mods
    _deviceManager = deviceManager;
    _segmentGroupManager = segmentGroupManager;
    _keyManager = keyManager;
    //_gcManager = 0;
    _logManager = logManager;
    _slaveValueManager = 0;

    ConfigManager &cm = ConfigManager::getInstance();

    _isSlave = isSlave;

    if (!isSlave && cm.useSlave()) {
        std::vector<DiskInfo> disks;
        if (cm.useSeparateColdStorageDevice()) {
            DiskInfo coldDisk(0, cm.getColdStorageDevice().c_str(), cm.getColdStorageCapacity());
            disks.push_back(coldDisk);
        } else {
            disks = deviceManager->getDisks();
            // Todo: array segment alignment for mulitple disks
            for (unsigned int i = 0; i < disks.size(); i++) {
                disks.at(i).skipOffset = cm.getMainSegmentSize() * cm.getNumMainSegment() + cm.getLogSegmentSize() * cm.getNumLogSegment();
            }
        }
        _slave.dm = new DeviceManager(disks, /* isSlave = */ true);
        _slave.cgm = new SegmentGroupManager(/* isSlave = */ true, _keyManager);
        _slaveValueManager = new ValueManager(_slave.dm, _slave.cgm, keyManager, 0, /* isSlave = */ true);
        _slave.gcm = new GCManager(keyManager, _slaveValueManager, _slave.dm, _slave.cgm, /* isSlave = */ true);
        _slaveValueManager->setGCManager(_slave.gcm);
        //printf("dm %p cgm %p svm %p\n", _slave.dm, _slave.cgm, _slaveValueManager);
    } else {
        _slave.dm = 0;
        _slave.cgm = 0;
        _slave.gcm = 0;
    }
    _slave.writtenBytes = _segmentGroupManager->getLogWrittenBytes();
    _slave.validBytes = _segmentGroupManager->getLogValidBytes();

    // always use vlog by default for slave
    bool vlogEnabled = _isSlave? true : cm.enabledVLogMode();

    // level of hotness
    int hotnessLevel = cm.getHotnessLevel();
    if (vlogEnabled) {
        hotnessLevel = 1;
    }

    // one coding for diff hotness
    _activeSegments.resize(hotnessLevel + spare);
    _curSegmentBuffer.resize(hotnessLevel);
    // pre-allocate k segments for each hotness level
    _segmentBuffers.resize(hotnessLevel);

    // init lock for spare buffers
    for (int i = 0; i < spare; i++) {
        // only init the record, leave the data to be mapped to reserved space pool on flush
        INIT_LIST_HEAD(&_activeSegments.at(i + hotnessLevel));
    }

    int numPipelinedBuffer = cm.getNumPipelinedBuffer();
    // abstract the pool as a segment
    // pool(s) for updates
    for (int i = 0; i < numPipelinedBuffer; i++) {
        _centralizedReservedPool[i].size = _isSlave ? cm.getMainSegmentSize() : cm.getUpdateKVBufferSize();
        if (_centralizedReservedPool[i].size <= cm.getLogSegmentSize())
            _centralizedReservedPool[i].size = cm.getLogSegmentSize();
        Segment::init(_centralizedReservedPool[i].pool, INVALID_SEGMENT, _centralizedReservedPool[i].size);
    }
    _centralizedReservedPoolIndex.flushNext = 0;
    _centralizedReservedPoolIndex.inUsed = 0;

    // special pool for log segments only flush
    Segment::init(_centralizedReservedPool[numPipelinedBuffer].pool, INVALID_SEGMENT, cm.getMainSegmentSize() * 2);

    // init spare reserved space buffers for flush
    _segmentReservedPool = new SegmentPool(cm.getNumParallelFlush() + NUM_RESERVED_GC_SEGMENT, SegmentPool::poolType::log);

    // segment of zeros
    Segment::init(_zeroSegment, INVALID_SEGMENT, cm.getMainSegmentSize());

    // segment for read
    Segment::init(_readBuffer, INVALID_SEGMENT, cm.getMainSegmentSize());

    _started = true;

    // restore from any log after failure
    boost::thread::attributes attrs;
    attrs.set_stack_size(1000 * 1024 * 1024);
    // boost::thread* th = new boost::thread(attrs, boost::bind(&ValueManager::flushCentralizedReservedPoolBgWorker, this));
    // thList_.push_back(th);

    if (_logManager) {
        if (ConfigManager::getInstance().enabledVLogMode()) {
            std::map<std::string, externalIndexInfo> keyValues;
	    struct timeval tv, tv2;
	    gettimeofday(&tv, 0);
            restoreVLog(keyValues);
	    gettimeofday(&tv2, 0);

	    printf("restore vLog time: %.3lf\n", 
		    tv2.tv_sec + tv2.tv_usec / 1000000.0 - tv.tv_sec -
		     tv.tv_usec / 1000000.0);
        } else {
            //            restoreFromUpdateLog();
            //            restoreFromGCLog();
        }
    }
}

ValueManager::~ValueManager()
{
    // flush and release segments
    // debug_info("forceSync() %s\n", "");
    // forceSync();
    // debug_info("forceSync() %s\n", "");

    // allow the bg thread to finish its job first
    cerr << "[valueManager] Try delete threads" << endl;
    _started = false;
    // pthread_cond_signal(&_needBgFlush);
    for (auto thIt : thList_) {
        thIt->join();
        delete thIt;
    }

    ConfigManager& cm = ConfigManager::getInstance();
    if (cm.scanAllRecordsUponStop()) {
        scanAllRecords();
    }
    // release buffers
    int hotnessLevel = cm.getHotnessLevel();
    if (cm.enabledVLogMode() || _isSlave) {
        hotnessLevel = 1;
    }
    list_head* ptr;
    // spare reserved buffers
    for (int i = 0; i < spare; i++) {
        list_for_each(ptr, &_activeSegments[hotnessLevel + i])
        {
            Segment::free(segment_of(ptr, SegmentBuffer, node)->segment);
        }
    }
    // centralized reserved pool
    for (int i = 0; i < cm.getNumPipelinedBuffer(); i++) {
        Segment::free(_centralizedReservedPool[i].pool);
    }
    // reserved pool
    assert(_segmentReservedPool->getUsage().second == 0);
    delete _segmentReservedPool;
    // segment of zeros
    Segment::free(_zeroSegment);
    Segment::free(_readBuffer);
    if (cm.persistLogMeta()) {
        len_t logOffset = _segmentGroupManager->getLogWriteOffset();
        _keyManager->writeMeta(SegmentGroupManager::LogTailString, strlen(SegmentGroupManager::LogTailString), to_string(logOffset));
        logOffset = _segmentGroupManager->getLogGCOffset();
        _keyManager->writeMeta(SegmentGroupManager::LogHeadString, strlen(SegmentGroupManager::LogHeadString), to_string(logOffset));
    }
    delete _slaveValueManager;
    delete _slave.gcm;
    delete _slave.cgm;
    delete _slave.dm;
}

ValueLocation ValueManager::putValue(char* keyStr, key_len_t keySize, char* valueStr, len_t valueSize, const ValueLocation& oldValueLoc, bool sync, int hotness)
{
    ValueLocation valueLoc;

    // segment_id_t segmentId = oldValueLoc.segmentId;

    // debug message for deleting key
    if (valueSize == INVALID_LEN) {
        debug_info("DELETE: [%.*s]\n", (int)keySize, KEY_OFFSET(keyStr));
    }

    // avoid GC
    _GCLock.lock();
    volatile int& poolIndex = _centralizedReservedPoolIndex.inUsed;
    unsigned char* key = (unsigned char*)keyStr;
    bool vlog = _isSlave || ConfigManager::getInstance().enabledVLogMode();

    // convert to reference to main group
    ValueLocation convertedLoc = oldValueLoc;

    // retain updates to a segment if it is in buffer
    group_id_t groupId = INVALID_GROUP;
    if (vlog) {
        groupId = 0;
        convertedLoc.segmentId = oldValueLoc.segmentId;
    } else if (oldValueLoc.segmentId != LSM_SEGMENT) { // not key-value in LSM-tree
        while (!_segmentGroupManager->convertRefMainSegment(convertedLoc, true))
            ;
        // locate the group
        groupId = _segmentGroupManager->getGroupBySegmentId(convertedLoc.segmentId);
        assert(groupId != INVALID_GROUP);
    } else {
        groupId = LSM_GROUP;
    }

    // check if in-place update is possible
    bool inPlaceUpdate = false;
    bool inPool = false;
    len_t oldValueSize = INVALID_LEN;

    std::unordered_map<unsigned char*, segment_len_t, hashKey, equalKey>::iterator keyIt = _centralizedReservedPool[poolIndex].keysInPool.find(key);
    inPool = keyIt != _centralizedReservedPool[poolIndex].keysInPool.end();

    if (inPool) {
        // always remove out-dated position of the key
        _centralizedReservedPool[poolIndex].keysInPool.erase(keyIt);

        // consider in-place update if size is the same (and such update is allowed)
        if (ConfigManager::getInstance().isInPlaceUpdate()) {
            off_len_t offLen(keyIt->second + KEY_REC_SIZE, sizeof(len_t));
            Segment::readData(_centralizedReservedPool[poolIndex].pool, &oldValueSize, offLen);
            assert(oldValueSize == INVALID_LEN || oldValueSize > 0);
            if (oldValueSize == INVALID_LEN) {
                oldValueSize = 0;
            }
            inPlaceUpdate = oldValueSize == valueSize;
        }
    }

    // flush before write, if no more place for updates
    if (!Segment::canFit(_centralizedReservedPool[poolIndex].pool, RECORD_SIZE)
	    && !inPlaceUpdate) {
        //        _GCLock.unlock();
        group_id_t prevGroupId = groupId;
        // printf("Flush before write\n");
        if (ConfigManager::getInstance().usePipelinedBuffer()) {
            flushCentralizedReservedPoolBg(StatsType::POOL_FLUSH);
        } else if (vlog) {
	    STAT_PROCESS(flushCentralizedReservedPoolVLog(true, poolIndex),
		    StatsType::POOL_FLUSH);
        } else {
            debug_error("settings wrong: no pipelined buffer and no vlog %s\n", "");
        }
        // need the kvserver to search if the location of key may be updated
        if (groupId == INVALID_GROUP && prevGroupId != LSM_GROUP /* not key-values in LSM */) {
            _segmentGroupManager->releaseGroupLock(prevGroupId);
            return valueLoc;
        }
        //        _GCLock.lock();
        inPool = false;
        inPlaceUpdate = false;
    }

    Segment& pool = _centralizedReservedPool[poolIndex].pool;

    if (groupId != LSM_GROUP && !vlog) {
        // update group id of key-values not in LSM just in case of GC
        groupId = _segmentGroupManager->getGroupBySegmentId(convertedLoc.segmentId);
        if (groupId == INVALID_GROUP) {
            _GCLock.unlock();
            return valueLoc;
        }
    }

    // update the value (in-place or append to buffer)
    segment_len_t poolOffset = Segment::getWriteFront(pool); // Should be always zero for vlog

    if (inPlaceUpdate) {
        // overwrite the value in-place
        poolOffset = keyIt->second;
        off_len_t offLen(poolOffset + KEY_REC_SIZE + sizeof(len_t), valueSize);
        Segment::overwriteData(pool, valueStr, offLen);
        debug_trace("Inplace update segment %lu len %lu\n", convertedLoc.segmentId, valueSize);

    } else {
        debug_trace("Appenddata: [%.*s] pool size %lu pool offset %lu value size %lu\n", keySize, KEY_OFFSET(keyStr),
            Segment::getSize(pool), poolOffset, valueSize);
        // append if new / cannot fit in-place
        Segment::appendData(pool, keyStr, KEY_REC_SIZE);
        Segment::appendData(pool, &valueSize, sizeof(len_t));
        if (valueSize > 0) {
            Segment::appendData(pool, valueStr, valueSize);
        }

        debug_trace("append update to segment %lu len %lu is full %d canfit %d\n", convertedLoc.segmentId, valueSize,
            Segment::isFull(pool), Segment::canFit(pool, 1));
        // increment the total update size counter for the segment
        _centralizedReservedPool[poolIndex].segmentsInPool[convertedLoc.segmentId].first += RECORD_SIZE;
        // add the offset (pointer) for the segment
        _centralizedReservedPool[poolIndex].segmentsInPool[convertedLoc.segmentId].second.insert(poolOffset);
        // mark the present of group in pool
        _centralizedReservedPool[poolIndex].groupsInPool[groupId].insert(RECORD_SIZE);
    }

    // add the updated key offset
    std::pair<unsigned char*, segment_len_t> keyOffset(Segment::getData(pool) + poolOffset, poolOffset);
    _centralizedReservedPool[poolIndex].keysInPool.insert(keyOffset);

    if (groupId != LSM_GROUP && !vlog)
        _segmentGroupManager->releaseGroupLock(groupId);

    // flush after write, if the pool is (too) full
    if (!Segment::canFit(pool, 1) || (ConfigManager::getInstance().getUpdateKVBufferSize() <= 0 || sync)) {
        //        _GCLock.unlock();
        if (ConfigManager::getInstance().usePipelinedBuffer()) {
            flushCentralizedReservedPoolBg(StatsType::POOL_FLUSH);
        } else if (vlog) {
	    STAT_PROCESS(flushCentralizedReservedPoolVLog(true, poolIndex),
		    StatsType::POOL_FLUSH);
        } else {
            debug_error("settings wrong: no pipelined buffer and no vlog %s\n", "");
        }
    } else {
        //        _GCLock.unlock();
    }

    _GCLock.unlock();

    valueLoc.segmentId = oldValueLoc.segmentId;
    valueLoc.length = valueSize + (groupId != LSM_GROUP ? sizeof(len_t) : 0);

    // use the old value location as a hints for GET before flush
    return valueLoc;
}

// here keyStr already become key records
bool ValueManager::getValueFromBuffer(const char* keyStr, key_len_t keySize, char*& valueStr, len_t& valueSize)
{

    unsigned char* key = (unsigned char*)keyStr;

    // always look into write buffer first
    // Todo (?) wait for bg flush to complete (metadata to settle)
    for (int idx = _centralizedReservedPoolIndex.inUsed; 1; decrementPoolIndex(idx)) {
        _centralizedReservedPool[idx].lock.lock();
        auto it = _centralizedReservedPool[idx].keysInPool.find(key);
        if (it != _centralizedReservedPool[idx].keysInPool.end()) {
            segment_len_t start = it->second;
            off_len_t offLen(start + KEY_REC_SIZE, sizeof(len_t));
            Segment::readData(_centralizedReservedPool[idx].pool, &valueSize, offLen);
            assert(valueSize > 0 || valueSize == INVALID_LEN);
            if (valueSize > 0) {
                // printf("Read update buf offset %lu\n", it->second);
                offLen = { start + KEY_REC_SIZE + sizeof(len_t), valueSize };
                valueStr = (char*)buf_malloc(valueSize);
                Segment::readData(_centralizedReservedPool[idx].pool, valueStr, offLen);
            }
            _centralizedReservedPool[idx].lock.unlock();
            return true;
        }
        _centralizedReservedPool[idx].lock.unlock();
        // loop at least one time to check the buffer in-use, if queue is empty, there is no more buffer to check, so skip looping all yet flushed buffers
        if (idx == _centralizedReservedPoolIndex.flushNext && _centralizedReservedPoolIndex.queue.empty())
            break;
    }

    return false;
}

// here keyStr already become key records
bool ValueManager::getValueFromDisk(const char* keyStr, key_len_t keySize, ValueLocation readValueLoc, char*& valueStr, len_t& valueSize)
{
    struct timeval tv;
    gettimeofday(&tv, 0);

    ConfigManager& cm = ConfigManager::getInstance();
    bool vlog = _isSlave || cm.enabledVLogMode();
    bool ret = false;
    Segment readBuffer;

    // Todo degraded read from device

    if (cm.useSlave() && readValueLoc.segmentId == cm.getNumSegment() && !_isSlave) {
        // access to slave
        ret = _slaveValueManager->getValueFromBuffer(keyStr, keySize, valueStr, valueSize);
        if (!ret) {
            ret = _slaveValueManager->getValueFromDisk(keyStr, keySize, readValueLoc, valueStr, valueSize);
        }
    } else if (vlog || _isSlave) {
        // vlog / slave mode
        valueStr = (char*)buf_malloc(KEY_REC_SIZE + sizeof(len_t) + readValueLoc.length);
        if (_isSlave && cm.segmentAsFile() && cm.segmentAsSeparateFile()) {
            _deviceManager->readDisk(cm.getNumSegment(), (unsigned char*)valueStr, readValueLoc.offset, readValueLoc.length + KEY_REC_SIZE + sizeof(len_t));
        } else {
            _deviceManager->readDisk(/* diskId = */ 0, (unsigned char*)valueStr, readValueLoc.offset, readValueLoc.length + KEY_REC_SIZE + sizeof(len_t));
        }
        off_len_t offLen = { KEY_REC_SIZE, sizeof(len_t) };
#ifndef NDEBUG
        memcpy(&valueSize, valueStr + KEY_REC_SIZE, sizeof(len_t));
#else
        valueSize = readValueLoc.length;
#endif // NDEBUG
        if (valueSize != readValueLoc.length) {
            debug_error("valueSize %lu v.s. readValueLoc.length %lu (offset %lu)\n", valueSize, readValueLoc.length, readValueLoc.offset);
        }

        // check the key and value size in the record
        uint64_t tmpValueSize;
        key_len_t tmpKeySize;
        memcpy(&tmpKeySize, valueStr, sizeof(key_len_t)); 
        if (tmpKeySize != keySize) {
            debug_error("read key size error! %d v.s. %d key %.*s off %lu\n",
                    (int)tmpKeySize, (int)keySize,
                    (int)keySize, 
                    keyStr + sizeof(key_len_t),
                    readValueLoc.offset);
            ret = false;
        } else {
            memcpy(&tmpValueSize, valueStr + sizeof(key_len_t) + keySize,
                    sizeof(len_t)); 
            if (tmpValueSize != valueSize) {
                debug_error("read value size error! %lu v.s. %lu\n",
                        (uint64_t)tmpValueSize, (uint64_t)valueSize);
                ret = false;
            } else {
                offLen = { sizeof(len_t) + KEY_REC_SIZE, valueSize };
                memmove(valueStr, valueStr + KEY_REC_SIZE + sizeof(len_t), valueSize);
                debug_trace("Read disk offset %lu length %lu %x\n", readValueLoc.offset, offLen.second, valueStr[0]);
                ret = true;
            }
        }
    } else {
        // read data
        valueStr = (char*)buf_malloc(readValueLoc.length + KEY_REC_SIZE);
        _deviceManager->readPartialSegment(readValueLoc.segmentId, readValueLoc.offset, readValueLoc.length + KEY_REC_SIZE, (unsigned char*)valueStr);
        // check and read value size
#ifndef NDEBUG
        memcpy(&valueSize, valueStr + KEY_REC_SIZE, sizeof(len_t));
#else
        valueSize = readValueLoc.length - sizeof(len_t);
#endif // NDEBUG
        assert(memcmp(valueStr, keyStr, KEY_REC_SIZE) == 0);
        assert(valueSize + sizeof(len_t) == readValueLoc.length);
        // adjust value position in buffer
        memmove(valueStr, valueStr + KEY_REC_SIZE + sizeof(len_t), valueSize);
        // printf("Read disk group %lu segment %lu offset %lu length %lu\n", _segmentGroupManager->getGroupBySegmentId(readValueLoc.segmentId), readValueLoc.segmentId, readValueLoc.offset, readValueLoc.length);
        ret = true;
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::GET_VALUE_DISK, tv);
    return ret;
}

bool ValueManager::outOfReservedSpace(offset_t flushFront, group_id_t groupId, int poolIndex)
{
    len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
    len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();
    segment_id_t numReserved = ConfigManager::getInstance().getNumPipelinedBuffer() - 1;
    bool oors = false;

    // for reserved segments, the max amount that can be write is (free groups * segment size)
    if (_segmentGroupManager->getNumFreeLogSegments() <= MIN_FREE_SEGMENTS + numReserved) {
        oors = true;
        return oors;
    }
    len_t logSegmentSpace = (len_t)(_segmentGroupManager->getNumFreeLogSegments() - (MIN_FREE_SEGMENTS + numReserved)) * logSegmentSize;

    // see if the sum of updates exceed the log space remains
    len_t sum = 0;
    if (flushFront > mainSegmentSize) {
        sum = (flushFront - mainSegmentSize) % logSegmentSize;
    }

    // pool is empty
    if (_centralizedReservedPool[poolIndex].segmentsInPool.empty()) {
        return oors;
    }

    // find the remaining log space
    for (auto u : _centralizedReservedPool[poolIndex].segmentsInPool.at(_segmentGroupManager->getGroupMainSegment(groupId)).second) {
        len_t valueSize = 0;
        key_len_t keySize = 0;
        off_len_t keyOffLen(u, sizeof(key_len_t));
        Segment::readData(_centralizedReservedPool[poolIndex].pool, &keySize, keyOffLen);
        off_len_t valueOffLen(u + KEY_REC_SIZE, sizeof(len_t));
        Segment::readData(_centralizedReservedPool[poolIndex].pool, &valueSize, valueOffLen);
        if (sum + RECORD_SIZE > logSegmentSpace) {
            oors = true;
            // printf("out of reserved for segment %d\n", cid);
            break;
        }
        sum += RECORD_SIZE;
    }

    return oors;
}

bool ValueManager::outOfReservedSpaceForObject(offset_t flushFront, len_t objectSize)
{
    ConfigManager& cm = ConfigManager::getInstance();
    len_t mainSegmentSize = cm.getMainSegmentSize();
    segment_id_t numReserved = cm.getNumPipelinedBuffer() - 1;
    bool toMain = flushFront < mainSegmentSize;

    // whether the object fits into main segment
    if (toMain && flushFront + objectSize <= mainSegmentSize) {
        return false;
    }

    // for reserved segments, the max amount that can be write is (free groups * segment size)
    if (_segmentGroupManager->getNumFreeLogSegments() <= MIN_FREE_SEGMENTS + numReserved) {
        return true;
    }

    return false;
}

void ValueManager::flushCentralizedReservedPoolVLog(bool update_lsm, 
	int poolIndex)
{
    // directly write the pool out
    _centralizedReservedPool[poolIndex].lock.lock();

    Segment& pool = _centralizedReservedPool[poolIndex].pool;
    len_t writeLength = INVALID_LEN;
    offset_t logOffset = INVALID_OFFSET;
    std::tie(logOffset, writeLength) = flushSegmentToWriteFront(pool, false);

    // store the status of flushing the segment
    last_log_offset = logOffset;
    last_write_length = writeLength;
    last_pool_index = poolIndex;

    if (update_lsm) {
	updateLSMtreeInflushVLog(false);
    }
}

bool ValueManager::forceFlushBufferToVlog() {
    std::lock_guard<std::mutex> gcLock(_GCLock);
    flushCentralizedReservedPoolVLog(false);
    return true;
}

bool ValueManager::updateLSMtreeInflushVLog(bool flush) {
    int poolIndex = last_pool_index;
    Segment& pool = _centralizedReservedPool[poolIndex].pool;
    offset_t logOffset = last_log_offset;
    len_t writeLength = last_write_length;
    static int flushTimes = 0;

    // nonthing to flush
    if (writeLength == 0) {
        _centralizedReservedPool[poolIndex].lock.unlock();
        return true;
    }
    ConfigManager& cm = ConfigManager::getInstance();
    // update metadata in LSM
    std::vector<char*> keys;
    std::vector<ValueLocation> values;
    key_len_t keySize = 0;
    len_t vs = INVALID_LEN;
    ValueLocation valueLoc;
    valueLoc.segmentId = _isSlave ? cm.getNumSegment() : 0;
    len_t capacity = _isSlave ? cm.getColdStorageCapacity() : cm.getSystemEffectiveCapacity();
    for (auto c : _centralizedReservedPool[poolIndex].segmentsInPool) {
        for (auto kv : c.second.second) {
            off_len_t keyOffLen(kv, sizeof(key_len_t));
            Segment::readData(pool, &keySize, keyOffLen);
            off_len_t offLen(kv + KEY_REC_SIZE, sizeof(len_t));
            Segment::readData(pool, &vs, offLen);
            keys.push_back((char*)Segment::getData(pool) + kv);
            valueLoc.offset = (logOffset + kv) % capacity;
            valueLoc.length = vs;
            values.push_back(valueLoc);
            // printf("Flush update to key %x%x at offset %lu value %x of length %lu\n", kv.first[0], kv.first[KEY_SIZE-1], valueLoc.offset, kv.first[KEY_SIZE + sizeof(len_t)], vs);
        }
    }

    STAT_PROCESS(_keyManager->writeKeyBatch(keys, values), StatsType::UPDATE_KEY_WRITE_LSM);

    // different from HashKV implementation. We write meta before writing values, so write the previous logOffset.
    if (ConfigManager::getInstance().persistLogMeta()) {
	if (flush || 
		ConfigManager::getInstance().getUpdateKVBufferSize() > 0 ||
		flushTimes >= 512) {
            _keyManager->writeMeta(SegmentGroupManager::LogTailString, strlen(SegmentGroupManager::LogTailString), to_string(logOffset));
            flushTimes = 0;
        } else {
            flushTimes++;
        }
        //        _keyManager->writeMeta(SegmentGroupManager::LogTailString, strlen(SegmentGroupManager::LogTailString), to_string(logOffset + writeLength));
        //        _keyManager->writeMeta(SegmentGroupManager::LogValidByteString, strlen(SegmentGroupManager::LogValidByteString), to_string(_slave.validBytes));
        //        _keyManager->writeMeta(SegmentGroupManager::LogWrittenByteString, strlen(SegmentGroupManager::LogWrittenByteString), to_string(_slave.writtenBytes));
    }

    // clean up the pool
    _centralizedReservedPool[poolIndex].groupsInPool.clear();
    _centralizedReservedPool[poolIndex].segmentsInPool.clear();
    _centralizedReservedPool[poolIndex].keysInPool.clear();
    Segment::resetFronts(_centralizedReservedPool[poolIndex].pool);

    _centralizedReservedPool[poolIndex].lock.unlock();
    return true;
}

std::pair<offset_t, len_t> ValueManager::flushSegmentToWriteFront(Segment& segment, bool isGC)
{
    assert(_isSlave || ConfigManager::getInstance().enabledVLogMode());

    if (ConfigManager::getInstance().useDirectIO()) {
        Segment::padPage(segment);
    }

    if (ConfigManager::getInstance().getUpdateKVBufferSize() == 0) {
        debug_trace("Segment writeFront %lu flushFront %lu\n", Segment::getWriteFront(segment), Segment::getFlushFront(segment));
    } else {
        debug_info("Segment writeFront %lu flushFront %lu\n", Segment::getWriteFront(segment), Segment::getFlushFront(segment));
    }

    segment_len_t flushFront = Segment::getFlushFront(segment);
    len_t writeLength = Segment::getWriteFront(segment) - flushFront;

    if (writeLength == 0)
        return std::pair<offset_t, len_t>(INVALID_OFFSET, writeLength);

    offset_t logOffset = _segmentGroupManager->getAndIncrementVLogWriteOffset(writeLength, isGC);

    if (logOffset == INVALID_OFFSET) {
        assert(isGC == false);
        // printSlaveStats();
        _gcManager->gcVLog();
        logOffset = _segmentGroupManager->getAndIncrementVLogWriteOffset(writeLength);
    }

    if (logOffset == INVALID_OFFSET) {
        debug_error("Space not enough: LogHead %lu LogTail %lu\n", _segmentGroupManager->getLogGCOffset(), _segmentGroupManager->getLogWriteOffset());
        scanAllRecords();
        assert(logOffset != INVALID_OFFSET);
    }

    len_t ret = 0;
    if (ConfigManager::getInstance().segmentAsFile() && _isSlave) {
        STAT_PROCESS(ret = _deviceManager->writeDisk(ConfigManager::getInstance().segmentAsSeparateFile() ? ConfigManager::getInstance().getNumSegment() : 0, Segment::getData(segment) + flushFront, logOffset, writeLength), StatsType::POOL_FLUSH_NO_GC);
    } else {
        STAT_PROCESS(ret = _deviceManager->writeDisk(/* diskId = */ 0, Segment::getData(segment) + flushFront, logOffset, writeLength), StatsType::POOL_FLUSH_NO_GC);
    }

    if (ret != writeLength) {
        debug_error("Failed to write updates at offset %lu of length %lu\n", logOffset, writeLength);
        assert(0);
    }

    _logManager->setLogHeadTail(_segmentGroupManager->getLogGCOffset(), logOffset + writeLength);

    return std::pair<offset_t, len_t>(logOffset, writeLength);
}

int ValueManager::getNextPoolIndex(int current)
{
    return (current + 1) % ConfigManager::getInstance().getNumPipelinedBuffer();
}

void ValueManager::decrementPoolIndex(int& current)
{
    int numPipelinedBuffers = ConfigManager::getInstance().getNumPipelinedBuffer();
    current = (current + numPipelinedBuffers - 1) % numPipelinedBuffers;
}

offset_t ValueManager::getLastSegmentFront(offset_t flushFront)
{
    segment_len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
    segment_len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();
    if (flushFront <= mainSegmentSize) {
        return flushFront % mainSegmentSize;
    } else {
        offset_t front = (flushFront - mainSegmentSize) % logSegmentSize;
        if (front == 0) {
            return logSegmentSize;
        } else {
            return front;
        }
    }
    return 0;
}

// void ValueManager::logMetaPersist(std::set<segment_id_t> &modifiedSegments, std::set<group_id_t> &modifiedGroups) {
//     // segments
//     for (segment_id_t cid : modifiedSegments) {
//         _segmentGroupManager->writeSegmentMeta(cid);
//     }
//     // group metadata
//     for (group_id_t gid : modifiedGroups) {
//         _segmentGroupManager->writeGroupMeta(gid);
//     }
// }

void ValueManager::restoreVLog(std::map<std::string, externalIndexInfo>& keyValues)
{
    offset_t gcFront, flushFront, keySizeOffset;
    offset_t lsmGc = 0, lsmWrite = 0;
    key_len_t keySize;
    len_t valueSize, remains;
    len_t pageSize = sysconf(_SC_PAGE_SIZE);
    len_t capacity = ConfigManager::getInstance().getSystemEffectiveCapacity();
    len_t gcSize = ConfigManager::getInstance().getVLogGCSize();

    _logManager->getLogHeadTail(gcFront, flushFront);

    lsmGc = _segmentGroupManager->getLogGCOffset();
    lsmWrite = _segmentGroupManager->getLogWriteOffset();

    debug_info("In file: gcFront (%lu - %lu) flushFront (%lu - %lu)\n", gcFront, lsmGc, flushFront, lsmWrite);

    // TODO consider more situations?
    keyValues.clear();

    Segment seg, writePool;
    Segment::init(seg, INVALID_SEGMENT, gcSize);

    offset_t start = lsmWrite, end = flushFront;

    if (lsmWrite > flushFront) { // Wrapped
        end = capacity;
    }

    while (start != end) {
        len_t readSize = std::min(gcSize, end - start);
        _deviceManager->readAhead(/* diskId = */ 0, start, readSize);
        _deviceManager->readDisk(/* diskId = */ 0, Segment::getData(seg), start, readSize);

        for (remains = readSize; remains > 0;) {
            debug_trace("remains %lu readSize %lu start %lu end %lu\n", remains, readSize, start, end);
            valueSize = INVALID_LEN;
            keySizeOffset = readSize - remains;

            if (sizeof(key_len_t) > remains) {
                break;
            }
            off_len_t offLen(keySizeOffset, sizeof(key_len_t));
            Segment::readData(seg, &keySize, offLen);

            debug_trace("keySize %d\n", (int)keySize);

            if (keySize == 0) {
                if ((remains + (pageSize - start % pageSize)) % pageSize == 0) {
                    debug_error("remains aligned; this page has no content (remains %lu)\n", remains);
                    assert(0);
                    exit(-1);
                }

                if (remains % pageSize == 0) {
                    break;
                }
                len_t compactedBytes = std::min((remains % pageSize + (pageSize - start % pageSize)) % pageSize, remains);
                remains -= compactedBytes;
                continue;
            }

            if (KEY_REC_SIZE + sizeof(len_t) > remains) {
                break;
            }

            offLen.first = keySizeOffset + KEY_REC_SIZE;
            offLen.second = sizeof(len_t);
            Segment::readData(seg, &valueSize, offLen);

            if (KEY_REC_SIZE + sizeof(len_t) + valueSize > remains) {
                break;
            }

            std::string key(KEY_OFFSET((char*)Segment::getData(seg) + keySizeOffset), (int)keySize);
            externalIndexInfo indexInfo;
            indexInfo.externalFileID_ = (uint32_t)((start + keySizeOffset) / (1ull << 32));
            indexInfo.externalFileOffset_ = (uint32_t)((start + keySizeOffset) % (1ull << 32));
            indexInfo.externalContentSize_ = valueSize;

            keyValues[key] = indexInfo;

            debug_trace("read key %s info (%u %u %u)\n", key.c_str(), indexInfo.externalFileID_, indexInfo.externalFileOffset_, indexInfo.externalContentSize_);

            remains -= RECORD_SIZE;
        }

        _segmentGroupManager->getAndIncrementVLogWriteOffset(readSize);
        debug_info("one loop -- start %lu readSize %lu end %lu remains %lu\n", start, readSize, end, remains);

        if (start != end) {
            start += readSize;
            continue;
        }

        if (end != flushFront) { // Wrapped
            start = 0;
            end = flushFront;
        } else {
            break;
        }
    }

    debug_info("restore finished: number of key values = %lu\n", keyValues.size());
}

void ValueManager::scanAllRecords()
{
    offset_t gcFront = INVALID_OFFSET, flushFront;
    offset_t logTail, keySizeOffset;
    struct timeval tv1, tv2, res;
    key_len_t keySize;
    len_t gcSize = ConfigManager::getInstance().getVLogGCSize();
    len_t valueSize, remains, readSize = gcSize;
    len_t pageSize = sysconf(_SC_PAGE_SIZE);
    len_t capacity = ConfigManager::getInstance().getSystemEffectiveCapacity();

    gettimeofday(&tv1, 0);

    _logManager->getLogHeadTail(gcFront, logTail);

    debug_info("logHead %lu logTail %lu\n", gcFront, logTail);

    Segment seg;
    char* readPool;
    Segment::init(seg, INVALID_SEGMENT, gcSize);
    readPool = (char*)Segment::getData(seg);

    // For scanKeyValue()
    // len_t nextKeySizeOffset;
    char *key, *value;
    len_t compactedBytes;

    // Statistics
    len_t keyNum = 0, keySizes = 0, valueSizes = 0;
    len_t scanBytes = 0;

    offset_t start = gcFront, end = logTail;

    //    if (gcFront > logTail) { // Wrapped
    //        end = capacity;
    //    }

    while (start != end) {
        flushFront = Segment::getFlushFront(seg);
        assert(flushFront != readSize);

        readSize = std::min(gcSize, (end + capacity - start) % capacity);
        _deviceManager->readAhead(/* diskId = */ 0, start + flushFront, readSize - flushFront);
        _deviceManager->readDisk(/* diskId = */ 0, (unsigned char*)readPool + flushFront, start + flushFront, readSize - flushFront);

        for (remains = readSize; remains > 0;) {
            debug_trace("remains %lu readSize %lu flushFront %lu start %lu end %lu\n", remains, readSize, flushFront, start, end);
            keySizeOffset = readSize - remains;

            // change key, keySize, value, valueSize, remains, compactedBytes
            bool ret = scanKeyValue((char*)readPool, start, readSize, keySizeOffset, key, keySize, value, valueSize, remains, compactedBytes, pageSize);

            if (!ret) {
                break;
            }

            off_len_t offLen(keySizeOffset, sizeof(key_len_t));
            Segment::readData(seg, &keySize, offLen);

            ValueLocation valueLoc = _keyManager->getKey(key);

            if (valueLoc.offset == (start + keySizeOffset + capacity) % capacity) {
                // matched, a valid KV pair.
                len_t mod = valueLoc.offset % pageSize;
                debug_trace("[KV] %s %s%s%s%s%s%s%s%s [%.*s] [%.*s] offset %lu\n",
                    mod == 0 ? "*" : ".",
                    mod > 0 && mod <= pageSize / 8 * 1 ? "*" : ".",
                    mod > pageSize / 8 * 1 && mod <= pageSize / 8 * 2 ? "*" : ".",
                    mod > pageSize / 8 * 2 && mod <= pageSize / 8 * 3 ? "*" : ".",
                    mod > pageSize / 8 * 3 && mod <= pageSize / 8 * 4 ? "*" : ".",
                    mod > pageSize / 8 * 4 && mod <= pageSize / 8 * 5 ? "*" : ".",
                    mod > pageSize / 8 * 5 && mod <= pageSize / 8 * 6 ? "*" : ".",
                    mod > pageSize / 8 * 6 && mod <= pageSize / 8 * 7 ? "*" : ".",
                    mod > pageSize / 8 * 7 && mod <= pageSize / 8 * 8 ? "*" : ".",
                    std::min((int)keySize, 16), KEY_OFFSET(key),
                    std::min((int)valueSize, 16), value,
                    valueLoc.offset);
                keyNum++;
                keySizes += keySize;
                valueSizes += valueSize;
            }
        }

        if (remains > 0) {
            Segment::setFlushFront(seg, remains);
            memmove(readPool, readPool + readSize - remains, remains);
        } else {
            Segment::resetFronts(seg);
        }

        debug_info("one loop -- start %lu readSize %lu end %lu remains %lu keyNum %lu keySizes %lu valueSizes %lu\n", start, readSize, end, remains, keyNum, keySizes, valueSizes);

        start = (start + readSize - remains) % capacity;
        scanBytes += readSize - remains;

        if (scanBytes > capacity) {
            debug_error("log corruptted: scanBytes %lu capacity %lu\n", scanBytes, capacity);
            assert(0);
            exit(-1);
        }

        //        if (start != end) {
        //            start += readSize - remains;
        //            if (start != end) {
        //                continue;
        //            }
        //        }
        //
        //        if (end != logTail) { // Wrapped
        //            start = 0;
        //            end = logTail;
        //        } else {
        //            break;
        //        }
    }

    len_t total = keySizes + valueSizes + keyNum * (sizeof(key_len_t) + sizeof(len_t));
    debug_info("keyNum %lu keySizes %lu valueSizes %lu, totally %llu GiB %llu MiB %llu KiB %llu B\n", keyNum, keySizes, valueSizes,
        total / (1ull << 30),
        total / (1ull << 20) % (1ull << 10),
        total / (1ull << 10) % (1ull << 10),
        total % (1ull << 10));

    gettimeofday(&tv2, 0);
    timersub(&tv2, &tv1, &res);
    debug_info("scan time: %.6lf s\n", (res.tv_sec * S2US + res.tv_usec) / 1000000.0);
}

// void ValueManager::restoreFromUpdateLog() {
//     std::vector<std::string> keys;
//     std::vector<ValueLocation> values;
//     std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t> > > groups;
//     std::map<segment_id_t, offset_t> segments;
//     // update log
//     if (_logManager->readBatchUpdateKeyValue(keys, values, groups)) {
//         // update kv locations
//         _keyManager->writeKeyBatch(keys, values);
//         // update group metadata
//         for (auto g : groups) {
//             //printf("Log group %lu front b4 %lu aft %lu\n", g.first, _segmentGroupManager->getGroupWriteFront(g.first, /* needsLock = */ false), g.second.first);
//             // in-memory
//             _segmentGroupManager->setGroupWriteFront(g.first, g.second.first, /* needsLock = */ false);
//             _segmentGroupManager->setGroupFlushFront(g.first, g.second.first, /* needsLock = */ false);
//             _segmentGroupManager->setGroupSegments(g.first, g.second.second);
//             // LSM-tree
//             _segmentGroupManager->writeGroupMeta(g.first);
//         }
//         // update segment metadata
//         for (size_t i = 0; i < keys.size(); i++) {
//             segment_id_t cid = values.at(i).segmentId;
//             if (cid == LSM_SEGMENT) {
//                 assert(0);
//                 continue;
//             }
//             offset_t writeFront = values.at(i).offset + values.at(i).length + KEY_SIZE;
//             if (segments.count(cid) == 0 || segments.at(cid) < writeFront) {
//                 segments[cid] = writeFront;
//             }
//         }
//         for (auto c : segments) {
//             //printf("Log segment %lu front %lu\n", c.first, c.second);
//             _segmentGroupManager->setSegmentFlushFront(c.first, c.second);
//             _segmentGroupManager->writeSegmentMeta(c.first);
//         }
//         // remove the log file
//         _logManager->ackBatchUpdateKeyValue();
//     }
// }
//
// void ValueManager::restoreFromGCLog() {
//     std::vector<std::string> keys;
//     std::vector<ValueLocation> values;
//     std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t> > > groups;
//     std::map<segment_id_t, Segment> segments;
//
//     if (_logManager->readBatchGCKeyValue(keys, values, groups)) {
//         ConfigManager &cm = ConfigManager::getInstance();
//         segment_len_t numMainSegments = cm.getNumMainSegment();
//         len_t mainSegmentSize = cm.getMainSegmentSize();
//         len_t logSegmentSize = cm.getLogSegmentSize();
//
//         std::map<segment_id_t, Segment> segmentBuf;
//         // make sure all values are there, and create segment buffers
//         for (size_t i = 0; i < keys.size(); i++) {
//             len_t valueSize = values.at(i).length;
//             // read the yet overwritten value from segment
//             if (values.at(i).value.empty()) {
//                 ValueLocation oldValueLoc = _keyManager->getKey(keys.at(i).c_str(), KEY_SIZE);
//                 char *value = 0;
//                 len_t valueSize = 0;
//                 getValueFromDisk(keys.at(i).c_str(), oldValueLoc, value, valueSize);
//                 assert(valueSize + sizeof(len_t) == values.at(i).length);
//                 values.at(i).value = std::string(value, valueSize);
//                 delete value;
//             } else {
//                 valueSize = values.at(i).length;
//                 if (values.at(i).length > 0) {
//                     valueSize -= sizeof(len_t);
//                 }
//             }
//             segment_id_t cid = values.at(i).segmentId;
//             if (segmentBuf.count(cid) == 0) {
//                 Segment::init(segmentBuf[cid], cid, cid >= numMainSegments? logSegmentSize : mainSegmentSize, /* needsSetZero = */ false);
//             }
//             segment_off_len_t offLen = {values.at(i).offset, KEY_SIZE};
//             Segment::overwriteData(segmentBuf[cid], keys.at(i).c_str(), offLen);
//             offLen.first += KEY_SIZE;
//             offLen.second = sizeof(len_t);
//             Segment::overwriteData(segmentBuf[cid], &valueSize, offLen);
//             offLen.first += sizeof(len_t);
//             offLen.second = valueSize;
//             Segment::overwriteData(segmentBuf[cid], values.at(i).value.c_str(), offLen);
//             if (offLen.first + valueSize > Segment::getWriteFront(segmentBuf.at(cid))) {
//                 Segment::setWriteFront(segmentBuf.at(cid), offLen.first + valueSize);
//                 Segment::setFlushFront(segmentBuf.at(cid), offLen.first + valueSize);
//             }
//         }
//         // write the segments
//         std::atomic<int> waitIO;
//         waitIO = 0;
//         offset_t writeOffset = 0;
//         for (auto segment : segments) {
//             waitIO += 1;
//             _iothreads.schedule(
//                     std::bind(
//                         &DeviceManager::writePartialSegmentMt,
//                         _deviceManager,
//                         Segment::getId(segment.second),
//                         Segment::getFlushFront(segment.second),
//                         Segment::getWriteFront(segment.second) - Segment::getFlushFront(segment.second),
//                         Segment::getData(segment.second) + Segment::getFlushFront(segment.second),
//                         boost::ref(writeOffset),
//                         boost::ref(waitIO)
//                     )
//             );
//         }
//         while (waitIO > 0);
//         // update the segment metadata
//         for (auto segment : segments) {
//             _segmentGroupManager->setSegmentFlushFront(segment.first, Segment::getWriteFront(segment.second));
//             _segmentGroupManager->writeSegmentMeta(segment.first);
//         }
//         // update the group metadata
//         for (auto g : groups) {
//             // in-memory
//             _segmentGroupManager->setGroupWriteFront(g.first, g.second.first, /* needsLock = */ false);
//             _segmentGroupManager->setGroupFlushFront(g.first, g.second.first, /* needsLock = */ false);
//             _segmentGroupManager->setGroupSegments(g.first, g.second.second);
//             // LSM-tree
//             _segmentGroupManager->writeGroupMeta(g.first);
//         }
//         // remove gc log file
//         _logManager->ackBatchGCKeyValue();
//     }
// }

void ValueManager::flushCentralizedReservedPoolBg(StatsType stats)
{
    int nextPoolIndex = getNextPoolIndex(_centralizedReservedPoolIndex.inUsed);
    // wait until next available buffer is available after flush
    // (assume multiple core is available)
    // the buffer into queue for flush

    struct timeval tv;
    gettimeofday(&tv, 0);

    while (nextPoolIndex == _centralizedReservedPoolIndex.flushNext) {
        pthread_cond_wait(&_centralizedReservedPoolIndex.flushedBuffer, &_centralizedReservedPoolIndex.queueLock);
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::POOL_FLUSH_WAIT, tv);

    debug_info("Put pool %d into queue\n", _centralizedReservedPoolIndex.inUsed);
    _centralizedReservedPoolIndex.queue.push(std::pair<int, StatsType>(_centralizedReservedPoolIndex.inUsed, stats));
    pthread_mutex_unlock(&_centralizedReservedPoolIndex.queueLock);
    // increment the index of pool to use
    _centralizedReservedPoolIndex.inUsed = nextPoolIndex;
    debug_info("Going to use pool %d\n", _centralizedReservedPoolIndex.inUsed);
    // signal the worker to wake and process buffers in queue
    pthread_cond_signal(&_needBgFlush);
}

void ValueManager::flushCentralizedReservedPoolBgWorker()
{
    //    ValueManager *instance = (ValueManager *) arg;
    // loop until valueManager is destoryed
    // fprintf(stderr, "Bg flush thread starts now, hello\n");
    while (_started) {
        // wait for signal after data is put into queue
        pthread_cond_wait(&_needBgFlush, &_centralizedReservedPoolIndex.queueLock);
        // lock before queue checking
        while (!_centralizedReservedPoolIndex.queue.empty()) {
            int poolIndex;
            StatsType stats;
            tie(poolIndex, stats) = _centralizedReservedPoolIndex.queue.front();
            _centralizedReservedPoolIndex.queue.pop();
            // unlock to allow producer to push items in while processing the current one
            pthread_mutex_unlock(&_centralizedReservedPoolIndex.queueLock);
            // do the flushing as usual
            debug_info("Pull and flush pool %d from queue\n", poolIndex);
            STAT_PROCESS(flushCentralizedReservedPoolVLog(true, poolIndex), stats);
            _centralizedReservedPoolIndex.flushNext = getNextPoolIndex(_centralizedReservedPoolIndex.flushNext);
            debug_info("Complete processing pool %d next %d \n", poolIndex, _centralizedReservedPoolIndex.flushNext);
            // lock before queue checking
            pthread_cond_signal(&_centralizedReservedPoolIndex.flushedBuffer);
            pthread_mutex_lock(&_centralizedReservedPoolIndex.queueLock);
        };
        // leave the mutex to pthread_cond_wait to unlock
    }
    debug_info("Bg flush thread exits now, bye%s\n", "");
}

bool ValueManager::forceSync()
{
    std::lock_guard<std::mutex> gcLock(_GCLock);
    flushCentralizedReservedPoolVLog(true);
    return true;
}

void ValueManager::printSlaveStats(FILE* out)
{
    if (_isSlave) {
        fprintf(out,
            "(This) Slave capacity: %lu; In-use: %lu; Valid: %lu\n", ConfigManager::getInstance().getColdStorageCapacity(), _slave.writtenBytes, _slave.validBytes);
        _gcManager->printStats(out);
    } else if (ConfigManager::getInstance().useSlave() && _slaveValueManager && _slave.gcm) {
        fprintf(out,
            "Slave capacity: %lu; In-use: %lu; Valid: %lu\n", ConfigManager::getInstance().getColdStorageCapacity(), _slaveValueManager->_slave.writtenBytes, _slaveValueManager->_slave.validBytes);
        _slave.gcm->printStats(out);
    }
}

}

#undef RECORD_SIZE
