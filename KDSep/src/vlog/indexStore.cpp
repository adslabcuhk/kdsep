#include "vlog/indexStore.hh"
#include "vlog/rocksdbKeyManager.hh"
#include "utils/debug.hpp"
#include "utils/statsRecorder.hh"
#include "utils/timer.hpp"
#include <stdlib.h>

namespace KDSEP_NAMESPACE {

KvServer::KvServer()
    : KvServer(0, 0)
{
    debug_error("No device and no LSM-tree%s\n", "");
}

KvServer::KvServer(DeviceManager* deviceManager, rocksdb::DB* pointerToRawRocksDBForGC)
{
    // devices
    if (deviceManager) {
        _deviceManager = deviceManager;
    } else {
        _deviceManager = new DeviceManager();
    }
    _freeDeviceManager = (deviceManager == 0);
    // metadata log
    _logManager = new LogManager(deviceManager);
    // keys and values
    _keyManager = new RocksDBKeyManager(pointerToRawRocksDBForGC);
    // segments and groups
    _segmentGroupManager = new SegmentGroupManager(/* isSlave = */ false, _keyManager);
    // values
    _valueManager = new ValueManager(_deviceManager, _segmentGroupManager, _keyManager, _logManager);
    // gc
    _gcManager = new GCManager(_keyManager, _valueManager, _deviceManager, _segmentGroupManager);

    _valueManager->setGCManager(_gcManager);

    _cache.lru = nullptr;
    if (ConfigManager::getInstance().valueCacheSize() > 0) {
        _cache.lru = new LruList(ConfigManager::getInstance().valueCacheSize());
    }

    // Threads for scans
    boost::thread::attributes attrs;
    attrs.set_stack_size(1000 * 1024 * 1024);

//    _scanthreads.size_controller().resize(ConfigManager::getInstance().getNumRangeScanThread());
    _scanthreads = new boost::asio::thread_pool(ConfigManager::getInstance().getNumRangeScanThread());
}

KvServer::~KvServer()
{
    delete _valueManager;
    delete _keyManager;
    delete _gcManager;
    delete _segmentGroupManager;
    delete _logManager;
    if (_cache.lru)
        delete _cache.lru;
    if (_freeDeviceManager)
        delete _deviceManager;
}

void KvServer::scanWorker() {
    struct timeval tve, empty_time;
    bool empty_started = false;
    while (true) {
        gettimeofday(&tve, 0);
        if (notifyScanMQ_->done == true && notifyScanMQ_->isEmpty() == true) {
            break;
        }

        getValueStruct* st;

        if (notifyScanMQ_->pop(st)) {
            empty_started = false;
            uint8_t ret;

            if (fake_scan_) {
                (*st->keysInProcess)--;
            } else {
                getValueMt(st->ckey, st->keySize, st->value, st->valueSize, 
                        st->storageInfo, ret, (*st->keysInProcess));
            }

        } else {
            if (empty_started == false) {
                gettimeofday(&empty_time, 0);
                empty_started = true;
            } else {
                if (tve.tv_sec - empty_time.tv_sec > 3) {
                    std::unique_lock<std::mutex> lk(scan_mtx_);
                    scan_cv_.wait(lk);
                    gettimeofday(&empty_time, 0);
                }
            }
        }
    }
}

bool KvServer::checkKeySize(len_t& keySize)
{
    if (keySize == 0)
        debug_error("Zero key size is not supported, key size must be larger than %d.\n", 0);
    return (keySize != 0);
}

bool KvServer::putValue(const char* key, len_t keySize, const char* value,
	len_t valueSize, bool sync)
{
    // static int putCount = 0;
    // putCount++;
    bool ret = false;
    ValueLocation oldValueLoc, curValueLoc;
    char* ckey = new char[KEY_REC_SIZE + 1];
    char* cvalue = new char[valueSize + 1];

    memcpy(ckey, &keySize, sizeof(key_len_t));
    memcpy(ckey + sizeof(key_len_t), key, keySize);
    ckey[sizeof(key_len_t) + keySize] = '\0';

    memcpy(cvalue, value, valueSize);
    cvalue[valueSize] = '\0';

    debug_trace("PUT key \"%.*s\" value \"%.*s\"\n", (int)keySize, ckey + sizeof(key_len_t), std::min((int)valueSize, 16), cvalue);

    if (_cache.lru && _cache.lru->get((unsigned char*)ckey).size() > 0) {
        debug_trace("update cache key %.*s\n", std::min(16, (int)keySize), key);
        _cache.lru->update((unsigned char*)ckey, (unsigned char*)cvalue, valueSize);
    }

    oldValueLoc.value.clear();
    // only support fixed key size
    if (checkKeySize(keySize) == false)
        return ret;
    if (valueSize >= ConfigManager::getInstance().getMainSegmentSize()) {
        debug_error("Value size larger than segment size is not supported (at most %lu).\n", ConfigManager::getInstance().getMainSegmentSize());
        return ret;
    }

    int retry = 0;
    // identify values writing directly to LSM-tree (1) smaller than threshold, or (2) key-value separation is disabled
    bool toLSM = valueSize < ConfigManager::getInstance().getMinValueSizeToLog() || ConfigManager::getInstance().disableKvSeparation();

retry_update:
    struct timeval keyLookupStartTime;
    gettimeofday(&keyLookupStartTime, 0);
    if (ConfigManager::getInstance().enabledVLogMode()) {
        // vlog virtually put all writes to one group
        oldValueLoc.segmentId = 0;
    } else if (toLSM) {
        // write to LSM-tree directly
        oldValueLoc.segmentId = LSM_SEGMENT;
    } else {
        // find the deterministic location
        oldValueLoc.segmentId = HashFunc::hash(KEY_OFFSET(ckey), (key_len_t)keySize) % ConfigManager::getInstance().getNumMainSegment();
        // always allocate the group if not exists
        group_id_t groupId = INVALID_GROUP;
        _segmentGroupManager->getNewMainSegment(groupId, oldValueLoc.segmentId, /* needsLock */ false);
    }
    bool inLSM = oldValueLoc.segmentId == LSM_SEGMENT;
    StatsRecorder::getInstance()->timeProcess(StatsType::UPDATE_KEY_LOOKUP, keyLookupStartTime);
    // update the value of the key, get the new location of value
    STAT_PROCESS(curValueLoc = _valueManager->putValue(ckey, keySize, cvalue,
		valueSize, oldValueLoc, sync), StatsType::UPDATE_VALUE);

    debug_trace("Update key [%d-%x%x] to segment id=%lu,ofs=%lu,len=%lu\n", (int)ckey[0], ckey[sizeof(key_len_t) + 1], ckey[sizeof(key_len_t) + keySize - 1], curValueLoc.segmentId, curValueLoc.offset, curValueLoc.length);
    // retry for UPDATE if failed (due to GC)
    if (!inLSM && curValueLoc.segmentId == INVALID_SEGMENT) {
        // best effort retry
        if (retry++ <= ConfigManager::getInstance().getRetryMax()) {
            debug_warn("Retry for update %d time(s)\n", retry);
            goto retry_update;
        }
        // report set failure
        debug_error("Failed to write value for key %x%x!\n", ckey[0], ckey[sizeof(key_len_t) + keySize - 1]);
        assert(0);
        return ret;
    } else {
        ret = (curValueLoc.length == valueSize + (curValueLoc.segmentId == LSM_SEGMENT ? 0 : sizeof(len_t)));
    }
    debug_trace("putValue curValueLoc offset %lu curValueLoc length %lu\n", curValueLoc.offset, curValueLoc.length);
    delete[] ckey;
    delete[] cvalue;

    //    if (putCount > 1000) {
    //        gc(false);
    //        putCount = 0;
    //    }

    return ret;
}

void KvServer::getValueMt(char *ckey, len_t keySize, char *&value, len_t
        &valueSize, externalIndexInfo storageInfo, uint8_t &ret,
        std::atomic<size_t> &keysInProcess) {
    if (checkKeySize(keySize) == false) {
        ret = 0;
        return;
    }

    struct timeval startTime;
    gettimeofday(&startTime, 0);

//    debug_error("value %p valueSize %lu index %u %u %u\n", 
//            value, valueSize, 
//            storageInfo.externalFileOffset_,
//            storageInfo.externalFileID_,
//            storageInfo.externalContentSize_);

    // get value using the location
    ret = (_valueManager->getValueFromBuffer(ckey, keySize, value, valueSize));

    ValueLocation readValueLoc;
    readValueLoc.segmentId = 0;
    readValueLoc.offset = storageInfo.externalFileOffset_ +
        ((uint64_t)storageInfo.externalFileID_ << 32);
    readValueLoc.length = storageInfo.externalContentSize_;

    // search on disk
//    if (!ret && !ConfigManager::getInstance().disableKvSeparation() && valueLoc.segmentId != INVALID_SEGMENT) 
    if (!ret && !ConfigManager::getInstance().disableKvSeparation())
    {
        ret = _valueManager->getValueFromDisk(ckey, keySize, readValueLoc, value, valueSize);
    }

    delete[] ckey;
    keysInProcess--;
}

bool KvServer::getValue(const char* key, len_t keySize, char*& value, 
        len_t& valueSize, externalIndexInfo storageInfoVec, bool timed)
{
    bool ret = false;
    char* ckey = new char[KEY_REC_SIZE + 1];

    memcpy(ckey, &keySize, sizeof(key_len_t));
    memcpy(KEY_OFFSET(ckey), key, keySize);
    ckey[sizeof(key_len_t) + keySize] = '\0';

    if (checkKeySize(keySize) == false)
        return ret;

    struct timeval startTime;
    gettimeofday(&startTime, 0);

    // get value using the location
    ret = (_valueManager->getValueFromBuffer(ckey, keySize, value, valueSize));

    if (ret) {
        delete[] ckey;
        StatsRecorder::getInstance()->timeProcess(StatsType::GET_VALUE, startTime);
        return ret;
    }

    // get the value's location
    //    STAT_PROCESS(readValueLoc = _keyManager->getKey(key), StatsType::GET_KEY_LOOKUP);

    // found for selective key-value separation or disabled key-value separation
    //    bool disableKvSep = ConfigManager::getInstance().disableKvSeparation();
    //    if ((readValueLoc.segmentId == LSM_SEGMENT || disableKvSep /* no segment id */) && readValueLoc.length != INVALID_LEN) {
    //        // key-value pairs found entirely in LSM
    //        value = new char [readValueLoc.length];
    //        valueSize = readValueLoc.length;
    //        readValueLoc.value.copy(value, valueSize);
    //        if (timed) StatsRecorder::getInstance()->timeProcess(StatsType::GET_VALUE, startTime);
    //        return true;
    //    }
    //
    //    // not found
    //    if (readValueLoc.segmentId == INVALID_SEGMENT) return false;

    // get value using the LRU cache
    std::string valueStr;
    if (_cache.lru) {
        STAT_PROCESS(valueStr = _cache.lru->get((unsigned char*)ckey), StatsType::KEY_GET_CACHE);
        if (valueStr.size() > 0) {
            debug_trace("get from cache key %.*s\n", std::min(16, (int)keySize), key);
            valueSize = valueStr.size();
            value = (char*)buf_malloc(valueSize);
            memcpy(value, valueStr.c_str(), valueSize);
            delete[] ckey;
            return ret;
        }
    }

    ValueLocation readValueLoc;
    readValueLoc.segmentId = 0;
    readValueLoc.offset = storageInfoVec.externalFileOffset_ + ((uint64_t)storageInfoVec.externalFileID_ << 32);
    readValueLoc.length = storageInfoVec.externalContentSize_;

    ret = _valueManager->getValueFromDisk(ckey, keySize, readValueLoc, value, valueSize);
    if (timed)
        StatsRecorder::getInstance()->timeProcess(StatsType::GET_VALUE, startTime);

    if (ret && _cache.lru) {
        STAT_PROCESS(_cache.lru->insert((unsigned char*)ckey, (unsigned char*)value, valueSize), StatsType::KEY_UPDATE_CACHE);
        debug_trace("insert to cache key %.*s value %.*s\n", std::min(16, (int)keySize), key, std::min(16, (int)valueSize), value);
    }

    delete[] ckey;
    return ret;
}

void KvServer::getRangeValuesDecoupled(
        const std::vector<string> &keys, int numKeys, 
        const std::vector<externalIndexInfo>& locs, 
        std::vector<string> &values) {
    struct timeval startTime;
    gettimeofday(&startTime, 0);

    std::vector<uint8_t> rets;
    std::vector<char*> valueChars;
    std::vector<len_t> valueSize;
    std::vector<getValueStruct*> sts;

    values.resize(numKeys);
    rets.resize(numKeys);
    valueChars.resize(numKeys);
    valueSize.resize(numKeys);
    sts.resize(numKeys);

    // keep track of the number of keys to process
    std::atomic<size_t> keysInProcess;
    keysInProcess = 0;

    bool useMultiThreading = ConfigManager::getInstance().getNumRangeScanThread() > 1;

//    bool disableKvSep = ConfigManager::getInstance().disableKvSeparation();
//    KeyManager::KeyIterator *kit = _keyManager->getKeyIterator(startingKey);
//    char *key = 0;

    for (uint32_t i = 0; i < numKeys; i++) {
        // get the key
        key_len_t keySize = keys.at(i).size();
        char* ckey = new char [KEY_REC_SIZE + 1];
        memcpy(ckey, &keySize, sizeof(key_len_t)); 
        memcpy(KEY_OFFSET(ckey), keys.at(i).c_str(), keySize);
        ckey[sizeof(key_len_t) + keySize] = '\0';

        keysInProcess += 1;

        externalIndexInfo loc = locs[i];
        ValueLocation readValueLoc;
        readValueLoc.segmentId = 0;
        readValueLoc.offset = loc.externalFileOffset_ + ((uint64_t)loc.externalFileID_ << 32);
        readValueLoc.length = loc.externalContentSize_;

//        while (keysInProcess >= 8) //(ConfigManager::getInstance().getNumRangeScanThread() * 2) 
//        {
//            asm volatile("");
//        }

        if (ConfigManager::getInstance().enabledScanReadAhead()) {
            _deviceManager->readAhead(readValueLoc.segmentId,
                    readValueLoc.offset, 
                    readValueLoc.length + KEY_REC_SIZE + sizeof(len_t));
        }

        // search into buffer and disk in parallel
        // TODO to multithreading

        if (useMultiThreading == false) {
            KvServer::getValueMt(ckey, keySize, valueChars.at(i),
                    valueSize.at(i), loc, rets.at(i), keysInProcess);
        } else {
            getValueStruct* st = new getValueStruct(ckey, keySize, loc, &keysInProcess);
            sts[i] = st;
//            notifyScanMQ_->push(st);
//            scan_cv_.notify_all();

            boost::asio::post(*_scanthreads, 
                    boost::bind(
                        &KvServer::getValueMt,
                        this,
                        ckey, 
                        keySize, 
                        boost::ref(st->value), 
                        boost::ref(st->valueSize),
                        st->storageInfo, 
                        boost::ref(rets.at(i)),
                        boost::ref(keysInProcess)
                        ));
            // threadpool implementation
//            _scanthreads.schedule(
//                    std::bind(
//                        &KvServer::getValueMt,
//                        this,
//                        ckey, // keys.at(i),
//                        keySize, // KEY_SIZE,
//                        boost::ref(st->value), // boost::ref(values.at(i)),
//                        boost::ref(st->valueSize), // boost::ref(valueSize.at(i)),
//                        st->storageInfo, // locs.at(i),
//                        boost::ref(rets.at(i)),
//                        boost::ref(keysInProcess)
//                        )
//                    );
        }
    }

//    kit->release();
//    delete kit;

    while (keysInProcess > 0);

    if (useMultiThreading == false) {
        for (int i = 0; i < numKeys; i++) {
            values[i] = string(valueChars.at(i), valueSize.at(i));
            free(valueChars.at(i));
        }
    } else {
        for (int i = 0; i < numKeys; i++) {
            if (fake_scan_) {
                values[i] = string("abc"); 
                delete sts.at(i);
            } else {
                values[i] = string(sts.at(i)->value, sts.at(i)->valueSize);
                free(sts.at(i)->value);
                delete sts.at(i);
            }
        }
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::GET_VALUE, startTime);
}

// bool KvServer::delValue(char *key, len_t keySize) {
//     int retry = 0;
//     ValueLocation valueLoc, retValueLoc;
//     if (checkKeySize(keySize) == false)
//         return false;
//     // get the value's location
//     valueLoc = _keyManager->getKey(key);
//     if (valueLoc.segmentId == INVALID_SEGMENT) {
//         debug_warn("Value for key %x%x not found.\n", key[0], key[KEY_SIZE-1]);
//         return false;
//     }
//     while (retValueLoc.segmentId == INVALID_SEGMENT) {
//         // best effort retry
//         if (retry++ > ConfigManager::getInstance().getRetryMax())
//             break;
//         retValueLoc = _valueManager->putValue(key, keySize, 0, INVALID_LEN, valueLoc, 1);
//     }
//     // always delete the key first ..
//     _keyManager->deleteKey(key);
//     return (retValueLoc.segmentId != INVALID_SEGMENT);
// }

bool KvServer::restoreVLog(std::map<std::string, externalIndexInfo>& keyValues)
{
    _valueManager->restoreVLog(keyValues);
    return true;
}

bool KvServer::flushBufferToVlog() {
    return _valueManager->forceFlushBufferToVlog();
}

// divide the flush buffer to two steps
bool KvServer::updateLSMtreeInflushVLog() {
    return _valueManager->updateLSMtreeInflushVLog(true);
}

bool KvServer::flushBuffer()
{
    return _valueManager->forceSync();
}

size_t KvServer::gc(bool all)
{
    //    if (ConfigManager::getInstance().enabledVLogMode()) {
    return _gcManager->gcVLog();
    //    }
    //    return (all ? _gcManager->gcAll() : _gcManager->gcGreedy());
}

void KvServer::printStorageUsage(FILE* out)
{
    _segmentGroupManager->printUsage(out);
}

void KvServer::printGroups(FILE* out)
{
    _segmentGroupManager->printGroups(out);
}

void KvServer::printBufferUsage(FILE* out)
{
    //    _valueManager->printUsage(out);
}

void KvServer::printKeyCacheUsage(FILE* out)
{
    _keyManager->printCacheUsage(out);
}

void KvServer::printKeyStats(FILE* out)
{
    _keyManager->printStats(out);
}

void KvServer::printValueSlaveStats(FILE* out)
{
    _valueManager->printSlaveStats(out);
}

void KvServer::printGCStats(FILE* out)
{
    _gcManager->printStats(out);
}

}
