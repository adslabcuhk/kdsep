#include "vlog/indexStoreInterface.hpp"

namespace KDSEP_NAMESPACE {

IndexStoreInterface::IndexStoreInterface(KDSepOptions* options, string workingDir, rocksdb::DB* pointerToRawRocksDB)
{

    internalOptionsPtr_ = options;
    workingDir_ = workingDir;
    pointerToRawRocksDBForGC_ = pointerToRawRocksDB;
    extractValueSizeThreshold_ = options->extract_to_valueStore_size_lower_bound;

    DiskInfo disk1(0, workingDir.c_str(), 1 * 1024 * 1024 * 1024);
    std::vector<DiskInfo> disks;
    disks.push_back(disk1);
    devices_ = new DeviceManager(disks);

    kvServer_ = new KvServer(devices_, pointerToRawRocksDBForGC_);
}

IndexStoreInterface::~IndexStoreInterface()
{
    cerr << "Delete kv server ..." << endl;
    delete kvServer_;
    cerr << "Delete devices ..." << endl;
    delete devices_;
    cerr << "Delete IndexStoreInterface complete ..." << endl;
}

uint64_t IndexStoreInterface::getExtractSizeThreshold()
{
    return extractValueSizeThreshold_;
}

bool IndexStoreInterface::put(mempoolHandler_t pool_obj, bool sync)
{
//    if (string(pool_obj.keyPtr_, pool_obj.keySize_) ==
//            "user6840842610087607159") {
//        debug_error("put key [%.*s]\n",
//                (int)pool_obj.keySize_, pool_obj.keyPtr_);
//    }

    char buffer[sizeof(uint32_t) + pool_obj.valueSize_];
    memcpy(buffer, &pool_obj.seq_num, sizeof(uint32_t));
    memcpy(buffer + sizeof(uint32_t), pool_obj.valuePtr_, pool_obj.valueSize_);
    STAT_PROCESS(kvServer_->putValue(pool_obj.keyPtr_, pool_obj.keySize_,
                buffer, pool_obj.valueSize_ + sizeof(uint32_t), sync),
            StatsType::UPDATE);
    return true;
}

bool IndexStoreInterface::multiPut(
	vector<mempoolHandler_t> objectPairMemPoolHandlerVec,
	bool update_lsm)
{
    for (auto i = 0; i < objectPairMemPoolHandlerVec.size(); i++) {
        put(objectPairMemPoolHandlerVec[i], false);
    }
    if (update_lsm) {
	kvServer_->flushBuffer();
    } else {
	kvServer_->flushBufferToVlog();
    }
    return true;
}

bool IndexStoreInterface::multiPutPostUpdate() {
    return kvServer_->updateLSMtreeInflushVLog();
}

bool IndexStoreInterface::get(const string keyStr, externalIndexInfo storageInfo, string* valueStrPtr, uint32_t* seqNumberPtr)
{

    char* key = new char[keyStr.length() + 2];
    char* value = nullptr;
    len_t valueSize = 0;

    strcpy(key, keyStr.c_str());

    debug_trace("get key [%.*s] offset %x%x valueSize %d\n", (int)keyStr.length(), keyStr.c_str(), storageInfo.externalFileID_, storageInfo.externalFileOffset_, storageInfo.externalContentSize_);

    uint64_t tmpAddr = ((uint64_t)(storageInfo.externalFileID_) << 32) + storageInfo.externalFileOffset_; 

    if (tmpAddr > 200ull * 1024 * 1024 * 1024) {
        debug_error("addr too large: %lu\n", tmpAddr);
    }

//    if (keyStr == "user6840842610087607159") {
//        debug_error("get key [%.*s] offset %x%x %lu valueSize %d\n",
//                (int)keyStr.length(), keyStr.c_str(),
//                storageInfo.externalFileID_, storageInfo.externalFileOffset_,
//                tmpAddr,
//                storageInfo.externalContentSize_);
//    }

    bool ret;
    STAT_PROCESS(ret = kvServer_->getValue(key, keyStr.length(), value, valueSize, storageInfo), StatsType::GET);

    if (ret == false) {
        debug_trace("get key [%.*s] offset %x%x valueSize %d\n",
                (int)keyStr.length(), keyStr.c_str(),
                storageInfo.externalFileID_, storageInfo.externalFileOffset_,
                storageInfo.externalContentSize_);
        exit(1);
    }

    if (seqNumberPtr != nullptr) {
        memcpy(seqNumberPtr, value, sizeof(uint32_t));
    }

    valueStrPtr->assign(std::string(value + sizeof(uint32_t), valueSize - sizeof(uint32_t)));

    debug_trace("get key [%.*s] valueSize %d seqnum %u\n", (int)keyStr.length(), keyStr.c_str(), (int)valueSize, (seqNumberPtr) ? *seqNumberPtr : 5678);
    if (value) {
        free(value);
    }
    delete[] key;
    return true;
}

bool IndexStoreInterface::multiGet(const vector<string>& keyStrVec, int numKeys, const vector<externalIndexInfo>& locs, vector<string>& values)
{
//    for (int i = 0; i < (int)keyStrVec.size(); i++) {
//        get(keyStrVec[i], locs[i], values[i]);
//    }
    kvServer_->getRangeValuesDecoupled(keyStrVec, numKeys, locs, values); 
    // remove the sequence number!
    for (int i = 0; i < values.size(); i++) {
        values[i] = values[i].substr(sizeof(uint32_t));
    }
    return true;
}

bool IndexStoreInterface::forcedManualGarbageCollection()
{
    kvServer_->gc(false);
    return true;
}

bool IndexStoreInterface::restoreVLog(std::map<std::string, externalIndexInfo>& keyValues)
{
    kvServer_->restoreVLog(keyValues);
    return true;
}

}
