#ifndef __KVSERVER_HH__
#define __KVSERVER_HH__

#include "common/indexStorePreDefines.hpp"
#include "deviceManager.hh"
#include "vlog/ds/lru.hh"
#include "interface/KDSepOptions.hpp"
#include "keyManager.hh"
#include "logManager.hh"
#include "segmentGroupManager.hh"
#include "valueManager.hh"
#include "utils/messageQueue.hpp"
#include <atomic>
#include <vector>

/**
 * KvServer -- Interface for applications
 */

namespace KDSEP_NAMESPACE {

struct getValueStruct {
    char* ckey;
    len_t keySize;
    char* value;
    len_t valueSize;
    externalIndexInfo storageInfo;
    std::atomic<size_t>* keysInProcess;

    getValueStruct(char* ckey, len_t keySize, externalIndexInfo loc, std::atomic<size_t>* keysInProcess):
        ckey(ckey), keySize(keySize), storageInfo(loc), keysInProcess(keysInProcess) {}
};

class KvServer {
public:
    KvServer();
    KvServer(DeviceManager* deviceManager, rocksdb::DB* lsm);
    ~KvServer();

    bool putValue(const char* key, len_t keySize, const char* value, len_t valueSize, bool sync = true);
    bool getValue(const char* key, len_t keySize, char*& value, len_t& valueSize, externalIndexInfo storageInfoVec, bool timed = true);
    void getRangeValuesDecoupled(const std::vector<string> &keys, 
            int numKeys,
            const std::vector<externalIndexInfo>& locs, 
            std::vector<string> &values);
    //    bool delValue (char *key, len_t keySize);

    bool restoreVLog(std::map<std::string, externalIndexInfo>& keyValues);
    bool flushBuffer();
    bool flushBufferToVlog();
    bool updateLSMtreeInflushVLog();
    size_t gc(bool all = false);

    void printStorageUsage(FILE* out = stdout);
    void printGroups(FILE* out = stdout);
    void printBufferUsage(FILE* out = stdout);
    void printKeyCacheUsage(FILE* out = stdout);
    void printKeyStats(FILE* out = stdout);
    void printValueSlaveStats(FILE* out = stdout);
    void printGCStats(FILE* out = stdout);

private:
    KeyManager* _keyManager;
    ValueManager* _valueManager;
    DeviceManager* _deviceManager;
    LogManager* _logManager;
    GCManager* _gcManager;
    SegmentGroupManager* _segmentGroupManager;

    messageQueue<getValueStruct*>* notifyScanMQ_ = nullptr;
    std::deque<getValueStruct*> scanDq_;
    std::mutex scan_dq_mtx_;

    bool fake_scan_ = false;

    std::mutex scan_mtx_;
    std::condition_variable scan_cv_;
    void scanWorker(); 

    boost::asio::thread_pool*  _scanthreads;
//    boost::threadpool::pool  _scanthreads;

    struct {
        LruList* lru;
    } _cache;

    bool _freeDeviceManager;
    bool checkKeySize(len_t& keySize);

    void getValueMt(char *ckey, len_t keySize, char *&value, len_t &valueSize, externalIndexInfo storageInfo, uint8_t &ret, std::atomic<size_t> &keysInProcess); 
};

}
#endif
