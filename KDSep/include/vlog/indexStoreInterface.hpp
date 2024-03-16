#include "indexStore.hh"
#include <bits/stdc++.h>

using namespace std;

namespace KDSEP_NAMESPACE {

class IndexStoreInterface {
public:
    IndexStoreInterface(KDSepOptions* options, string workingDir, rocksdb::DB* pointerToRawRocksDB);
    ~IndexStoreInterface();

    uint64_t getExtractSizeThreshold();
    bool put(mempoolHandler_t objectPairMemPoolHandler, bool sync);
    bool multiPut(vector<mempoolHandler_t> objectPairMemPoolHandlerVec,
	    bool update_lsm = true);
    bool multiPutPostUpdate();
    bool get(const string keyStr, externalIndexInfo storageInfo, string* valueStrPtr, uint32_t* seqNumberPtr = nullptr);
    bool multiGet(const vector<string>& keyStrVec, int numKeys, const vector<externalIndexInfo>& storageInfoVec, vector<string>& valueStrPtrVec);
    bool forcedManualGarbageCollection();
    bool restoreVLog(std::map<std::string, externalIndexInfo>& keyValues);

private:
    uint64_t extractValueSizeThreshold_ = 0;
    string workingDir_;
    KDSepOptions* internalOptionsPtr_;
    rocksdb::DB* pointerToRawRocksDBForGC_;

    DeviceManager* devices_;
    KvServer* kvServer_;
};

}
