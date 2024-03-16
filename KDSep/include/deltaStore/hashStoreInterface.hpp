#pragma once

#include "deltaStore/bucketManager.hpp"
#include "deltaStore/bucketOperator.hpp"
#include "interface/KDSepOptions.hpp"
#include "utils/mempool.hpp"
#include <bits/stdc++.h>

using namespace std;

namespace KDSEP_NAMESPACE {

class HashStoreInterface {
public:
    HashStoreInterface(KDSepOptions* options, const string& workingDirStr,
            shared_ptr<BucketManager>& bucketManager,
            shared_ptr<BucketOperator>& bucketOperator);
    ~HashStoreInterface();

    uint64_t getExtractSizeThreshold();

    // not enable consistency: false & false 
    // consistency + has value: (false (not full) / true (full)) & false 
    //                         need_flush determined by bm->putCommitLog()
    // consistency + no value: arbitrary & true 
    bool multiPut(vector<mempoolHandler_t>& objects,
            bool need_flush, bool need_commit);
    bool putCommitLog(vector<mempoolHandler_t>& objects, bool& need_flush);
    bool commitToCommitLog();

    bool get(const string& keyStr, vector<string>& valueStrVecPtr);
    bool get(const string& keyStr, vector<string>& valueStrVec, vector<KDRecordHeader>& recordVec);
    bool multiGet(const vector<string>& keyStrVec, vector<vector<string>>& valueStrVecVecPtr);
    bool scan(const string& key, int len, map<string, string>& keys_values);
    bool wrapUpGC(uint64_t& num_bucket_pushed);
    bool setJobDone();
    bool Recovery();
    bool isEmpty();

    uint64_t getNumOfBuckets();

private:
    bool anyBucketInitedFlag_ = false;
    KDSepOptions* internalOptionsPtr_ = nullptr;
    uint64_t fileFlushThreshold_ = 0;
    bool shouldUseDirectOperationsFlag_;
    bool enable_lsm_tree_delta_meta_ = true;
    bool enable_crash_consistency_ = false;
    bool enable_parallel_get_hdl_ = true;
    // size information
    uint64_t extractValueSizeThreshold_;

    bool recoverFromCommitLog(uint64_t min_seq_num);
    // get function pointers
    shared_ptr<BucketManager> file_manager_;
    shared_ptr<BucketOperator> file_operator_;

    mutex multiop_mtx_;
    shared_ptr<KDLRUCache> kd_cache_;
};

}
