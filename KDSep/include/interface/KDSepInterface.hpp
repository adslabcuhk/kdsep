#pragma once

#include "interface/lsmTreeInterface.hpp"

using namespace std;

namespace KDSEP_NAMESPACE {


class KDSep {
public:
    KDSep();
    KDSep(KDSepOptions& options, const string& name);
    // No copying allowed
    KDSep(const KDSep&) = delete;
    void operator=(const KDSep&) = delete;
    // Abstract class dector
    ~KDSep();

    bool Open(KDSepOptions& options, const string& name);
    bool Close();

    bool Put(const string& key, const string& value);
    bool Merge(const string& key, const string& value);
    bool Get(const string& key, string* value);

    void GetRocksDBProperty(const string& property, string* str);
    bool Scan(const string& startKey, int len, vector<string>& keys, vector<string>& values);
//    bool SingleDelete(const string& key);

private:
    KeyValueMemPoolBase* objectPairMemPool_ = nullptr;
    // batched write
    unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>* batch_map_[2]; // key to <operation type, value>
    uint64_t batch_in_use_ = 0;
    uint64_t maxBatchOperationBeforeCommitNumber_ = 3;
    uint64_t write_buffer_size_ = 2 * 1024 * 1024;
    messageQueue<unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>*>* write_buffer_mq_ = nullptr;
    uint64_t batch_nums_[2] = { 0UL, 0UL };
    uint64_t batch_sizes_[2] = { 0UL, 0UL };
//    boost::atomic<bool>* write_stall_ = nullptr;
    shared_ptr<bool> write_stall_;

    boost::atomic<bool> oneBufferDuringProcessFlag_ = false;
    boost::atomic<bool> writeBatchOperationWorkExitFlag = false;

    enum DBRunningMode {
        kWithDeltaStore = 0,
        kWithNoDeltaStore = 1,
        kBatchedWithDeltaStore = 2,
        kBatchedWithNoDeltaStore = 3
    };

    DBRunningMode KDSepRunningMode_ = kBatchedWithNoDeltaStore;

    // operations
    bool PutWithWriteBatch(mempoolHandler_t objectPairMemPoolHandler);
    bool MergeWithWriteBatch(mempoolHandler_t objectPairMemPoolHandler);

    bool PutImpl(const string& key, const string& value);
    bool GetInternal(const string& key, string* value, bool writing_back);

    str_t extractRawLsmValue(const string& lsm_value); 
    bool MultiGetFullMergeInternal(const vector<string>& keys,
	const vector<string>& lsm_values,
	const vector<vector<string>>& key_deltas,
	vector<string>& values); 
    bool BatchFullMergeInternal(const vector<string>& lsm_keys, 
            const vector<string>& lsm_values, const map<string, string>&
            key_deltas, int len, map<string, string>& keys_values); 

    bool MultiGetInternalForWriteBack(const vector<string>& keys,
            vector<string>& values); 
    bool GetKeysByTargetNumber(const string& targetStartKey, const uint64_t& targetGetNumber, vector<string>& keys, vector<string>& values);

    bool GetCurrentValuesThenWriteBack(const vector<string>& keys);
//    bool GetFromBuffer(const string& key, vector<string>& values);

    bool writeBufferDedup(unordered_map<str_t, vector<pair<DBOperationType,
            mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>*&
            operationsMap);

    void processBatchedOperationsWorker();
    void processWriteBackOperationsWorker();
    void processLsmInterfaceOperationsWorker();

    void Recovery();

    bool enable_delta_store_ = false;
    bool enable_write_buffer_ = false;
    bool enable_delta_gc_ = false;
    bool enableParallelLsmInterface = true;
    bool enable_crash_consistency_ = false;
    bool enable_bucket_merge_ = true;

    int writeBackWhenReadDeltaNumerThreshold_ = 4;
    int writeBackWhenReadDeltaSizeThreshold_ = 4;
    uint64_t deltaExtractSize_ = 0;
    uint64_t valueExtractSize_ = 0;
    shared_mutex KDSepOperationsMtx_;

    uint32_t globalSequenceNumber_ = 0;
    shared_mutex globalSequenceNumberGeneratorMtx_;

    shared_mutex write_buffer_mtx_;

    shared_ptr<lockQueue<vector<writeBackObject*>*>> write_back_queue_;
    shared_ptr<condition_variable> write_back_cv_;
    shared_ptr<mutex> write_back_mutex_; // for cv lock

    messageQueue<lsmInterfaceOperationStruct*>* lsm_interface_mq_ = nullptr;
    mutex lsm_interface_mutex;
    condition_variable lsm_interface_cv;
    bool enable_write_back_ = false;
    shared_mutex writeBackOperationsMtx_;

    // thread management
    vector<boost::thread*> thList_;
    bool deleteExistingThreads();
    // for separated operations
    bool extractDeltas(string internalValue, uint64_t skipSize,
            vector<pair<bool, string>>& mergeOperatorsVec);
    bool extractDeltas(string internalValue, uint64_t skipSize,
            vector<pair<bool, string>>& mergeOperatorsVec, 
            vector<KvHeader>& mergeOperatorsRecordVec);
    void pushWriteBuffer();
    // Storage component for delta store

    // tune the block cache size
    shared_ptr<rocksdb::Cache> rocks_block_cache_;
    struct timeval tv_tune_cache_;
    void tryTuneCache();
    uint64_t extra_mem_step_ = 16 * 1024 * 1024;
    uint64_t extra_mem_threshold_ = extra_mem_step_;
    uint64_t max_kd_cache_size_ = 0;
    uint64_t min_block_cache_size_ = 0;
    uint64_t memory_budget_ = 4ull * 1024 * 1024 * 1024;
    shared_ptr<KDLRUCache> kd_cache_;

    unique_ptr<HashStoreInterface> delta_store_;
    shared_ptr<BucketManager> bucket_manager_;
    shared_ptr<BucketOperator> bucket_operator_;
    shared_ptr<KDSepMergeOperator> KDSepMergeOperatorPtr_;
    LsmTreeInterface lsmTreeInterface_;

    bool should_exit_ = false;
    bool buffer_in_process_ = false; 
    bool probeThread();
};

} // namespace KDSEP_NAMESPACE
