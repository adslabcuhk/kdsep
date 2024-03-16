#pragma once

#include "common/dataStructure.hpp"
#include "interface/KDSepOptions.hpp"
#include "utils/messageQueue.hpp"
#include "utils/murmurHash.hpp"
#include <bits/stdc++.h>
#include <shared_mutex>
#include <string_view>

namespace KDSEP_NAMESPACE {

class BucketManager;

class BucketOperator {
public:
    BucketOperator(KDSepOptions* options, string workingDirStr, 
            shared_ptr<BucketManager> bucketManager);
    ~BucketOperator();
    // file operations with job queue support
    bool putWriteOperationIntoJobQueue(BucketHandler* bucket, mempoolHandler_t* memPoolHandler);
    bool putIntoJobQueue(deltaStoreOpHandler* op_hdl); 
    bool startJob(deltaStoreOpHandler* op_hdl);

    // file operations without job queue support-> only support single operation
    bool directlyReadOperation(BucketHandler* bucket, string key,
            vector<string>& valueVec);
    bool directlyScanOperation(BucketHandler* bucket, string key, int len,
            map<string, string>& keys_values); 
    bool waitOperationHandlerDone(deltaStoreOpHandler* op_hdl, 
            bool need_delete = true);
    // threads with job queue support
    bool setJobDone();
    bool probeThread();

private:
    // settings
    string working_dir_;
    uint64_t perFileFlushBufferSizeLimit_;
    uint64_t perFileGCSizeLimit_;
    uint64_t singleFileSizeLimit_;
    uint64_t operationNumberThresholdForForcedSingleFileGC_;
    shared_ptr<KDSepMergeOperator> KDSepMergeOperatorPtr_;
    bool enable_gc_ = false;
    bool enable_index_block_ = true;

    void asioSingleOperation(deltaStoreOpHandler* op_hdl);
    void singleOperation(deltaStoreOpHandler* op_hdl);
    bool operationGet(deltaStoreOpHandler* op_hdl);
    bool operationMultiGet(deltaStoreOpHandler* op_hdl);
    bool operationMultiPut(deltaStoreOpHandler* op_hdl, bool& gc_pushed);
    bool operationFlush(deltaStoreOpHandler* op_hdl, bool& gc_pushed);
    bool operationFind(deltaStoreOpHandler* op_hdl);

    uint64_t readWholeFile(BucketHandler* bucket, char** buf);
    // for scan or multiget 
    uint64_t readWholeFileOrRetrieveFromCache(BucketHandler* bucket, char** buf);
    uint64_t readUnsortedPart(BucketHandler* bucket, char** buf);
    uint64_t readSortedPart(BucketHandler* bucket, const string_view& key_view, char** buf, bool& key_exists);
    uint64_t readBothParts(BucketHandler* bucket, const string_view& key_view, char** buf);

    bool writeToFile(BucketHandler* bucket, 
            char* contentBuffer, uint64_t contentSize, 
            uint64_t contentObjectNumber, bool need_flush = false);
    bool readAndProcessSortedPart(BucketHandler* bucket,
            string& key, vector<string_view>& kd_list, char** buf);
    bool readAndProcessWholeFile(BucketHandler* bucket,
            string& key, vector<string_view>& kd_list, char** buf);
    // for a scan; may update the cache
    bool readAndProcessWholeFileWithCache(BucketHandler* bucket,
            map<string_view, vector<string_view>>& kd_lists, char** buf);
    // for a normal read; hits only the unsorted part
    bool readAndProcessUnsortedPart(BucketHandler* bucket,
            string& key, vector<string_view>& kd_list, char** buf);
    // for a normal read; hits both sorted and unsorted part
    bool readAndProcessBothParts(BucketHandler* bucket,
            string& key, vector<string_view>& kd_list, char** buf);
    
    // for multiget (when writing back)
    bool readAndProcessWholeFileKeyList(
            BucketHandler* bucket, vector<string*>* key,
            vector<vector<string_view>>& kd_list, char** buf);

    uint64_t processReadContentToValueLists(char* contentBuffer, uint64_t contentSize, unordered_map<str_t, vector<str_t>, mapHashKeyForStr_t, mapEqualKeForStr_t>& resultMapInternal);
    uint64_t processReadContentToValueLists(char* contentBuffer, uint64_t contentSize, unordered_map<string_view, vector<string_view>>& resultMapInternal, const string_view& currentKey);
    uint64_t processReadContentToValueLists(char* contentBuffer, 
            uint64_t contentSize, vector<string_view>& kd_list, 
            const string_view& currentKey);
    uint64_t processReadContentToValueLists(char* read_buf, 
            uint64_t read_buf_size,
            map<string_view, vector<string_view>>& kd_lists);
    uint64_t processReadContentToValueListsWithKeyList(char* read_buf, 
            uint64_t read_buf_size, vector<vector<string_view>>& kd_list,
            const vector<string_view>& keys);
    void putKeyValueToAppendableCacheIfExist(char* keyPtr, size_t keySize, char* valuePtr, size_t valueSize, bool isAnchor);
    void putKeyValueVectorToAppendableCacheIfNotExist(char* keyPtr, size_t keySize, vector<str_t>& values);
    void updateKDCacheIfExist(str_t key, str_t delta, bool isAnchor);
    void updateKDCache(char* keyPtr, size_t keySize, str_t delta); 
    bool pushGcIfNeeded(BucketHandler* bucket);
    bool pushGcIfNeeded(deltaStoreOpHandler* bucket);
    bool pushGcForOpHandler(deltaStoreOpHandler* op_hdl);
    void operationBoostThreadWorker(deltaStoreOpHandler* op_hdl);
    // boost thread
    unique_ptr<boost::asio::thread_pool> workerThreads_;
    // message management
    shared_ptr<BucketManager> bucket_manager_;
    shared_ptr<KDLRUCache> kd_cache_;
    mutex operationNotifyMtx_;
    condition_variable operationNotifyCV_;

    boost::atomic<uint64_t> workingThreadExitFlagVec_;
    boost::atomic<uint64_t> num_threads_;
    uint64_t workerThreadNumber_ = 0;
    bool syncStatistics_;
    shared_ptr<bool> write_stall_;
    bool should_exit_ = false;
    bool enable_crash_consistency_ = false;
};

} // namespace KDSEP_NAMESPACE
