#pragma once

#include "deltaStore/bucketManager.hpp"
#include "deltaStore/bucketOperator.hpp"
#include "deltaStore/hashStoreInterface.hpp"
#include "vlog/indexStoreInterface.hpp"
#include "interface/KDSepOptions.hpp"
#include "interface/mergeOperation.hpp"
#include "utils/debug.hpp"
#include "utils/headers.hh"
#include "utils/messageQueue.hpp"
#include <bits/stdc++.h>
#include <boost/atomic.hpp>
#include <boost/bind/bind.hpp>
#include <boost/thread/thread.hpp>
#include <shared_mutex>

using namespace std;

namespace KDSEP_NAMESPACE {

class LsmTreeInterface {
public:
    LsmTreeInterface();
    ~LsmTreeInterface();

    // No copying allowed
    LsmTreeInterface(const LsmTreeInterface&) = delete;
    void operator=(const LsmTreeInterface&) = delete;

    bool Open(KDSepOptions& options, const string& name);
    bool Close();

    bool Put(const mempoolHandler_t& mempoolHandler);
    bool Merge(const mempoolHandler_t& memPoolHandler);
    bool Merge(const char* key, uint32_t keySize, const char* value, uint32_t valueSize);
    bool Get(const string& key, string* value);
    bool Scan(const string& key, int len, vector<string>& keys, vector<string>& values);
    bool MultiGet(const vector<string>& key, vector<string>& values);
    bool MultiWriteWithBatch(
	    const vector<mempoolHandler_t>& memPoolHandlersPut,
	    rocksdb::WriteBatch* mergeBatch,
	    bool& need_post_update);
    bool updateVlogLsmTree();

    void GetRocksDBProperty(const string& property, string* str);
//    bool RangeScan(const string& startKey, uint64_t targetScanNumber, vector<string*> valueVec);
//    bool SingleDelete(const string& key);

private:
    rocksdb::DB* rocksdb_;
    // batched write

    enum LsmTreeRunningMode { kValueLog = 0, kNoValueLog = 1};
    LsmTreeRunningMode lsmTreeRunningMode_ = kValueLog;

    bool vLogMultiGetInternal(const vector<string>& keys,
	const vector<string>& values_lsm, vector<string>& values);

    // operations

    rocksdb::WriteOptions internalWriteOption_;
    rocksdb::WriteOptions internalMergeOption_;
    // Storage component for value store
    IndexStoreInterface* vlog_ = nullptr;
    MergeOperator* mergeOperator_ = nullptr; 
    uint64_t valueExtractSize_ = 0;
    bool isValueStoreInUseFlag_ = false;
    bool enable_crash_consistency_ = false;
    bool vlog_lsm_not_updated_ = false;
    boost::asio::thread_pool* multiget_threads_ = nullptr;
};

} // namespace KDSEP_NAMESPACE
