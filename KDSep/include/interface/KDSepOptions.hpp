#pragma once

#include "common/dataStructure.hpp"
#include "interface/mergeOperation.hpp"
#include "rocksdb/options.h"
#include "utils/KDLRUCache.hpp"
#include "utils/appendAbleLRUCache.hpp"
#include "utils/appendAbleLRUCacheStrVector.hpp"
#include "utils/debug.hpp"
#include "utils/fileOperation.hpp"
#include "utils/messageQueue.hpp"
#include "utils/lockQueue.hpp"
#include <bits/stdc++.h>

using namespace std;

namespace KDSEP_NAMESPACE {

class KDSepOptions {
public:
    KDSepOptions() = default;
    ~KDSepOptions() = default;

    rocksdb::Options rocks_opt;
    shared_ptr<rocksdb::Cache> rocks_block_cache = nullptr;

    enum class contentStoreMode {
        kAppendOnlyLogWithIndex = 0,
        kHashBasedBucketWithoutIndex = 1,
        kErrorUnknownStoreMode = 2,
    }; // Mainly used for deltaStore (Baseline and Current Design)

    enum class contentCacheMode {
        kLRUCache = 0,
        kAdaptiveReplacementCache = 1,
        kErrorUnknownCacheMode = 2,
    };

    // deltaStore options
    bool enable_deltaStore = false;
    bool enable_deltaStore_KDLevel_cache = false;
    bool enable_bucket_gc = false;
    contentCacheMode deltaStore_base_cache_mode = contentCacheMode::kLRUCache;
    contentStoreMode deltaStore_base_store_mode = contentStoreMode::kHashBasedBucketWithoutIndex;
    uint64_t deltaStore_KDCache_item_number_ = 1 * 1024 * 1024;
    bool deltaStore_KDLevel_cache_use_str_t = true;
    uint64_t deltaStore_KDLevel_cache_peritem_value_number = 1 * 1024;
    uint64_t extract_to_deltaStore_size_lower_bound = 0;
    uint64_t extract_to_deltaStore_size_upper_bound = 0x3f3f3f;
    uint64_t deltaStore_bucket_size_ = 1 * 1024 * 1024;
    uint64_t deltaStore_total_storage_maximum_size = 64 * 1024 * deltaStore_bucket_size_;
    uint64_t deltaStore_op_worker_thread_number_limit_ = 4;
    uint64_t deltaStore_gc_worker_thread_number_limit_ = 1;
    uint64_t deltaStore_bucket_flush_buffer_size_limit_ = 4096;
    uint64_t deltaStore_operationNumberForForcedSingleFileGCThreshold_ = 50;
    float deltaStore_gc_threshold = 0.9;
    float deltaStore_gc_split_threshold_ = 0.8;
    uint64_t deltaStore_write_back_during_reads_threshold = 0;
    uint64_t deltaStore_write_back_during_reads_size_threshold = 0;
    uint64_t deltaStore_gc_write_back_delta_num = 0;
    uint64_t deltaStore_gc_write_back_delta_size = 0;
    uint64_t deltaStore_init_k_ = 10;
    uint32_t deltaStore_mem_pool_object_number_ = 5;
    uint32_t deltaStore_mem_pool_object_size_ = 4096;

    // valueStore options
    bool enable_valueStore = false;
    bool enable_valueStore_fileLvel_cache = false;
    bool enable_valueStore_KDLevel_cache = false;
    bool enable_valueStore_garbage_collection = false;
    contentCacheMode valueStore_base_cache_mode = contentCacheMode::kLRUCache;
    contentStoreMode valueStore_base_store_mode = contentStoreMode::kAppendOnlyLogWithIndex;
    uint64_t extract_to_valueStore_size_lower_bound = 0;
    uint64_t extract_to_valueStore_size_upper_bound = 0x3f3f3f;
    uint64_t valueStore_single_file_maximum_size = 1 * 1024 * 1024;
    uint64_t valueStore_total_storage_maximum_size = 1024 * 1024 * valueStore_single_file_maximum_size;
    uint64_t valueStore_thread_number_limit = 3;

    // common options
    uint64_t KDSep_thread_number_limit = 4;
    uint64_t deltaStore_max_bucket_number_ = 16;
    shared_ptr<KDSepMergeOperator> KDSep_merge_operation_ptr;
    fileOperationType fileOperationMethod_ = kDirectIO;
    bool enable_write_back_optimization_ = true;
    bool enable_parallel_lsm_interface_ = false;
    bool enable_crash_consistency = false;
    bool enable_bucket_split = true;
    bool enable_bucket_merge = true;
    bool enable_batched_operations = true;
    uint64_t key_value_cache_object_number_ = 1000;
    uint64_t write_buffer_num = 5;
    uint64_t write_buffer_size = 2 * 1024 * 1024;
    bool rocksdb_sync_put = false;
    bool rocksdb_sync_merge = false;
    bool rocksdb_sync = false;

    bool enable_index_block = true;
    bool test_recovery = false;

    uint64_t commit_log_size = 1024 * 1024 * 1024;

    // tune the block cache size
    uint64_t min_block_cache_size = 0;
    uint64_t memory_budget;

    // not message queue
    shared_ptr<lockQueue<vector<writeBackObject*>*>> write_back_queue;
    shared_ptr<condition_variable> write_back_cv;
    shared_ptr<bool> write_stall;
    shared_ptr<KDLRUCache> kd_cache;

    // dump options
    bool dumpOptions(string dumpPath);
    bool dumpDataStructureInfo(string dumpPath);
};

} // namespace KDSEP_NAMESPACE
