

#include "KDSep_db.h"

#include <bits/stdc++.h>
#include <sys/time.h>

#include <iostream>

#include "db/extern_db_config.h"

using namespace std;

namespace ycsbc {

vector<string> split(string str, string token) {
    vector<string> result;
    while (str.size()) {
        size_t index = str.find(token);
        if (index != std::string::npos) {
            result.push_back(str.substr(0, index));
            str = str.substr(index + token.size());
            if (str.size() == 0) result.push_back(str);
        } else {
            result.push_back(str);
            str = "";
        }
    }
    return result;
}

class FieldUpdateMergeOperator : public MergeOperator {
   public:
    // Gives the client a way to express the read -> modify -> write semantics
    // key:         (IN) The key that's associated with this merge operation.
    // existing:    (IN) null indicates that the key does not exist before this op
    // operand_list:(IN) the sequence of merge operations to apply, front() first.
    // new_value:  (OUT) Client is responsible for filling the merge result here
    // logger:      (IN) Client could use this to log errors during merge.
    //
    // Return true on success, false on failure/corruption/etc.
    bool FullMerge(const Slice &key, const Slice *existing_value,
                   const std::deque<std::string> &operand_list,
                   std::string *new_value, Logger *logger) const override {
        // cout << "Do full merge operation in as field update" << endl;
        // cout << existing_value->data() << "\n Size=" << existing_value->size() << endl;
        // new_value->assign(existing_value->ToString());
        // if (existing_value == nullptr) {
        //     cout << "Merge operation existing value = nullptr" << endl;
        //     return false;
        // }
        // cout << "Merge operation existing value size = " << existing_value->size() << endl;
        vector<std::string> words = split(existing_value->ToString(), ",");
        // for (long unsigned int i = 0; i < words.size(); i++) {
        //     cout << "Index = " << i << ", Words = " << words[i] << endl;
        // }
        for (auto q : operand_list) {
            // cout << "Operand list content = " << q << endl;
            vector<string> operandVector = split(q, ",");
            for (long unsigned int i = 0; i < operandVector.size(); i += 2) {
                words[stoi(operandVector[i])] = operandVector[i + 1];
            }
        }
        string temp;
        for (long unsigned int i = 0; i < words.size() - 1; i++) {
            temp += words[i] + ",";
        }
        temp += words[words.size() - 1];
        new_value->assign(temp);
        // cout << new_value->data() << "\n Size=" << new_value->length() <<endl;
        return true;
    };

    // This function performs merge(left_op, right_op)
    // when both the operands are themselves merge operation types.
    // Save the result in *new_value and return true. If it is impossible
    // or infeasible to combine the two operations, return false instead.
    bool PartialMerge(const Slice &key, const Slice &left_operand,
                      const Slice &right_operand, std::string *new_value,
                      Logger *logger) const override {
        // cout << "Do partial merge operation in as field update" << endl;
        string allOperandListStr = left_operand.ToString();
        allOperandListStr.append(",");
        allOperandListStr.append(right_operand.ToString());
        // cerr << "Find raw partial merge = " << allOperandListStr << endl
        //      << endl;
        unordered_map<int, string> findIndexMap;
        stack<pair<string, string>> finalResultStack;
        vector<string> operandVector = split(allOperandListStr, ",");
        // cerr << "result vec size = " << operandVector.size() << endl;
        for (long unsigned int i = 0; i < operandVector.size(); i += 2) {
            // cerr << "result vec index = " << i << ", content = " << operandVector[i] << endl;
            int index = stoi(operandVector[i]);
            if (findIndexMap.find(index) == findIndexMap.end()) {
                findIndexMap.insert(make_pair(index, operandVector[i + 1]));
            } else {
                findIndexMap.at(index).assign(operandVector[i + 1]);
            }
        }
        // cerr << "result map size = " << findIndexMap.size() << endl;
        string finalResultStr = "";
        for (auto it : findIndexMap) {
            finalResultStr.append(to_string(it.first));
            finalResultStr.append(",");
            finalResultStr.append(it.second);
            finalResultStr.append(",");
        }
        finalResultStr = finalResultStr.substr(0, finalResultStr.size() - 1);
        // cerr << "Find partial merge = " << finalResultStr << endl
        //      << endl;
        new_value->assign(finalResultStr);
        return true;
    };

    // The name of the MergeOperator. Used to check for MergeOperator
    // mismatches (i.e., a DB created with one MergeOperator is
    // accessed using a different MergeOperator)
    const char *Name() const override { return "FieldUpdateMergeOperator"; }
};

KDSepDB::KDSepDB(const char *dbfilename, const std::string &config_file_path) {
    // get rocksdb config
    struct timeval tv1;
    KDSEP_NAMESPACE::StatsRecorder::getInstance()->openStatistics(tv1);
    ExternDBConfig config = ExternDBConfig(config_file_path);
    int bloomBits = config.getBloomBits();
    size_t blockCacheSize = config.getBlockCache();
    size_t blobCacheSize = config.getBlobCacheSize();
    // bool seekCompaction = config.getSeekCompaction();
    bool compression = config.getCompression();
    bool directIO = config.getDirectIO();
    bool directReads = config.getDirectReads();
    bool useMmap = config.getUseMmap();
    bool keyValueSeparation = config.getKeyValueSeparation();
    bool keyDeltaSeparation = config.getKeyDeltaSeparation();
    bool blobDbKeyValueSeparation = config.getBlobDbKeyValueSeparation();
    size_t memtableSize = config.getMemtable();
    uint64_t debugLevel = config.getDebugLevel();
    DebugManager::getInstance().setDebugLevel(debugLevel);
    size_t memory_budget = config.getMemoryBudget();

    options_.memory_budget = memory_budget;
    options_.min_block_cache_size = blockCacheSize;

    // set optionssc
    rocksdb::BlockBasedTableOptions bbto;

    if (directIO == true) {
        options_.rocks_opt.use_direct_reads = directReads;
        options_.rocks_opt.use_direct_io_for_flush_and_compaction = true;
        options_.fileOperationMethod_ = (directReads) ? KDSEP_NAMESPACE::kDirectIO : KDSEP_NAMESPACE::kAlignLinuxIO;
    } else {
        options_.rocks_opt.allow_mmap_reads = useMmap;
        options_.rocks_opt.allow_mmap_writes = useMmap;
        options_.fileOperationMethod_ = KDSEP_NAMESPACE::kAlignLinuxIO;
    }

    if (config.getUsePwrite() == true) {
        options_.fileOperationMethod_ = KDSEP_NAMESPACE::kPreadWrite;
    }

    if (blobDbKeyValueSeparation == true) {
        bbto.block_cache = (blockCacheSize == 0) ? nullptr : rocksdb::NewLRUCache(blockCacheSize);
        if (blockCacheSize == 0) {
            bbto.no_block_cache = true;
        }
        options_.rocks_opt.enable_blob_files = true;
        options_.rocks_opt.min_blob_size = config.getMinBlobSize();                                           // Default 1024
        options_.rocks_opt.blob_file_size = config.getBlobFileSize() * 1024;                                  // Default 256*1024*1024
        options_.rocks_opt.blob_compression_type = kNoCompression;                                            // Default kNoCompression
        options_.rocks_opt.enable_blob_garbage_collection = true;                                             // Default false
        options_.rocks_opt.blob_garbage_collection_age_cutoff = 0.25;                                         // Default 0.25
        options_.rocks_opt.blob_garbage_collection_force_threshold = config.getBlobGCForce();                 // Default 1.0
        options_.rocks_opt.blob_compaction_readahead_size = 0;                                                // Default 0
        options_.rocks_opt.blob_file_starting_level = 0;                                                      // Default 0
        options_.rocks_opt.blob_cache = (blobCacheSize > 0) ? rocksdb::NewLRUCache(blobCacheSize) : nullptr;  // rocksdb::NewLRUCache(blockCacheSize / 8 * 7);         // Default nullptr, bbto.block_cache
        options_.rocks_opt.prepopulate_blob_cache = rocksdb::PrepopulateBlobCache::kDisable;                  // Default kDisable
        assert(!keyValueSeparation);
    } else {
        bbto.block_cache = (blockCacheSize == 0) ? nullptr : rocksdb::NewLRUCache(memory_budget);
        if (blockCacheSize == 0) {
            bbto.no_block_cache = true;
        }
    }
    bbto.block_size = config.getBlockSize();
    bbto.cache_index_and_filter_blocks = true;

    options_.rocks_block_cache = bbto.block_cache;

    if (keyValueSeparation == true) {
        options_.enable_valueStore = true;
    }
    if (keyDeltaSeparation == true) {
        // KDSep settings
        options_.enable_deltaStore = true;
        uint64_t ds_kdcache_size = config.getDSKDCacheSize();
        if (ds_kdcache_size != 0) {
            options_.enable_deltaStore_KDLevel_cache = true;
            options_.deltaStore_KDCache_item_number_ = ds_kdcache_size;
        }
        options_.deltaStore_init_k_ = config.getPrefixTreeBitNumber();
        options_.deltaStore_bucket_size_ = config.getDeltaStoreBucketSize();
        options_.deltaStore_bucket_flush_buffer_size_limit_ = config.getDeltaStoreBucketFlushSize();
        options_.deltaStore_op_worker_thread_number_limit_ = config.getDeltaStoreOpWorkerThreadNumber();
        options_.deltaStore_gc_worker_thread_number_limit_ = config.getDeltaStoreGCWorkerThreadNumber();
        options_.deltaStore_max_bucket_number_ = config.getDeltaStoreMaxBucketNumber();
        bool enable_gc_flag = config.getDeltaStoreGCEnableStatus();
        if (enable_gc_flag == true) {
            options_.enable_bucket_gc = true;
            options_.enable_bucket_split = config.getEnableBucketSplit();
            options_.enable_bucket_merge = config.getEnableBucketMerge();
            options_.deltaStore_gc_split_threshold_ = config.getDeltaStoreSplitGCThreshold();
            options_.deltaStore_gc_threshold = config.getDeltaStoreGCThreshold();
        } else {
            options_.enable_bucket_gc = false;
        }
    }
    options_.write_buffer_size = config.getKDSepWriteBufferSize();
    if (options_.write_buffer_size > 0) {
        options_.deltaStore_mem_pool_object_number_ = 300000;
        // ceil(options_.write_buffer_size * 3);
        long pagesize = sysconf(_SC_PAGE_SIZE);
        options_.deltaStore_mem_pool_object_size_ =
            (config.getMaxKeyValueSize() + pagesize - 1) / pagesize * pagesize;
    }

    options_.rocksdb_sync_put = !keyValueSeparation;
    options_.rocksdb_sync_merge = !keyDeltaSeparation;
    options_.enable_parallel_lsm_interface_ = config.getParallelLsmTreeInterface();
    cerr << "parallel: " << (int)options_.enable_parallel_lsm_interface_ << endl;
    options_.enable_crash_consistency = (keyDeltaSeparation &&
            config.getEnableCrashConsistency());
    options_.commit_log_size = config.getCommitLogSize();

    options_.KDSep_merge_operation_ptr.reset(new KDSEP_NAMESPACE::KDSepFieldUpdateMergeOperator);
    options_.rocks_opt.merge_operator.reset(new FieldUpdateMergeOperator);

    options_.rocks_opt.create_if_missing = true;
    options_.rocks_opt.write_buffer_size = memtableSize;
    // options_.rocks_opt.compaction_pri = rocksdb::kMinOverlappingRatio;
    if (config.getTiered()) {
        options_.rocks_opt.compaction_style = rocksdb::kCompactionStyleUniversal;
    }
    options_.rocks_opt.max_background_jobs = config.getNumThreads();
    options_.rocks_opt.disable_auto_compactions = config.getNoCompaction();
    options_.rocks_opt.level_compaction_dynamic_level_bytes = true;
    options_.rocks_opt.target_file_size_base = config.getSSTSize();
    options_.rocks_opt.max_bytes_for_level_base = config.getL1Size();

    if (config.getEnableGcWriteStall()) {
        options_.write_stall.reset(new bool);
        *options_.write_stall = false;
    }

    if (config.getTestRecovery()) {
        options_.test_recovery = true;
    }

    if (!compression) {
        options_.rocks_opt.compression = rocksdb::kNoCompression;
    }
    if (bloomBits > 0) {
        bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bloomBits));
    }
    options_.rocks_opt.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));

    options_.rocks_opt.statistics = rocksdb::CreateDBStatistics();
    options_.rocks_opt.report_bg_io_stats = true;

    KDSEP_NAMESPACE::ConfigManager::getInstance().setConfigPath(config_file_path.c_str());

    bool dbOpenStatus = db_.Open(options_, dbfilename);
    if (!dbOpenStatus) {
        cerr << "Can't open KDSep " << dbfilename << endl;
        exit(0);
    } else {
        rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTime);
        rocksdb::get_perf_context()->Reset();
        rocksdb::get_iostats_context()->Reset();
    }
    cerr << "--- Create finished" << endl;

    gettimeofday(&tv_, 0);
}

int KDSepDB::Read(const std::string &table, const std::string &key,
                  const std::vector<std::string> *fields,
                  std::vector<KVPair> &result) {
    string value;
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    KDSEP_NAMESPACE::StatsRecorder::getInstance()->timeProcess(KDSEP_NAMESPACE::StatsType::WORKLOAD_OTHERS, tv_);
    // cerr << "read " << key << endl;
    int ret = db_.Get(key, &value);
    KDSEP_NAMESPACE::StatsRecorder::getInstance()->timeProcess(KDSEP_NAMESPACE::StatsType::KDSep_GET, tv);
    gettimeofday(&tv_, 0);
    // outputStream_ << "YCSB Read " << key << " " << value << endl;
    return ret;
}

int KDSepDB::Scan(const std::string &table, const std::string &key, int len,
                  const std::vector<std::string> *fields,
                  std::vector<std::vector<KVPair>> &result) {
    vector<string> keys, values;
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    KDSEP_NAMESPACE::StatsRecorder::getInstance()->timeProcess(KDSEP_NAMESPACE::StatsType::WORKLOAD_OTHERS, tv_);
    db_.Scan(key, len, keys, values);
    KDSEP_NAMESPACE::StatsRecorder::getInstance()->timeProcess(KDSEP_NAMESPACE::StatsType::KDSep_SCAN, tv);
    gettimeofday(&tv_, 0);
    return 1;
}

int KDSepDB::Insert(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values) {
    rocksdb::Status s;
    string fullValue;
    struct timeval tv;
    //    static uint64_t key_size = 0;
    //    static uint64_t key_cnt = 0;
    //    static uint64_t value_size = 0;
    //    static uint64_t value_cnt = 0;

    KDSEP_NAMESPACE::StatsRecorder::getInstance()->timeProcess(KDSEP_NAMESPACE::StatsType::WORKLOAD_OTHERS, tv_);
    gettimeofday(&tv, nullptr);
    for (long unsigned int i = 0; i < values.size() - 1; i++) {
        fullValue += (values[i].second + ",");
    }
    fullValue += values[values.size() - 1].second;
    //    fprintf(stdout, "insert key %s value %s\n", key.c_str(), fullValue.c_str());
    //    key_size += key.size();
    //    key_cnt ++;
    //    value_size += fullValue.size();
    //    value_cnt ++;
    //
    //    if (value_cnt % 50 == 0) {
    //        fprintf(stderr, "key average %.2lf\n", (double)key_size / key_cnt);
    //        fprintf(stderr, "value average %.2lf\n", (double)value_size / value_cnt);
    //    }
    bool status = db_.Put(key, fullValue);
    KDSEP_NAMESPACE::StatsRecorder::getInstance()->timeProcess(KDSEP_NAMESPACE::StatsType::KDSep_PUT, tv);
    gettimeofday(&tv_, 0);
    if (!status) {
        cerr << "insert error"
             << endl;
        exit(0);
    }
    return 1;
}

int KDSepDB::Update(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values) {
    struct timeval tv;
    KDSEP_NAMESPACE::StatsRecorder::getInstance()->timeProcess(KDSEP_NAMESPACE::StatsType::WORKLOAD_OTHERS, tv_);
    gettimeofday(&tv, nullptr);
    // cerr << "update " << key << endl;
    for (KVPair &p : values) {
        bool status = db_.Merge(key, p.second);
        if (!status) {
            cout << "Merge value failed" << endl;
            exit(-1);
        }
        // outputStream_ << "[YCSB] Update op, key = " << key << ", op value = " << p.second << endl;
    }
    KDSEP_NAMESPACE::StatsRecorder::getInstance()->timeProcess(KDSEP_NAMESPACE::StatsType::KDSep_MERGE, tv);
    gettimeofday(&tv_, 0);
    // s = db_.Flush(rocksdb::FlushOptions());
    return 1;
}

int KDSepDB::OverWrite(const std::string &table, const std::string &key,
                       std::vector<KVPair> &values) {
    string fullValue;
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    for (long unsigned int i = 0; i < values.size() - 1; i++) {
        fullValue += (values[i].second + ",");
    }
    fullValue += values[values.size() - 1].second;
    bool status = db_.Put(key, fullValue);
    if (!status) {
        cerr << "OverWrite error" << endl;
        exit(0);
    }
    KDSEP_NAMESPACE::StatsRecorder::getInstance()->timeProcess(KDSEP_NAMESPACE::StatsType::KDSep_PUT, tv);
    // outputStream_ << "[YCSB] Overwrite op, key = " << key << ", value = " << fullValue << endl;
    return 1;
}

int KDSepDB::Delete(const std::string &table, const std::string &key) {
    return true;
    //    return db_.SingleDelete(key);  // Undefined result
}

void KDSepDB::printStats() {
    //    db_.pointerToRawRocksDB_->Flush(rocksdb::FlushOptions());
    string stats;
    db_.GetRocksDBProperty("rocksdb.stats", &stats);
    cout << stats << endl;
    // cout << options_.statistics->ToString() << endl;
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    cout << "Get RocksDB Build-in Perf Context: " << endl;
    cout << rocksdb::get_perf_context()->ToString() << endl;
    cout << "Get RocksDB Build-in I/O Stats Context: " << endl;
    cout << rocksdb::get_iostats_context()->ToString() << endl;
    cout << "Get RocksDB Build-in Total Stats Context: " << endl;
    cout << options_.rocks_opt.statistics->ToString() << endl;
}

KDSepDB::~KDSepDB() {
    cerr << "1. Close stats recorder" << endl;
    KDSEP_NAMESPACE::StatsRecorder::DestroyInstance();
    cerr << "2. Close output stream" << endl;
    outputStream_.close();
    cerr << "3. Close db" << endl;
    db_.Close();
    cerr << "4. Close KDSepDB complete" << endl;
}
}  // namespace ycsbc
