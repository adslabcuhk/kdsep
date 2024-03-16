#include "rocksdb_db.h"

#include <bits/stdc++.h>
#include <sys/time.h>

#include <iostream>

#include "db/extern_db_config.h"

using namespace std;
using namespace rocksdb;

namespace ycsbc {

struct timeval timestartFull;
struct timeval timeendFull;
struct timeval timestartPart;
struct timeval timeendPart;
uint64_t counter_full = 0;
double totalTimeFull = 0;
uint64_t counter_part = 0;
double totalTimePart = 0;

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
        counter_full++;
        gettimeofday(&timestartFull, NULL);
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
        gettimeofday(&timeendFull, NULL);
        totalTimeFull += 1000000 * (timeendFull.tv_sec - timestartFull.tv_sec) +
                         timeendFull.tv_usec - timestartFull.tv_usec;
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
        counter_part++;
        gettimeofday(&timestartPart, NULL);
        new_value->assign(left_operand.ToString() + "," + right_operand.ToString());
        gettimeofday(&timeendPart, NULL);
        totalTimePart += 1000000 * (timeendPart.tv_sec - timestartPart.tv_sec) +
                         timeendPart.tv_usec - timestartPart.tv_usec;
        // cout << left_operand.data() << "\n Size=" << left_operand.size() << endl;
        // cout << right_operand.data() << "\n Size=" << right_operand.size() <<
        // endl; cout << new_value << "\n Size=" << new_value->length() << endl;
        // new_value->assign(left_operand.data(), left_operand.size());
        return true;
    };

    // The name of the MergeOperator. Used to check for MergeOperator
    // mismatches (i.e., a DB created with one MergeOperator is
    // accessed using a different MergeOperator)
    const char *Name() const override { return "FieldUpdateMergeOperator"; }
};

RocksDB::RocksDB(const char *dbfilename, const std::string &config_file_path) {
    // outputStream_.open("operations.log", ios::binary | ios::out);
    // if (!outputStream_.is_open()) {
    //     cerr << "Load logging files error" << endl;
    // }
    // get rocksdb config
    ExternDBConfig config = ExternDBConfig(config_file_path);
    int bloomBits = config.getBloomBits();
    size_t blockCacheSize = config.getBlockCache();
    // bool seekCompaction = config.getSeekCompaction();
    bool compression = config.getCompression();
    bool directIO = config.getDirectIO();
    bool keyValueSeparation = config.getKeyValueSeparation();
    bool keyDeltaSeparation = config.getKeyDeltaSeparation();
    size_t memtableSize = config.getMemtable();
    // set optionssc
    rocksdb::BlockBasedTableOptions bbto;
    if (directIO == true) {
        options_.use_direct_reads = true;
        options_.use_direct_io_for_flush_and_compaction = true;
    } else {
        options_.allow_mmap_reads = true;
        options_.allow_mmap_writes = true;
    }
    if (keyValueSeparation == true) {
        cerr << "Enabled Blob based KV separation" << endl;
        options_.enable_blob_files = true;
        options_.min_blob_size = 0;                                                 // Default 0
        options_.blob_file_size = config.getBlobFileSize() * 1024;                  // Default 256*1024*1024
        options_.blob_compression_type = kNoCompression;                            // Default kNoCompression
        options_.enable_blob_garbage_collection = false;                            // Default false
        options_.blob_garbage_collection_age_cutoff = 0.25;                         // Default 0.25
        options_.blob_garbage_collection_force_threshold = 1.0;                     // Default 1.0
        options_.blob_compaction_readahead_size = 0;                                // Default 0
        options_.blob_file_starting_level = 0;                                      // Default 0
        options_.blob_cache = nullptr;                                              // Default nullptr
        options_.prepopulate_blob_cache = rocksdb::PrepopulateBlobCache::kDisable;  // Default kDisable
    }
    if (keyDeltaSeparation == true) {
        cerr << "Enabled DeltaLog based KD separation" << endl;
        options_.enable_deltaLog_files = true;
        options_.min_deltaLog_size = 0;                                                     // Default 0
        options_.deltaLog_file_size = config.getDeltaLogFileSize() * 1024;                  // Default 256*1024*1024
        options_.deltaLog_compression_type = kNoCompression;                                // Default kNoCompression
        options_.enable_deltaLog_garbage_collection = false;                                // Default false
        options_.deltaLog_garbage_collection_age_cutoff = 0.25;                             // Default 0.25
        options_.deltaLog_garbage_collection_force_threshold = 1.0;                         // Default 1.0
        options_.deltaLog_compaction_readahead_size = 0;                                    // Default 0
        options_.deltaLog_file_starting_level = 0;                                          // Default 0
        options_.deltaLog_cache = nullptr;                                                  // Default nullptr
        options_.prepopulate_deltaLog_cache = rocksdb::PrepopulateDeltaLogCache::kDisable;  // Default kDisable
    }
    options_.create_if_missing = true;
    options_.write_buffer_size = memtableSize;
    // options_.compaction_pri = rocksdb::kMinOverlappingRatio;
    if (config.getTiered()) {
        options_.compaction_style = rocksdb::kCompactionStyleUniversal;
    }
    options_.max_background_jobs = config.getNumThreads();
    options_.disable_auto_compactions = config.getNoCompaction();
    options_.level_compaction_dynamic_level_bytes = true;
    options_.target_file_size_base = config.getTargetFileSizeBase() * 1024;
    cerr << "write buffer size " << options_.write_buffer_size << endl;
    cerr << "write buffer number " << options_.max_write_buffer_number << endl;
    cerr << "num compaction trigger "
         << options_.level0_file_num_compaction_trigger << endl;
    cerr << "targe file size base " << options_.target_file_size_base << endl;
    cerr << "level size base " << options_.max_bytes_for_level_base << endl;

    if (!compression) options_.compression = rocksdb::kNoCompression;
    if (bloomBits > 0) {
        bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bloomBits));
    }
    bbto.block_cache = rocksdb::NewLRUCache(blockCacheSize);
    options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));

    options_.merge_operator.reset(
        new FieldUpdateMergeOperator);  // merge operators
    options_.statistics = rocksdb::CreateDBStatistics();
    cerr << "Start create RocksDB instance" << endl;

    rocksdb::Status s = rocksdb::DB::Open(options_, dbfilename, &db_);
    if (!s.ok()) {
        cerr << "Can't open rocksdb " << dbfilename << endl;
        exit(0);
    } else {
        // RocksDB perf
        rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTime);
        rocksdb::get_perf_context()->Reset();
        rocksdb::get_iostats_context()->Reset();
    }
    // cout << "\nbloom bits:" << bloomBits << "bits\ndirectIO:" << (bool)directIO
    // << "\nseekCompaction:" << (bool)seekCompaction << endl;
}

int RocksDB::Read(const std::string &table, const std::string &key,
                  const std::vector<std::string> *fields,
                  std::vector<KVPair> &result) {
    string value;
    // cerr << "[YCSB] Read op, key = " << key << endl;
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &value);
    // s = db_->Put(rocksdb::WriteOptions(), key, value); // write back
    // cerr << "[YCSB] Read op, value = " << value << endl;
    // outputStream_ << "[YCSB] Read op, key = " << key << ", value = " << value << endl;
    return s.ok();
}

int RocksDB::Scan(const std::string &table, const std::string &key, int len,
                  const std::vector<std::string> *fields,
                  std::vector<std::vector<KVPair>> &result) {
    auto it = db_->NewIterator(rocksdb::ReadOptions());
    it->Seek(key);
    std::string val;
    std::string k;
    int i;
    int cnt = 0;
    for (i = 0; i < len && it->Valid(); i++) {
        k = it->key().ToString();
        val = it->value().ToString();
        it->Next();
        if (val.empty()) cnt++;
    }
    delete it;
    return DB::kOK;
}

int RocksDB::Insert(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values) {
    rocksdb::Status s;
    string fullValue;
    for (long unsigned int i = 0; i < values.size() - 1; i++) {
        fullValue += (values[i].second + ",");
    }
    fullValue += values[values.size() - 1].second;
    s = db_->Put(rocksdb::WriteOptions(), key, fullValue);
    if (!s.ok()) {
        cerr << "insert error" << s.ToString() << "\n"
             << endl;
        exit(0);
    }
    // outputStream_ << "[YCSB] Insert op, key = " << key << ", value = " << fullValue << endl;
    return DB::kOK;
}

int RocksDB::Update(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values) {
    rocksdb::Status s;
    for (KVPair &p : values) {
        s = db_->Merge(rocksdb::WriteOptions(), key, p.second);
        if (!s.ok()) {
            cout << "Merge value failed: " << s.ToString() << endl;
            exit(-1);
        }
        // outputStream_ << "[YCSB] Update op, key = " << key << ", op value = " << p.second << endl;
    }
    // s = db_->Flush(rocksdb::FlushOptions());
    return s.ok();
}

int RocksDB::OverWrite(const std::string &table, const std::string &key,
                       std::vector<KVPair> &values) {
    rocksdb::Status s;
    string fullValue;
    for (long unsigned int i = 0; i < values.size() - 1; i++) {
        fullValue += (values[i].second + ",");
    }
    fullValue += values[values.size() - 1].second;
    s = db_->Put(rocksdb::WriteOptions(), key, fullValue);
    if (!s.ok()) {
        cerr << "OverWrite error" << s.ToString() << endl;
        exit(0);
    }
    // outputStream_ << "[YCSB] Overwrite op, key = " << key << ", value = " << fullValue << endl;
    return DB::kOK;
}

int RocksDB::Delete(const std::string &table, const std::string &key) {
    std::string value;
    rocksdb::Status s = db_->SingleDelete(rocksdb::WriteOptions(), key);  // Undefined result
    return s.ok();
}

void RocksDB::printStats() {
    db_->Flush(rocksdb::FlushOptions());
    cout << "Full merge operation number = " << counter_full << endl;
    cout << "Full merge operation running time = " << totalTimeFull / 1000000.0
         << " s" << endl;
    cout << "Partial merge operation number = " << counter_part << endl;
    cout << "Partial merge operation running time = " << totalTimePart / 1000000.0
         << " s" << endl;
    string stats;
    db_->GetProperty("rocksdb.stats", &stats);
    cout << stats << endl;
    // cout << options_.statistics->ToString() << endl;
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    cout << "Get RocksDB Build-in Perf Context: " << endl;
    cout << rocksdb::get_perf_context()->ToString() << endl;
    cout << "Get RocksDB Build-in I/O Stats Context: " << endl;
    cout << rocksdb::get_iostats_context()->ToString() << endl;
}

RocksDB::~RocksDB() {
    delete db_;
    outputStream_.close();
}
}  // namespace ycsbc
