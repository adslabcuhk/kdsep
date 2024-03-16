#ifndef YCSB_C_EXTRA_CONFIG_H
#define YCSB_C_EXTRA_CONFIG_H

#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <string>

#include "core/db.h"
// #include "leveldb/db.h"

using std::string;

namespace ycsbc {
class ExternDBConfig {
   private:
    boost::property_tree::ptree pt_;
    int bloomBits_;
    bool seekCompaction_;
    bool compression_;
    bool directIO_;
    bool direct_reads_;
    bool useMmap_;
    bool use_pwrite_;
    bool noCompaction_;
    int numThreads_;
    size_t blockCache_;
    size_t blobCacheSize;
    size_t memory_budget_;
    size_t memtable_;
    bool tiered_;
    double GCRatio_;
    bool key_value_separation_;
    bool key_delta_separation_;
    bool blob_db_key_value_separation_;
    uint64_t sst_size;
    uint64_t l1_size;
    uint64_t blob_file_size_;
    uint64_t ds_bucket_size_;
    uint64_t ds_bucket_flush_size_;
    double ds_gc_threshold_;
    double ds_split_gc_threshold_;
    uint64_t ds_kdcache_size_;
    uint64_t ds_bucket_number_;
    uint64_t ds_worker_thread_number_limit;
    uint64_t ds_gc_thread_number_limit;
    bool ds_enable_gc;
    uint64_t write_buffer_size_ = 2 * 1024 * 1024;
    uint64_t prefixTreeBitNumber_;
    uint64_t blockSize;
    uint64_t minBlobSize;
    uint64_t max_kv_size;
    bool parallel_lsm_interface_;
    bool enable_crash_consistency_;
    bool enable_bucket_split_;
    bool enable_bucket_merge_;
    bool enable_index_block_;
    double blobgcforce_;
    bool enable_gc_write_stall_;
    bool test_recovery_;
    uint64_t test_final_scan_ops_;
    uint64_t commit_log_size_;

    struct {
        uint64_t level;
    } debug_;

   public:
    ExternDBConfig(std::string file_path) {
        boost::property_tree::ini_parser::read_ini(file_path, pt_);
        bloomBits_ = pt_.get<int>("config.bloomBits");
        seekCompaction_ = pt_.get<bool>("config.seekCompaction");
        compression_ = pt_.get<bool>("config.compression");
        directIO_ = pt_.get<bool>("config.directIO", true);
        direct_reads_ = pt_.get<bool>("config.directReads", true);
        useMmap_ = pt_.get<bool>("config.useMmap", true);
        use_pwrite_ = pt_.get<bool>("config.usepwrite", false);
        blockCache_ = pt_.get<size_t>("config.blockCache");
        blobCacheSize = pt_.get<size_t>("config.blobCacheSize", 0);
        memory_budget_ = pt_.get<size_t>("config.memory_budget", 4ull * 1024 * 1024 * 1024);
        blobgcforce_ = pt_.get<double>("config.blobgcforce", 1.0);
        memtable_ = pt_.get<size_t>("config.memtable");
        noCompaction_ = pt_.get<bool>("config.noCompaction");
        numThreads_ = pt_.get<int>("config.numThreads");
        tiered_ = pt_.get<bool>("config.tiered");
        GCRatio_ = pt_.get<double>("config.gcRatio");
        key_value_separation_ = pt_.get<bool>("config.keyValueSeparation");
        key_delta_separation_ = pt_.get<bool>("config.keyDeltaSeparation");
        blob_db_key_value_separation_ = pt_.get<bool>("config.blobDbKeyValueSeparation");
        sst_size = pt_.get<uint64_t>("config.sst_size", 65536 * 1024);
        l1_size = pt_.get<uint64_t>("config.l1_size", 262144 * 1024);
        blob_file_size_ = pt_.get<uint64_t>("config.blobFileSize");
        ds_bucket_size_ = pt_.get<uint64_t>("deltaStore.ds_bucket_size");
        ds_bucket_flush_size_ = pt_.get<uint64_t>("deltaStore.ds_bucket_buffer_size");
        ds_gc_threshold_ = pt_.get<double>("deltaStore.ds_gc_thres", 0.9);
        ds_split_gc_threshold_ = pt_.get<double>("deltaStore.ds_split_thres");
        ds_kdcache_size_ = pt_.get<uint64_t>("deltaStore.ds_kdcache_size");
        ds_bucket_number_ = pt_.get<uint64_t>("deltaStore.ds_bucket_num");
        ds_worker_thread_number_limit = pt_.get<uint64_t>("deltaStore.ds_worker_thread_number_limit");
        ds_gc_thread_number_limit = pt_.get<uint64_t>("deltaStore.ds_gc_thread_number_limit");
        debug_.level = pt_.get<uint64_t>("debug.level");
        ds_enable_gc = pt_.get<bool>("deltaStore.deltaStoreEnableGC", true);
        write_buffer_size_ = pt_.get<uint64_t>("config.write_buffer_size", 2 * 1024 * 1024);
        prefixTreeBitNumber_ = pt_.get<uint64_t>("deltaStore.ds_init_bit");
        max_kv_size = pt_.get<uint64_t>("config.max_kv_size", 4096);
        minBlobSize = pt_.get<uint64_t>("rocksdb.minBlobSize", 0);
        blockSize = pt_.get<uint64_t>("rocksdb.blockSize", 65536);
        parallel_lsm_interface_ = pt_.get<bool>("config.parallel_lsm_tree_interface", true);
        enable_crash_consistency_ = pt_.get<bool>("config.crash_consistency", false);
        enable_bucket_split_ = pt_.get<bool>("config.enable_bucket_split", true); 
        enable_bucket_merge_ = pt_.get<bool>("config.enable_bucket_merge", true);
        enable_index_block_ = pt_.get<bool>("config.enable_index_block", true);
        enable_gc_write_stall_ = pt_.get<bool>("config.enable_gc_write_stall", true);
        commit_log_size_ = pt_.get<uint64_t>("config.commit_log_size", 1024 *1024 * 1024);
        test_final_scan_ops_ = pt_.get<uint64_t>("config.test_final_scan_ops", 0);
	test_recovery_ = pt_.get<bool>("debug.test_recovery", false);
    }

    int getBloomBits() {
        return bloomBits_;
    }
    bool getSeekCompaction() {
        return seekCompaction_;
    }
    bool getCompression() {
        return compression_;
    }
    bool getDirectIO() {
        return directIO_;
    }
    bool getDirectReads() {
        return direct_reads_;
    }
    bool getUseMmap() {
        return useMmap_;
    }
    bool getUsePwrite() {
        return use_pwrite_;
    }
    int getNumThreads() {
        return numThreads_;
    }
    size_t getBlockCache() {
        return blockCache_;
    }
    size_t getBlobCacheSize() {
        return blobCacheSize;
    }
    size_t getMemoryBudget() {
        return memory_budget_;
    }
    size_t getMemtable() {
        return memtable_;
    }
    bool getNoCompaction() {
        return noCompaction_;
    }
    double getGCRatio() {
        return GCRatio_;
    }
    bool getTiered() {
        return tiered_;
    }
    bool getKeyValueSeparation() {
        return key_value_separation_;
    }
    bool getKeyDeltaSeparation() {
        return key_delta_separation_;
    }
    bool getBlobDbKeyValueSeparation() {
        return blob_db_key_value_separation_;
    }
    uint64_t getSSTSize() {
        return sst_size;
    }
    uint64_t getL1Size() {
        return l1_size;
    }
    uint64_t getBlobFileSize() {
        return blob_file_size_;
    }
    uint64_t getDeltaStoreBucketSize() {
        return ds_bucket_size_;
    }

    uint64_t getDeltaStoreBucketFlushSize() {
        return ds_bucket_flush_size_;
    }

    double getDeltaStoreGCThreshold() {
        return ds_gc_threshold_;
    }

    double getDeltaStoreSplitGCThreshold() {
        return ds_split_gc_threshold_;
    }

    uint64_t getDSKDCacheSize() {
        return ds_kdcache_size_;
    }

    uint64_t getDeltaStoreMaxBucketNumber() {
        return ds_bucket_number_;
    }

    uint64_t getDeltaStoreOpWorkerThreadNumber() {
        return ds_worker_thread_number_limit;
    }

    uint64_t getDeltaStoreGCWorkerThreadNumber() {
        return ds_gc_thread_number_limit;
    }

    uint64_t getDebugLevel() {
        return debug_.level;
    }

    bool getDeltaStoreGCEnableStatus() {
        return ds_enable_gc;
    }

    uint64_t getKDSepWriteBufferSize() {
        return write_buffer_size_;
    }

    uint64_t getPrefixTreeBitNumber() {
        return prefixTreeBitNumber_;
    }

    uint64_t getBlockSize() {
        return blockSize;
    }
    uint64_t getMaxKeyValueSize() {
        return max_kv_size;
    }
    uint64_t getMinBlobSize() {
	return minBlobSize;
    }
    bool getParallelLsmTreeInterface() {
        return parallel_lsm_interface_;
    }
    bool getEnableCrashConsistency() {
        return enable_crash_consistency_;
    }
    bool getEnableBucketMerge() {
        return enable_bucket_merge_;
    }
    bool getEnableBucketSplit() {
        return enable_bucket_split_;
    }
    bool getEnableIndexBlock() {
        return enable_index_block_;
    }
    bool getEnableGcWriteStall() {
        return enable_gc_write_stall_;
    }
    bool getTestRecovery() {
        return test_recovery_;
    }
    uint64_t getCommitLogSize() {
        return commit_log_size_;
    }
    bool getUseFinalScan() {
        return test_final_scan_ops_ > 0;
    }
    uint64_t getFinalScanOps() {
        return test_final_scan_ops_;
    }
    double getBlobGCForce() {
        return blobgcforce_;
    }
};
}  // namespace ycsbc

#endif  // YCSB_C_EXTRA_CONFIG_H
