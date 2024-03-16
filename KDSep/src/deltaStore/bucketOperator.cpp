#include "deltaStore/bucketOperator.hpp"
#include "deltaStore/bucketManager.hpp"
#include "utils/bucketKeyFilter.hpp"
#include "utils/bucketIndexBlock.hpp"
#include "utils/statsRecorder.hh"

namespace KDSEP_NAMESPACE {

BucketOperator::BucketOperator(KDSepOptions* options, string workingDirStr,
        shared_ptr<BucketManager> bucketManager)
{
    perFileFlushBufferSizeLimit_ = options->deltaStore_bucket_flush_buffer_size_limit_;
    perFileGCSizeLimit_ = options->deltaStore_gc_threshold * options->deltaStore_bucket_size_;
    singleFileSizeLimit_ = options->deltaStore_bucket_size_;
    enable_crash_consistency_ = options->enable_crash_consistency;
    if (options->deltaStore_op_worker_thread_number_limit_ >= 2) {
        workerThreads_.reset(new boost::asio::thread_pool(options->deltaStore_op_worker_thread_number_limit_));
        num_threads_ = 0;
        debug_info("Total thread number for operationWorker >= 2, use multithread operation%s\n", "");
    }
    if (options->kd_cache != nullptr) {
        kd_cache_ = options->kd_cache;
    }
    enable_gc_ = options->enable_bucket_gc;
    enable_index_block_ = options->enable_index_block;
    bucket_manager_ = bucketManager;
    working_dir_ = workingDirStr;
    operationNumberThresholdForForcedSingleFileGC_ = options->deltaStore_operationNumberForForcedSingleFileGCThreshold_;
    if (options->deltaStore_op_worker_thread_number_limit_ >= 2) {
        syncStatistics_ = true;
        workerThreadNumber_ = options->deltaStore_op_worker_thread_number_limit_;
    }
    KDSepMergeOperatorPtr_ = options->KDSep_merge_operation_ptr;
    write_stall_ = options->write_stall;
    fprintf(stdout, "read use partial merged delta in the KD cache!\n");
    fprintf(stdout, "put use partial merged delta in the KD cache!\n");
//    fprintf(stdout, "use all deltas in the KD cache!\n");
}

BucketOperator::~BucketOperator()
{
    if (num_threads_ > 0) {
        cerr << "Warning: " << num_threads_ << " threads are still running" <<
            endl;
        while (num_threads_ > 0) {
            asm volatile("");
        }
    }
}

bool BucketOperator::setJobDone()
{
    should_exit_ = true;
    return true;
}

bool BucketOperator::putIntoJobQueue(deltaStoreOpHandler* op_hdl)
{
    startJob(op_hdl);
    return true;
}

bool BucketOperator::startJob(deltaStoreOpHandler* op_hdl)
{
    boost::asio::post(*workerThreads_, 
            boost::bind(
                &BucketOperator::singleOperation,
                this,
                op_hdl));
    return true;
}

bool BucketOperator::waitOperationHandlerDone(deltaStoreOpHandler* op_hdl, bool need_delete) {
    while (op_hdl->job_done == kNotDone) {
        asm volatile("");
    }
    if (op_hdl->job_done == kDone) {
        if (need_delete) {
            delete op_hdl;
        }
        return true;
    } else {
        debug_error("[ERROR] Process %d operation ID = %lu\n",
                (int)op_hdl->op_type, op_hdl->bucket->file_id);
        if (need_delete) {
            delete op_hdl;
        }
        return false;
    }
}

uint64_t BucketOperator::readWholeFile(BucketHandler* bucket, char** read_buf)
{
    auto& file_size = bucket->total_object_bytes;
    debug_trace("Read content from file ID = %lu\n", bucket->file_id);
    *read_buf = new char[file_size];
    FileOpStatus read_s;
    STAT_PROCESS(
            read_s = bucket->io_ptr->readFile(*read_buf, file_size),
            StatsType::KDSep_HASHSTORE_GET_IO_ALL);
    StatsRecorder::getInstance()->DeltaOPBytesRead(bucket->total_on_disk_bytes,
            file_size, syncStatistics_);
    if (read_s.success_ == false) {
        debug_error("[ERROR] Read bucket error, internal file operation fault,"
                " could not read content from file ID = %lu\n",
                bucket->file_id);
        return 0;
    } else {
        return file_size;
    }
}

uint64_t BucketOperator::readWholeFileOrRetrieveFromCache(BucketHandler* bucket,
        char** buf) {
    static int numHit = 0;
    static int numMiss = 0;
    if (numHit + numMiss > 0 && (numHit + numMiss) % 10000 == 0) {
        debug_error("Bucket cache hit rate = %d/%d = %f\n", 
                numHit, numHit + numMiss, 
                (float)numHit / (numHit + numMiss + 0.0001));
    }
    bool readFromFileStatus;
    auto& file_size = bucket->total_object_bytes;
    str_t cachedBucket(nullptr, 0);
    if (kd_cache_ != nullptr) {
        str_t str_key = str_t(const_cast<char*>(bucket->key.c_str()),
                bucket->key.size());
        struct timeval tv;
        gettimeofday(&tv, NULL);
        cachedBucket = kd_cache_->getFromBucketCache(str_key);
        if (cachedBucket.data_ != nullptr) {
            numHit++;
            if (file_size != cachedBucket.size_) { 
                debug_error("[ERROR] cached bucket size %u not equal to"
                        " file size %lu for key %s\n", cachedBucket.size_,
                        file_size, bucket->key.c_str());
                exit(1);
            } 
            *buf = new char[file_size];
            memcpy(*buf, cachedBucket.data_, file_size);
            readFromFileStatus = true;
        } else {
            numMiss++;
            readFromFileStatus = readWholeFile(bucket, buf);
            char* cachedBuf = new char[file_size];
            memcpy(cachedBuf, *buf, file_size);
            str_t insert_bucket(cachedBuf, file_size);
            kd_cache_->updateCacheWholeBucket(str_key, insert_bucket);
        }
    } else {
        readFromFileStatus = readWholeFile(bucket, buf);
    }

    if (readFromFileStatus) {
        return file_size;
    }
    return 0;
}

uint64_t BucketOperator::readSortedPart(
        BucketHandler* bucket, const string_view& key_view, 
        char** read_buf, bool& key_exists) {
    if (bucket->index_block->GetSortedPartSize() == 0) {
        debug_error("No index block but read the sorted part %s\n", "");
        exit(1);
    }

    pair<uint64_t, uint64_t> offlen = bucket->index_block->Search(key_view);
    if (offlen.second == 0) {
        key_exists = false;
        return 0;
    } else {
        key_exists = true;
    }
    FileOpStatus read_s;

    uint64_t read_file_offset = offlen.first; 
    *read_buf = new char[offlen.second];

    STAT_PROCESS(
            read_s = bucket->io_ptr->positionedReadFile(
                *read_buf, read_file_offset, offlen.second),
            StatsType::KDSep_HASHSTORE_GET_IO_SORTED);
    StatsRecorder::getInstance()->DeltaOPBytesRead(
            read_s.physicalSize_, read_s.logicalSize_, syncStatistics_);
    return read_s.logicalSize_;
}

uint64_t BucketOperator::readUnsortedPart(
        BucketHandler* bucket, char** read_buf)
{
    auto sorted_part_size = bucket->index_block->GetSortedPartSize(); 
    auto file_size = bucket->total_object_bytes;
    auto unsorted_part_off = bucket->unsorted_part_offset;
        // Fixed to:
        //sorted_part_size + header_sz;
    auto unsorted_part_size = file_size - unsorted_part_off;

    *read_buf = new char[unsorted_part_size];

    debug_trace("Read content from file ID = %lu\n", bucket->file_id);
    FileOpStatus read_s;
    STAT_PROCESS(read_s = bucket->io_ptr->positionedReadFile(
               *read_buf, unsorted_part_off, unsorted_part_size), 
            StatsType::KDSep_HASHSTORE_GET_IO_UNSORTED);
    StatsRecorder::getInstance()->DeltaOPBytesRead(
            read_s.physicalSize_, read_s.logicalSize_,
            syncStatistics_);
    if (read_s.success_ == false) {
        debug_error("[ERROR] internal file operation fault,"
                " could not read content from file ID = %lu,"
                " file size %lu unsorted part off %lu,"
                " sorted %lu\n",
                bucket->file_id,
                file_size, unsorted_part_off, 
                sorted_part_size);
        return 0;
    } else {
        return read_s.logicalSize_;
    }

}

uint64_t BucketOperator::readBothParts(
        BucketHandler* bucket, const string_view& key_view, 
        char** read_buf) {
    if (bucket->index_block->GetSortedPartSize() == 0) {
        debug_error("No index block but read the sorted part %s\n", "");
        exit(1);
    }

    pair<uint64_t, uint64_t> offlen = bucket->index_block->Search(key_view);

//    if (offlen.first == 0) 
    // Wrong. Fix later
    if (offlen.second == 0) {
        // not exist in the sorted part
        return readUnsortedPart(bucket, read_buf);
    }

    FileOpStatus read_s;

    uint64_t offset = offlen.first; 
    uint64_t len = bucket->total_object_bytes - offset; 

    *read_buf = new char[len];

    STAT_PROCESS(
            read_s = bucket->io_ptr->positionedReadFile(
                *read_buf, offset, len),
            StatsType::KDSep_HASHSTORE_GET_IO_BOTH);
    StatsRecorder::getInstance()->DeltaOPBytesRead(
            read_s.physicalSize_, read_s.logicalSize_, syncStatistics_);
    return read_s.logicalSize_;
}

bool BucketOperator::readAndProcessWholeFile(
        BucketHandler* bucket, string& key,
        vector<string_view>& kd_list, char** buf)
{
    auto& file_size = bucket->total_object_bytes;
    bool readFromFileStatus = readWholeFile(bucket, buf);

    kd_list.clear();
    if (readFromFileStatus == false) {
        debug_error("[ERROR] Could not read from file for key = %s\n",
                key.c_str());
        exit(1);
        return false;
    }

    string_view key_view(key);
    uint64_t process_delta_num = 0;

    STAT_PROCESS(process_delta_num = processReadContentToValueLists(
                *buf, file_size, kd_list, key_view),
            StatsType::KDSep_HASHSTORE_GET_PROCESS);
    if (process_delta_num != bucket->total_object_cnt) {
        debug_error("[ERROR] processed object number during read = %lu, not"
                " equal to object number in metadata = %lu\n",
                process_delta_num, bucket->total_object_cnt);
        return false;
    }

    return true;
}

bool BucketOperator::readAndProcessWholeFileWithCache(
    BucketHandler* bucket, map<string_view, vector<string_view>>& kd_lists,
    char** buf) {

    auto& file_size = bucket->total_object_bytes;
    uint64_t read_size = readWholeFileOrRetrieveFromCache(bucket, buf);
    kd_lists.clear();

    if (read_size == 0) {
        debug_error("[ERROR] Could not read from file bucket id %lu\n",
                bucket->file_id);
        exit(1);
    }

    uint64_t process_delta_num = 0;
    STAT_PROCESS(process_delta_num = processReadContentToValueLists(
                *buf, file_size, kd_lists),
            StatsType::KDSep_HASHSTORE_GET_PROCESS);
    
    if (process_delta_num != bucket->total_object_cnt) {
        debug_error("[ERROR] processed object number during read = %lu, not"
                " equal to object number in metadata = %lu\n",
                process_delta_num, bucket->total_object_cnt);
        return false;
    }
    return true;
}

// for multiGet
bool BucketOperator::readAndProcessWholeFileKeyList(
        BucketHandler* bucket, vector<string*>* keys,
        vector<vector<string_view>>& kd_lists, char** buf)
{
    auto& file_size = bucket->total_object_bytes;
    bool readFromFileStatus = readWholeFile(bucket, buf);
        // don't want to test this
        //readWholeFileOrRetrieveFromCache(bucket, buf);

    kd_lists.clear();
    if (readFromFileStatus == false) {
        debug_error("[ERROR] Could not read from file for key list %lu\n",
                keys->size());
        exit(1);
    }

    vector<string_view> key_views;
    key_views.resize(keys->size());
    for (int i = 0; i < keys->size(); i++) {
        key_views[i] = string_view((*(*keys)[i]));
    }

    uint64_t process_delta_num = 0;

    STAT_PROCESS(process_delta_num = processReadContentToValueListsWithKeyList(
                *buf, file_size, kd_lists, key_views),
            StatsType::KDSep_HASHSTORE_GET_PROCESS);
    if (process_delta_num != bucket->total_object_cnt) {
        debug_error("[ERROR] processed object number during read = %lu, not"
                " equal to object number in metadata = %lu\n",
                process_delta_num, bucket->total_object_cnt);
        return false;
    }

    return true;
}

bool BucketOperator::readAndProcessSortedPart(
        BucketHandler* bucket, string& key,
        vector<string_view>& kd_list, char** buf)
{
    string_view key_view(key);
    bool key_exists;

    kd_list.clear();
    *buf = nullptr;
    uint64_t read_sz = readSortedPart(bucket, key_view, buf, key_exists);

    if (key_exists == false) {
        // The key does not exist in the index block, skip
        return true;
    }

    kd_list.clear();
    if (read_sz == 0) {
        debug_error("[ERROR] Could not read from file for key = %s\n",
                key.c_str());
        return false;
    }

    uint64_t process_delta_num = 0;
    STAT_PROCESS(process_delta_num = processReadContentToValueLists(
                *buf, read_sz, kd_list, key_view),
            StatsType::KDSep_HASHSTORE_GET_PROCESS);

    if (process_delta_num == 0) {
        debug_error("[ERROR] processed object num = 0, read %lu fs %lu\n", 
                read_sz, bucket->total_object_bytes);
        exit(1);
    }

    return true;
}

bool BucketOperator::readAndProcessBothParts(
        BucketHandler* bucket, string& key,
        vector<string_view>& kd_list, char** buf)
{
    string_view key_view(key);

    kd_list.clear();
    *buf = nullptr;
    uint64_t read_sz = readBothParts(bucket, key_view, buf);

    if (read_sz == 0) {
        debug_error("[ERROR] Could not read from file for key = %s\n",
                key.c_str());
        return false;
    }

    uint64_t process_delta_num = 0;
    STAT_PROCESS(process_delta_num = processReadContentToValueLists(
                *buf, read_sz, kd_list, key_view),
            StatsType::KDSep_HASHSTORE_GET_PROCESS);

    if (process_delta_num == 0) {
        debug_error("[ERROR] no object, read %lu fs %lu, id %lu\n", 
                read_sz, bucket->total_object_bytes,
                bucket->file_id);
        exit(1);
    }

    return true;
}

bool BucketOperator::readAndProcessUnsortedPart(
        BucketHandler* bucket, string& key,
        vector<string_view>& kd_list, char** buf)
{
    string_view key_view(key);
    uint64_t read_sz = readUnsortedPart(bucket, buf);

    kd_list.clear();
    if (read_sz == 0) {
        debug_error("[ERROR] Could not read from file for key = %s\n",
                key.c_str());
        return false;
    }

    uint64_t process_delta_num = 0;
    STAT_PROCESS(process_delta_num = processReadContentToValueLists(
                *buf, read_sz, kd_list, key_view),
            StatsType::KDSep_HASHSTORE_GET_PROCESS);

    if (process_delta_num == 0) {
        debug_error("[ERROR] processed object num = 0, "
		"read %lu fs %lu fid %lu, unsorted part off %lu\n", 
		read_sz, bucket->total_object_bytes, bucket->file_id,
		bucket->unsorted_part_offset);
        exit(1);
    }

    return true;
}

uint64_t BucketOperator::processReadContentToValueLists(
        char* read_buf, uint64_t read_buf_size, 
        unordered_map<str_t, vector<str_t>, mapHashKeyForStr_t,
        mapEqualKeForStr_t>& resultMapInternal)
{
    // Do not consider header
    uint64_t i = 0;
    uint64_t process_delta_num = 0;
    size_t header_sz = sizeof(KDRecordHeader);;
    KDRecordHeader header;
    bool has_gc_done = false;
    uint64_t gc_done_offset = 0;
    while (i < read_buf_size) {
        process_delta_num++;
        if (use_varint_d_header == false) {
            memcpy(&header, read_buf + i, header_sz);
        } else {
            header = GetDeltaHeaderVarint(read_buf + i, header_sz);
        }
        i += header_sz;
        if (header.is_gc_done_ == true) {
            // skip since it is gc flag, no content.
	    if (has_gc_done) {
		debug_error("read error: gc done appeared before"
			"%lu %lu\n", gc_done_offset, i);
		exit(1);
	    }
	    has_gc_done = true;
	    gc_done_offset = i;
            continue;
        }
        // get key str_t
        str_t currentKey(read_buf + i, header.key_size_);
        i += header.key_size_;
        if (header.is_anchor_ == true) {
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).clear();
            }
        } else {
            str_t currentValue(read_buf + i, header.value_size_);
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).push_back(currentValue);
            } else {
                vector<str_t> newValuesRelatedToCurrentKeyVec;
                newValuesRelatedToCurrentKeyVec.push_back(currentValue);
                resultMapInternal.insert(make_pair(currentKey, newValuesRelatedToCurrentKeyVec));
            }
            i += header.value_size_;
        }
    }

    if (i > read_buf_size) {
        debug_error("error i: %lu %lu\n", i, read_buf_size);
        exit(1);
    }
    return process_delta_num;
}

uint64_t BucketOperator::processReadContentToValueLists(
        char* read_buf, uint64_t read_buf_size, 
        unordered_map<string_view, vector<string_view>>& resultMapInternal,
        const string_view& key)
{
    uint64_t i = 0;
    uint64_t process_delta_num = 0;
    size_t header_sz = sizeof(KDRecordHeader);
    KDRecordHeader header;
    bool has_gc_done = false;
    uint64_t gc_done_offset = 0;
    while (i != read_buf_size) {
        process_delta_num++;
        if (use_varint_d_header == false) {
            memcpy(&header, read_buf + i, header_sz);
        } else {
            header = GetDeltaHeaderVarint(read_buf + i, header_sz);
        }
        i += header_sz;
        if (header.is_gc_done_ == true) {
            // skip since it is gc flag, no content.
	    if (has_gc_done) {
		debug_error("read error: gc done appeared before"
			"%lu %lu\n", gc_done_offset, i);
		exit(1);
	    }
	    has_gc_done = true;
	    gc_done_offset = i;
            continue;
        }
        // get key 
        string_view currentKey(read_buf + i, header.key_size_);
        if (key.size() != currentKey.size() || memcmp(key.data(), currentKey.data(), key.size()) != 0) {
            i += header.key_size_ + ((header.is_anchor_) ? 0 : header.value_size_);
            continue;
        }

        i += header.key_size_;
        if (header.is_anchor_ == true) {
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).clear();
            }
        } else {
            string_view currentValue(read_buf + i, header.value_size_);
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).push_back(currentValue);
            } else {
                vector<string_view> newValuesRelatedToCurrentKeyVec;
                newValuesRelatedToCurrentKeyVec.push_back(currentValue);
                resultMapInternal.insert(make_pair(currentKey, newValuesRelatedToCurrentKeyVec));
            }
            i += header.value_size_;
        }
    }
    return process_delta_num;
}

uint64_t BucketOperator::processReadContentToValueLists(
        char* read_buf, uint64_t read_buf_size, vector<string_view>& kd_list,
        const string_view& key)
{
    uint64_t i = 0;
    uint64_t processed_delta_num = 0;
    size_t header_sz = sizeof(KDRecordHeader);
    KDRecordHeader header;
    bool has_gc_done = false;
    uint64_t gc_done_offset = 0;
    while (i < read_buf_size) {
        processed_delta_num++;
        if (use_varint_d_header == false) {
            memcpy(&header, read_buf + i, header_sz);
        } else {
            header = GetDeltaHeaderVarint(read_buf + i, header_sz);
        }
        i += header_sz;
        if (header.is_gc_done_ == true) {
            // skip since it is gc flag, no content.
	    if (has_gc_done) {
		debug_error("read error: gc done appeared before"
			"%lu %lu\n", gc_done_offset, i);
		exit(1);
	    }
	    has_gc_done = true;
	    gc_done_offset = i;
            continue;
        }

        // get key 
        string_view currentKey(read_buf + i, header.key_size_);
        if (key != currentKey) {
            i += header.key_size_ +
                ((header.is_anchor_) ? 0 :
                 header.value_size_);
            continue;
        }

        i += header.key_size_;
        if (header.is_anchor_ == true) {
            kd_list.clear();
        } else {
            string_view currentValue(read_buf + i, header.value_size_);
            kd_list.push_back(currentValue);
            i += header.value_size_;
        }
    }
    if (i > read_buf_size) {
        debug_error("[ERROR] read buf index error! %lu v.s. %lu"
	       " already processed %lu\n", 
                i, read_buf_size, processed_delta_num);
        return 0;
    }
    return processed_delta_num;
}

uint64_t BucketOperator::processReadContentToValueListsWithKeyList(
    char* read_buf, uint64_t read_buf_size,
    vector<vector<string_view>>& kd_lists,
    const vector<string_view>& keys)
{
    kd_lists.clear();
    kd_lists.resize(keys.size());

    uint64_t i = 0;
    uint64_t processed_delta_num = 0;
    size_t header_sz = sizeof(KDRecordHeader);
    KDRecordHeader header;
    bool has_gc_done = false;
    uint64_t gc_done_offset = 0;
    while (i < read_buf_size) {
        processed_delta_num++;
        if (use_varint_d_header == false) {
            memcpy(&header, read_buf + i, header_sz);
        } else {
            header = GetDeltaHeaderVarint(read_buf + i, header_sz);
        }
        i += header_sz;
        if (header.is_gc_done_ == true) {
            // skip since it is gc flag, no content.
	    if (has_gc_done) {
		debug_error("read error: gc done appeared before"
			" %lu %lu\n", gc_done_offset, i);
		exit(1);
	    }
	    has_gc_done = true;
	    gc_done_offset = i;
            continue;
        }

        // get key 
        string_view currentKey(read_buf + i, header.key_size_);

        int key_i = 0;
        bool has_key = false;
        for (key_i = 0; key_i < (int)keys.size(); key_i++) {
            string_view key = keys[key_i];
            if (key != currentKey) {
                continue;
            }

            has_key = true;
            i += header.key_size_;
            if (header.is_anchor_ == false) {
                string_view currentValue(read_buf + i, header.value_size_);
                kd_lists[key_i].push_back(currentValue);
                i += header.value_size_;
            }
        }

        if (has_key == false) {
	    i += header.key_size_ +
		((header.is_anchor_) ? 0 :
		 header.value_size_);
	}
    }
    if (i > read_buf_size) {
        debug_error("[ERROR] read buf index error! %lu v.s. %lu\n", 
                i, read_buf_size);
        return 0;
    }
    return processed_delta_num;
}

uint64_t BucketOperator::processReadContentToValueLists(
    char* read_buf, uint64_t read_buf_size,
    map<string_view, vector<string_view>>& kd_lists)
{
    kd_lists.clear();

    uint64_t i = 0;
    uint64_t processed_delta_num = 0;
    size_t header_sz = sizeof(KDRecordHeader);
    KDRecordHeader header;
    bool has_gc_done = false;
    uint64_t gc_done_offset = 0;
    while (i < read_buf_size) {
        processed_delta_num++;
        if (use_varint_d_header == false) {
            memcpy(&header, read_buf + i, header_sz);
        } else {
            header = GetDeltaHeaderVarint(read_buf + i, header_sz);
        }
        i += header_sz;
        if (header.is_gc_done_ == true) {
            // skip since it is gc flag, no content.
            if (has_gc_done) {
                debug_error("read error: gc done appeared before"
                            " %lu %lu\n",
                    gc_done_offset, i);
                exit(1);
            }
            has_gc_done = true;
            gc_done_offset = i;
            continue;
        }

        // get key 
        string_view currentKey(read_buf + i, header.key_size_);

        i += header.key_size_;
        if (header.is_anchor_ == false) {
            string_view currentValue(read_buf + i, header.value_size_);
            kd_lists[currentKey].push_back(currentValue);
            i += header.value_size_;
        } else {
            // is an anchor, clean all KD pairs for this key
            if (kd_lists.count(currentKey)) {
                kd_lists.at(currentKey).clear();
                kd_lists.erase(currentKey);
            }
        }
    }
    if (i > read_buf_size) {
        debug_error("[ERROR] read buf index error! %lu v.s. %lu\n", 
                i, read_buf_size);
        return 0;
    }
    return processed_delta_num;
}

bool BucketOperator::writeToFile(
        BucketHandler* bucket, char* write_buf, uint64_t
        write_buf_size, uint64_t contentObjectNumber, bool need_flush)
{
    debug_trace("Write content to file ID = %lu\n", bucket->file_id);
    FileOpStatus status;
    if (need_flush == false) {
	STAT_PROCESS(status = bucket->io_ptr->writeFile(
		    write_buf, write_buf_size),
		StatsType::KDSep_HASHSTORE_PUT_IO_TRAFFIC);
    } else {
	STAT_PROCESS(status = bucket->io_ptr->writeAndFlushFile(
		    write_buf, write_buf_size),
		StatsType::KDSep_HASHSTORE_PUT_IO_TRAFFIC);
    }

    StatsRecorder::getInstance()->DeltaOPBytesWrite(
	    status.physicalSize_, status.logicalSize_, syncStatistics_);
    if (status.success_ == false) {
        debug_error("[ERROR] Write bucket error, internal file operation "
                "fault, could not write content to file ID = %lu, "
                " logical size = %lu, physical size = %lu\n", 
                bucket->file_id, bucket->total_object_bytes,
                bucket->total_on_disk_bytes);
        return false;
    } else {
        // update metadata
        bucket->total_object_bytes += write_buf_size;
        bucket->total_on_disk_bytes += status.physicalSize_;
        bucket->total_object_cnt += contentObjectNumber;
        return true;
    }
}

bool BucketOperator::operationMultiPut(deltaStoreOpHandler* op_hdl, 
        bool& gc_pushed)
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    auto& bucket = op_hdl->bucket;

    if (bucket->io_ptr->isFileOpen() == false) {
        // prepare write buffer, file not open, may load, skip;
        bool onlyAnchorFlag = true;
        for (auto index = 0; index < op_hdl->multiput_op.size; index++) {
            str_t currentKeyStr(op_hdl->multiput_op.objects[index].keyPtr_, 
                    op_hdl->multiput_op.objects[index].keySize_);
            if (op_hdl->multiput_op.objects[index].isAnchorFlag_ == true) {
                if (bucket->sorted_filter->MayExist(currentKeyStr) == true) {
                    bucket->filter->Insert(currentKeyStr);
                    bucket->filter->Erase(currentKeyStr); // Add the count
                } else {
                    if (bucket->filter->MayExist(currentKeyStr) == true) {
                        bucket->filter->Erase(currentKeyStr);
                    }
                }
            } else {
                onlyAnchorFlag = false;
                bucket->filter->Insert(currentKeyStr);
            }
        }
        if (onlyAnchorFlag == true) {
            debug_info("Only contains anchors for file ID = %lu, and file is not opened, skip\n", bucket->file_id);
            return true;
        }
    } else {
        for (auto index = 0; index < op_hdl->multiput_op.size; index++) {
            str_t currentKeyStr(op_hdl->multiput_op.objects[index].keyPtr_, 
                    op_hdl->multiput_op.objects[index].keySize_);
            if (op_hdl->multiput_op.objects[index].isAnchorFlag_ == true) {
                if (bucket->sorted_filter->MayExist(currentKeyStr) == true) {
                    bucket->filter->Insert(currentKeyStr);
                    bucket->filter->Erase(currentKeyStr); // Add the count
                } else {
                    if (bucket->filter->MayExist(currentKeyStr) == true) {
                        bucket->filter->Erase(currentKeyStr);
                    }
                }
            } else {
                bucket->filter->Insert(currentKeyStr);
            }
        }
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_UPDATE_FILTER, tv);

    gettimeofday(&tv, 0);
    uint64_t targetWriteBufferSize = 0;
    if (bucket->io_ptr->isFileOpen() == false) {
        string targetFilePath = working_dir_ + "/" + to_string(bucket->file_id) + ".delta";
        if (std::filesystem::exists(targetFilePath) == false) {
            bucket->io_ptr->createThenOpenFile(targetFilePath);
        } else {
            bucket->io_ptr->openFile(targetFilePath);
        }
    }
    // leave more space for the buffer
    for (auto i = 0; i < op_hdl->multiput_op.size; i++) {
        targetWriteBufferSize += (sizeof(KDRecordHeader) + 
                op_hdl->multiput_op.objects[i].keySize_);
        if (op_hdl->multiput_op.objects[i].isAnchorFlag_ == true) {
            continue;
        } else {
            targetWriteBufferSize += op_hdl->multiput_op.objects[i].valueSize_;
        }
    }

    char write_buf[targetWriteBufferSize];
    uint64_t write_i = 0;

    StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_PREPARE_FILE_HEADER, tv);
    gettimeofday(&tv, 0);

    KDRecordHeader header;
    size_t header_sz = sizeof(KDRecordHeader);
    for (auto i = 0; i < op_hdl->multiput_op.size; i++) {
        auto& obj = op_hdl->multiput_op.objects[i];

        // write header
        header.is_anchor_ = obj.isAnchorFlag_;
        header.key_size_ = obj.keySize_;
        header.value_size_ = obj.valueSize_;
        header.seq_num = obj.seq_num;
//	if (bucket->unsorted_part_offset > 0) {
//	    debug_error("fid %lu header key %d value %d anchor %d\n",
//		    bucket->file_id, header.key_size_,
//		    header.value_size_, (int)header.is_anchor_);
//	}
        if (use_varint_d_header == false) {
            copyInc(write_buf, write_i, &header, header_sz);
        } else {
            write_i += PutDeltaHeaderVarint(write_buf + write_i, header);
        }

        // write key
        copyInc(write_buf, write_i, obj.keyPtr_, header.key_size_);

        // write value (if anchor)
        if (header.is_anchor_ == true) {
            continue;
        } else {
            copyInc(write_buf, write_i, obj.valuePtr_, header.value_size_);
        }
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_PREPARE_FILE_CONTENT, tv);
    uint64_t targetObjectNumber = op_hdl->multiput_op.size;

    // write content
    bool writeContentStatus = true;

    // before write, check
    if (bucket->io_ptr->canWriteFile(write_i) == false) {
        // directly do GC on this file. 
        // can skip the write stall check, because there will be no write back
        bucket->extra_wb = new char[write_i]; 
        bucket->extra_wb_size = write_i;
        memcpy(bucket->extra_wb, write_buf, write_i);
        bool ret;
        if (op_hdl->need_flush) {
            ret = pushGcIfNeeded(op_hdl);
        } else {
            ret = pushGcIfNeeded(bucket);
        } 
        if (ret == false) {
            debug_error("[ERROR] target file %lu exceed limit %lu, extra "
                    "write buffer %lu, total bytes %lu, but no GC\n",
                    bucket->file_id, singleFileSizeLimit_, write_i,
                    bucket->total_object_bytes);
            exit(1);
        }
        gc_pushed = true;
    } else {
        STAT_PROCESS(writeContentStatus = writeToFile(bucket,
                    write_buf, write_i, targetObjectNumber,
                    op_hdl->need_flush),
                StatsType::DS_WRITE_FUNCTION);
    }

    if (writeContentStatus == false) {
        debug_error("[ERROR] Could not write content to file, target file ID"
                " = %lu, content size = %lu, content bytes number = %lu\n",
                bucket->file_id, targetObjectNumber, write_i);
        exit(1);
        return false;
    } else {
        // insert to cache if need
        struct timeval tv;
        gettimeofday(&tv, 0);
        if (kd_cache_ != nullptr) {
            struct timeval tv;
            gettimeofday(&tv, 0);

            for (uint32_t i = 0; i < op_hdl->multiput_op.size; i++) {
                auto& it = op_hdl->multiput_op.objects[i];
                updateKDCacheIfExist(str_t(it.keyPtr_, it.keySize_),
                        str_t(it.valuePtr_, it.valueSize_),
                        it.isAnchorFlag_);
            }
        }
        StatsRecorder::getInstance()->timeProcess(
                StatsType::DS_MULTIPUT_INSERT_CACHE, tv);
        return true;
    }
}

bool BucketOperator::operationFlush(deltaStoreOpHandler* op_hdl, bool& gc_pushed)
{
    struct timeval tv;
    gettimeofday(&tv, 0);

    if (op_hdl->bucket->io_ptr->isFileOpen() == false) {
        // prepare write buffer, file not open, may load, skip;
        return true;
    }

    auto bucket = op_hdl->bucket;
    if (bucket->io_ptr->canWriteFile(0) == false) {
        // directly do GC on this file. 
        bool ret;
        if (enable_crash_consistency_) {
            op_hdl->need_flush = true;
            ret = pushGcIfNeeded(op_hdl);
        } else {
            ret = pushGcIfNeeded(bucket);
        }
        if (ret == false) {
            debug_error("[ERROR] target file %lu exceed limit %lu, "
                    "total bytes %lu, but no GC\n",
                    bucket->file_id, singleFileSizeLimit_,
                    bucket->total_object_bytes);
            exit(1);
        }
        gc_pushed = true;
    } else {
        // write content
        FileOpStatus status = op_hdl->bucket->io_ptr->flushFile();
        //    debug_error("flush file %lu\n", op_hdl->bucket->file_id);
        if (status.success_ == false) {
            debug_error("[ERROR] Could not flush to file, target file ID = %lu\n",
                    op_hdl->bucket->file_id);
            exit(1);
        } 
        op_hdl->bucket->total_on_disk_bytes += status.physicalSize_;
    }
    return true;
}

bool BucketOperator::operationFind(deltaStoreOpHandler* op_hdl) {
    auto obj = op_hdl->object;
    bool status = bucket_manager_->getBucketWithKey(
            string(obj->keyPtr_, obj->keySize_), kMultiPut, op_hdl->bucket,
            obj->isAnchorFlag_);

    if (status == false) {
        debug_error("[ERROR] Get file handler for key = %.*s"
                " error during multiput\n", 
                (int)obj->keySize_, obj->keyPtr_);
    }

    return true;
}

bool BucketOperator::pushGcIfNeeded(BucketHandler* bucket)
{
    // insert into GC job queue if exceed the threshold
    if (bucket->DiskAndBufferSizeExceeds(perFileGCSizeLimit_)) {
        bucket->ownership = -1;
        return bucket_manager_->pushToGCQueue(bucket);
    } else {
        debug_trace("Current file ID = %lu should not GC, skip\n",
                bucket->file_id);
        return false;
    }
}

bool BucketOperator::pushGcIfNeeded(deltaStoreOpHandler* op_hdl) {
    if (op_hdl->bucket->DiskAndBufferSizeExceeds(perFileGCSizeLimit_)) {
        op_hdl->bucket->ownership = -1;
        return bucket_manager_->pushToGCQueue(op_hdl);
    } else {
        return false;
    }
}

// for put
inline void BucketOperator::updateKDCacheIfExist(
        str_t key, str_t delta, bool isAnchor)
{
    // insert into cache only if the key has been read
    if (isAnchor == true) {
        kd_cache_->cleanCacheIfExist(key);
    } else {
        str_t old_delta = kd_cache_->getFromCache(key);

        // TODO a bug here. vec may be evicted and deleted before the
        // update
        if (old_delta.data_ != nullptr && old_delta.size_ > 0) {
            // if there is a delta. Merge and then update 
            vector<str_t> temp_vec;
            str_t merged_delta;
            temp_vec.push_back(old_delta);
            temp_vec.push_back(delta);

            // allocated by partial merge
            KDSepMergeOperatorPtr_->PartialMerge(temp_vec, merged_delta);
            kd_cache_->updateCache(key, merged_delta);
        } else if (old_delta.data_ == nullptr && old_delta.size_ == 0) {
            // if there is an anchor. directly update
            str_t inserted_delta(new char[delta.size_], delta.size_);
            memcpy(inserted_delta.data_, delta.data_, delta.size_);
            kd_cache_->updateCache(key, inserted_delta);
        }
    }
}

// for get
inline void BucketOperator::updateKDCache(
        char* keyPtr, size_t keySize, str_t delta) {
    str_t key(keyPtr, keySize);
    kd_cache_->updateCache(key, delta);
}

bool BucketOperator::directlyReadOperation(BucketHandler* bucket,
	string key, vector<string>& valueVec)
{
    std::scoped_lock<std::shared_mutex> r_lock(bucket->op_mtx);
    // check if not flushed anchors exit, return directly.
    // try extract from cache first
    if (kd_cache_ != nullptr) {
        str_t currentKey(key.data(), key.size());
        str_t delta = kd_cache_->getFromCache(currentKey);
        if (delta.data_ != nullptr && delta.size_ > 0) {
            // get a delta from the cache 
            struct timeval tv;
            gettimeofday(&tv, 0);
            valueVec.clear();
            valueVec.push_back(string(delta.data_, delta.size_));
            StatsRecorder::getInstance()->timeProcess(
                    StatsType::DS_GET_CACHE_HIT_DELTA, tv);
            bucket->ownership = 0;
            return true;
        } else if (delta.data_ == nullptr && delta.size_ == 0) {
            // get an anchor from the cache
            struct timeval tv;
            gettimeofday(&tv, 0);
            valueVec.clear();
            StatsRecorder::getInstance()->timeProcess(
                    StatsType::DS_GET_CACHE_HIT_ANCHOR, tv);
            bucket->ownership = 0;
            return true;
        } else if (enable_index_block_) {
            // Do not enable index block, directly write 
            // Not exist in cache, find the content in the file
            str_t key_str_t(key.data(), key.size());

            vector<string_view> kd_list;

            char* buf = nullptr;
            bool success;

            if (bucket->index_block != nullptr) {
                if (bucket->sorted_filter->MayExist(key_str_t) == true) {
                    if (bucket->filter->MayExist(key_str_t) == false) {
                        // only in sorted part
                        success = readAndProcessSortedPart(bucket, key,
                                kd_list, &buf);
                    } else {
                        // both parts 
                        success = readAndProcessBothParts(bucket, key,
                                kd_list, &buf);
                    }
                } else {
                    if (bucket->filter->MayExist(key_str_t) == false) {
                        // does not exist, or not stored in the memory 
                        valueVec.clear();
                        bucket->ownership = 0;
                        return true;
                    } else {
                        // only in unsorted part
                        success = readAndProcessUnsortedPart(bucket, key,
                                kd_list, &buf);
                    }
                }
            } else {
                success = readAndProcessWholeFile(bucket, key, kd_list, &buf);
            }

            valueVec.clear();
            if (success == false) {
                debug_error("[ERROR] read and process failed %s\n", key.c_str());
                exit(1);
            } else if (buf == nullptr) {
                // Key miss because (partially) sorted part does not have key
                bucket->ownership = 0;
                return true;
            } 

//            if (kd_list.size() > 0) {
//                valueVec.reserve(kd_list.size());
//                for (auto vecIt : kd_list) {
//                    valueVec.push_back(string(vecIt.data(), vecIt.size()));
//                }
//            }

            struct timeval tv;
            gettimeofday(&tv, 0);
            vector<str_t> deltas;
            for (auto& it : kd_list) {
                deltas.push_back(str_t(const_cast<char*>(it.data()),
                            it.size()));
            }

            str_t merged_delta(nullptr, 0);
            if (deltas.size() > 0) {
                KDSepMergeOperatorPtr_->PartialMerge(deltas, merged_delta);
                valueVec.push_back(string(merged_delta.data_, merged_delta.size_));
            }

            updateKDCache(key.data(), key.size(), merged_delta);
            StatsRecorder::getInstance()->timeProcess(
                    StatsType::KDSep_HASHSTORE_GET_INSERT_CACHE, tv);

            delete[] buf;

            bucket->ownership = 0;
            return true;
        } else {
            // Do not enable index block, directly read the whole file 
            // Not exist in cache, find the content in the file
            vector<string_view> kd_list;
            char* buf;
            bool s = readAndProcessWholeFile(bucket, key, kd_list, &buf);
            valueVec.clear();

            if (s == false) {
                debug_error("[ERROR] read and process failed %s\n", key.c_str());
                bucket->ownership = 0;
                return false;
            }

//            if (kd_list.size() > 0) {
//                valueVec.reserve(kd_list.size());
//                for (auto vecIt : kd_list) {
//                    valueVec.push_back(string(vecIt.data(), vecIt.size()));
//                }
//            }

            struct timeval tv;
            gettimeofday(&tv, 0);
            vector<str_t> deltas;
            for (auto& it : kd_list) {
                deltas.push_back(str_t(const_cast<char*>(it.data()), it.size()));
            }

            str_t merged_delta(nullptr, 0);
            if (deltas.size() > 0) {
                KDSepMergeOperatorPtr_->PartialMerge(deltas, merged_delta);
                valueVec.push_back(string(merged_delta.data_,
                            merged_delta.size_));
            }

            updateKDCache(key.data(), key.size(), merged_delta);
            StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_HASHSTORE_GET_INSERT_CACHE, tv);

            delete[] buf;

            bucket->ownership = 0;
            return true;
        }
    } else if (enable_index_block_) {
        // Do not enable index block, directly write 
        // Not exist in cache, find the content in the file
        str_t key_str_t(key.data(), key.size());

        vector<string_view> kd_list;

        char* buf = nullptr;
        bool success;

        if (bucket->index_block != nullptr) {
            if (bucket->sorted_filter->MayExist(key_str_t) == true) {
                if (bucket->filter->MayExist(key_str_t) == false) {
                    // only in sorted part
                    success = readAndProcessSortedPart(bucket, key,
                            kd_list, &buf);
                } else {
                    // both parts 
                    success = readAndProcessBothParts(bucket, key,
                            kd_list, &buf);
                }
            } else {
                if (bucket->filter->MayExist(key_str_t) == false) {
                    // does not exist, or not stored in the memory 
                    valueVec.clear();
                    bucket->ownership = 0;
                    return true;
                } else {
                    // only in unsorted part
                    success = readAndProcessUnsortedPart(bucket, key,
                            kd_list, &buf);
                }
            }
        } else {
            success = readAndProcessWholeFile(bucket, key, kd_list, &buf);
        }

        valueVec.clear();
        if (success == false) {
            debug_error("[ERROR] read and process failed %s\n", key.c_str());
            exit(1);
        } else if (buf == nullptr) {
            // Key miss because (partially) sorted part does not have key
            bucket->ownership = 0;
            return true;
        } 

        if (kd_list.size() > 0) {
            valueVec.reserve(kd_list.size());
            for (auto vecIt : kd_list) {
                valueVec.push_back(string(vecIt.data(), vecIt.size()));
            }
        }

        delete[] buf;

        bucket->ownership = 0;
        return true;
    } else {
        // no cache, directly read the whole file
        vector<string_view> kd_list;
        char* buf;
        bool s = readAndProcessWholeFile(bucket, key, kd_list, &buf);
        valueVec.clear();

        if (s == false) {
            debug_error("[ERROR] read and process failed %s\n", key.c_str());
            bucket->ownership = 0;
            return false;
        }

        if (kd_list.empty() == false) {
            valueVec.reserve(kd_list.size());
            for (auto vecIt : kd_list) {
                valueVec.push_back(string(vecIt.data(), vecIt.size()));
            }
        }

        delete[] buf;
        bucket->ownership = 0;
        return true;
    }
}

bool BucketOperator::directlyScanOperation(BucketHandler* bucket,
	string key, int len, map<string, string>& keys_values) {

    string cur_key = key;
    BucketHandler* prev_bucket = nullptr;

    while (true) {
        std::scoped_lock<std::shared_mutex> r_lock(bucket->op_mtx);

        // Do not enable index block, directly write 
        // Not exist in cache, find the content in the file
        map<string_view, vector<string_view>> kd_lists;

        char* buf = nullptr;
        bool success;

        success = readAndProcessWholeFileWithCache(bucket, kd_lists, &buf);
//        debug_error("kd_lists size %lu\n", kd_lists.size());
        // TODO here

        if (success == false) {
            debug_error("[ERROR] read and process failed %s\n", key.c_str());
            exit(1);
        }

        struct timeval tv;
        gettimeofday(&tv, 0);
        vector<str_t> deltas;

        for (auto& it : kd_lists) {
            if (it.first < cur_key || len == 0) {
                continue;
            }

            len--;

            vector<str_t> deltas;
            for (auto& delta : it.second) {
                deltas.push_back(str_t(const_cast<char*>(delta.data()),
                            delta.size()));
            }

            str_t merged_delta(nullptr, 0);
            if (deltas.size() > 0) {
                KDSepMergeOperatorPtr_->PartialMerge(deltas, merged_delta);
                keys_values[string(it.first.data(), it.first.size())] =
                    string(merged_delta.data_, merged_delta.size_);
            }

            // do not update kd cache after scanning
            delete[] merged_delta.data_;
//            if (kd_cache_ != nullptr) {
//                updateKDCache(const_cast<char*>(it.first.data()),
//                        it.first.size(), merged_delta);
//            }
        }

        StatsRecorder::getInstance()->timeProcess(
                StatsType::KDSep_HASHSTORE_GET_INSERT_CACHE, tv);

        delete[] buf;
        bucket->ownership = 0;

        if (len) {
            cur_key = (keys_values.empty()) ? cur_key :
                (--keys_values.end())->first;
            // TODO a bug: if the bucket does not have the key, it will go into
            // a dead loop
            prev_bucket = bucket;
            bucket_manager_->getNextBucketWithKey(cur_key, bucket);
            if (prev_bucket == bucket) {
                debug_error("[ERROR] Exit because of empty bucket %s len %d\n",
                        cur_key.c_str(), len);
                prev_bucket->ownership = 0;
                return true;
            } else if (bucket == nullptr) {
                debug_error("Exit because of going to the end, len %d\n",
                    len);
                return true;
            }
        } else {
            break;
        }
    }
    return true;
}

bool BucketOperator::operationGet(deltaStoreOpHandler* op_hdl)
{
    auto& bucket = op_hdl->bucket; 
    // check if not flushed anchors exit, return directly.
    // try extract from cache first

    // only read the first key
    auto& multiget_op = op_hdl->multiget_op;
    auto& key = *((*multiget_op.keys)[0]);
    auto& valueVec = (*multiget_op.values);

    if (kd_cache_ != nullptr) {
        // does not check the cache. Already checked in the interface
         if (enable_index_block_) {
            // Do not enable index block, directly write 
            // Not exist in cache, find the content in the file
            str_t key_str_t(key.data(), key.size());

            vector<string_view> kd_list;

            char* buf = nullptr;
            bool success;

            if (bucket->index_block != nullptr) {
                if (bucket->sorted_filter->MayExist(key_str_t) == true) {
                    if (bucket->filter->MayExist(key_str_t) == false) {
                        // only in sorted part
                        success = readAndProcessSortedPart(bucket, key,
                                kd_list, &buf);
                    } else {
                        // both parts 
                        success = readAndProcessBothParts(bucket, key,
                                kd_list, &buf);
                    }
                } else {
                    if (bucket->filter->MayExist(key_str_t) == false) {
                        // does not exist, or not stored in the memory 
                        valueVec.clear();
                        bucket->ownership = 0;
                        return true;
                    } else {
                        // only in unsorted part
                        success = readAndProcessUnsortedPart(bucket, key,
                                kd_list, &buf);
                    }
                }
            } else {
                success = readAndProcessWholeFile(bucket, key, kd_list, &buf);
            }

            valueVec.clear();
            if (success == false) {
                debug_error("[ERROR] read and process failed %s\n", key.c_str());
                exit(1);
            } else if (buf == nullptr) {
                // Key miss because (partially) sorted part does not have key
                bucket->ownership = 0;
                return true;
            } 

//            if (kd_list.size() > 0) {
//                valueVec.reserve(kd_list.size());
//                for (auto vecIt : kd_list) {
//                    valueVec.push_back(string(vecIt.data(), vecIt.size()));
//                }
//            }

            struct timeval tv;
            gettimeofday(&tv, 0);
            vector<str_t> deltas;
            for (auto& it : kd_list) {
                deltas.push_back(str_t(const_cast<char*>(it.data()),
                            it.size()));
            }

            str_t merged_delta(nullptr, 0);
            if (deltas.size() > 0) {
                KDSepMergeOperatorPtr_->PartialMerge(deltas, merged_delta);
                valueVec.push_back(new string(merged_delta.data_, merged_delta.size_));
            }

            updateKDCache(key.data(), key.size(), merged_delta);
            StatsRecorder::getInstance()->timeProcess(
                    StatsType::KDSep_HASHSTORE_GET_INSERT_CACHE, tv);

            delete[] buf;

            bucket->ownership = 0;
            return true;
        } else {
            // Do not enable index block, directly read the whole file 
            // Not exist in cache, find the content in the file
            vector<string_view> kd_list;
            char* buf;
            bool s = readAndProcessWholeFile(bucket, key, kd_list, &buf);
            valueVec.clear();

            if (s == false) {
                debug_error("[ERROR] read and process failed %s\n", key.c_str());
                bucket->ownership = 0;
                return false;
            }

//            if (kd_list.size() > 0) {
//                valueVec.reserve(kd_list.size());
//                for (auto vecIt : kd_list) {
//                    valueVec.push_back(string(vecIt.data(), vecIt.size()));
//                }
//            }

            struct timeval tv;
            gettimeofday(&tv, 0);
            vector<str_t> deltas;
            for (auto& it : kd_list) {
                deltas.push_back(str_t(const_cast<char*>(it.data()), it.size()));
            }

            str_t merged_delta(nullptr, 0);
            if (deltas.size() > 0) {
                KDSepMergeOperatorPtr_->PartialMerge(deltas, merged_delta);
                valueVec.push_back(new string(merged_delta.data_,
                            merged_delta.size_));
            }

            updateKDCache(key.data(), key.size(), merged_delta);
            StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_HASHSTORE_GET_INSERT_CACHE, tv);

            delete[] buf;

            bucket->ownership = 0;
            return true;
        }
        
        return true;
    } 
    
    if (enable_index_block_) {
        // Do not enable index block, directly write 
        // Not exist in cache, find the content in the file
        str_t key_str_t(key.data(), key.size());

        vector<string_view> kd_list;

        char* buf = nullptr;
        bool success;

        if (bucket->index_block != nullptr) {
            if (bucket->sorted_filter->MayExist(key_str_t) == true) {
                if (bucket->filter->MayExist(key_str_t) == false) {
                    // only in sorted part
                    success = readAndProcessSortedPart(bucket, key,
                            kd_list, &buf);
                } else {
                    // both parts 
                    success = readAndProcessBothParts(bucket, key,
                            kd_list, &buf);
                }
            } else {
                if (bucket->filter->MayExist(key_str_t) == false) {
                    // does not exist, or not stored in the memory 
                    valueVec.clear();
                    bucket->ownership = 0;
                    return true;
                } else {
                    // only in unsorted part
                    success = readAndProcessUnsortedPart(bucket, key,
                            kd_list, &buf);
                }
            }
        } else {
            success = readAndProcessWholeFile(bucket, key, kd_list, &buf);
        }

        valueVec.clear();
        if (success == false) {
            debug_error("[ERROR] read and process failed %s\n", key.c_str());
            exit(1);
        } else if (buf == nullptr) {
            // Key miss because (partially) sorted part does not have key
            bucket->ownership = 0;
            return true;
        } 

        if (kd_list.size() > 0) {
            valueVec.reserve(kd_list.size());
            for (auto vecIt : kd_list) {
                valueVec.push_back(new string(vecIt.data(), vecIt.size()));
            }
        }

        delete[] buf;

        bucket->ownership = 0;
        return true;
    } else {
        // no cache, directly read the whole file
        vector<string_view> kd_list;
        char* buf;
        bool s = readAndProcessWholeFile(bucket, key, kd_list, &buf);
        valueVec.clear();

        if (s == false) {
            debug_error("[ERROR] read and process failed %s\n", key.c_str());
            bucket->ownership = 0;
            return false;
        }

        if (kd_list.empty() == false) {
            valueVec.reserve(kd_list.size());
            for (auto vecIt : kd_list) {
                valueVec.push_back(new string(vecIt.data(), vecIt.size()));
            }
        }

        delete[] buf;
        bucket->ownership = 0;
        return true;
    }
}

bool BucketOperator::operationMultiGet(deltaStoreOpHandler* op_hdl)
{
    if (op_hdl->multiget_op.keys->size() == 1) {
	return operationGet(op_hdl);
    }

    auto& bucket = op_hdl->bucket; 

    // check if not flushed anchors exit, return directly.
    // try extract from cache first

    // only read the first key
    auto& multiget_op = op_hdl->multiget_op;
    //    auto& key = *((*multiget_op.keys)[0]);
    auto& keys = multiget_op.keys;
    auto& valueVec = (*multiget_op.values);

    vector<vector<string_view>> kd_lists;

    char* buf = nullptr;
    bool success;

//    debug_error("read hdl %p keys %lu\n", bucket, keys->size());

    success = readAndProcessWholeFileKeyList(bucket, keys, kd_lists, &buf);

    multiget_op.values->clear();

    if (success == false) {
	debug_error("[ERROR] read and process failed: key num %lu\n",
		keys->size());
	exit(1);
    } else if (buf == nullptr) {
	// Key miss because (partially) sorted part does not have key
	bucket->ownership = 0;
	return true;
    } 

    struct timeval tv;
    gettimeofday(&tv, 0);

    valueVec.resize(keys->size());
    for (auto key_i = 0; key_i < keys->size(); key_i++) {
	valueVec[key_i] = nullptr;

	vector<str_t> deltas;
	for (auto& it : kd_lists[key_i]) {
	    deltas.push_back(str_t(const_cast<char*>(it.data()),
			it.size()));
	}

	str_t merged_delta(nullptr, 0);
	if (deltas.size() > 0) {
	    KDSepMergeOperatorPtr_->PartialMerge(deltas, merged_delta);
	    valueVec[key_i] = new string(merged_delta.data_,
		    merged_delta.size_);
	}

	if (kd_cache_ != nullptr) {
	    updateKDCache((*keys)[key_i]->data(), 
		    (*keys)[key_i]->size(), merged_delta);
	}
    }
    StatsRecorder::getInstance()->timeProcess(
	    StatsType::KDSep_HASHSTORE_GET_INSERT_CACHE, tv);

    delete[] buf;

    bucket->ownership = 0;
    return true;
}

void BucketOperator::asioSingleOperation(deltaStoreOpHandler* op_hdl) {
    num_threads_++;
    singleOperation(op_hdl);
    num_threads_--;
}

void BucketOperator::singleOperation(deltaStoreOpHandler* op_hdl) {
    bool operationsStatus = true;
    bool bucket_is_input = true;
    auto bucket = op_hdl->bucket;

    std::scoped_lock<std::shared_mutex>* w_lock = nullptr;
    if (bucket != nullptr) {
        w_lock = new
            std::scoped_lock<std::shared_mutex>(bucket->op_mtx);
    }

    bool gc_pushed = false;

    switch (op_hdl->op_type) {
        case kMultiGet:
            STAT_PROCESS(operationsStatus =
                    operationMultiGet(op_hdl),
                    StatsType::OP_MULTIGET);
            break;
        case kMultiPut:
            debug_trace("receive operations, type = kMultiPut, file ID = %lu, put deltas key number = %u\n", bucket->file_id, op_hdl->multiput_op.size);
            STAT_PROCESS(operationsStatus = operationMultiPut(op_hdl,
                        gc_pushed), StatsType::OP_MULTIPUT);
            break;
        case kFlush:
            STAT_PROCESS(operationsStatus = operationFlush(op_hdl, gc_pushed),
                    StatsType::OP_FLUSH);
            break;
        case kFind:
            STAT_PROCESS(operationsStatus = operationFind(op_hdl),
                    StatsType::OP_FIND);
            bucket_is_input = false;
            break;
        default:
            debug_error("[ERROR] Unknown operation type = %d\n", op_hdl->op_type);
            break;
    }

    if (operationsStatus == false) {
        bucket->ownership = 0;
        debug_trace("Process file ID = %lu error\n", bucket->file_id);
        op_hdl->job_done = kError;
    } else if (bucket_is_input) {
        if ((op_hdl->op_type == kPut || op_hdl->op_type == kMultiPut) &&
                enable_gc_ && !gc_pushed) {
            bool putIntoGCJobQueueStatus = pushGcIfNeeded(bucket);
            if (putIntoGCJobQueueStatus == false) {
                bucket->ownership = 0;
                op_hdl->job_done = kDone;
            } else {
                op_hdl->job_done = kDone;
            }
        } else {
            if (!gc_pushed) {
                // read operation, or write/flush but no gc
                op_hdl->bucket->ownership = 0;
                op_hdl->job_done = kDone;
            } else if (op_hdl->need_flush == false) {
                op_hdl->job_done = kDone;
            } else {
                // let the GC thread to handle the bucket job_done flag
                //op_hdl->job_done = kDone;
            }
        }
    } else {
        op_hdl->job_done = kDone;
    }

    if (w_lock) {
        delete w_lock;
    }

}

bool BucketOperator::probeThread() {
    while (true) {
        sleep(1);
        int gc_threads = num_threads_;
        debug_error("gc_threads %d\n", gc_threads); 
        if (should_exit_ == true) {
            break;
        }
    }
    return true;

}

} // namespace KDSEP_NAMESPACE
