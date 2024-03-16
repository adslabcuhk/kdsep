#include "deltaStore/hashStoreInterface.hpp"
#include "utils/bucketKeyFilter.hpp"
#include "utils/statsRecorder.hh"

namespace KDSEP_NAMESPACE {

HashStoreInterface::HashStoreInterface(KDSepOptions* options, 
    const string& workingDirStr, shared_ptr<BucketManager>& bucketManager,
    shared_ptr<BucketOperator>& bucketOperator)
{
    if (options->deltaStore_max_bucket_number_ == 0) {
        options->deltaStore_max_bucket_number_ = 1;
    }
    internalOptionsPtr_ = options;
    extractValueSizeThreshold_ = options->extract_to_deltaStore_size_lower_bound;

    if (options->enable_deltaStore_KDLevel_cache == true) {
        options->kd_cache.reset(new KDLRUCache(options->deltaStore_KDCache_item_number_));
        kd_cache_ = options->kd_cache;
    }
    bucketManager.reset(new BucketManager(options, workingDirStr));
    bucketOperator.reset(new BucketOperator(options, workingDirStr, bucketManager));
    fileFlushThreshold_ = options->deltaStore_bucket_flush_buffer_size_limit_;
    enable_crash_consistency_ = options->enable_crash_consistency;

    file_manager_ = bucketManager;
    file_operator_ = bucketOperator;
//    unordered_map<string, vector<pair<bool, string>>> targetListForRedo;
//    file_manager_->recoveryFromFailureOld(targetListForRedo);
}

HashStoreInterface::~HashStoreInterface()
{
}

bool HashStoreInterface::isEmpty() {
    return file_manager_->isEmpty();
}

bool HashStoreInterface::setJobDone()
{
    if (file_manager_->setJobDone() == true) {
        if (file_operator_->setJobDone() == true) {
            return true;
        } else {
            return false;
        }
        return true;
    } else {
        return false;
    }
}

uint64_t HashStoreInterface::getExtractSizeThreshold()
{
    return extractValueSizeThreshold_;
}

uint64_t HashStoreInterface::getNumOfBuckets() {
    return file_manager_->getNumOfBuckets();
}

bool HashStoreInterface::Recovery() {
    uint64_t min_seq_num = file_manager_->GetMinSequenceNumber();

    // get the minimum of all the maximum seq numbers
    return recoverFromCommitLog(min_seq_num);
}

bool HashStoreInterface::recoverFromCommitLog(uint64_t min_seq_num) {
    char* read_buf;
    uint64_t read_buf_size;
    file_manager_->readCommitLog(read_buf, read_buf_size);

    if (read_buf == nullptr) {
        return true;
    }

    uint64_t i = 0;
    uint64_t processed_delta_num = 0;
    size_t header_sz = sizeof(KDRecordHeader);
    KDRecordHeader header;

    // one key only belongs to one record
    bool sorted = true;

    struct timeval tv;
    gettimeofday(&tv, 0);

    vector<deltaStoreOpHandler*> find_hdls;

    str_t key;

    // retrieve all headers, and get the file handlers
    while (i < read_buf_size) {
        processed_delta_num++;
        //  if (processed_delta_num % 100000 == 0) {
        //      debug_error("processed_delta_num %lu hdls num %lu\n", 
        //          processed_delta_num, find_hdls.size());
        //  }
        // get header
        if (use_varint_d_header == false) {
            memcpy(&header, read_buf + i, header_sz);
        } else {
            header = GetDeltaHeaderVarint(read_buf + i, header_sz);
        }

        i += header_sz;
        if (header.is_gc_done_ == true) {
            // skip since it is gc flag, no content.
            continue;
        }

        key = str_t(read_buf + i, header.key_size_);
        string_view value(nullptr, 0);

        i += header.key_size_;
        // no sorted part if there is an anchor 
        if (header.is_anchor_ == true) {
            sorted = false;
        } else {
            value = string_view(read_buf + i, header.value_size_);
            i += header.value_size_;
        }

        if (header.seq_num < min_seq_num) {
            continue;
        }

        deltaStoreOpHandler* find_op = new deltaStoreOpHandler;
        find_op->op_type = kFind;
        find_op->object = new mempoolHandler_t;

        auto& object = find_op->object;
        object->keyPtr_ = key.data_;    
        object->keySize_ = key.size_;   
        object->isAnchorFlag_ = header.is_anchor_;
        object->seq_num = header.seq_num;
        if (header.is_anchor_ == false) {
            object->valuePtr_ = const_cast<char*>(value.data());
            object->valueSize_ = value.size();
        }

        file_operator_->putIntoJobQueue(find_op);
        find_hdls.push_back(find_op);
    }

    StatsRecorder::staticProcess(StatsType::DS_RECOVERY_GET_FILE_HANDLER, tv);
    gettimeofday(&tv, 0);

    if (i > read_buf_size) {
        debug_error("i too large: %lu %lu\n", i, read_buf_size);
    }
    debug_error("start retreive: %lu handlers\n", find_hdls.size());

    deltaStoreOpHandler* hdls_arr[find_hdls.size()];
    copy(find_hdls.begin(), find_hdls.end(), hdls_arr);

    unordered_map<BucketHandler*, vector<int>> bucket2index;

    for (auto i = 0; i < find_hdls.size(); i++) {
        file_operator_->waitOperationHandlerDone(find_hdls[i], false);
        auto bucket = find_hdls[i]->bucket;
        auto& seq_num = find_hdls[i]->object->seq_num;

        string_view key_view(find_hdls[i]->object->keyPtr_,
                find_hdls[i]->object->keySize_);

        if (bucket == nullptr || bucket->max_seq_num >= seq_num) {
            continue;
        }

        if (bucket2index.find(bucket) != bucket2index.end()) {
            bucket2index.at(bucket).push_back(i);
        } else {
            vector<int> indexVec;
            indexVec.push_back(i);
            bucket2index.insert(make_pair(bucket, indexVec));
        }
    }


    // well... the variable names are in a mess
    uint32_t write_obj_i = 0;
    uint32_t write_obj_start = 0;
    uint32_t write_op_hdls_i = 0; 
    mempoolHandler_t write_objects[find_hdls.size()];
    vector<deltaStoreOpHandler*> write_hdls;
    write_hdls.resize(bucket2index.size());

    for (auto mapIt : bucket2index) {
        struct timeval tv;
        gettimeofday(&tv, 0);

        write_obj_start = write_obj_i;
        for (auto& index : mapIt.second) {
            write_objects[write_obj_i++] = *find_hdls[index]->object;
        }

        mapIt.first->markedByMultiPut = false;

        auto write_op_hdl = new deltaStoreOpHandler(mapIt.first);
        write_op_hdl->multiput_op.objects = write_objects + write_obj_start;
        write_op_hdl->multiput_op.size = write_obj_i - write_obj_start;
        write_op_hdl->op_type = kMultiPut;
        write_op_hdl->need_flush = true; 

        STAT_PROCESS(file_operator_->putIntoJobQueue(write_op_hdl),
            StatsType::DS_RECOVERY_PUT_TO_QUEUE_OP);
        write_hdls[write_op_hdls_i++] = write_op_hdl;
    }

    StatsRecorder::staticProcess(StatsType::DS_RECOVERY_PUT_TO_QUEUE, tv);

    gettimeofday(&tv, 0);
    for (auto& it : write_hdls) {
        file_operator_->waitOperationHandlerDone(it);
    }
    StatsRecorder::staticProcess(StatsType::DS_RECOVERY_WAIT_HANDLERS, tv);

    delete[] read_buf;

    return processed_delta_num > 0;
}

bool HashStoreInterface::putCommitLog(
    vector<mempoolHandler_t>& objects, bool& need_flush) {
    bool allAnchoarsFlag = true;
    for (auto it : objects) {
        if (it.isAnchorFlag_ == false) {
            allAnchoarsFlag = false;
            break;
        }
    }
    if (allAnchoarsFlag == true && anyBucketInitedFlag_ == false) {
        return true;
    } else {
        anyBucketInitedFlag_ = true;
    }

    struct timeval tv;
    gettimeofday(&tv, 0);
    unordered_map<BucketHandler*, vector<int>> fileHandlerToIndexMap;

    need_flush = false;
    if (enable_crash_consistency_ == false) {
        return true;
    }

    // write to the commit log
    bool write_commit_log_status = 
        file_manager_->writeToCommitLog(objects, need_flush, false);
    if (write_commit_log_status == false) {
        debug_error("[ERROR] write to commit log failed: %lu objects\n",
                objects.size());
        exit(1);
    }

    return true;
}

bool HashStoreInterface::commitToCommitLog() {
    return file_manager_->commitToCommitLog();
}

bool HashStoreInterface::multiPut(vector<mempoolHandler_t>& objects,
    bool need_flush, bool need_commit)
{
    static int tstatic = 0; //50;
    int rssBefore;
    if (tstatic) {
        rssBefore = getRssNoTrim();
        debug_error("rss no trim before multiput: %.2lf, num buc %lu\n",
            rssBefore / 1024.0, getNumOfBuckets());
    }
    bool allAnchoarsFlag = true;
    for (auto it : objects) {
        if (it.isAnchorFlag_ == false) {
            allAnchoarsFlag = false;
            break;
        }
    }
    if (allAnchoarsFlag == true && anyBucketInitedFlag_ == false) {
        return true;
    } else {
        anyBucketInitedFlag_ = true;
    }

    struct timeval tv;
    gettimeofday(&tv, 0);
    unordered_map<BucketHandler*, vector<int>> fileHandlerToIndexMap;

    // already written by the previous function
    // come here because there are no values written by the interface
    if (enable_crash_consistency_ == true && need_commit == true) {
        // directly write to the commit log
        need_flush = false;
        bool write_commit_log_status = 
            file_manager_->writeToCommitLog(objects, need_flush, true);
        if (write_commit_log_status == false) {
            debug_error("[ERROR] write to commit log failed: %lu objects\n",
                    objects.size());
            exit(1);
        }
    }

    if (enable_crash_consistency_ == false) {
        // TODO may still have some bugs
        need_flush = false; 
    }
    // otherwise don't need to touch the commit message. The commit log has
    // been well written. does not change the need_flush variable

    multiop_mtx_.lock();

    // get all the file handlers 
    { 
    gettimeofday(&tv, 0);
    deltaStoreOpHandler hdls[objects.size()];
        // push to queue
    for (auto i = 0; i < objects.size(); i++) {
        hdls[i].op_type = kFind;
        hdls[i].object = &objects[i]; 
        file_operator_->putIntoJobQueue(&hdls[i]);
    }
    StatsRecorder::staticProcess(StatsType::DS_MULTIPUT_GET_HANDLER, tv);

        // get the results
    for (auto i = 0; i < objects.size(); i++) {
        file_operator_->waitOperationHandlerDone(&hdls[i], false);
        auto bucket = hdls[i].bucket;
        if (bucket == nullptr) {
        // should skip current key since it is only an anchor
        continue;
        }

        if (fileHandlerToIndexMap.find(bucket) != fileHandlerToIndexMap.end()) {
        fileHandlerToIndexMap.at(bucket).push_back(i);
        } else {
        vector<int> indexVec;
        indexVec.push_back(i);
        fileHandlerToIndexMap.insert(make_pair(bucket, indexVec));
        }
    }
    }

    multiop_mtx_.unlock();

    gettimeofday(&tv, 0);
    {
        uint32_t handlerVecIndex = 0;
        uint32_t handlerStartVecIndex = 0;
        uint32_t opHandlerIndex = 0;
        mempoolHandler_t handlerVecTemp[objects.size()];
        vector<deltaStoreOpHandler*> handlers; 
        handlers.resize(fileHandlerToIndexMap.size());
        for (auto mapIt : fileHandlerToIndexMap) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            handlerStartVecIndex = handlerVecIndex;
        map<int, int> sequence2index;

        for (auto& index : mapIt.second) {
                sequence2index[objects[index].seq_num] = index;
            }

        for (auto& it : sequence2index) {
                handlerVecTemp[handlerVecIndex++] = objects[it.second];
            }

            StatsRecorder::staticProcess(StatsType::DS_MULTIPUT_PROCESS_HANDLERS, tv);
            mapIt.first->markedByMultiPut = false;
            deltaStoreOpHandler* op_hdl = new deltaStoreOpHandler(mapIt.first);
            op_hdl->multiput_op.objects = handlerVecTemp + handlerStartVecIndex;
            op_hdl->multiput_op.size = handlerVecIndex - handlerStartVecIndex;
            op_hdl->op_type = kMultiPut;
            op_hdl->need_flush = need_flush;
            STAT_PROCESS(file_operator_->putIntoJobQueue(op_hdl),
                    StatsType::DS_MULTIPUT_PUT_TO_JOB_QUEUE_OPERATOR);
            handlers[opHandlerIndex++] = op_hdl;
        }
        StatsRecorder::staticProcess(StatsType::DS_MULTIPUT_PUT_TO_JOB_QUEUE, tv);
        gettimeofday(&tv, 0);

        for (auto& it : handlers) {
            file_operator_->waitOperationHandlerDone(it);
        }
        StatsRecorder::staticProcess(StatsType::DS_MULTIPUT_WAIT_HANDLERS, tv);
    }

    if (need_flush) {
        // Flush all buffers in the buckets. In the FileManager
        bool status;
        vector<BucketHandler*> buckets; 
        status = file_manager_->prepareForUpdatingMetadata(buckets); 

        if (status == false) {
            debug_error("Error for updating meta: %d\n", (int)status);
            exit(1);
        }

        vector<deltaStoreOpHandler*> handlers; 

        struct timeval tv;
        gettimeofday(&tv, 0);

        for (auto& it : buckets) {
            if (it != nullptr && 
                    it->io_ptr->getFileBufferedSize() > 0) {
//      debug_error("real flush file %lu\n", it->file_id);
                if (it->ownership != 0) {
                    debug_error("wait file owner ship %lu %d\n",
                            it->file_id,
                            (int)it->ownership);
                }
                while (it->ownership != 0) {
                    asm volatile("");
                }
                if (it->gc_status == kShouldDelete) {
                    continue;
                }

                it->ownership = 1;
                deltaStoreOpHandler* op_hdl = new deltaStoreOpHandler(it);
                op_hdl->op_type = kFlush;
                file_operator_->putIntoJobQueue(op_hdl);
                handlers.push_back(op_hdl);
            }
        }

        for (auto& it : handlers) {
            STAT_PROCESS(
            file_operator_->waitOperationHandlerDone(it),
            StatsType::KDSep_HASHSTORE_WAIT_SYNC);
        }

        StatsRecorder::staticProcess(StatsType::KDSep_HASHSTORE_SYNC, tv);

        file_manager_->cleanCommitLog();
    }

    if (tstatic) {
        auto tmp = getRssNoTrim();
        debug_error("rss no trim after multiput: %.2lf (diff: %.2lf)\n",
            tmp / 1024.0, (tmp - rssBefore) / 1024.0);
        tstatic--;
    }

    return true;
}

bool HashStoreInterface::get(const string& keyStr, vector<string>& valueStrVec)
{
    debug_info("New OP: get deltas for key = %s\n", keyStr.c_str());
    BucketHandler* tempFileHandler;
    bool ret;

    if (file_manager_->isEmpty()) {
        valueStrVec.clear();
        return true;
    }

    if (kd_cache_ != nullptr) {
        str_t key(const_cast<char*>(keyStr.data()), keyStr.size());
        str_t delta = kd_cache_->getFromCache(key);
        if (delta.data_ != nullptr && delta.size_ > 0) {
            valueStrVec.push_back(string(delta.data_, delta.size_));
            return true;
            // non-empty delta for this key
        } else if (delta.data_ == nullptr && delta.size_ == 0) {
            valueStrVec.clear();
            // empty delta for this key
            return true;
        }
    }

    STAT_PROCESS(ret = file_manager_->getBucketWithKey(keyStr, kGet,
                tempFileHandler, false),
            StatsType::KDSep_HASHSTORE_GET_FILE_HANDLER);
    if (ret != true) {
        valueStrVec.clear();
        return false;
    } else {
        StatsRecorder::getInstance()->totalProcess(StatsType::FILTER_READ_TIMES, 1, 1);
        // No bucket. Exit
        if (tempFileHandler == nullptr) {
            valueStrVec.clear();
            return true;
        }

        if (tempFileHandler->filter->MayExist(keyStr) ||
                tempFileHandler->sorted_filter->MayExist(keyStr)) {
            ret = file_operator_->directlyReadOperation(tempFileHandler, keyStr, valueStrVec);
            bool deltaExistFlag = (!valueStrVec.empty());
            if (deltaExistFlag) {
                StatsRecorder::getInstance()->totalProcess(StatsType::FILTER_READ_EXIST_TRUE, 1, 1);
            } else {
                StatsRecorder::getInstance()->totalProcess(StatsType::FILTER_READ_EXIST_FALSE, 1, 1);
            }
            if (ret == false) {
                debug_error("failed %s\n", keyStr.c_str());
                exit(1);
            }
            return ret;
        } else {
            tempFileHandler->ownership = 0;
            return true;
        }
    }
}

bool HashStoreInterface::multiGet(const vector<string>& keys,
    vector<vector<string>>& valueStrVecVec)
{
    bool ret;

    bool get_result[keys.size()]; 
    valueStrVecVec.resize(keys.size());
    int need_read = keys.size();
    int all = keys.size();

    // Go through the cache one by one
    // TODO parallelize
    for (int i = 0; i < keys.size(); i++) {
        get_result[i] = false;
        if (kd_cache_ != nullptr) {
        str_t key(const_cast<char*>(keys[i].data()), keys[i].size());
            str_t delta = kd_cache_->getFromCache(key);
            if (delta.data_ != nullptr && delta.size_ > 0) {
                valueStrVecVec[i].push_back(string(delta.data_, delta.size_));
                // non-empty delta for this key
                get_result[i] = true;
                need_read--;
            } else if (delta.data_ == nullptr && delta.size_ == 0) {
                valueStrVecVec[i].clear();
                // empty delta for this key
                get_result[i] = true;
                need_read--;
            }
        }
    }

    // Check the file pointer one by one
    // TODO parallelize
    vector<BucketHandler*> buckets;
    buckets.resize(all);
    unordered_map<BucketHandler*, vector<int>> fileHandlerToIndexMap;

    multiop_mtx_.lock();
    for (int i = 0; i < all; i++) {
        if (get_result[i] == false) {
        // get the file handlers in parallel
            STAT_PROCESS(ret = 
                    file_manager_->getBucketWithKey(keys[i], kMultiGet,
                        buckets[i], false),
                    StatsType::KDSep_HASHSTORE_GET_FILE_HANDLER);
            if (ret == false) {
                debug_error("Get handler error for key %s\n", keys[i].c_str());
                exit(1);
            }

            auto& bucket = buckets[i];

            if (bucket == nullptr) { 
                // no bucket
                valueStrVecVec[i].clear();
                get_result[i] = true;
                need_read--;
            } else if (bucket->filter->MayExist(keys[i]) == false && 
                    bucket->sorted_filter->MayExist(keys[i]) == false) {
                // bucket does not have the delta
                valueStrVecVec[i].clear();
                get_result[i] = true;
                need_read--;
                if (fileHandlerToIndexMap.find(bucket) ==
                        fileHandlerToIndexMap.end()) {
                    bucket->ownership = 0;
                    bucket->markedByMultiGet = false;
                    buckets[i] = nullptr;
                }
            } else {
                // need to 
                if (fileHandlerToIndexMap.find(bucket) !=
                        fileHandlerToIndexMap.end()) {
                    fileHandlerToIndexMap.at(bucket).push_back(i);
                } else {
                    vector<int> indexVec;
                    indexVec.push_back(i);
                    fileHandlerToIndexMap.insert(make_pair(bucket, indexVec));
                }
            }
        } else {
            buckets[i] = nullptr;
        }
    }
    multiop_mtx_.unlock();

    StatsRecorder::getInstance()->totalProcess(StatsType::FILTER_READ_TIMES, 1, 1);

    // operation handlers
    vector<deltaStoreOpHandler*> handlers;
    handlers.resize(fileHandlerToIndexMap.size());

    // map the small array (all needs read) to the large array (some needs)
    int needed_i = 0;

    for (auto& mapIt : fileHandlerToIndexMap) {
        auto& bucket = mapIt.first;
        auto& index_vec = mapIt.second;
        auto op_hdl = new deltaStoreOpHandler(bucket);
        bucket->markedByMultiGet = false;
        op_hdl->multiget_op.keys = new vector<string*>; 
        op_hdl->multiget_op.values = new vector<string*>; 
        op_hdl->multiget_op.key_indices = index_vec;
        op_hdl->op_type = kMultiGet;

        for (auto index = 0; index < index_vec.size(); index++) {
            auto& key_i = index_vec[index];
            if (key_i >= keys.size() || get_result[key_i] == true) {
                debug_error("key_i %d keys.size() %lu get result %d\n",
                        key_i, keys.size(), (int)get_result[key_i]);
                exit(1);
            }
            op_hdl->multiget_op.keys->push_back(const_cast<string*>(
                        &keys[index_vec[index]])); 
        }
        
        file_operator_->startJob(op_hdl);
        handlers[needed_i++] = op_hdl;
    }

    for (int i = 0; i < needed_i; i++) {
        auto& op_hdl = handlers[i];
        // do not delete the handler
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        file_operator_->waitOperationHandlerDone(op_hdl, false);
        auto& values = *(op_hdl->multiget_op.values);

        for (int index_i = 0; index_i < values.size(); index_i++) {
            auto& key_i = op_hdl->multiget_op.key_indices[index_i];
            if (get_result[key_i] == true) {
                debug_error("key_i %d has result?\n", key_i);
            }
            if (values[index_i] != nullptr) {
                valueStrVecVec[key_i].clear();
                valueStrVecVec[key_i].push_back(*values[index_i]);

                delete values[index_i];
            }
        }

        delete op_hdl->multiget_op.keys;
        delete op_hdl->multiget_op.values;
        delete op_hdl;
        StatsRecorder::staticProcess(StatsType::DS_MULTIGET_ONE_FILE, tv);
    }

    return true;
}

bool HashStoreInterface::scan(const string& key, int len, 
        map<string, string>& keys_values)
{
    // skip the cache, directly read the bucket
    string cur_key = key;

    BucketHandler* bucket;
    bool ret;

    STAT_PROCESS(ret =
    file_manager_->getBucketWithKey(cur_key, kGet, bucket, false),
    StatsType::KDSep_HASHSTORE_GET_FILE_HANDLER);

    // No bucket. Next one 
    if (bucket == nullptr) {
        debug_error("No bucket for key %s\n", cur_key.c_str());
        keys_values.clear();
        return true;
    }

    if (bucket->total_object_bytes > 0) { 
        ret = file_operator_->directlyScanOperation(bucket, key, len,
            keys_values);
        if (ret == false) {
            debug_error("failed %s\n", key.c_str());
            exit(1);
        }
        return ret;
    } else {
        bucket->ownership = 0;
        return true;
    }
}

bool HashStoreInterface::wrapUpGC(uint64_t& wrap_up_gc_num)
{
    return file_manager_->wrapUpGC(wrap_up_gc_num);
}

}
