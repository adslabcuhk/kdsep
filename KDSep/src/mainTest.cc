#include "interface/KDSepInterface.hpp"
#include "interface/KDSepOptions.hpp"
#include "interface/mergeOperation.hpp"
#include "utils/appendAbleLRUCache.hpp"
#include "utils/fileOperation.hpp"
#include "utils/murmurHash.hpp"
#include "utils/prefixTree.hpp"
#include "utils/timer.hpp"

using namespace KDSEP_NAMESPACE;

vector<string> split(string str, string token)
{
    vector<string> result;
    while (str.size()) {
        size_t index = str.find(token);
        if (index != std::string::npos) {
            result.push_back(str.substr(0, index));
            str = str.substr(index + token.size());
            if (str.size() == 0)
                result.push_back(str);
        } else {
            result.push_back(str);
            str = "";
        }
    }
    return result;
}

class FieldUpdateMergeOperatorInternal : public MergeOperator {
public:
    // Gives the client a way to express the read -> modify -> write semantics
    // key:         (IN) The key that's associated with this merge operation.
    // existing:    (IN) null indicates that the key does not exist before this op
    // operand_list:(IN) the sequence of merge operations to apply, front() first.
    // new_value:  (OUT) Client is responsible for filling the merge result here
    // logger:      (IN) Client could use this to log errors during merge.
    //
    // Return true on success, false on failure/corruption/etc.
    bool FullMerge(const Slice& key, const Slice* existing_value,
        const std::deque<std::string>& operand_list,
        std::string* new_value, Logger* logger) const override
    {
        vector<std::string> words = split(existing_value->ToString(), ",");
        for (auto q : operand_list) {
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
        return true;
    };

    // This function performs merge(left_op, right_op)
    // when both the operands are themselves merge operation types.
    // Save the result in *new_value and return true. If it is impossible
    // or infeasible to combine the two operations, return false instead.
    bool PartialMerge(const Slice& key, const Slice& left_operand,
        const Slice& right_operand, std::string* new_value,
        Logger* logger) const override
    {
        new_value->assign(left_operand.ToString() + "," + right_operand.ToString());
        return true;
    };

    // The name of the MergeOperator. Used to check for MergeOperator
    // mismatches (i.e., a DB created with one MergeOperator is
    // accessed using a different MergeOperator)
    const char* Name() const override { return "FieldUpdateMergeOperatorInternal"; }
};

bool testPut(KDSep& db_, string key, string& value)
{
    debug_info("Test put operation with key = %s\n", key.c_str());
    if (!db_.Put(key, value)) {
        debug_error("[ERROR] Could not put KV pairs to DB for key = %s\n", key.c_str());
        return false;
    } else {
        return true;
    }
}

bool testGet(KDSep& db_, string key, string& value)
{
    debug_info("Test get operation with key = %s\n", key.c_str());
    if (!db_.Get(key, &value)) {
        debug_error("[ERROR] Could not get KV pairs from DB for key = %s\n", key.c_str());
        return false;
    } else {
        return true;
    }
}

bool testMerge(KDSep& db_, string key, string& value)
{
    debug_info("Test merge operation with key = %s\n", key.c_str());
    if (!db_.Merge(key, value)) {
        debug_error("[ERROR] Could not merge KV pairs to DB for key = %s\n", key.c_str());
        return false;
    } else {
        return true;
    }
}

bool testBatchedPut(KDSep& db_, string key, string& value)
{
    debug_info("Test batched put operation with key = %s\n", key.c_str());
    if (!db_.PutWithWriteBatch(key, value)) {
        debug_error("[ERROR] Could not put KV pairs to DB for key = %s\n", key.c_str());
        return false;
    } else {
        return true;
    }
}

bool testBatchedMerge(KDSep& db_, string key, string& value)
{
    debug_info("Test batched merge operation with key = %s\n", key.c_str());
    if (!db_.MergeWithWriteBatch(key, value)) {
        debug_error("[ERROR] Could not merge KV pairs to DB for key = %s\n", key.c_str());
        return false;
    } else {
        return true;
    }
}

bool LRUCacheTest()
{
    AppendAbleLRUCache<string, vector<string>*>* keyToValueListCache_ = nullptr;
    keyToValueListCache_ = new AppendAbleLRUCache<string, vector<string>*>(1000);

    string keyStr = "key1";
    vector<string>* testValueVec = new vector<string>;
    testValueVec->push_back("test1");
    keyToValueListCache_->insertToCache(keyStr, testValueVec);
    if (keyToValueListCache_->existsInCache(keyStr) == true) {
        vector<string>* testValueVecRead = keyToValueListCache_->getFromCache(keyStr);
        for (auto index = 0; index < testValueVecRead->size(); index++) {
            cout << testValueVecRead->at(index) << endl;
        }
        testValueVecRead->push_back("test2");
        cout << "After insert" << endl;
        keyToValueListCache_->getFromCache(keyStr);
        for (auto index = 0; index < testValueVecRead->size(); index++) {
            cout << testValueVecRead->at(index) << endl;
        }
    }
    return true;
}

bool murmurHashTest(string rawStr)
{
    u_char murmurHashResultBuffer[16];
    MurmurHash3_x64_128((void*)rawStr.c_str(), rawStr.size(), 0, murmurHashResultBuffer);
    uint64_t firstFourByte;
    string prefixStr;
    memcpy(&firstFourByte, murmurHashResultBuffer, sizeof(uint64_t));
    while (firstFourByte != 0) {
        prefixStr += ((firstFourByte & 1) + '0');
        firstFourByte >>= 1;
    }
    cout << "\tPrefix first 64 bit in BIN: " << prefixStr << endl;
    return true;
}

bool fstreamTestFlush(uint64_t testNumber)
{
    Timer newTimer;
    fstream testStream;
    testStream.open("test.flush", ios::out);
    testStream.close();
    testStream.open("test.flush", ios::in | ios::out | ios::binary);
    char writeBuffer[4096];
    memset(writeBuffer, 1, 4096);
    newTimer.startTimer();
    for (int i = 0; i < testNumber; i++) {
        testStream.write(writeBuffer, 4096);
        testStream.flush();
    }
    newTimer.stopTimer("Flush");
    testStream.close();
    return true;
}

bool fstreamTestFlushSync(uint64_t testNumber)
{
    Timer newTimer;
    fstream testStream;
    testStream.open("test.sync", ios::out);
    testStream.close();
    testStream.open("test.sync", ios::in | ios::out | ios::binary);
    char writeBuffer[4096];
    memset(writeBuffer, 1, 4096);
    newTimer.startTimer();
    for (int i = 0; i < testNumber; i++) {
        testStream.write(writeBuffer, 4096);
        testStream.flush();
        testStream.sync();
    }
    newTimer.stopTimer("Flush+Sync");
    testStream.close();
    return true;
}

bool fstreamTestDIRECT(uint64_t testNumber)
{
    Timer newTimer;
    char* writeBuffer;
    auto ret = posix_memalign((void**)&writeBuffer, sysconf(_SC_PAGESIZE), 4096);
    if (ret) {
        printf("posix_memalign failed: %d %s\n", errno, strerror(errno));
        return false;
    }
    memset(writeBuffer, 1, 4096);
    const int fileDesc = open("test.direct", O_CREAT | O_WRONLY | O_DIRECT);
    if (-1 != fileDesc) {
        newTimer.startTimer();
        for (int i = 0; i < testNumber; i++) {
            write(fileDesc, writeBuffer, 4096);
        }
        newTimer.stopTimer("O_DIRECT");
    }
    free(writeBuffer);
    close(fileDesc);
    return true;
}

bool directIOTest(string path)
{
    cout << "Page size = " << sysconf(_SC_PAGE_SIZE) << endl;
    FileOperation currentFilePtr(kDirectIO);
    currentFilePtr.createFile(path);
    currentFilePtr.openFile(path);
    string writeStr = "test";
    char writeBuffer[4];
    memcpy(writeBuffer, writeStr.c_str(), 4);
    currentFilePtr.writeFile(writeBuffer, 4);
    cout << "Write content = " << writeBuffer << endl;
    char readBuffer[4];
    currentFilePtr.readFile(readBuffer, 4);
    string readStr(readBuffer, 4);
    cout << "Read content = " << readStr << endl;
    currentFilePtr.closeFile();
    return true;
}

bool prefixTreeTest()
{
    PrefixTree<uint64_t> testTree(2, 16);
    uint64_t number1 = 2;
    uint64_t number2 = 3;
    testTree.insert("0000", number1);
    uint64_t newContent;
    bool status = testTree.get("0000", newContent);
    if (status == true) {
        cerr << "Find value = " << newContent << endl;
    } else {
        cerr << "Not find value for 0000" << endl;
    }
    testTree.insert("00000", number2);
    testTree.insertWithFixedBitNumber("00000", 5, number2);
    status = testTree.get("0000", newContent);
    if (status == true) {
        cerr << "Find value = " << newContent << endl;
    } else {
        cerr << "Not find value for 0000" << endl;
    }
    status = testTree.get("00000", newContent);
    if (status == true) {
        cerr << "Find value = " << newContent << endl;
    } else {
        cerr << "Not find value for 00000" << endl;
    }
    testTree.printNodeMap();
    return true;
}

pair<uint64_t, uint64_t> deconstructAndGetValidContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_map<string, vector<string>>& resultMap)
{
    uint64_t processedKeepObjectNumber = 0;
    uint64_t processedTotalObjectNumber = 0;
    uint64_t currentProcessLocationIndex = 0;
    // skip file header
    hashStoreFileHeader currentFileHeader;
    memcpy(&currentFileHeader, fileContentBuffer, sizeof(currentFileHeader));
    currentProcessLocationIndex += sizeof(currentFileHeader);
    while (currentProcessLocationIndex != fileSize) {
        processedKeepObjectNumber++;
        processedTotalObjectNumber++;
        KDRecordHeader currentObjectRecordHeader;
        memcpy(&currentObjectRecordHeader, fileContentBuffer + currentProcessLocationIndex, sizeof(currentObjectRecordHeader));
        currentProcessLocationIndex += sizeof(currentObjectRecordHeader);
        if (currentObjectRecordHeader.is_anchor_ == true) {
            string currentKeyStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                processedKeepObjectNumber -= (resultMap.at(currentKeyStr).size());
                resultMap.at(currentKeyStr).clear();
                resultMap.erase(currentKeyStr);
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                continue;
            } else {
                processedKeepObjectNumber--;
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                continue;
            }
        } else if (currentObjectRecordHeader.is_gc_done_ == true) {
            processedKeepObjectNumber--;
            continue;
        } else {
            string currentKeyStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                string currentValueStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                resultMap.at(currentKeyStr).push_back(currentValueStr);
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            } else {
                vector<string> newValuesRelatedToCurrentKeyVec;
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                string currentValueStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                newValuesRelatedToCurrentKeyVec.push_back(currentValueStr);
                resultMap.insert(make_pair(currentKeyStr, newValuesRelatedToCurrentKeyVec));
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            }
        }
    }
    printf("deconstruct current file header done, file ID = %lu, create reason = %u, prefix length used in this file = %lu, target process file size = %lu, find different key number = %lu, total processed object number = %lu, target keep object number = %lu\n", currentFileHeader.file_id_, currentFileHeader.file_create_reason_, currentFileHeader.current_prefix_used_bit_, fileSize, resultMap.size(), processedTotalObjectNumber, processedKeepObjectNumber);
    return make_pair(processedKeepObjectNumber, processedTotalObjectNumber);
}

bool dump(string path)
{
    FileOperation tempOp(kAlignLinuxIO);
    tempOp.openFile(path);
    uint64_t fileSize = tempOp.getFileSize();
    char readBuffer[fileSize];
    tempOp.readFile(readBuffer, fileSize);
    unordered_map<string, vector<string>> resultMap;
    deconstructAndGetValidContentsFromFile(readBuffer, fileSize, resultMap);
    tempOp.closeFile();
    return true;
}

int main(int argc, char* argv[])
{
    // dump(argv[1]);
    prefixTreeTest();
    return 0;
    // string path = "test.log";
    // directIOTest(path);
    // fstreamTestFlush(100000);
    // fstreamTestFlushSync(100000);
    // fstreamTestDIRECT(100000);
    // return 0;

    KDSep db_;
    KDSepOptions options_;
    int bloomBits = 10;
    size_t blockCacheSize = 128;
    bool directIO = true;
    size_t memtableSize = 64;
    // set optionssc
    rocksdb::BlockBasedTableOptions bbto;
    if (directIO == true) {
        options_.rocks_opt.use_direct_reads = true;
        options_.rocks_opt.use_direct_io_for_flush_and_compaction = true;
    } else {
        options_.rocks_opt.allow_mmap_reads = true;
        options_.rocks_opt.allow_mmap_writes = true;
    }
    options_.rocks_opt.create_if_missing = true;
    options_.rocks_opt.write_buffer_size = memtableSize;
    options_.rocks_opt.max_background_jobs = 8;
    options_.rocks_opt.disable_auto_compactions = false;
    options_.rocks_opt.level_compaction_dynamic_level_bytes = true;
    options_.rocks_opt.target_file_size_base = 65536 * 1024;
    options_.rocks_opt.compression = rocksdb::kNoCompression;
    if (bloomBits > 0) {
        bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bloomBits));
    }
    bbto.block_cache = rocksdb::NewLRUCache(blockCacheSize);
    options_.rocks_opt.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));

    options_.rocks_opt.statistics = rocksdb::CreateDBStatistics();

    // KDSep settings
    options_.enable_deltaStore = true;
    options_.enable_valueStore = true;
    options_.enable_deltaStore_KDLevel_cache = true;
    if (options_.enable_deltaStore == false) {
        options_.rocks_opt.merge_operator.reset(new FieldUpdateMergeOperatorInternal);
    }
    options_.KDSep_merge_operation_ptr.reset(new KDSepFieldUpdateMergeOperator);
    options_.enable_batched_operations_ = false;
    options_.fileOperationMethod_ = kDirectIO;

    string dbNameStr = "TempDB";
    bool dbOpenStatus = db_.Open(options_, dbNameStr);
    if (!dbOpenStatus) {
    }
    // dump operations
    options_.dumpOptions("TempDB/options.dump");
    options_.dumpDataStructureInfo("TempDB/structure.dump");
    // operations
    Timer newTimer;
    newTimer.startTimer();
    int testNumber = 10;
    if (argc != 1) {
        testNumber = stoi(argv[1]);
    }
    for (int i = 0; i < testNumber; i++) {
        string key1 = "Key001" + to_string(i);
        string key2 = "Key002" + to_string(i);
        string value1 = "value1,value2";
        string value1Merged = "value5,value6";
        string value2 = "value3,value4";
        string merge1 = "0,value5";
        string merge2 = "1,value6";
        vector<bool> testResultBoolVec;
        if (options_.enable_batched_operations_ == true) {
            // put
            bool statusPut1 = testBatchedPut(db_, key1, value1);
            testResultBoolVec.push_back(statusPut1);
            string tempReadStr1;
            // get raw value
            bool statusGet1 = testGet(db_, key1, tempReadStr1);
            testResultBoolVec.push_back(statusGet1);
            if (tempReadStr1.compare(value1) != 0) {
                debug_error("[ERROR]: Error read raw value, raw value = %s, read value = %s\n", value1.c_str(), tempReadStr1.c_str());
                break;
            }
            for (int i = 0; i < 4; i++) {
                string mergeStr = "1,value" + to_string(i);
                testBatchedMerge(db_, key1, mergeStr);
            }
            // get merged value
            string tempReadStrTemp;
            bool statusGetTemp = testGet(db_, key1, tempReadStrTemp);
            testResultBoolVec.push_back(statusGetTemp);
            string resultStr = "value3,value2";
            if (tempReadStrTemp.compare(resultStr) != 0) {
                debug_error("[ERROR]: Error read merged value, merged value = %s, read value = %s\n", resultStr.c_str(), tempReadStrTemp.c_str());
                break;
            }
        } else {
            // put
            bool statusPut1 = testPut(db_, key1, value1);
            testResultBoolVec.push_back(statusPut1);
            string tempReadStr1;
            // get raw value
            bool statusGet1 = testGet(db_, key1, tempReadStr1);
            testResultBoolVec.push_back(statusGet1);
            if (tempReadStr1.compare(value1) != 0) {
                debug_error("[ERROR]: Error read raw value, raw value = %s, read value = %s\n", value1.c_str(), tempReadStr1.c_str());
                break;
            }
            bool statusMerge1 = testMerge(db_, key1, merge1);
            testResultBoolVec.push_back(statusMerge1);
            bool statusMerge2 = testMerge(db_, key1, merge2);
            testResultBoolVec.push_back(statusMerge2);

            // get merged value
            string tempReadStr2;
            bool statusGet2 = testGet(db_, key1, tempReadStr2);
            testResultBoolVec.push_back(statusGet2);
            if (tempReadStr2.compare(value1Merged) != 0) {
                debug_error("[ERROR]: Error read merged value, merged value = %s, read value = %s\n", value1Merged.c_str(), tempReadStr2.c_str());
                break;
            }

            bool statusPut2 = testPut(db_, key1, value2);
            testResultBoolVec.push_back(statusPut2);
            // get overwrited value
            string tempReadStr3;
            bool statusGet3 = testGet(db_, key1, tempReadStr3);
            testResultBoolVec.push_back(statusGet3);
            if (tempReadStr3.compare(value2) != 0) {
                debug_error("[ERROR]: Error read overwrited value, raw value = %s, read value = %s\n", value2.c_str(), tempReadStr3.c_str());
                break;
            }
        }
    }
    newTimer.stopTimer("Running DB operations time");
    db_.Close();
    return 0;
}
