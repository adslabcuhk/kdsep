#include "vlog/rocksdbKeyManager.hh"
#include "utils/debug.hpp"
#include "utils/statsRecorder.hh"

namespace KDSEP_NAMESPACE {

RocksDBKeyManager::RocksDBKeyManager(rocksdb::DB* lsm)
{
    _lsm = lsm;
}

RocksDBKeyManager::~RocksDBKeyManager()
{
    //    if (_cache.lru != 0)
    //        delete _cache.lru;
    //    delete _lsm;
}
//
// bool RocksDBKeyManager::writeKey (char *keyStr, ValueLocation valueLoc, int needCache) {
//    bool ret = false;
//
//    // update cache
//    if (_cache.lru && needCache) {
//        unsigned char* key = (unsigned char*) keyStr;
//        if (needCache > 1) {
//            STAT_PROCESS(_cache.lru->update(key, valueLoc.segmentId), StatsType::KEY_UPDATE_CACHE);
//        } else {
//            STAT_PROCESS(_cache.lru->insert(key, valueLoc.segmentId), StatsType::KEY_SET_CACHE);
//        }
//    }
//
//    rocksdb::WriteOptions wopt;
//    wopt.sync = ConfigManager::getInstance().syncAfterWrite();
//
//    // put the key into LSM-tree
//    STAT_PROCESS(ret = _lsm->Put(wopt, rocksdb::Slice(keyStr, KEY_SIZE), rocksdb::Slice(valueLoc.serialize())).ok(), KEY_SET_LSM);
//    return ret;
//}
//

bool RocksDBKeyManager::writeKeyBatch(std::vector<char*> keys, std::vector<ValueLocation> valueLocs)
{
    bool ret = true;
    assert(keys.size() == valueLocs.size());
    if (keys.empty())
        return ret;

    // update to LSM-tree
    rocksdb::WriteBatch batch;
    for (size_t i = 0; i < keys.size(); i++) {
        key_len_t keySize;
        memcpy(&keySize, keys.at(i), sizeof(key_len_t));

        batch.Put(rocksdb::Slice(keys.at(i) + sizeof(key_len_t), keySize), rocksdb::Slice(valueLocs.at(i).serializeIndexWrite()));

//        string k(keys.at(i) + sizeof(key_len_t), (int)keySize); 
//        if (k == "user6840842610087607159") {
//            debug_error("%s offset %lu length %lu\n",
//                    k.c_str(), valueLocs[i].offset,
//                    valueLocs[i].length);
//        }
    }

    rocksdb::WriteOptions wopt;
    wopt.sync = ConfigManager::getInstance().syncAfterWrite();
    rocksdb::Status s;

    STAT_PROCESS(s = _lsm->Write(wopt, &batch), StatsType::KDSep_PUT_ROCKSDB);
    if (!s.ok()) {
        debug_error("[ERROR] %s\n", s.ToString().c_str());
        exit(-1);
    }
    debug_info("Put keys: %d status %s\n", (int)keys.size(), s.ToString().c_str());

    return s.ok();
}

bool RocksDBKeyManager::writeKeyBatch(std::vector<std::string>& keys, std::vector<ValueLocation> valueLocs)
{
    bool ret = true;
    assert(keys.size() == valueLocs.size());
    if (keys.empty())
        return ret;

    // update to LSM-tree
    rocksdb::WriteBatch batch;
    for (size_t i = 0; i < keys.size(); i++) {
        // construct the batch for LSM-tree write
        key_len_t keySize;
        memcpy(&keySize, keys.at(i).c_str(), sizeof(key_len_t));
        batch.Put(rocksdb::Slice(keys.at(i).c_str() + sizeof(key_len_t), (int)keySize), rocksdb::Slice(valueLocs.at(i).serializeIndexWrite()));
//        string k = keys.at(i).substr(sizeof(key_len_t), (int)keySize); 
//        if (k == "user6840842610087607159") {
//            debug_error("%s offset %lu length %lu\n",
//                    k.c_str(), valueLocs[i].offset,
//                    valueLocs[i].length);
//        }
    }

    rocksdb::WriteOptions wopt;
    wopt.sync = ConfigManager::getInstance().syncAfterWrite();
    rocksdb::Status s;

    // put the keys into LSM-tree
    STAT_PROCESS(s = _lsm->Write(wopt, &batch), StatsType::KDSep_PUT_ROCKSDB);
    if (!s.ok()) {
        debug_error("[ERROR] %s\n", s.ToString().c_str());
        exit(-1);
    }
    return s.ok();
}

bool RocksDBKeyManager::writeMeta(const char* keyStr, int keySize, std::string metadata)
{
    rocksdb::WriteOptions wopt;
    wopt.sync = ConfigManager::getInstance().syncAfterWrite();

    debug_info("write META %.*s %s\n", keySize, keyStr, metadata.c_str());

    return _lsm->Put(wopt, rocksdb::Slice(keyStr, keySize), rocksdb::Slice(metadata)).ok();
    return true;
}

std::string RocksDBKeyManager::getMeta(const char* keyStr, int keySize)
{
    std::string value;

    _lsm->Get(rocksdb::ReadOptions(), rocksdb::Slice(keyStr, keySize), &value);
    debug_trace("get META %.*s %s\n", keySize, keyStr, value.c_str());

    return value;
}

bool RocksDBKeyManager::persistMeta()
{
    // Flush the LSM-tree
    if (ConfigManager::getInstance().testDirectIOCorrectness()) {
        if (ConfigManager::getInstance().getTestIODelayUs() > 0) {
            usleep(ConfigManager::getInstance().getTestIODelayUs());
        }
    } else {
        _lsm->FlushWAL(true);
    }
    return true;
}

bool RocksDBKeyManager::mergeKeyBatch(std::vector<char*> keys, std::vector<ValueLocation> valueLocs)
{
    bool ret = true;
    assert(keys.size() == valueLocs.size());
    if (keys.empty())
        return ret;

    rocksdb::Status s;

    // update to LSM-tree
    rocksdb::WriteOptions wopt;
    wopt.sync = ConfigManager::getInstance().syncAfterWrite();

    // put the keys into LSM-tree
    Slice kslice, vslice;
    std::string valueString;
    key_len_t keySize;
    for (int i = 0; i < (int)keys.size(); i++) {
        memcpy(&keySize, keys.at(i), sizeof(key_len_t));
        kslice = rocksdb::Slice(KEY_OFFSET(keys.at(i)), (int)keySize);
        valueString = valueLocs[i].serializeIndexUpdate();
        vslice = rocksdb::Slice(valueString);
        debug_trace("mergeKeyBatch %s offset %lu length %lu\n", kslice.ToString().c_str(), valueLocs[i].offset, valueLocs[i].length);
//        if (string(KEY_OFFSET(keys.at(i)), (int)keySize) == "user6840842610087607159") {
//            debug_error("mergeKeyBatch %s offset %lu length %lu\n",
//                    kslice.ToString().c_str(), valueLocs[i].offset,
//                    valueLocs[i].length);
//        }
        STAT_PROCESS(s = _lsm->Merge(wopt, kslice, vslice), StatsType::MERGE_INDEX_UPDATE);
        if (!s.ok()) {
            debug_error("mergeKeyBatch failed: %s\n", s.ToString().c_str());
            return false;
        }
    }
    return ret;
}

bool RocksDBKeyManager::mergeKeyBatch(std::vector<std::string>& keys, std::vector<ValueLocation> valueLocs)
{
    bool ret = true;
    assert(keys.size() == valueLocs.size());
    if (keys.empty())
        return ret;

    rocksdb::Status s;

    // update to LSM-tree
    rocksdb::WriteOptions wopt;
    wopt.sync = ConfigManager::getInstance().syncAfterWrite();

    // put the keys into LSM-tree
    Slice kslice, vslice;
    std::string valueString;
    key_len_t keySize;
    for (int i = 0; i < (int)keys.size(); i++) {
        memcpy(&keySize, keys.at(i).c_str(), sizeof(key_len_t));
        assert((int)keySize + sizeof(key_len_t) == keys.at(i).length());
        kslice = rocksdb::Slice(keys.at(i).substr(sizeof(key_len_t), (int)keySize));
        vslice = rocksdb::Slice(valueLocs.at(i).serializeIndexUpdate());
//        if (keys.at(i).substr(sizeof(key_len_t), (int)keySize) == "user6840842610087607159") {
//            debug_error("mergeKeyBatch %s offset %lu length %lu\n",
//                    kslice.ToString().c_str(), valueLocs[i].offset,
//                    valueLocs[i].length);
//        }
        STAT_PROCESS(s = _lsm->Merge(wopt, kslice, vslice), StatsType::MERGE_INDEX_UPDATE);
        if (!s.ok()) {
            debug_error("%s\n", s.ToString().c_str());
            return false;
        }
    }
    return ret;
}

ValueLocation RocksDBKeyManager::getKey(const char* keyStr, bool checkExist)
{
    std::string value;
    ValueLocation valueLoc;
    valueLoc.segmentId = INVALID_SEGMENT;
    // only use cache for checking before SET
    //    if (checkExist && _cache.lru /* cache enabled */) {
    //        STAT_PROCESS(valueLoc.segmentId = _cache.lru->get((unsigned char*) keyStr), StatsType::KEY_GET_CACHE);
    //    }
    // if not in cache, search in the LSM-tree
    if (valueLoc.segmentId == INVALID_SEGMENT) {
        key_len_t keySize;
        memcpy(&keySize, keyStr, sizeof(key_len_t));
        rocksdb::Slice key(KEY_OFFSET(keyStr), (int)keySize);
        rocksdb::Status status;
        STAT_PROCESS(status = _lsm->Get(rocksdb::ReadOptions(), key, &value), StatsType::KEY_GET_LSM);
        // value location found
        if (status.ok()) {
            valueLoc.deserialize(value);
            debug_trace("valueLoc length %lu offset %lu segmentId %lu\n", valueLoc.length, valueLoc.offset, valueLoc.segmentId);
        }
    }
    return valueLoc;
}

void RocksDBKeyManager::getKeys(char* startingKey, uint32_t n, std::vector<char*>& keys, std::vector<ValueLocation>& locs)
{
    // use the iterator to find the range of keys
    rocksdb::Iterator* it = _lsm->NewIterator(rocksdb::ReadOptions());
    ValueLocation loc;
    char* key;
    key_len_t keySize;
    int keyRecSize;

    memcpy(&keySize, startingKey, sizeof(key_len_t));
    it->Seek(rocksdb::Slice(KEY_OFFSET(startingKey), (int)keySize));

    // NOT SURE
    for (uint32_t i = 0; i < n && it->Valid(); i++, it->Next()) {
        key = new char[sizeof(key_len_t) + it->key().ToString().length()];
        keyRecSize = it->key().ToString().length();
        memcpy(key, &keyRecSize, sizeof(key_len_t));
        memcpy(KEY_OFFSET(key), it->key().ToString().c_str(), it->key().ToString().length());
        // printf("FIND (%u of %u) [%0x][%0x][%0x][%0x]\n", i, n, key[0], key[1], key[2], key[3]);
        keys.push_back(key);
        loc.deserialize(it->value().ToString());
        locs.push_back(loc);
        debug_info("loc length %lu offset %lu segmentId %lu\n", loc.length, loc.offset, loc.segmentId);
    }
    delete it;
}

RocksDBKeyManager::RocksDBKeyIterator *RocksDBKeyManager::getKeyIterator (char *startingKey) {
    rocksdb::Iterator *it = _lsm->NewIterator(rocksdb::ReadOptions());
    it->Seek(rocksdb::Slice(startingKey));

    RocksDBKeyManager::RocksDBKeyIterator *kit = new RocksDBKeyManager::RocksDBKeyIterator(it);
    return kit;
}

// bool RocksDBKeyManager::deleteKey (char *keyStr) {
//     // remove the key from cache
////    if (_cache.lru) {
////        _cache.lru->removeItem((unsigned char*) keyStr);
////    }
//
//    rocksdb::WriteOptions wopt;
//    wopt.sync = ConfigManager::getInstance().syncAfterWrite();
//
//
//    // remove the key from LSM-tree
//    return _lsm->Delete(wopt, rocksdb::Slice(keyStr, KEY_SIZE)).ok();
//}

void RocksDBKeyManager::printCacheUsage(FILE* out)
{
    fprintf(out,
        "Cache Usage (KeyManager):\n");
    //    if (_cache.lru) {
    //        fprintf(out,
    //                " LRU\n"
    //        );
    //        _cache.lru->print(out, true);
    //    }
    fprintf(out,
        " Shadow Hash Table\n");
}

void RocksDBKeyManager::printStats(FILE* out)
{
    std::string stats;
    _lsm->GetProperty("rocksdb.stats", &stats);
    fprintf(out,
        "LSM (KeyManager):\n"
        "%s\n",
        stats.c_str());
}

}
