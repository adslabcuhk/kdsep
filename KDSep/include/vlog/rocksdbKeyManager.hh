#ifndef __LEVELDB_KEY_MANAGER_HH__
#define __LEVELDB_KEY_MANAGER_HH__

#include <unordered_map>
#include <unordered_set>
#include <cassert>
#include <string>
#include <vector>
#include "interface/KDSepOptions.hpp"
#include "vlog/keyManager.hh"

#define LSM_KEY_MOD_INVALID_ID  (-1)

namespace KDSEP_NAMESPACE {

class RocksDBKeyManager : public KeyManager {
public:
    class RocksDBKeyIterator : public KeyIterator {
        public:
            RocksDBKeyIterator(rocksdb::Iterator *it) {
                _it = it;
            }

            ~RocksDBKeyIterator() {
                release();
            }

            virtual bool isValid() {
                if (_it == 0)
                    return false;
                return _it->Valid();
            }

            virtual void next() {
                if (_it == 0)
                    return;
                return _it->Next();
            }

            virtual void release() {
                delete _it;
                _it = 0;
            }

            virtual std::string key() {
                if (_it == 0)
                    return std::string();
                return _it->key().ToString();
            }

            virtual std::string value() {
                if (_it == 0)
                    return std::string();
                return _it->value().ToString();
            }

        private:
            rocksdb::Iterator *_it;
    };

    RocksDBKeyManager(rocksdb::DB* lsm);
    ~RocksDBKeyManager();

    // interface to access keys and mappings to values
//    bool writeKey (char *keyStr, ValueLocation valueLoc, int needCache = 1);
    bool writeKeyBatch (std::vector<char *> keys, std::vector<ValueLocation> valueLocs);
    bool writeKeyBatch (std::vector<std::string> &keys, std::vector<ValueLocation> valueLocs);
    bool mergeKeyBatch (std::vector<char *> keys, std::vector<ValueLocation> valueLocs);
    bool mergeKeyBatch (std::vector<std::string> &keys, std::vector<ValueLocation> valueLocs);
//
    bool writeMeta (const char *keyStr, int keySize, std::string metadata);
    std::string getMeta (const char *keyStr, int keySize);
    bool persistMeta ();

    ValueLocation getKey (const char *keyStr, bool checkExist = false);
    RocksDBKeyManager::RocksDBKeyIterator *getKeyIterator (char *keyStr);
    void getKeys (char *startingkey, uint32_t n, std::vector<char*> &keys, std::vector<ValueLocation> &locs);

//    bool deleteKey (char *keyStr);
    void printCacheUsage (FILE *out);
    void printStats (FILE *out);

private:
    // lsm tree by leveldb
    rocksdb::DB *_lsm;

    // cached locations of flushed keys
//    struct {
//        LruList *lru;
//    } _cache;

};

}

#endif // ifndef __LEVELDB_KEY_MANAGER_HH__
