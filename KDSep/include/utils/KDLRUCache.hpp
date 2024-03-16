#pragma once

#include "common/dataStructure.hpp" 
#include "utils/statsRecorder.hh"

using namespace std;

namespace KDSEP_NAMESPACE {

typedef str_t ktype;
//typedef vector<str_t>* vtype;
typedef str_t vtype;
typedef std::list<ktype> ltype;
typedef std::unordered_map<ktype, std::pair<vtype, ltype::iterator>, 
        mapHashKeyForStr_t, mapEqualKeForStr_t> mtype;

class lru_cache_str_t {
public:

    lru_cache_str_t(size_t capacity)
        : m_capacity(capacity), data_size(0)
    {
    }

    ~lru_cache_str_t();

    static inline size_t size_of_key(ktype key) {
        size_t result = sizeof(str_t) + key.size_;
        return result;
    }

    static inline size_t size_of_value(vtype value) {
//        size_t result = value->size() * sizeof(str_t);
//        for (auto& it : *value) {
//            result += it.size_;
//        }
        size_t result = sizeof(str_t) + value.size_;
        return result;
    }

    static inline size_t size_of_meta() {
        return 24; 
    }

    // release the whole value 
    inline void delete_value(vtype old_value) {
        data_size -= size_of_value(old_value);
        if (old_value.size_ > 0) {
            delete[] old_value.data_;
        }
//        data_size -= old_value->size() * sizeof(str_t);
//        for (auto& it : *old_value) {
//            delete[] it.data_;
//            data_size -= it.size_;
//        }
//        delete old_value;
    } 

    // merely clean the value string and change it to nullptr
    inline void clean_value(vtype old_value) {
        data_size -= old_value.size_;
        if (old_value.size_ > 0) {
            delete[] old_value.data_;
        }
//        data_size -= old_value->size() * sizeof(str_t);
//        for (auto& it : *old_value) {
//            delete[] it.data_;
//            data_size -= it.size_;
//        }
//        old_value->clear();
    }

    size_t size()
    {
        return data_size; //m_map.size();
    }

    void set_capacity(size_t capacity)
    {
        m_capacity = capacity;
        while (size() > m_capacity) {
            uint64_t old_size = size();
            evict();
            if (size() >= old_size) {
                fprintf(stderr, "Error: failed to evict items\n");
                exit(1);
            }
        }
    }

    size_t capacity()
    {
        return m_capacity;
    }

    bool empty()
    {
        return m_map.empty();
    }

    bool contains(ktype& key)
    {
        return m_map.find(key) != m_map.end();
    }

    void insert(ktype& key, vtype value)
    {
        mtype::iterator i = m_map.find(key);
        if (i == m_map.end()) {
            size_t insert_size = size_of_value(value) + size_of_key(key); 
            // insert item into the cache, but first check if it is full
            while (size() + insert_size >= m_capacity) {
                // cache is full, evict the least recently used item
                evict();
            }

            // insert the new item
            data_size += insert_size;
            m_list.push_front(key);
            m_map[key] = std::make_pair(value, m_list.begin());
        }
    }

    // The caller don't need to allocate space for key.
    // But the caller needs to allocate space for the value first
    void update(ktype& key, vtype value) {
        mtype::iterator i = m_map.find(key);
        if (i == m_map.end()) {
            size_t insert_size = size_of_value(value) + size_of_key(key); 
            data_size += insert_size;
                
            // insert item into the cache, but first check if it is full
            while (size() >= m_capacity) {
                // cache is full, evict the least recently used item
                evict();
            }

            // insert the new item
            str_t new_key(new char[key.size_], key.size_);
            memcpy(new_key.data_, key.data_, key.size_);
            m_list.push_front(new_key);
            m_map[new_key] = std::make_pair(value, m_list.begin());
        } else {
            size_t insert_size = size_of_value(value); 

            vtype old_value = i->second.first;
            delete_value(old_value);
            i->second.first = value;
            data_size += insert_size;

            promote(i, value);

            while (size() >= m_capacity) {
                evict();
            }
        }
    }

    void updateIfExist(ktype& key, vtype value) {
        struct timeval tv;
        gettimeofday(&tv, 0);
        mtype::iterator i = m_map.find(key);
        StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_HASHSTORE_CACHE_FIND, tv);
        if (i != m_map.end()) {
            size_t insert_size = size_of_value(value); 

            vtype old_value = i->second.first;
            delete_value(old_value);
            i->second.first = value;
            data_size += insert_size;

            promote(i, value);

            while (size() >= m_capacity) {
                evict();
            }
        }
    }

    void cleanIfExist(ktype& key) {
        struct timeval tv;
        gettimeofday(&tv, 0);
        mtype::iterator i = m_map.find(key);
        StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_HASHSTORE_CACHE_FIND, tv);
        if (i != m_map.end()) {
            vtype old_value = i->second.first;
            clean_value(old_value);
            // indicate an empty delta 
            i->second.first = old_value = str_t(nullptr, 0); 

            promote(i, old_value);
            while (size() >= m_capacity) {
                evict();
            }
        }
    }

    // Removed the append interfaces
    vtype get(const ktype& key)
    {
        // lookup value in the cache
        mtype::iterator i = m_map.find(key);
        if (i == m_map.end()) {
            return str_t(nullptr, 1); // Special label for not exist
        }

        // return the value, but first update its place in the most
        // recently used list
        vtype value = i->second.first;
        promote(i, value);
        return value;
    }

    void clear(); 

    size_t getNumberOfItems() {
        return m_map.size();
    } 

private:
    inline void evict()
    {
//        if (evicted <= 10) {
//            debug_error("evict, size %lu\n", data_size); 
//            evicted++;
//        }
        // evict item from the end of most recently used list
        if (m_list.empty()) {
            return;
        }

        ltype::iterator i = --m_list.end();
        char* char_ptr;
        vtype evict_value = m_map[*i].first; 
        data_size -= size_of_key(*i) + size_of_value(evict_value);

        // value: delete
        if (evict_value.size_ > 0) {
            delete[] evict_value.data_; 
        }
        
        // key; prepare to delete
        char_ptr = i->data_;

        m_map.erase(*i);
        m_list.erase(i);
        delete[] char_ptr;
    }

    inline void promote(mtype::iterator i, vtype v) {
        struct timeval tv;
        gettimeofday(&tv, 0);
        ltype::iterator j = i->second.second;
        str_t keyStored = i->first;
        if (j != m_list.begin()) {
            // move item to the front of the most recently used list
            m_list.erase(j);
            m_list.push_front(keyStored);

            // update iterator in map
            j = m_list.begin();
            m_map[keyStored] = std::make_pair(v, j);
        }
        StatsRecorder::getInstance()->timeProcess(
                StatsType::KDSep_HASHSTORE_CACHE_PROMOTE, tv);
    }

    mtype m_map;
    ltype m_list;
    size_t m_capacity;
    size_t data_size;
//    unsigned int evicted = 0;
};

class AppendAbleLRUCacheStrTShard {
private:
    lru_cache_str_t* Cache_;
    uint64_t cacheSize_ = 0;
    std::shared_mutex cacheMtx_;

public:
    AppendAbleLRUCacheStrTShard(uint64_t cacheSize) {
        cacheSize_ = cacheSize;
        Cache_ = new lru_cache_str_t(cacheSize_);
    }

    ~AppendAbleLRUCacheStrTShard() {
        delete Cache_;
    }

    void insertToCache(str_t& cache_key, vtype data) {
        std::scoped_lock<std::shared_mutex> w_lock(cacheMtx_);
        Cache_->insert(cache_key, data);
    }

    bool existsInCache(str_t& cache_key) {
        std::scoped_lock<std::shared_mutex> r_lock(cacheMtx_);
        bool status = Cache_->contains(cache_key);
        if (status == true) {
            return true;
        } else {
            return false;
        }
    }

    // The cache_key does not needs allocation by the caller
    // But the data needs allocation.
    void updateCache(str_t& cache_key, vtype data) {
        std::scoped_lock<std::shared_mutex> w_lock(cacheMtx_);
        Cache_->update(cache_key, data);
    }

    void set_capacity(size_t cap) {
        std::scoped_lock<std::shared_mutex> w_lock(cacheMtx_);
        Cache_->set_capacity(cap);
    } 

    void updateCacheIfExist(str_t& cache_key, vtype data) {
        std::scoped_lock<std::shared_mutex> r_lock(cacheMtx_);
        Cache_->updateIfExist(cache_key, data);
    }

    void cleanCacheIfExist(str_t& cache_key) {
        std::scoped_lock<std::shared_mutex> r_lock(cacheMtx_);
        Cache_->cleanIfExist(cache_key);
    }

    void clear() {
        std::scoped_lock<std::shared_mutex> r_lock(cacheMtx_);
        Cache_->clear();
    }

    vtype getFromCache(str_t& cache_key) {
        std::scoped_lock<std::shared_mutex> r_lock(cacheMtx_);
        return Cache_->get(cache_key);
    }

    size_t size() {
        return Cache_->size();
    }

    size_t getNumberOfItems() {
        return Cache_->getNumberOfItems();
    }
};

class KDLRUCache {
private:
    AppendAbleLRUCacheStrTShard** shards_ = nullptr;
    uint32_t shard_num_ = 64;
    const uint32_t SHARD_MASK = 63;
    uint64_t cacheSize_ = 2 * 1024 * 1024;
    uint64_t bucket_cache_capacity_ = 1 * 1024 * 1024;

    inline unsigned int hash(str_t& cache_key) {
        return SHARD_MASK & charBasedHashFunc(cache_key.data_, cache_key.size_);
    }

    AppendAbleLRUCacheStrTShard* bucket_shard_ = nullptr;

public:
    KDLRUCache(uint64_t cacheSize) {
        cacheSize_ = cacheSize;
        shards_ = new AppendAbleLRUCacheStrTShard*[shard_num_];
        for (int i = 0; i < shard_num_; i++) {
            shards_[i] = new AppendAbleLRUCacheStrTShard(cacheSize / shard_num_);
        }
        bucket_shard_ = new AppendAbleLRUCacheStrTShard(cacheSize);
    }

    ~KDLRUCache(); 

    void insertToCache(str_t& cache_key, vtype data) {
        shards_[hash(cache_key)]->insertToCache(cache_key, data);
    }

    bool existsInCache(str_t& cache_key) {
        return shards_[hash(cache_key)]->existsInCache(cache_key);
    }

    void updateCacheWholeBucket(str_t& cache_key, vtype data) {
        // TODO
        bucket_shard_->updateCache(cache_key, data);
        if (bucket_shard_->size() > bucket_cache_capacity_ && 
                bucket_cache_capacity_ < cacheSize_) {
            bucket_cache_capacity_ += 1 * 1024 * 1024;
            size_t per_shard_capacity = 
                (cacheSize_ - bucket_cache_capacity_) / shard_num_;
            for (int i = 0; i < shard_num_; i++) {
                shards_[i]->set_capacity(per_shard_capacity);
            }
        }
    }

    inline void resetBucketCache() {
        if (bucket_shard_->size() > 0) {
            bucket_shard_->clear();
            for (int i = 0; i < shard_num_; i++) {
                shards_[i]->set_capacity(cacheSize_ / shard_num_);
            }
            bucket_cache_capacity_ = 1 * 1024 * 1024;
        }
    }

    vtype getFromBucketCache(str_t& cache_key) {
        return bucket_shard_->getFromCache(cache_key);
    }

    // The cache_key does not needs allocation by the caller
    // But the data needs allocation.
    void updateCache(str_t& cache_key, vtype data) {
        resetBucketCache();
        shards_[hash(cache_key)]->updateCache(cache_key, data);
    }

    void updateCacheIfExist(str_t& cache_key, vtype data) {
        resetBucketCache();
        shards_[hash(cache_key)]->updateCacheIfExist(cache_key, data);
    }

    // called when an anchor is written
    void cleanCacheIfExist(str_t& cache_key) {
        resetBucketCache();
        shards_[hash(cache_key)]->cleanCacheIfExist(cache_key);
    }

    vtype getFromCache(str_t& cache_key) {
        return shards_[hash(cache_key)]->getFromCache(cache_key);
    }

    uint64_t getUsage() {
        uint64_t ans = 0;
        for (int i = 0; i < shard_num_; i++) {
            ans += shards_[i]->size();
        }
        return ans;
    }
};

} // KDSEP_NAMESPACE
