#pragma once

#include "common/dataStructure.hpp" 
#include "utils/statsRecorder.hh"

using namespace std;

namespace KDSEP_NAMESPACE {

typedef str_t key_type;
typedef vector<str_t>* value_type;
typedef std::list<key_type> list_type;
typedef std::unordered_map<key_type, std::pair<value_type, list_type::iterator>, 
        mapHashKeyForStr_t, mapEqualKeForStr_t> map_type;

class lru_cache_str_vector {
public:

    lru_cache_str_vector(size_t capacity)
        : m_capacity(capacity), data_size(0)
    {
    }

    ~lru_cache_str_vector()
    {
//        debug_error("deconstruct %lu, data size %lu\n", m_map.size(), data_size);
        clear();
    }

    static inline size_t size_of_value(value_type value) {
        size_t result = value->size() * sizeof(str_t);
        for (auto& it : *value) {
            result += it.size_;
        }
        return result;
    }

    size_t size()
    {
        return data_size; //m_map.size();
    }

    size_t capacity()
    {
        return m_capacity;
    }

    bool empty()
    {
        return m_map.empty();
    }

    bool contains(key_type& key)
    {
        return m_map.find(key) != m_map.end();
    }

    void insert(key_type& key, value_type value)
    {
        map_type::iterator i = m_map.find(key);
        if (i == m_map.end()) {
            size_t insert_size = size_of_value(value) + key.size_ + sizeof(key_type); 
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
    void update(key_type& key, value_type value) {
        map_type::iterator i = m_map.find(key);
        if (i == m_map.end()) {
            size_t insert_size = size_of_value(value) + key.size_ + sizeof(key_type); 
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

            value_type old_value = i->second.first;
            data_size -= old_value->size() * sizeof(str_t);
            for (auto& it : *old_value) {
                delete[] it.data_;
                data_size -= it.size_;
            }
            delete old_value;
            i->second.first = value;
            data_size += insert_size;

            promote(i, value);

            while (size() >= m_capacity) {
                evict();
            }
        }
    }

    void updateIfExist(key_type& key, value_type value) {
        struct timeval tv;
        gettimeofday(&tv, 0);
        map_type::iterator i = m_map.find(key);
        StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_HASHSTORE_CACHE_FIND, tv);
        if (i != m_map.end()) {
            size_t insert_size = size_of_value(value); 

            value_type old_value = i->second.first;
            data_size -= old_value->size() * sizeof(str_t);
            for (auto& it : *old_value) {
                delete[] it.data_;
                data_size -= it.size_;
            }
            delete old_value;
            i->second.first = value;
            data_size += insert_size;

            promote(i, value);

            while (size() >= m_capacity) {
                evict();
            }
        }
    }

    void cleanIfExist(key_type& key) {
        struct timeval tv;
        gettimeofday(&tv, 0);
        map_type::iterator i = m_map.find(key);
        StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_HASHSTORE_CACHE_FIND, tv);
        if (i != m_map.end()) {
            value_type old_value = i->second.first;
            data_size -= old_value->size() * sizeof(str_t);
            for (auto& it : *old_value) {
                delete[] it.data_;
                data_size -= it.size_;
            }
            old_value->clear();

            promote(i, old_value);
            while (size() >= m_capacity) {
                evict();
            }
        }
    }

    void append(key_type& key, value_type value) {
        map_type::iterator i = m_map.find(key);
        if (i == m_map.end()) {
            size_t insert_size = size_of_value(value) + key.size_ + sizeof(key_type);
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
            data_size += insert_size;
            value_type old_value = i->second.first;

            for (auto& it : *value) {
                old_value->push_back(it);
            }
            promote(i, old_value);
            while (size() >= m_capacity) {
                evict();
            }
        }
    }

    void append(key_type& key, str_t& value_element) {
        map_type::iterator i = m_map.find(key);
        if (i == m_map.end()) {
            size_t insert_size = value_element.size_ + key.size_ + 2 * sizeof(str_t);
            data_size += insert_size;

            // insert item into the cache, but first check if it is full
            while (size() >= m_capacity) {
                // cache is full, evict the least recently used item
                evict();
            }

            value_type value = new vector<str_t>; 
            value->push_back(value_element);

            // insert the new item
            str_t new_key(new char[key.size_], key.size_);
            memcpy(new_key.data_, key.data_, key.size_);
            m_list.push_front(new_key);
            m_map[new_key] = std::make_pair(value, m_list.begin());
        } else {
            size_t insert_size = value_element.size_ + sizeof(str_t);
            data_size += insert_size;
            value_type old_value = i->second.first;
            old_value->push_back(value_element);
            promote(i, old_value);
            while (size() >= m_capacity) {
                evict();
            }
        }
    }

    // value_element is external
    // allocate value space in the function.
    void appendIfExist(key_type& key, str_t& value_element) {
        struct timeval tv;
        gettimeofday(&tv, 0);
        map_type::iterator i = m_map.find(key);
        StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_HASHSTORE_CACHE_FIND, tv);
        if (i != m_map.end()) {
            size_t insert_size = value_element.size_ + sizeof(str_t);
            data_size += insert_size;
            value_type old_value = i->second.first;
            str_t new_value_ele(new char[value_element.size_], value_element.size_);
            memcpy(new_value_ele.data_, value_element.data_, new_value_ele.size_);
            old_value->push_back(new_value_ele);
            promote(i, old_value);
            while (size() >= m_capacity) {
                evict();
            }
        }
    }

    value_type get(const key_type& key)
    {
        // lookup value in the cache
        map_type::iterator i = m_map.find(key);
        if (i == m_map.end()) {
            return nullptr;
        }

        // return the value, but first update its place in the most
        // recently used list
        value_type value = i->second.first;
        promote(i, value);
        return value;
//        if (j != m_list.begin()) {
//            // move item to the front of the most recently used list
//            m_list.erase(j);
//            m_list.push_front(keyStored);
//
//            // update iterator in map
//            j = m_list.begin();
//            value_type value = i->second.first;
//            m_map[keyStored] = std::make_pair(value, j);
//
//            // return the value
//            return value;
//        } else {
//            // the item is already at the front of the most recently
//            // used list so just return it
//            return i->second.first;
//        }
    }

    void clear()
    {
//        uint64_t valueSizes = 0;
//        uint64_t keySizes = 0;
        m_list.clear();
        for (auto& it : m_map) {
            for (auto& it0 : *it.second.first) {
//                valueSizes += it0.size_;
                delete[] it0.data_;
            }
//            keySizes += it.first.size_;
            delete[] it.first.data_;
            delete it.second.first;
        }
//        debug_error("clear, key total sizes %lu, value total sizes %lu\n", keySizes, valueSizes);
        m_map.clear();
        data_size = 0;
    }

private:
    inline void evict()
    {
//        if (evicted <= 10) {
//            debug_error("evict, size %lu\n", data_size); 
//            evicted++;
//        }
        // evict item from the end of most recently used list
        list_type::iterator i = --m_list.end();
        vector<char*> char_ptr_vec;
        value_type evict_value = m_map[*i].first; 
        data_size -= i->size_ + (1 + evict_value->size()) * sizeof(str_t);

        // values
        for (auto& it : *evict_value) {
            char_ptr_vec.push_back(it.data_);
            data_size -= it.size_;
        }
        // key
        char_ptr_vec.push_back(i->data_);

        m_map.erase(*i);
        m_list.erase(i);
        for (auto& it : char_ptr_vec) {
            delete[] it;
        }
        delete evict_value;
    }

    inline void promote(map_type::iterator i, value_type v) {
        struct timeval tv;
        gettimeofday(&tv, 0);
        list_type::iterator j = i->second.second;
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

    map_type m_map;
    list_type m_list;
    size_t m_capacity;
    size_t data_size;
//    unsigned int evicted = 0;
};

class AppendAbleLRUCacheStrVectorShard {
private:
    lru_cache_str_vector* Cache_;
    uint64_t cacheSize_ = 0;
    std::shared_mutex cacheMtx_;

public:
    AppendAbleLRUCacheStrVectorShard(uint64_t cacheSize) {
        cacheSize_ = cacheSize;
        Cache_ = new lru_cache_str_vector(cacheSize_);
    }

    ~AppendAbleLRUCacheStrVectorShard() {
        delete Cache_;
    }

    void insertToCache(str_t& cache_key, value_type data) {
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
    void updateCache(str_t& cache_key, value_type data) {
        std::scoped_lock<std::shared_mutex> r_lock(cacheMtx_);
        Cache_->update(cache_key, data);
    }

    void updateCacheIfExist(str_t& cache_key, value_type data) {
        std::scoped_lock<std::shared_mutex> r_lock(cacheMtx_);
        Cache_->updateIfExist(cache_key, data);
    }

    void cleanCacheIfExist(str_t& cache_key) {
        std::scoped_lock<std::shared_mutex> r_lock(cacheMtx_);
        Cache_->cleanIfExist(cache_key);
    }

    void appendToCache(str_t& cache_key, value_type data) {
        std::scoped_lock<std::shared_mutex> r_lock(cacheMtx_);
        Cache_->append(cache_key, data);
    }

    // data is from external
    void appendToCacheIfExist(str_t& cache_key, str_t& data) {
        std::scoped_lock<std::shared_mutex> r_lock(cacheMtx_);
        Cache_->appendIfExist(cache_key, data);
    }

    value_type getFromCache(str_t& cache_key) {
        std::scoped_lock<std::shared_mutex> r_lock(cacheMtx_);
        return Cache_->get(cache_key);
    }
};

class AppendAbleLRUCacheStrVector {
private:
    AppendAbleLRUCacheStrVectorShard** shards_ = nullptr;
    uint32_t shard_num_ = 64;
    const uint32_t SHARD_MASK = 63;
    uint64_t cacheSize_ = 0;

    inline unsigned int hash(str_t& cache_key) {
        return charBasedHashFunc(cache_key.data_, cache_key.size_) & SHARD_MASK;
    }

public:
    AppendAbleLRUCacheStrVector(uint64_t cacheSize) {
        cacheSize_ = cacheSize;
        shards_ = new AppendAbleLRUCacheStrVectorShard*[shard_num_];
        for (int i = 0; i < shard_num_; i++) {
            shards_[i] = new AppendAbleLRUCacheStrVectorShard(cacheSize / shard_num_);
        }
    }

    ~AppendAbleLRUCacheStrVector() {
        for (int i = 0; i < shard_num_; i++) {
            delete shards_[i];
        }
        delete[] shards_;
    }

    void insertToCache(str_t& cache_key, value_type data) {
        shards_[hash(cache_key)]->insertToCache(cache_key, data);
    }

    bool existsInCache(str_t& cache_key) {
        return shards_[hash(cache_key)]->existsInCache(cache_key);
    }

    // The cache_key does not needs allocation by the caller
    // But the data needs allocation.
    void updateCache(str_t& cache_key, value_type data) {
        shards_[hash(cache_key)]->updateCache(cache_key, data);
    }

    void updateCacheIfExist(str_t& cache_key, value_type data) {
        shards_[hash(cache_key)]->updateCacheIfExist(cache_key, data);
    }

    void cleanCacheIfExist(str_t& cache_key) {
        shards_[hash(cache_key)]->cleanCacheIfExist(cache_key);
    }

    void appendToCache(str_t& cache_key, value_type data) {
        shards_[hash(cache_key)]->appendToCache(cache_key, data);
    }

    // data is from external
    void appendToCacheIfExist(str_t& cache_key, str_t& data) {
        shards_[hash(cache_key)]->appendToCacheIfExist(cache_key, data);
    }

    value_type getFromCache(str_t& cache_key) {
        return shards_[hash(cache_key)]->getFromCache(cache_key);
    }
};

} // KDSEP_NAMESPACE
