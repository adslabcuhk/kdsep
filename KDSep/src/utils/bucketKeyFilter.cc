#include "utils/bucketKeyFilter.hpp"

namespace KDSEP_NAMESPACE {

uint32_t BucketKeyFilter::hash1(const str_t& s, int arrSize) {
    uint32_t hash = 0;
    for (auto i = 0; i < s.size_; i++) {
        hash = hash + ((uint32_t)s.data_[i]);
        hash = hash % arrSize;
    }
    return hash;
}

uint32_t BucketKeyFilter::hash2(const str_t& s, int arrSize) {
    uint32_t hash = 1;
    for (auto i = 0; i < s.size_; i++) {
        hash = hash + pow(19, i) * s.data_[i];
        hash = hash % arrSize;
    }
    return hash % arrSize;
}

uint32_t BucketKeyFilter::hash3(const str_t& s, int arrSize) {
    uint32_t hash = 7;
    for (auto i = 0; i < s.size_; i++) {
        hash = (hash * 31 + s.data_[i]) % arrSize; 
    }
    return hash;
}

BucketKeyFilter::BucketKeyFilter() {
    bm = new BitMap(BITMAP_SIZE);
}

BucketKeyFilter::~BucketKeyFilter() {
    Clear(false);
}

bool BucketKeyFilter::SingleInsertToBitmap(const str_t& key) {
    if (bm == nullptr) bm = new BitMap(BITMAP_SIZE);
    bm->setBit(hash1(key, BITMAP_SIZE));
//    bm->setBit(hash2(key, BITMAP_SIZE));
    bm->setBit(hash3(key, BITMAP_SIZE));
    return true;
}

bool BucketKeyFilter::Insert(const str_t& key) {
    if (bm == nullptr) {
        str_t keyCopy(new char[key.size_], key.size_);
        memcpy(keyCopy.data_, key.data_, key.size_);
        keys.insert(keyCopy);
        if (keys.size() >= KEYS_THRESHOLD) {
            for (auto& it : keys) {
                SingleInsertToBitmap(it);
                delete[] it.data_;
            }
            keys.clear();
        }
    } else {
//        if (keys.size() >= KEYS_THRESHOLD) {
//            keys.clear();
//        }
//        keys.insert(key);
//        auto iter = erased_keys.find(key);
//        if (iter != erased_keys.end()) {
//            delete[] iter->data_;
//            erased_keys.erase(iter);
//        } 
        SingleInsertToBitmap(key);
    }
    return true;
}

bool BucketKeyFilter::Insert(const char* str, size_t len) {
    str_t key(const_cast<char*>(str), len);
    return Insert(key);
}

bool BucketKeyFilter::MayExist(const string& key) {
    str_t keyStr(const_cast<char*>(key.data()), key.size());
    return MayExist(keyStr);
}

bool BucketKeyFilter::MayExist(const str_t& key) {
    if (bm == nullptr) {
        return keys.count(key);
    } else {
        // check the erased_keys!
//        if (keys.count(key)) {
//            return true;
//        }
//        if (erased_keys.count(key)) {
//            return false;
//        }
        if (bm->getBit(hash1(key, BITMAP_SIZE)) == false) {
            return false;
        }
//        if (bm->getBit(hash2(key, BITMAP_SIZE)) == false) {
//            return false;
//        }
        if (bm->getBit(hash3(key, BITMAP_SIZE)) == false) {
            return false;
        }
        return true;
    }
}

bool BucketKeyFilter::Erase(const str_t& key) {
    if (bm == nullptr) {
        auto setIt = keys.find(key);
        if (setIt != keys.end()) {
            delete[] setIt->data_;
            keys.erase(setIt);
        }
    } else {
        erase_times++;
//        str_t keyCopy(new char[key.size_], key.size_);
//        memcpy(keyCopy.data_, key.data_, key.size_);
//        erased_keys.insert(keyCopy);
//        if (keys.count(key)) {
//            keys.erase(key);
//        }
//        if (erased_keys.size() > REBUILD_THRESHOLD) {
//            debug_error("[WARN] erased_keys too many: %d v.s. threshold %d\n",
//                    (int)erased_keys.size(), REBUILD_THRESHOLD);
//        }
    }
    return true;
}

void BucketKeyFilter::Clear(bool build) {
    if (bm != nullptr) {
        delete bm;
        if (build) {
            bm = new BitMap(BITMAP_SIZE);
        } else {
            bm = nullptr;
        }
    }
    for (auto& it : keys) {
        delete[] it.data_;
    }
    keys.clear();
    for (auto& it : erased_keys) {
        delete[] it.data_;
    }
    erased_keys.clear();
    erase_times = 0;
}

bool BucketKeyFilter::ShouldRebuild() {
//    return erased_keys.size() > REBUILD_THRESHOLD;
    return erase_times > REBUILD_THRESHOLD;
}

}
