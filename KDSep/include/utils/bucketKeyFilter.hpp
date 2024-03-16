#pragma once

#include "vlog/ds/bitmap.hh"
#include <unordered_set>

using namespace std;

namespace KDSEP_NAMESPACE {

class BucketKeyFilter {
    public:
    BucketKeyFilter();
    ~BucketKeyFilter();
    bool Insert(const str_t& key);
    bool Insert(const char* str, size_t len);
    bool MayExist(const string& key);
    bool MayExist(const str_t& key);
    bool Erase(const str_t& key);
    void Clear(bool build = true);

    // keys are used only when bm is disabled.
    unordered_set<str_t, mapHashKeyForStr_t, mapEqualKeForStr_t> keys;

    // erased_keys are used only when bm is enabled.
    unordered_set<str_t, mapHashKeyForStr_t, mapEqualKeForStr_t> erased_keys; 
    BitMap* bm = nullptr;

    const int KEYS_THRESHOLD = 10;
    const int BITMAP_SIZE = 16384; //65536; 
    const int REBUILD_THRESHOLD = 20; 

    uint32_t erase_times = 0;
    
    static unsigned int hash1(const str_t& s, int arrSize); 
    static unsigned int hash2(const str_t& s, int arrSize); 
    static unsigned int hash3(const str_t& s, int arrSize); 

    bool ShouldRebuild();
    private:
    bool SingleInsertToBitmap(const str_t& key);
};

}
