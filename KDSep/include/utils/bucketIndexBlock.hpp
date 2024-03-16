#pragma once

#include "utils/utils.hpp"
#include "utils/murmurHash.hpp"
#include "vlog/ds/bitmap.hh"
#include <unordered_set>

using namespace std;

namespace KDSEP_NAMESPACE {

class BucketIndexBlock {
    public:
    BucketIndexBlock();
    ~BucketIndexBlock();
    void Insert(const str_t& key, size_t kd_size);
    void Insert(const string_view& key, size_t kd_size);
    void Build();
    pair<uint64_t, uint64_t> Search(const string_view& key);
    uint64_t Serialize(char* buf);
    uint64_t GetSize();
    uint64_t GetSortedPartSize();
    void EnlargeBuffer(size_t needed_size);
    void IndicesClear();
    void Clear();

    map<string_view, size_t> indices;
//    map<string, size_t> indices;

    private:
    map<string_view, size_t> mp_; 
    char* key_buf_ = nullptr;
    const int THRES = 4088;
    uint64_t buf_size_ = 1024;
    uint64_t key_buf_size_ = 0;
    uint64_t index_block_size_ = 0;
    uint64_t sorted_part_size_ = 0;
};

}
