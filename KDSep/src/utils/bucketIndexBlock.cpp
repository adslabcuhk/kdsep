#include "utils/bucketIndexBlock.hpp"

namespace KDSEP_NAMESPACE {

BucketIndexBlock::BucketIndexBlock() {
    key_buf_ = new char[buf_size_];
}

BucketIndexBlock::~BucketIndexBlock() {
    mp_.clear();
    delete[] key_buf_;
}

void BucketIndexBlock::Insert(const str_t& key, size_t kd_size) {
    Insert(string_view(key.data_, key.size_), kd_size);
}

void BucketIndexBlock::Insert(const string_view& key, size_t kd_size) {
//    indices[string(key)] = kd_size;
    indices[key] = kd_size;
}

void BucketIndexBlock::EnlargeBuffer(size_t needed_size) {
    if (needed_size <= buf_size_) {
        return;
    }
    buf_size_ *= 2;
    char* new_buf = new char[buf_size_];
    memcpy(new_buf, key_buf_, key_buf_size_);

    uint64_t offsets[mp_.size()];
    uint64_t sizes[mp_.size()];
    uint64_t v[mp_.size()];

    // renew the mapping
    int i = 0;
    for (auto& it : mp_) {
        auto& str = it.first;
        offsets[i] = str.data() - key_buf_;
        sizes[i] = str.size(); 
        v[i++] = it.second;
    }

    mp_.clear();
    for (int j = 0; j < i; j++) {
        mp_[string_view(new_buf + offsets[j], sizes[j])] = v[j];
    }

    delete[] key_buf_;
    key_buf_ = new_buf;
}

void BucketIndexBlock::Build() {
    size_t last_addr = 0;
    index_block_size_ = 0;
    sorted_part_size_ = 0;
    for (auto& it : indices) {
        if (last_addr == 0 || sorted_part_size_ - last_addr > THRES) {
            // copy the key to the buffer
            EnlargeBuffer(key_buf_size_ + it.first.size());  
            if (key_buf_size_ + it.first.size() > buf_size_) {
                fprintf(stderr, "ERROR: buf space not enough %lu %lu %lu\n",
                        key_buf_size_, it.first.size(), buf_size_);
            }
            memcpy(key_buf_ + key_buf_size_, it.first.data(), it.first.size());
            // insert the key (in the buffer) to the map
//            mp_[string_view(key_buf_ + key_buf_size_, it.first.size())] =
//                sorted_part_size_;
            mp_[string_view(key_buf_ + key_buf_size_, it.first.size())] =
                sorted_part_size_;

//            fprintf(stderr, "---- BUILD ---- key %.*s size %lu\n", 
//                   (int)it.first.size(), it.first.data(), index_block_size_);
//
            key_buf_size_ += it.first.size();
            index_block_size_ += sizeof(it.first.size()) + it.first.size() +
                sizeof(size_t);
            last_addr = sorted_part_size_;
        }
        sorted_part_size_ += it.second;
    }
}

uint64_t BucketIndexBlock::Serialize(char* buf) {
    uint64_t ptr = 0;
    for (auto& it : mp_) {
        auto key_size = it.first.size();
        copyInc(buf, ptr, &key_size, sizeof(key_size)); 
        copyInc(buf, ptr, it.first.data(), key_size); 
        // The size of this data block
        copyInc(buf, ptr, &it.second, sizeof(it.second)); 
//        fprintf(stderr, "%.*s %lu\n", 
//               (int)key_size, it.first.data(), it.second);
    }
    return ptr;
}

// Returns the offset and length of the current index block
pair<uint64_t, uint64_t> BucketIndexBlock::Search(const string_view& key_view) {
    // TODO: change to binary search. Not necessary
    auto end = mp_.end();
//    for (auto i = mp_.begin(); i != end; i++) {
//        fprintf(stderr, "* %.*s %lu\n",
//                (int)i->first.size(), i->first.data(), i->second);
//    }
    for (auto i = mp_.begin(); i != end; i++) {
        if (i == mp_.begin() && key_view < i->first) {
            // the key does not in the part.
            break;
        }
        auto next_i = next(i);
//        fprintf(stderr, "-- %.*s %lu %lu\n", 
//                (int)i->first.size(), i->first.data(), i->second,
//                (next_i == end) ? sorted_part_size_ : next_i->second); 
        if (next_i == end || key_view < next_i->first) {
            uint64_t next_addr = 
                (next_i == end) ? sorted_part_size_ : next_i->second;
//            fprintf(stderr, "get key %.*s %lu %lu (size %lu part %lu) "
//                    "reason %d\n",
//                    (int)key_view.size(), key_view.data(),
//                    i->second, next_addr - i->second,
//                    mp_.size(), sorted_part_size_, 
//                    (next_i == end) ? 1 : 2);
//            if (next_i != end) {
//                fprintf(stderr, "next key %.*s %lu\n",
//                        (int)next_i->first.size(), next_i->first.data(),
//                        next_i->second);
//            }
            return make_pair(i->second, next_addr - i->second);
        }
    }
    return make_pair(0ull, 0ull);
}

uint64_t BucketIndexBlock::GetSize() {
    return index_block_size_;
}

uint64_t BucketIndexBlock::GetSortedPartSize() {
    return sorted_part_size_;
}

void BucketIndexBlock::IndicesClear() {
    indices.clear();
}

void BucketIndexBlock::Clear() {
    indices.clear();
    mp_.clear();
    key_buf_size_ = 0;
    index_block_size_ = 0;
    sorted_part_size_ = 0;
}
}
