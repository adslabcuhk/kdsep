#pragma once

#include "common/dataStructure.hpp"

namespace KDSEP_NAMESPACE {

const bool use_varint_index = true;
//const bool use_varint_kv_header = true;
const bool use_varint_d_header = true;
const bool use_sequence_number = true;

inline char* EncodeVarint32(char* dst, uint32_t v) {
    // Operate on characters as unsigneds
    unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
    static const int B = 128;
    if (v < (1 << 7)) {
        *(ptr++) = v;
    } else if (v < (1 << 14)) {
        *(ptr++) = v | B;
        *(ptr++) = v >> 7;
    } else if (v < (1 << 21)) {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = v >> 14;
    } else if (v < (1 << 28)) {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = (v >> 14) | B;
        *(ptr++) = v >> 21;
    } else {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = (v >> 14) | B;
        *(ptr++) = (v >> 21) | B;
        *(ptr++) = v >> 28;
    }
    return reinterpret_cast<char*>(ptr);
} 

inline char* EncodeVarint64(char* dst, uint64_t v) {
  static const unsigned int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B - 1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

inline void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5];
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint32Varint32(std::string* dst, uint32_t v1, uint32_t v2) {
  char buf[10];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint32Varint32Varint32(std::string* dst, uint32_t v1,
                                        uint32_t v2, uint32_t v3) {
  char buf[15];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  ptr = EncodeVarint32(ptr, v3);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline size_t PutVarint32(char* buf, uint32_t v) {
  char* ptr = EncodeVarint32(buf, v);
  return static_cast<size_t>(ptr - buf);
}

inline size_t PutVarint32Varint32(char* buf, uint32_t v1, uint32_t v2) {
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  return static_cast<size_t>(ptr - buf);
}

inline size_t PutVarint32Varint32Varint32(char* buf, uint32_t v1,
                                        uint32_t v2, uint32_t v3) {
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  ptr = EncodeVarint32(ptr, v3);
  return static_cast<size_t>(ptr - buf);
}

inline void PutVarint64(std::string* dst, uint64_t v) {
  char buf[10];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline size_t PutVarint64(char* buf, uint64_t v) {
  char* ptr = EncodeVarint64(buf, v);
  return static_cast<size_t>(ptr - buf);
}

extern const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                   uint32_t* value); 

inline const char* GetVarint32Ptr(const char* p,
                                  const char* limit,
                                  uint32_t* value) {
  if (p < limit) {
    uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}

inline size_t GetVarint32(const char* input, uint32_t* value) {
  const char* p = input;
  const char* limit = p + 5;
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == nullptr) {
    return 0;
  } else {
    return static_cast<size_t>(q - p);
  }
}

// encoding and decoding of vlog index
inline size_t GetVlogIndexVarintSize(externalIndexInfo index) {
    char buf[15]; 
    return PutVarint32Varint32Varint32(buf, 
            index.externalFileID_, 
            index.externalFileOffset_,
            index.externalContentSize_);
}

inline size_t PutVlogIndexVarint(char* buf, externalIndexInfo index) {
    return PutVarint32Varint32Varint32(buf, 
            index.externalFileID_, 
            index.externalFileOffset_,
            index.externalContentSize_);
}

inline size_t GetVlogIndexVarintSize(char* buf) {
    char* ptr = buf;
    uint32_t num;
    size_t sz1 = GetVarint32(ptr, &num);
    size_t sz2 = GetVarint32(ptr + sz1, &num);
    size_t sz3 = GetVarint32(ptr + sz1 + sz2, &num);
    return sz1 + sz2 + sz3;
}

inline externalIndexInfo GetVlogIndexVarint(const char* buf, size_t& offset) {
    const char* ptr = buf;
    externalIndexInfo index;
    size_t sz1 = GetVarint32(ptr, &index.externalFileID_);
    size_t sz2 = GetVarint32(ptr + sz1, &index.externalFileOffset_);
    size_t sz3 = GetVarint32(ptr + sz1 + sz2, &index.externalContentSize_);
    if (sz1 == 0 || sz2 == 0 || sz3 == 0) {
        fprintf(stderr, "ERROR: GetVarint32 fault. %lu %lu %lu\n",
                sz1, sz2, sz3);
        exit(1);
    }
    offset = sz1 + sz2 + sz3;

    return index;
}

// do not care about the offset
inline externalIndexInfo GetVlogIndexVarint(const char* buf) {
    const char* ptr = buf;
    externalIndexInfo index;
    size_t sz1 = GetVarint32(ptr, &index.externalFileID_);
    size_t sz2 = GetVarint32(ptr + sz1, &index.externalFileOffset_);
    size_t sz3 = GetVarint32(ptr + sz1 + sz2, &index.externalContentSize_);
    if (sz1 == 0 || sz2 == 0 || sz3 == 0) {
        fprintf(stderr, "ERROR: GetVarint32 fault. %lu %lu %lu\n",
                sz1, sz2, sz3);
        exit(1);
    }
    return index;
}


//// encoding and decoding of header
//extern KvHeader GetKVHeaderVarint(char* buf, size_t& offset); 
//extern size_t PutKVHeaderVarint(char* buf, KvHeader header, 
//        bool use_header, bool use_no_delta_meta);

// first byte: [8][4][2][1].
// [1]: The header exists
// [2]: The merge flag is true
// [4]: The value separated flag is true
// [8]: There is a raw value size starting from the next byte
// second byte: raw value size
inline size_t PutKVHeaderVarint(char* buf, KvHeader header, 
        bool use_header = true, bool use_value_size = true) {
    if (use_header == false) {
        buf[0] = 0;
        return 1;
    }
    uint8_t c = 1;
    c |= ((header.mergeFlag_) ? 2 : 0);
    c |= ((header.valueSeparatedFlag_) ? 4 : 0);
    // no need to encode the sequence number because we don't put it in the 
    // LSM-tree

//    if (header.valueSeparatedFlag_ == true || is_delta == false) 
        // No need to put the value size:
        // 1. Whole value, or whole vLog index (is_delta == false)
        // 2. Metadata of deltas in the LSM-tree (is_delta == true &&
        // valueSeparatedFlag_ == true)
    if (use_value_size == false || header.rawValueSize_ == 0) {
        // Giving up...
        buf[0] = c;
        return 1;
    }
    buf[0] = c | 8;
    size_t sz0 = PutVarint32(buf + 1, header.rawValueSize_);
    return sz0 + 1;
}

inline KvHeader GetKVHeaderVarint(const char* buf, size_t& offset) {
    KvHeader header;
    const char* ptr = buf;
    uint8_t c = ptr[0];
    if (c == 0) {
        offset = 1;
        return header; // empty header
    }
    header.mergeFlag_ = ((c & 2) > 0);
    header.valueSeparatedFlag_ = ((c & 4) > 0);
    size_t sz0 = 0;
    if ((c & 8) > 0) {
        sz0 = GetVarint32(ptr + 1, &header.rawValueSize_);
    }
    offset = 1 + sz0;
    return header;
}

inline size_t GetKVHeaderVarintSize(const char* buf) {
    const char* ptr = buf;
    uint8_t c = ptr[0];
    uint32_t num;
    if (c == 0) {
        return 1;
    }
    size_t sz0 = 0;
    if ((c & 8) > 0) {
        sz0 = GetVarint32(ptr + 1, &num);
    }
    return 1 + sz0;
}

// encode the header and get size
inline size_t GetKVHeaderVarintSize(KvHeader header) {
    char buf[5];
    if (header.rawValueSize_ == 0) {
        return 1;
    }
    size_t sz0 = PutVarint32(buf, header.rawValueSize_);
    return sz0 + 1;
}

inline size_t PutDeltaHeaderVarint(char* buf, KDRecordHeader header) {
    uint8_t c = 0;
    c |= ((header.is_anchor_) ? 1 : 0);
    c |= ((header.is_gc_done_) ? 2 : 0);
    c |= (header.key_size_ << 2) & 252;

    buf[0] = c;
    if (header.is_gc_done_) {
        return 1;
    }
    size_t sz0 = 1;
    if (use_sequence_number) {
        sz0 += PutVarint32(buf + sz0, header.seq_num);
    }
    if (header.is_anchor_ == false) {
        sz0 += PutVarint32(buf + sz0, header.value_size_);
    }
    return sz0;
}

inline KDRecordHeader GetDeltaHeaderVarint(const char* buf, 
        size_t& offset) {
    KDRecordHeader header;
    const char* ptr = buf;
    uint8_t c = ptr[0];
    // sanity check:
    if ((c >> 2) >= 26) {
        fprintf(stderr, "key size too large: %u\n", c >> 2);
        exit(1);
    }

    header.is_gc_done_ = ((c & 2) > 0);
    header.is_anchor_ = ((c & 1) > 0);
    if (header.is_gc_done_) {
        offset = 1;
        return header;
    }
    header.key_size_ = c >> 2;
    size_t sz0 = 1; 
    if (use_sequence_number) {
        sz0 += GetVarint32(ptr + sz0, &header.seq_num);
    }
    if (header.is_anchor_ == false) {
        sz0 += GetVarint32(ptr + sz0, &header.value_size_);
    }
    offset = sz0;
    return header;
}

inline KDRecordHeader GetDeltaHeaderVarintMayFail(const char* buf, 
	uint64_t buf_len, size_t& offset, bool& success) {
    KDRecordHeader header;

    success = true;
    if (buf_len == 0) {
	success = false;
	return header;
    }

    const char* ptr = buf;
    uint8_t c = ptr[0];
    header.is_gc_done_ = ((c & 2) > 0);
    header.is_anchor_ = ((c & 1) > 0);
    if (header.is_gc_done_) {
        offset = 1;
        return header;
    }
    header.key_size_ = c >> 2;
    size_t sz0 = 1; 
    char tmpbuf[15];
    memset(tmpbuf, 0xff, sizeof(tmpbuf));
    memcpy(tmpbuf, buf + 1, ((buf_len - 1 >= 14) ? 14 : buf_len - 1)); 

    if (use_sequence_number) {
        sz0 += GetVarint32(tmpbuf, &header.seq_num);
    }
    if (header.is_anchor_ == false) {
        sz0 += GetVarint32(tmpbuf + sz0 - 1, &header.value_size_);
    }

    if (sz0 > buf_len) {
	success = false;
	return header;
    }

    offset = sz0;
    return header;
}

inline size_t GetDeltaHeaderVarintSize(KDRecordHeader header) {
    if (header.is_gc_done_) {
        return 1;
    }
    size_t sz = 0; 
    char buf[5];
    if (use_sequence_number) {
        sz += PutVarint32(buf, header.seq_num);
    }
    if (header.is_anchor_ == false) {
        sz += PutVarint32(buf, header.value_size_);
    }
    return sz + 1;
}

}
