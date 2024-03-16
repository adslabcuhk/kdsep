#pragma once

#include <bits/stdc++.h>
#include "malloc.h"

using namespace std;

namespace KDSEP_NAMESPACE {

inline static uint64_t prefixSubstr(uint64_t prefix, uint64_t len) {
    return prefix & ((1 << len) - 1);
} 

inline static uint64_t prefixStrToU64(const string& prefix) {
    uint64_t prefix_u64 = 0;
    if (prefix.size() == 0) {
	return 0;
    }
    for (auto i = prefix.size() - 1; i >= 0; i--) {
        prefix_u64 = (prefix_u64 << 1) + (prefix[i] - '0');
	if (i == 0) break;
    }
    return prefix_u64;
}

inline static uint64_t prefixExtract(uint64_t k) {
    return k & ((1ull << 56) - 1);
}

inline static uint64_t prefixLenExtract(uint64_t k) {
    return k >> 56;
}

inline static uint64_t prefixConcat(uint64_t prefix_u64, uint64_t len) {
    return (len << 56) | prefixSubstr(prefix_u64, len);
}

inline static void copyInc(char* buf, uint64_t& index, 
        const void* read_buf, size_t size) {
    memcpy(buf + index, (const char*)read_buf, size);
    index += size;
}

inline static void copyIncBuf(char*& buf, const void* read_buf, size_t size) {
    memcpy(buf, (const char*)read_buf, size);
    buf += size;
}

unsigned long long inline timevalToMicros(struct timeval& res) {
    return res.tv_sec * 1000000ull + res.tv_usec;
}

inline void printValueString(const string& value) {
    for (int i = 0; i < value.size(); i++) {
        auto c = value[i];
        if (!isdigit(c) && !isalpha(c) && c != ',') {
            fprintf(stderr, "[%d]", (int)c);
        } else {
            fprintf(stderr, "%c", c); 
        }
        if (i % 160 == 159) {
            fprintf(stderr, "\n");
        }
    }
    fprintf(stderr, "\n");
}

size_t getRss(); 
size_t getRssNoTrim(); 
// should add "inline" unless there are link errors
}
