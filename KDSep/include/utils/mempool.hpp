#pragma once

#include "utils/debug.hpp"
#include <bits/stdc++.h>

using namespace std;

namespace KDSEP_NAMESPACE {

typedef struct mempoolHandler_t {
    uint32_t mempoolHandlerID_;
    uint32_t keySize_;
    uint32_t valueSize_;
    uint32_t seq_num;
    bool isAnchorFlag_;
    char* keyPtr_ = nullptr;
    char* valuePtr_ = nullptr;
    mempoolHandler_t(uint32_t mempoolHandlerID,
        uint32_t keySize,
        uint32_t valueSize,
        char* keyPtr,
        char* valuePtr)
    {
        mempoolHandlerID_ = mempoolHandlerID;
        keySize_ = keySize;
        valueSize_ = valueSize;
        keyPtr_ = keyPtr;
        valuePtr_ = valuePtr;
    }
    mempoolHandler_t() { }
} mempoolHandler_t;

class KeyValueMemPoolBase {
public:
    virtual ~KeyValueMemPoolBase() {}
    virtual bool insertContentToMemPoolAndGetHandler(const string& keyStr, const string& valueStr, uint32_t sequenceNumber, bool isAnchorFlag, mempoolHandler_t& mempoolHandler) = 0;
    virtual bool eraseContentFromMemPool(mempoolHandler_t mempoolHandler) = 0;

};

class KeyValueMemPool : public KeyValueMemPoolBase {
public:
    KeyValueMemPool(uint32_t objectNumberThreshold, uint32_t maxBlockSize);
    ~KeyValueMemPool();
    bool insertContentToMemPoolAndGetHandler(const string& keyStr, const string& valueStr, uint32_t sequenceNumber, bool isAnchorFlag, mempoolHandler_t& mempoolHandler) override;
    bool eraseContentFromMemPool(mempoolHandler_t mempoolHandler) override;

private:
    char** mempool_;
    uint32_t mempoolBlockNumberThreshold_;
    uint32_t mempoolBlockSizeThreshold_;
    uint32_t* mempoolFreeHandlerVec_;
    uint32_t mempoolFreeHandlerVecStartPtr_;
    uint32_t mempoolFreeHandlerVecEndPtr_;
    std::shared_mutex managerMtx_;
};

class KeyValueMemPoolSimple : public KeyValueMemPoolBase {
public:
    KeyValueMemPoolSimple(uint32_t objectNumberThreshold, uint32_t maxBlockSize);
    ~KeyValueMemPoolSimple();
    bool insertContentToMemPoolAndGetHandler(const string& keyStr, const string& valueStr, uint32_t sequenceNumber, bool isAnchorFlag, mempoolHandler_t& mempoolHandler) override;
    bool eraseContentFromMemPool(mempoolHandler_t mempoolHandler) override;

private:
    uint32_t mempoolBlockSizeThreshold_;
    std::shared_mutex managerMtx_;
};
}
