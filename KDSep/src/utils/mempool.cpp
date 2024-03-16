#include "utils/mempool.hpp"

using namespace std;

namespace KDSEP_NAMESPACE {

KeyValueMemPool::KeyValueMemPool(uint32_t objectNumberThreshold, uint32_t maxBlockSize)
{
    cerr << "Mempool object number = " << objectNumberThreshold << ", block size = " << maxBlockSize << endl;
    mempoolBlockNumberThreshold_ = objectNumberThreshold;
    mempoolBlockSizeThreshold_ = maxBlockSize;
    mempool_ = new char*[mempoolBlockNumberThreshold_];
    mempoolFreeHandlerVec_ = new uint32_t[mempoolBlockNumberThreshold_];
    for (uint32_t i = 0; i < mempoolBlockNumberThreshold_; i++) {
        mempool_[i] = new char[mempoolBlockSizeThreshold_];
        mempoolFreeHandlerVec_[i] = i;
    }
    mempoolFreeHandlerVecStartPtr_ = 0;
    mempoolFreeHandlerVecEndPtr_ = objectNumberThreshold - 1;
}

KeyValueMemPool::~KeyValueMemPool()
{
    delete[] mempoolFreeHandlerVec_;
    for (uint32_t i = 0; i < mempoolBlockNumberThreshold_; i++) {
        if (mempool_[i] != nullptr) {
            delete[] mempool_[i];
        }
    }
    delete[] mempool_;
}

bool KeyValueMemPool::insertContentToMemPoolAndGetHandler(const string& keyStr, const string& valueStr, uint32_t sequenceNumber, bool isAnchorFlag, mempoolHandler_t& mempoolHandler)
{
    if (valueStr.size() + keyStr.size() >= mempoolBlockSizeThreshold_) {
        debug_error("[ERROR] current key size = %lu, value size = %lu, may exceed mempool block size = %u\n", keyStr.size(), valueStr.size(), mempoolBlockSizeThreshold_);
        char buf[64];
        int ptr = 0;
        for (int i = 0; i < valueStr.size(); i++) {
            int c = valueStr[i];
            if ((c >= '0' && c <= '9') || c == ',' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
                buf[ptr] = c;
                ptr++;
            } else {
                buf[ptr] = (c / 16 >= 10) ? (c / 16 - 10 + 'a') : (c / 16 + '0');
                buf[ptr+1] = (c % 16 >= 10) ? (c % 16 - 10 + 'a') : (c % 16 + '0');
                ptr+=2;
            }
            if (ptr >= 62 || i == valueStr.size() - 1) {
                buf[ptr]='\0';
                fprintf(stderr, "%s\n", buf);
                ptr = 0;
            }
        }
        return false;
    }
    std::scoped_lock<std::shared_mutex> wlock(managerMtx_);
    if (mempoolFreeHandlerVec_[mempoolFreeHandlerVecStartPtr_] == mempoolBlockNumberThreshold_) {
        if (mempoolFreeHandlerVecStartPtr_ > mempoolFreeHandlerVecEndPtr_) {
            debug_error("[ERROR]No free mempool handler, threshold = %u, current free object number = %u, start = %u, end = %u\n", mempoolBlockNumberThreshold_, mempoolFreeHandlerVecStartPtr_ - mempoolFreeHandlerVecEndPtr_, mempoolFreeHandlerVecStartPtr_, mempoolFreeHandlerVecEndPtr_);
        } else {
            debug_error("[ERROR] No free mempool handler, threshold = %u, current free object number = %u, start = %u, end = %u\n", mempoolBlockNumberThreshold_, mempoolFreeHandlerVecEndPtr_ - mempoolFreeHandlerVecStartPtr_, mempoolFreeHandlerVecStartPtr_, mempoolFreeHandlerVecEndPtr_);
        }
        for (uint32_t i = 0; i < mempoolBlockNumberThreshold_; i++) {
            debug_error("[ERROR] Index = %u, ID = %u\n", i, mempoolFreeHandlerVec_[i]);
        }
        return false;
    } else {
        mempoolHandler.mempoolHandlerID_ = mempoolFreeHandlerVec_[mempoolFreeHandlerVecStartPtr_];
        mempoolFreeHandlerVec_[mempoolFreeHandlerVecStartPtr_] = mempoolBlockNumberThreshold_;
        // cerr << "mempoolFreeHandlerVecStartPtr_ = " << mempoolFreeHandlerVecStartPtr_ << ", mempoolHandler.mempoolHandlerID_ = " << mempoolHandler.mempoolHandlerID_ << endl;
        if (mempoolFreeHandlerVecStartPtr_ == mempoolBlockNumberThreshold_ - 1) {
            mempoolFreeHandlerVecStartPtr_ = 0;
        } else {
            mempoolFreeHandlerVecStartPtr_++;
        }
        if (mempoolHandler.mempoolHandlerID_ > mempoolBlockNumberThreshold_ - 1) {
            if (mempoolFreeHandlerVecStartPtr_ > mempoolFreeHandlerVecEndPtr_) {
                debug_error("[ERROR] Get overflowed mempool handler, ID = %u, threshold = %u, current free object number = %u\n", mempoolHandler.mempoolHandlerID_, mempoolBlockNumberThreshold_, mempoolFreeHandlerVecStartPtr_ - mempoolFreeHandlerVecEndPtr_);
            } else {
                debug_error("[ERROR] Get overflowed mempool handler, ID = %u, threshold = %u, current free object number = %u\n", mempoolHandler.mempoolHandlerID_, mempoolBlockNumberThreshold_, mempoolFreeHandlerVecEndPtr_ - mempoolFreeHandlerVecStartPtr_);
            }
            for (uint32_t i = 0; i < mempoolBlockNumberThreshold_; i++) {
                debug_error("[ERROR] Index = %u, ID = %u\n", i, mempoolFreeHandlerVec_[i]);
            }
            return false;
        }
        mempoolHandler.keySize_ = keyStr.size();
        mempoolHandler.valueSize_ = valueStr.size();
        memcpy(mempool_[mempoolHandler.mempoolHandlerID_], keyStr.c_str(), keyStr.size());
        memcpy(mempool_[mempoolHandler.mempoolHandlerID_] + mempoolHandler.keySize_, valueStr.c_str(), valueStr.size());
        mempoolHandler.keyPtr_ = mempool_[mempoolHandler.mempoolHandlerID_];
        mempoolHandler.valuePtr_ = mempool_[mempoolHandler.mempoolHandlerID_] + mempoolHandler.keySize_;
        mempoolHandler.seq_num = sequenceNumber;
        mempoolHandler.isAnchorFlag_ = isAnchorFlag;
        return true;
    }
}

bool KeyValueMemPool::eraseContentFromMemPool(mempoolHandler_t mempoolHandler)
{
    std::scoped_lock<std::shared_mutex> wlock(managerMtx_);
    if (mempoolHandler.mempoolHandlerID_ > mempoolBlockNumberThreshold_ - 1) {
        debug_error("[ERROR] Push back overflowed mempool handler, ID = %u, threshold = %u\n", mempoolHandler.mempoolHandlerID_, mempoolBlockNumberThreshold_);
        return false;
    }
    // cerr << "mempoolFreeHandlerVecEndPtr_ = " << mempoolFreeHandlerVecEndPtr_ << endl;
    if (mempoolFreeHandlerVecEndPtr_ == mempoolFreeHandlerVecStartPtr_) {
        debug_error("[ERROR] Coule not push back, free list start = %u, end = %u\n", mempoolFreeHandlerVecStartPtr_, mempoolFreeHandlerVecEndPtr_);
        for (uint32_t i = 0; i < mempoolBlockNumberThreshold_; i++) {
            debug_error("[ERROR] Index = %u, ID = %u\n", i, mempoolFreeHandlerVec_[i]);
        }
        return false;
    }
    if (mempoolFreeHandlerVecEndPtr_ == mempoolBlockNumberThreshold_ - 1) {
        mempoolFreeHandlerVecEndPtr_ = 0;
    } else {
        mempoolFreeHandlerVecEndPtr_++;
    }
    mempoolFreeHandlerVec_[mempoolFreeHandlerVecEndPtr_] = mempoolHandler.mempoolHandlerID_;
    return true;
}

// Simple version

KeyValueMemPoolSimple::KeyValueMemPoolSimple(uint32_t objectNumberThreshold, uint32_t maxBlockSize)
{
    cerr << "Mempool object number = " << objectNumberThreshold << ", block size = " << maxBlockSize << endl;
    mempoolBlockSizeThreshold_ = maxBlockSize;
}

KeyValueMemPoolSimple::~KeyValueMemPoolSimple()
{
//    delete[] mempoolFreeHandlerVec_;
//    for (uint32_t i = 0; i < mempoolBlockNumberThreshold_; i++) {
//        if (mempool_[i] != nullptr) {
//            delete[] mempool_[i];
//        }
//    }
//    delete[] mempool_;
}

bool KeyValueMemPoolSimple::insertContentToMemPoolAndGetHandler(const string& keyStr, const string& valueStr, uint32_t sequenceNumber, bool isAnchorFlag, mempoolHandler_t& mempoolHandler)
{
    std::scoped_lock<std::shared_mutex> wlock(managerMtx_);
    {
        mempoolHandler.mempoolHandlerID_ = 0;
        mempoolHandler.keySize_ = keyStr.size();
        mempoolHandler.valueSize_ = valueStr.size();
        char* buffer = new char[mempoolBlockSizeThreshold_]; //[keyStr.size() + valueStr.size()];
        memcpy(buffer, keyStr.c_str(), keyStr.size());
        memcpy(buffer + mempoolHandler.keySize_, valueStr.c_str(), valueStr.size());
        mempoolHandler.keyPtr_ = buffer;
        mempoolHandler.valuePtr_ = buffer + mempoolHandler.keySize_;
        mempoolHandler.seq_num = sequenceNumber;
        mempoolHandler.isAnchorFlag_ = isAnchorFlag;
        return true;
    }
}

bool KeyValueMemPoolSimple::eraseContentFromMemPool(mempoolHandler_t mempoolHandler)
{
    std::scoped_lock<std::shared_mutex> wlock(managerMtx_);
    delete[] mempoolHandler.keyPtr_;
    return true;
}
}
