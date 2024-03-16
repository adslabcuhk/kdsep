#ifndef __KEYVALUE_HH__
#define __KEYVALUE_HH__

#include "common/dataStructure.hpp"
#include "common/indexStorePreDefines.hpp"
#include "vlog/configManager.hh"
#include "utils/debug.hpp"
#include "utils/headers.hh"
#include "utils/hash.hpp"
#include <stdlib.h>
#define LSM_MASK (0x80000000)

namespace KDSEP_NAMESPACE {

// Scan a KV pair in the buffer.
// Parameters:
//   Input:
//     diskStart - The corresponding starting offset in the disk
//     maxScanSize - The size of buffer
//   Input and Output:
//     keySizeOffset - The relative offset in the buffer where the scan starts
//     remains - The remaining data bytes in the buffer after scanning
//   Output:
//     key, value - The pointer to the real key and value
//     keySize, valueSize - The key and value sizes
//     compactedBytes - The number of bytes with empty bytes (not invalid data. Just zero bytes created because of alignment in direct I/O)
//
// Compared with the original GC design in gcVLog():
//
// diskStart                   <-> gcFront
// maxScanSize                 <-> gcSize
//
// Return false: the KV record is not complete

inline bool scanKeyValue(char* buffer, len_t diskStart, len_t maxScanSize, len_t& keySizeOffset, char*& key, key_len_t& keySize, char*& value, len_t& valueSize, len_t& remains, len_t& compactedBytes, len_t pageSize = 4096)
{
    compactedBytes = 0;

    valueSize = INVALID_LEN;

    assert(remains == maxScanSize - keySizeOffset);
    bool flag = false;
    while (true) {
        // Step 1: get key size
        if (sizeof(key_len_t) > remains) {
            break;
        }

        memcpy(&keySize, buffer + keySizeOffset, sizeof(key_len_t));
        if (keySize == 0) {
            if ((remains + (pageSize - diskStart % pageSize)) % pageSize == 0) {
                debug_error("[ERROR] remains aligned; this page has no content (remains %lu) gcFront %lu keySizeOffset %lu\n", remains, diskStart, keySizeOffset);
                assert(0);
                exit(-1);
            }
            debug_trace("diskStart %lu remains %lu pageSize %lu ... %lu\n", diskStart, remains, pageSize, (remains % pageSize + (pageSize - diskStart % pageSize)) % pageSize);
            len_t compacted = std::min((remains % pageSize + (pageSize - diskStart % pageSize)) % pageSize, remains);
            compactedBytes += compacted;
            keySizeOffset += compacted;
            remains -= compacted;
            debug_trace("keySizeOffset %lu -> %lu compacted %lu (remains %lu)\n", keySizeOffset - compacted, keySizeOffset, compacted, remains);
            continue;
        }

        // Step 2: get key
        if (KEY_REC_SIZE + sizeof(len_t) > remains) {
            break;
        }
        key = buffer + keySizeOffset;

        // Step 3: get value size
        memcpy(&valueSize, buffer + keySizeOffset + KEY_REC_SIZE, sizeof(len_t));

        // Step 4: get value
        if (KEY_REC_SIZE + sizeof(len_t) + valueSize > remains) {
            debug_trace("break3: keySize %lu [%.*s] remains %lu\n", (len_t)keySize, std::min((int)keySize, 16), key, remains);
            break;
        }
        value = buffer + keySizeOffset + KEY_REC_SIZE + sizeof(len_t);

        remains -= KEY_REC_SIZE + sizeof(len_t) + valueSize;
        flag = true;
        break;
    }

    return flag;
}

struct equalKey {
    bool operator()(unsigned char* const& a, unsigned char* const& b) const
    {
        key_len_t keySizeA, keySizeB;
        memcpy(&keySizeA, a, sizeof(key_len_t));
        memcpy(&keySizeB, b, sizeof(key_len_t));

        if (keySizeA != keySizeB) {
            key_len_t keySizeMin = keySizeA > keySizeB ? keySizeB : keySizeA;
            int ret = memcmp(a, b, keySizeMin);
            if (ret == 0) {
                if (keySizeA > keySizeB)
                    return 1;
                return -1;
            }
            return ret;
        }
        return (memcmp(a, b, keySizeA) == 0);
    }
};

struct hashKey {
    size_t operator()(unsigned char* const& s) const
    {
        key_len_t keySize;
        memcpy(&keySize, s, sizeof(key_len_t));
        return HashFunc::hash((char*)s + sizeof(key_len_t), (int)keySize);
        // return std::hash<unsigned char>{}(*s);
    }
};

/*
class KeyValueRecord {
public:
    KeyValueRecord() {
        _keySize = _valueSize = 0;
        _key = _value = nullptr;
        _paddingSize = 0;
        _totalSize = 0;
    }

    bool read(char* recordOffset, len_t maxSize = INVALID_LEN) {
        if (sizeof(key_len_t) > maxSize) {
            return false;
        }
        memcpy(&_keySize, recordOffset, sizeof(key_len_t));

        if (sizeof(key_len_t) + _keySize > maxSize) {
            return false;
        }
        _key = recordOffset + sizeof(key_len_t);

        if (sizeof(key_len_t) + _keySize + sizeof(len_t) > maxSize) {
            return false;
        }
        memcpy(&_valueSize, recordOffset + sizeof(key_len_t) + _keySize);

        if (sizeof(key_len_t) + _keySize + sizeof(len_t) + _valueSize > maxSize) {
            return false;
        }
        _value = recordOffset + sizeof(key_len_t) + _keySize + sizeof(len_t);

        if (sizeof(key_len_t) + _keySize + sizeof(len_t) * 2 + _valueSize > maxSize) {
            return true;
        }
        memcpy(&_paddingSize, recordOffset + sizeof(key_len_t) + _keySize + sizeof(len_t) + _valueSize, sizeof(len_t));
        return true;
    }

    bool write(char* recordOffset, len_t maxSize = INVALID_LEN) {
        if (sizeof(key_len_t) > maxSize) {
            return false;
        }
        memcpy(recordOffset, &_keySize, sizeof(key_len_t));
        recordOffset += sizeof(key_len_t);

        if (sizeof(key_len_t) + _keySize > maxSize) {
            return false;
        }
        memcpy(recordOffset, _key, );
        _key = recordOffset + sizeof(key_len_t);

        if (sizeof(key_len_t) + _keySize + sizeof(len_t) > maxSize) {
            return false;
        }
        memcpy(&_valueSize, recordOffset + sizeof(key_len_t) + _keySize);

        if (sizeof(key_len_t) + _keySize + sizeof(len_t) + _valueSize > maxSize) {
            return false;
        }
        _value = recordOffset + sizeof(key_len_t) + _keySize + sizeof(len_t);

        if (sizeof(key_len_t) + _keySize + sizeof(len_t) * 2 + _valueSize > maxSize) {
            return true;
        }
        memcpy(&_paddingSize, recordOffset + sizeof(key_len_t) + _keySize + sizeof(len_t) + _valueSize, sizeof(len_t));
        return true;
    }

    char* getKey() {
        return _key;
    }

    char* getValue() {
        return _value;
    }

    key_len_t getKeySize() {
        return _keySize;
    }

    len_t getValueSize() {
        return _valueSize;
    }

    len_t getPaddingSize() {
        return _paddingSize;
    }

private:
    key_len_t _keySize;
    len_t _valueSize;
    char* _key;
    char* _value;
    len_t _paddingSize;
    len_t _totalSize;
}
*/

class ValueLocation {
public:
    segment_len_t length;
    segment_id_t segmentId;
    segment_offset_t offset;
    std::string value;

    ValueLocation()
    {
        segmentId = INVALID_SEGMENT;
        offset = INVALID_OFFSET;
        length = INVALID_LEN;
    }

    ~ValueLocation() { }

    static unsigned int size()
    {
        return sizeof(segment_id_t) + sizeof(segment_offset_t) + sizeof(segment_len_t);
    }

    std::string serialize()
    {
        bool disableKvSep = ConfigManager::getInstance().disableKvSeparation();
        bool vlog = ConfigManager::getInstance().enabledVLogMode();
        len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
        len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();
        segment_id_t numMainSegment = ConfigManager::getInstance().getNumMainSegment();

        std::string str;
        // write length
        len_t flength = this->length;
        if (this->segmentId == LSM_SEGMENT) {
            flength |= LSM_MASK;
        }
        str.append((char*)&flength, sizeof(this->length));
        // write segment id (if kv-separation is enabled)
        offset_t foffset = this->offset;
        if (!disableKvSep && !vlog) {
            if (this->segmentId == LSM_SEGMENT) {
                foffset = INVALID_OFFSET;
            } else if (this->segmentId < numMainSegment) {
                foffset = this->segmentId * mainSegmentSize + this->offset;
            } else {
                foffset = mainSegmentSize * numMainSegment + (this->segmentId - numMainSegment) * logSegmentSize + this->offset;
            }
            // str.append((char*) &this->segmentId, sizeof(this->segmentId));
        } else if (!vlog) {
            this->segmentId = LSM_SEGMENT;
        }
        // write value or offset
        if (this->segmentId == LSM_SEGMENT) {
            str.append(value);
        } else {
            str.append((char*)&foffset, sizeof(this->offset));
        }
        return str;
    }

    std::string serializeIndexUpdate()
    {
        bool disableKvSep = ConfigManager::getInstance().disableKvSeparation();
        bool vlog = ConfigManager::getInstance().enabledVLogMode();
        len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
        len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();
        segment_id_t numMainSegment = ConfigManager::getInstance().getNumMainSegment();
        char buf[sizeof(KvHeader) + sizeof(externalIndexInfo) + 1];
        KvHeader header;
        externalIndexInfo index;
        header.mergeFlag_ = header.valueSeparatedFlag_ = true;

        // write length
        len_t flength = this->length;
        if (this->segmentId == LSM_SEGMENT) {
            flength |= LSM_MASK;
        }
        // write segment id (if kv-separation is enabled)
        offset_t foffset = this->offset;
        if (!disableKvSep && !vlog) {
            if (this->segmentId == LSM_SEGMENT) {
                foffset = INVALID_OFFSET;
            } else if (this->segmentId < numMainSegment) {
                foffset = this->segmentId * mainSegmentSize + this->offset;
            } else {
                foffset = mainSegmentSize * numMainSegment + (this->segmentId - numMainSegment) * logSegmentSize + this->offset;
            }
            // str.append((char*) &this->segmentId, sizeof(this->segmentId));
        } else if (!vlog) {
            this->segmentId = LSM_SEGMENT;
        }
        // write value or offset

        index.externalFileID_ = (uint32_t)(foffset >> 32);
        index.externalFileOffset_ = (uint32_t)(foffset % (1ull << 32));
        index.externalContentSize_ = flength;

        size_t header_sz = sizeof(KvHeader);
        size_t index_sz = sizeof(externalIndexInfo);

        // encode header
        header_sz = PutKVHeaderVarint(buf, header);

        // encode index
        if (use_varint_index == false) {
            memcpy(buf + header_sz, &index, index_sz);
        } else {
            index_sz = PutVlogIndexVarint(buf + header_sz, index);
        }

        return std::string(buf, header_sz + index_sz);
    }

    std::string serializeIndexWrite() {
        char buf[sizeof(KvHeader) + sizeof(externalIndexInfo)];
        KvHeader header;
        externalIndexInfo index;

        // put header
        header.mergeFlag_ = false;
        header.valueSeparatedFlag_ = true;

        // put index
        index.externalFileID_ = (uint32_t)(this->offset >> 32);
        index.externalFileOffset_ = (uint32_t)(this->offset % (1ull << 32));
        index.externalContentSize_ = this->length;

        size_t header_sz = sizeof(KvHeader);
        size_t index_sz = sizeof(externalIndexInfo);

        // encode header
        header_sz = PutKVHeaderVarint(buf, header);

        // encode index
        if (use_varint_index == false) {
            memcpy(buf + header_sz, &index, index_sz);
        } else {
            index_sz = PutVlogIndexVarint(buf + header_sz, index);
        }

        return std::string(buf, header_sz + index_sz);
    }

    // Not used in this system now
    bool deserialize(std::string str)
    {
        bool disableKvSep = ConfigManager::getInstance().disableKvSeparation();
        bool vlog = ConfigManager::getInstance().enabledVLogMode();
        len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
        len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();
        segment_id_t numMainSegment = ConfigManager::getInstance().getNumMainSegment();
        segment_id_t numLogSegment = ConfigManager::getInstance().getNumLogSegment();

        const char* cstr = str.c_str();
        size_t offset = 0;
        size_t header_sz = sizeof(KvHeader);

        header_sz = GetKVHeaderVarintSize(cstr);

        externalIndexInfo index;
        if (use_varint_index == false) {
            memcpy(&index, cstr + header_sz, sizeof(externalIndexInfo));
        } else {
            index = GetVlogIndexVarint(cstr + header_sz);
        }

        // read length
        //        memcpy(&this->length, cstr + offset, sizeof(this->length));
        this->length = index.externalContentSize_;
        this->offset = (uint64_t)index.externalFileOffset_ +
            ((uint64_t)(index.externalFileID_) << 32);

        offset += sizeof(this->length);
        if (this->length & LSM_MASK) {
            this->segmentId = LSM_SEGMENT;
            this->length ^= LSM_MASK;
        }
        // read segment id (if kv-separation is enabled)
        if (!disableKvSep && !vlog) {
            // memcpy(&this->segmentId, cstr + offset, sizeof(this->segmentId));
            // offset += sizeof(this->segmentId);
        } else if (!vlog) {
            this->segmentId = LSM_SEGMENT;
        } else {
            this->segmentId = 0;
        }
        // read value or offset
        if (this->segmentId == LSM_SEGMENT) {
            value.assign(cstr + offset, this->length);
        } else {
            //            memcpy(&this->offset, cstr + offset, sizeof(this->offset));
            if (!vlog) {
                if (this->offset < numMainSegment * mainSegmentSize) {
                    this->segmentId = this->offset / mainSegmentSize;
                    this->offset %= mainSegmentSize;
                } else if (this->offset - mainSegmentSize * numMainSegment > logSegmentSize * numLogSegment) {
                    // appended cold storage
                    this->segmentId = numMainSegment + numLogSegment;
                    this->offset = this->offset - (mainSegmentSize * numMainSegment + numLogSegment * logSegmentSize);
                } else {
                    this->segmentId = (this->offset - mainSegmentSize * numMainSegment) / logSegmentSize;
                    this->offset = this->offset - (mainSegmentSize * numMainSegment + this->segmentId * logSegmentSize);
                    this->segmentId += numMainSegment;
                }
            }
        }
        return true;
    }

    inline bool operator==(ValueLocation& vl)
    {
        bool ret = false;
        if (
            (this->segmentId == LSM_SEGMENT && vl.segmentId == LSM_SEGMENT) || ConfigManager::getInstance().disableKvSeparation()) {
            ret = (this->length == vl.length && this->value == vl.value);
        } else {
            ret = (this->segmentId == vl.segmentId && this->offset == vl.offset && this->length == vl.length);
        }
        return ret;
    }
};

}

#endif
