#ifndef __LOG_MOD_HH__
#define __LOG_MOD_HH__

#include "common/indexStorePreDefines.hpp"
#include "configManager.hh"
#include "deviceManager.hh"
#include "ds/keyvalue.hh"
#include "ds/segment.hh"
#include <map>
#include <vector>

namespace KDSEP_NAMESPACE {

class LogManager {
public:
    LogManager(DeviceManager* deviceManager);
    ~LogManager();

    bool setBatchUpdateKeyValue(std::vector<char*>& keys, std::vector<ValueLocation>& values, std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t>>>& groups);
    bool setBatchGCKeyValue(std::vector<char*>& keys, std::vector<ValueLocation>& values, std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t>>>& groups);
    bool setLogHeadTail(offset_t gcFront, offset_t flushFront);

    bool readBatchUpdateKeyValue(std::vector<std::string>& keys, std::vector<ValueLocation>& values, std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t>>>& groups);
    bool readBatchGCKeyValue(std::vector<std::string>& keys, std::vector<ValueLocation>& values, std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t>>>& groups, bool removeIfCorrupted = true);
    bool getLogHeadTail(offset_t& gcFront, offset_t& flushFront);

    bool ackBatchUpdateKeyValue();
    bool ackBatchGCKeyValue();

    void print(FILE* out = stdout);

    static const char* LOG_MAGIC;

private:
    DeviceManager* _deviceManager;

    struct {
        bool update;
        bool gc;
    } _logAcked;

    struct {
        Segment dataSegment;
        Segment readSegment;
        std::mutex lock;
    } _buffer;

    bool _enabled;

    // FILE* logLengthFd;

    bool setBatchKeyValue(std::vector<char*>& keys, std::vector<ValueLocation>& values, std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t>>>& groups, bool isUpdate);
    bool readBatchKeyValue(std::vector<std::string>& keys, std::vector<ValueLocation>& values, std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t>>>& groups, bool isUpdate, bool removeIfCorrupted = true);
};

}

#endif // define __LOG_MOD_HH__
