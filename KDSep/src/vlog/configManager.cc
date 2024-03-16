#include "vlog/configManager.hh"
#include "utils/debug.hpp"
#include <thread>

namespace KDSEP_NAMESPACE {

void ConfigManager::setConfigPath(const char* path)
{
    boost::property_tree::ini_parser::read_ini(path, _pt);
    assert(!_pt.empty());

    // basic
    _basic.mainSegmentSize = readULL("valueStore.mainSegmentSize");
    _basic.logSegmentSize = readULL("valueStore.logSegmentSize");
    _basic.numMainSegment = readULL("valueStore.numMainSegment");
    _basic.numLogSegment = readULL("valueStore.numLogSegment");
    _basic.retryMax = readInt("valueStore.retryMax");
    _basic.segmentAsFile = readBool("valueStore.segmentAsFile");
    _basic.segmentAsSeparateFile = readBool("valueStore.separateSegmentFile");

    assert(_basic.mainSegmentSize > 0);
    assert(_basic.logSegmentSize > 0);
    assert(_basic.numMainSegment > 0);
    if (_basic.retryMax < 0)
        _basic.retryMax = 0;

    // buffer
    _buffer.updateKVBufferSize = readULL("valueStore.updateKVBufferSize");
    _buffer.inPlaceUpdate = readBool("valueStore.inPlaceUpdate");
    _buffer.numPipelinedBuffer = readInt("valueStore.numPipelinedBuffer");
    if (_buffer.numPipelinedBuffer > MAX_CP_NUM) {
        _buffer.numPipelinedBuffer = MAX_CP_NUM;
    } else if (_buffer.numPipelinedBuffer < 1) {
        _buffer.numPipelinedBuffer = 1;
    }
    _buffer.directIO = readBool("config.directReads"); // TODO
    _buffer.testDirectIO = false;
    _buffer.testIODelayUs = readULL("valueStore.testIODelayUs");
    _buffer.valueCacheSize = readULL("valueStore.valueCacheSize");

    // hotness
    _hotness.levels = 2;
    // if (_hotness.levels <= 0) _hotness.levels = 1;
    // _hotness.useSlave = readBool("hotness.coldVLog");
    // if (!_hotness.useSlave) {
    //     _hotness.coldStorageSize = 0;
    // } else {
    //     _hotness.coldStorageSize = readULL("hotness.coldStorageSize");
    //     if (_hotness.coldStorageSize == 0) {
    //         _hotness.useSlave = false;
    //     }
    // }
    // _hotness.coldStorageDevice = readString("hotness.coldStorageDevice");

    _hotness.useSlave = false;
    _hotness.coldStorageSize = 0;
    _hotness.coldStorageDevice = "";

    // logmeta
    _logmeta.persist = readBool("valueStore.persist");

    // gc
    _gc.greedyGCSize = readUInt("valueStore.greedyGCSize");
    if (_gc.greedyGCSize < 0)
        _gc.greedyGCSize = 1;
    _gc.mode = LOG_ONLY;
    // _gc.numReadThread = readUInt("valueStore.numReadThread");
    // if (_gc.numReadThread < 1) {
    //     _gc.numReadThread = 8;
    // }
    _gc.numReadThread = 8;
    // kv-separation
    _kvsep.minValueSizeToLog = readUInt("valueStore.minValueSizeToLog");
    if (_kvsep.minValueSizeToLog < 0) {
        _kvsep.minValueSizeToLog = 0;
    }
    _kvsep.disabled = false;

    // vlog
    _vlog.enabled = readBool("valueStore.enabled");
    _vlog.gcSize = readUInt("valueStore.gcSize");
    if (_vlog.gcSize <= _buffer.updateKVBufferSize) {
        _vlog.gcSize = (_buffer.updateKVBufferSize == 0 ? 4096 : _buffer.updateKVBufferSize);
    }
    if (_vlog.enabled) {
        //_basic.logSegmentSize = 0;
        _basic.numLogSegment = 0;
        if (_kvsep.disabled) {
            debug_error("Invalid configuration with VLog enabled but KV-separation disabled (%d, %d)\n", _vlog.enabled, _kvsep.disabled);
            exit(-1);
        }
    } else {
        assert(_basic.numLogSegment > 0);
    }

    // consistency
    _consistency.crash = readBool("valueStore.crashProtected");
    if (_consistency.crash && !_basic.segmentAsFile) {
        debug_error("Do not support block device crash consistency (%d, %d)\n", _consistency.crash, _basic.segmentAsFile);
        exit(-1);
    }
    if (_kvsep.disabled) {
        _consistency.crash = false;
    }

    // misc
    _misc.numParallelFlush = readUInt("valueStore.numParallelFlush");
    _misc.numIoThread = readUInt("valueStore.numIoThread");
    _misc.numCPUThread = std::thread::hardware_concurrency();
    _misc.syncAfterWrite = readBool("valueStore.syncAfterWrite");
    _misc.numRangeScanThread = readUInt("valueStore.numRangeScanThread");
    _misc.scanReadAhead = readBool("valueStore.enableScanReadAhead");
    _misc.batchWriteThreshold = readInt("valueStore.writeBatchSize");
    _misc.useMmap = readBool("valueStore.enableMmap");
    _misc.maxOpenFiles = readInt("valueStore.maxOpenFiles");

    if (_misc.numParallelFlush == 0)
        _misc.numParallelFlush = 1;
    if (_misc.numIoThread <= 0)
        _misc.numIoThread = 1;
    if (_misc.numCPUThread <= 0) {
        _misc.numCPUThread = NUM_THREAD;
    }
    if (_misc.numRangeScanThread == 0) {
        _misc.numRangeScanThread = 1;
    }
    if (_misc.maxOpenFiles < -1) {
        _misc.maxOpenFiles = -1;
    }

    // debug
    _debug.level = (DebugLevel)readInt("debug.level");
    if (_debug.level < DebugLevel::NONE) {
        _debug.level = DebugLevel::NONE;
    }
    _debug.scanAllRecordsUponStop = false;

    printConfig();
}

bool ConfigManager::readBool(const char* key)
{
    return _pt.get<bool>(key);
}

int ConfigManager::readInt(const char* key)
{
    return _pt.get<int>(key);
}

unsigned int ConfigManager::readUInt(const char* key)
{
    return _pt.get<unsigned int>(key);
}

LL ConfigManager::readLL(const char* key)
{
    return _pt.get<LL>(key);
}

ULL ConfigManager::readULL(const char* key)
{
    return _pt.get<ULL>(key);
}

double ConfigManager::readFloat(const char* key)
{
    return _pt.get<double>(key);
}

std::string ConfigManager::readString(const char* key)
{
    return _pt.get<std::string>(key);
}

segment_len_t ConfigManager::getSegmentSize(bool isLog) const
{
    assert(!_pt.empty());
    return (isLog) ? _basic.logSegmentSize : _basic.mainSegmentSize;
}

segment_len_t ConfigManager::getMainSegmentSize() const
{
    assert(!_pt.empty());
    return _basic.mainSegmentSize;
}

segment_len_t ConfigManager::getLogSegmentSize() const
{
    assert(!_pt.empty());
    return _basic.logSegmentSize;
}

segment_len_t ConfigManager::getNumMainSegment() const
{
    assert(!_pt.empty());
    return _basic.numMainSegment;
}

segment_len_t ConfigManager::getNumLogSegment() const
{
    assert(!_pt.empty());
    return _basic.numLogSegment;
}

segment_len_t ConfigManager::getNumSegment() const
{
    assert(!_pt.empty());
    return _basic.numMainSegment + _basic.numLogSegment;
}

len_t ConfigManager::getSystemEffectiveCapacity() const
{
    assert(!_pt.empty());
    return (len_t)_basic.numMainSegment * _basic.mainSegmentSize;
}

int ConfigManager::getRetryMax() const
{
    assert(!_pt.empty());
    return _basic.retryMax;
}

bool ConfigManager::segmentAsFile() const
{
    assert(!_pt.empty());
    return _basic.segmentAsFile;
}

bool ConfigManager::segmentAsSeparateFile() const
{
    assert(!_pt.empty());
    return _basic.segmentAsSeparateFile;
}

bool ConfigManager::isUpdateKVBufferEnabled() const
{
    assert(!_pt.empty());
    return (_buffer.updateKVBufferSize > 0);
}

segment_len_t ConfigManager::getUpdateKVBufferSize() const
{
    assert(!_pt.empty());
    return _buffer.updateKVBufferSize;
}

bool ConfigManager::isInPlaceUpdate() const
{
    assert(!_pt.empty());
    return _buffer.inPlaceUpdate;
}

int ConfigManager::getNumPipelinedBuffer() const
{
    assert(!_pt.empty());
    return _buffer.numPipelinedBuffer;
}

bool ConfigManager::usePipelinedBuffer() const
{
    assert(!_pt.empty());
    return _buffer.numPipelinedBuffer > 1;
}

bool ConfigManager::useDirectIO() const
{
    assert(!_pt.empty());
    return _buffer.directIO;
}

bool ConfigManager::testDirectIOCorrectness() const
{
    assert(!_pt.empty());
    return _buffer.testDirectIO;
}

len_t ConfigManager::getTestIODelayUs() const
{
    assert(!_pt.empty());
    return _buffer.testIODelayUs;
}

len_t ConfigManager::valueCacheSize() const
{
    assert(!_pt.empty());
    return _buffer.valueCacheSize;
}

int ConfigManager::getHotnessLevel() const
{
    assert(!_pt.empty());
    return _hotness.levels;
}

bool ConfigManager::useSlave() const
{
    assert(!_pt.empty());
    return false; // _hotness.useSlave;
}

len_t ConfigManager::getColdStorageCapacity() const
{
    assert(!_pt.empty());
    return _hotness.coldStorageSize;
}

std::string ConfigManager::getColdStorageDevice() const
{
    assert(!_pt.empty());
    return _hotness.coldStorageDevice;
}

bool ConfigManager::useSeparateColdStorageDevice() const
{
    assert(!_pt.empty());
    return !_hotness.coldStorageDevice.empty();
}

bool ConfigManager::persistLogMeta() const
{
    assert(!_pt.empty());
    return _logmeta.persist;
}

uint32_t ConfigManager::getGreedyGCSize() const
{
    assert(!_pt.empty());
    return _gc.greedyGCSize;
}

GCMode ConfigManager::getGCMode() const
{
    assert(!_pt.empty());
    return _gc.mode;
}

uint32_t ConfigManager::getNumGCReadThread() const
{
    assert(!_pt.empty());
    return _gc.numReadThread;
}

uint32_t ConfigManager::getMinValueSizeToLog() const
{
    assert(!_pt.empty());
    return _kvsep.minValueSizeToLog;
}

bool ConfigManager::disableKvSeparation() const
{
    assert(!_pt.empty());
    return _kvsep.disabled;
}

bool ConfigManager::enabledVLogMode() const
{
    assert(!_pt.empty());
    return true; // _vlog.enabled;
}

uint32_t ConfigManager::getVLogGCSize() const
{
    assert(!_pt.empty());
    return _vlog.gcSize;
}

bool ConfigManager::enableCrashConsistency() const
{
    assert(!_pt.empty());
    return false; // _consistency.crash;
}

uint32_t ConfigManager::getNumParallelFlush() const
{
    assert(!_pt.empty());
    return _misc.numParallelFlush;
}

uint32_t ConfigManager::getNumIOThread() const
{
    assert(!_pt.empty());
    return _misc.numIoThread;
}

uint32_t ConfigManager::getNumCPUThread() const
{
    assert(!_pt.empty());
    return _misc.numCPUThread;
}

bool ConfigManager::syncAfterWrite() const
{
    assert(!_pt.empty());
    return _misc.syncAfterWrite;
}

uint32_t ConfigManager::getNumRangeScanThread() const
{
    assert(!_pt.empty());
    return _misc.numRangeScanThread;
}

bool ConfigManager::enabledScanReadAhead() const
{
    assert(!_pt.empty());
    return _misc.scanReadAhead;
}

len_t ConfigManager::getBatchWriteThreshold() const
{
    assert(!_pt.empty());
    return _misc.batchWriteThreshold;
}

bool ConfigManager::useMmap() const
{
    assert(!_pt.empty());
    return _misc.useMmap;
}

int ConfigManager::getMaxOpenFiles() const
{
    assert(!_pt.empty());
    return _misc.maxOpenFiles;
}

DebugLevel ConfigManager::getDebugLevel() const
{
    assert(!_pt.empty());
    return _debug.level;
}

bool ConfigManager::scanAllRecordsUponStop() const
{
    assert(!_pt.empty());
    return _debug.scanAllRecordsUponStop;
}

void ConfigManager::printConfig() const
{

    printf(
        "------- Basic -------\n"
        " Main segment size         : %lu B\n"
        " Log segment size          : %lu B\n"
        " Max. no. of main segment  : %lu  \n"
        " Max. no. of log segment   : %lu  \n"
        " Retry max.                  : %d   \n"
        " Store segments as files   : %s   \n",
        getMainSegmentSize(), getLogSegmentSize(), getNumMainSegment(), getNumLogSegment(), getRetryMax(), segmentAsFile() ? "true" : "false");

    printf(
        "------- Buffer ------\n"
        " Update buffer size          : %lu\n"
        "  - Pipe depth               : %d\n"
        " In-place update             : %s\n"
        " Direct I/O                  : %s\n"
        " Value cache size            : %lu\n",
        getUpdateKVBufferSize(), getNumPipelinedBuffer(), isInPlaceUpdate() ? "true" : "false", useDirectIO() ? "true" : "false", valueCacheSize());
    printf(
        "------- Hotness -----\n"
        " Levels                      : %d\n"
        " Use cold storage            : %s\n"
        " Cold storage size           : %lu\n"
        " Cold storage device         : %s\n",
        getHotnessLevel(), useSlave() ? "true" : "false", getColdStorageCapacity(), useSeparateColdStorageDevice() ? getColdStorageDevice().c_str() : "(same, append)");
    printf(
        "--------- GC --------\n"
        " Greedy size                 : %d\n"
        " Mode                        : %s\n"
        " Read threads                : %u\n",
        getGreedyGCSize(), getGCMode() == ALL ? "all" : getGCMode() == LOG_ONLY ? "selctive (ratio)"
                                                                                : "unknown",
        getNumGCReadThread());
    printf(
        "-------  Keys  ------\n"
        " DB Type                     : %s\n"
        "------ Log Meta -----\n"
        " Persist                     : %s\n",
        "LevelDB", persistLogMeta() ? "true" : "false");
    printf(
        "--- KV-separation ---\n"
        " Disabled                    : %s\n"
        " Min. value size             : %u\n",
        disableKvSeparation() ? "true" : "false", getMinValueSizeToLog());
    printf(
        "--- VLog mode ---\n"
        " Enabled                     : %s\n"
        " GC size                     : %u\n",
        enabledVLogMode() ? "true" : "false", getVLogGCSize());
    printf(
        "---- Consistency ----\n"
        " Crash protection            : %s\n",
        enableCrashConsistency() ? "true" : "false");
    printf(
        "-------- Misc -------\n"
        " Use direct IO               : %s\n"
        " Max. no. of I/O threads     : %d\n"
        " Max. no. of CPU threads     : %d\n"
        " No. of parallel segment   : %d\n"
        " Sync. after write           : %s\n"
        " Max. size for batched write : %lu\n"
        " No. of scan threads         : %u\n"
        " Use mmap                    : %s\n"
        "------- Debug  ------\n"
        " Debug Level                 : %d\n"
        " Scan all records upon stop  : %d\n",
#ifdef DISK_DIRECT_IO
        "true"
#else // ifdef DISK_DIRECT_IO
        "false"
#endif // ifdef DISK_DIRECT_IO
        ,
        getNumIOThread(), getNumCPUThread(), getNumParallelFlush(), syncAfterWrite() ? "true" : "false", getBatchWriteThreshold(), getNumRangeScanThread(), useMmap() ? "true" : "false", (int)getDebugLevel(), (int)scanAllRecordsUponStop());
}

}
