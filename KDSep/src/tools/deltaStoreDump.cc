#include "interface/KDSepInterface.hpp"
#include "interface/KDSepOptions.hpp"
#include "interface/mergeOperation.hpp"
#include "utils/appendAbleLRUCache.hpp"
#include "utils/fileOperation.hpp"
#include "utils/murmurHash.hpp"
#include "utils/prefixTree.hpp"
#include "utils/timer.hpp"

using namespace KDSEP_NAMESPACE;

pair<uint64_t, uint64_t> deconstructAndGetValidContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_map<string, vector<string>>& resultMap)
{
    uint64_t processedKeepObjectNumber = 0;
    uint64_t processedTotalObjectNumber = 0;
    uint64_t currentProcessLocationIndex = 0;
    // skip file header
    hashStoreFileHeader currentFileHeader;
    memcpy(&currentFileHeader, fileContentBuffer, sizeof(currentFileHeader));
    currentProcessLocationIndex += sizeof(currentFileHeader);
    while (currentProcessLocationIndex != fileSize) {
        processedKeepObjectNumber++;
        processedTotalObjectNumber++;
        KDRecordHeader currentObjectRecordHeader;
        memcpy(&currentObjectRecordHeader, fileContentBuffer + currentProcessLocationIndex, sizeof(currentObjectRecordHeader));
        currentProcessLocationIndex += sizeof(currentObjectRecordHeader);
        if (currentObjectRecordHeader.is_anchor_ == true) {
            string currentKeyStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                processedKeepObjectNumber -= (resultMap.at(currentKeyStr).size());
                resultMap.at(currentKeyStr).clear();
                resultMap.erase(currentKeyStr);
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                continue;
            } else {
                processedKeepObjectNumber--;
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                continue;
            }
        } else if (currentObjectRecordHeader.is_gc_done_ == true) {
            processedKeepObjectNumber--;
            continue;
        } else {
            string currentKeyStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                string currentValueStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                resultMap.at(currentKeyStr).push_back(currentValueStr);
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            } else {
                vector<string> newValuesRelatedToCurrentKeyVec;
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                string currentValueStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                newValuesRelatedToCurrentKeyVec.push_back(currentValueStr);
                resultMap.insert(make_pair(currentKeyStr, newValuesRelatedToCurrentKeyVec));
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            }
        }
    }
    printf("deconstruct current file header done, file ID = %lu, create reason = %u, prefix length used in this file = %lu, target process file size = %lu, find different key number = %lu, total processed object number = %lu, target keep object number = %lu\n", currentFileHeader.file_id_, currentFileHeader.file_create_reason_, currentFileHeader.current_prefix_used_bit_, fileSize, resultMap.size(), processedTotalObjectNumber, processedKeepObjectNumber);
    return make_pair(processedKeepObjectNumber, processedTotalObjectNumber);
}

bool dump(string path)
{
    FileOperation tempOp(kAlignLinuxIO);
    tempOp.openFile(path);
    uint64_t fileSize = tempOp.getFileSize();
    char readBuffer[fileSize];
    tempOp.readFile(readBuffer, fileSize);
    unordered_map<string, vector<string>> resultMap;
    deconstructAndGetValidContentsFromFile(readBuffer, fileSize, resultMap);
    tempOp.closeFile();
    return true;
}

int main(int argc, char* argv[])
{
    dump(argv[1]);
    return 0;
}
