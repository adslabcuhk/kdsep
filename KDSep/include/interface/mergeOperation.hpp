#pragma once

#include "common/dataStructure.hpp"
#include "common/rocksdbHeaders.hpp"
#include "utils/statsRecorder.hh"
#include "utils/debug.hpp"
#include "utils/headers.hh"
#include <bits/stdc++.h>
using namespace std;
using namespace rocksdb;

namespace KDSEP_NAMESPACE {

class KDSepMergeOperator {
public:
    virtual bool Merge(const string& rawValue, 
            const vector<string>& operandList, string* finalValue) = 0;
    virtual bool Merge(const str_t& rawValue, 
            const vector<str_t>& operandList, string* finalValue) = 0;
    virtual bool PartialMerge(const vector<string>& operandList, 
            vector<string>& finalOperandList) = 0;
    virtual bool PartialMerge(const vector<str_t>& operandList, 
            str_t& finalOperand) = 0;
    virtual string kClassName() = 0;
};

class KDSepFieldUpdateMergeOperator : public KDSepMergeOperator {
public:
    // All functions do not consider the headers
    bool Merge(const string& rawValue, const vector<string>& operandList,
            string* finalValue);
    bool Merge(const str_t& rawValue, const vector<str_t>& operandList, 
            string* finalValue);
    bool PartialMerge(const vector<string>& operands, 
            vector<string>& finalOperandList);
    bool PartialMerge(const vector<str_t>& operands, str_t& finalOperandList);
    string kClassName();
};

class RocksDBInternalMergeOperator : public MergeOperator {
public:
    bool FullMerge(const Slice& key, const Slice* existing_value,
        const std::deque<std::string>& operand_list,
        std::string* new_value, Logger* logger) const override;

    bool PartialMerge(const Slice& key, const Slice& left_operand,
        const Slice& right_operand, std::string* new_value,
        Logger* logger) const override;

    static const char* kClassName() { return "RocksDBInternalMergeOperator"; }
    const char* Name() const override { return kClassName(); }

private:
    // Extract deltas, but the value index does not extract headers
    bool ExtractDeltas(bool value_separated, str_t& operand, 
            uint64_t& delta_off, vector<str_t>& deltas, str_t& new_value_index,
            int& leading_index) const; 
    // Do not include the headers
    bool FullMergeFieldUpdates(str_t& rawValue, vector<str_t>& operandList,
            str_t* finalValue) const;
    // Include the headers
    bool PartialMergeFieldUpdatesWithHeader(
            vector<pair<KvHeader, str_t>>& batchedOperandVec, 
            str_t& finalDeltaListStr) const;
};

} // namespace KDSEP_NAMESPACE
