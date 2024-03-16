#include "interface/mergeOperation.hpp"

namespace KDSEP_NAMESPACE {

bool StringSplit(string str, const string& token, vector<string>& result)
{
    while (str.size()) {
        size_t index = str.find(token);
        if (index != std::string::npos) {
            result.push_back(str.substr(0, index));
            str = str.substr(index + token.size());
        } else {
            result.push_back(str);
            str = "";
        }
    }
    return true;
}

bool StringSplit(const str_t& str, const char& tokenChar, vector<str_t>& result)
{
    char* data = str.data_;
    char* anchor = data;
    int anchorIndex = 0;

    for (int i = 0; i < str.size_; i++) {
        if (data[i] == tokenChar) {
            if (i > anchorIndex) {
                result.push_back(str_t(anchor, i - anchorIndex));
            }
            if (i + 1 < str.size_) {
                anchorIndex = i + 1;
                anchor = data + anchorIndex;
            } else {
                break;
            }
        } else if (i == str.size_ - 1) {
            result.push_back(str_t(anchor, i + 1 - anchorIndex));
        }
    }
    return true;
}

vector<string> StringSplit(string str, const string& token) {
    vector<string> result;
    StringSplit(str, token, result);
    return result;
}

vector<str_t> StringSplit(const str_t& str, const char tokenChar) {
    vector<str_t> result;
    StringSplit(str, tokenChar, result);
    return result;
}

int str_t_stoi(const str_t& str) {
    int ret = 0;

    for (int i = 0; i < str.size_; i++) {
        ret = ret * 10 + (str.data_[i] - '0');
    }
    return ret;
}

int IntToStringSize(int num) {
    int result = 1;
    while (num >= 10) {
        result++;
        num /= 10;
    }
    return result;
}

// Do not consider the headers
bool KDSepFieldUpdateMergeOperator::Merge(
        const string& raw_value, const vector<string>& operands, 
        string* result) {
    str_t rawValueStrT(const_cast<char*>(raw_value.data()), raw_value.size());
    vector<str_t> operands_str;
    for (auto& it : operands) {
        operands_str.push_back(str_t(const_cast<char*>(it.data()), it.size()));
    }
    return Merge(rawValueStrT, operands_str, result);
}

bool KDSepFieldUpdateMergeOperator::Merge(
        const str_t& raw_value, const vector<str_t>& operands, string* result)
{
    unordered_map<int, str_t> operandMap;
    vector<str_t> rawOperandListVec;
    for (auto& it : operands) {
        StringSplit(it, ',', rawOperandListVec);
    }
    for (auto it = 0; it < rawOperandListVec.size(); it += 2) {
        int index = str_t_stoi(rawOperandListVec[it]);
        operandMap[index] = rawOperandListVec[it + 1];
    }
    vector<str_t> raw_value_fields = StringSplit(raw_value, ',');
    for (auto q : operandMap) {
        raw_value_fields[q.first] = q.second;
    }

    int resultSize = -1;
    int index = 0;
    for (auto& it : raw_value_fields) {
        resultSize += it.size_ + 1;
    }

    result->resize(resultSize);
    for (auto i = 0; i < raw_value_fields.size(); i++) {
        auto& it = raw_value_fields[i];
        memcpy(result->data() + index, it.data_, it.size_); 
        if (i < raw_value_fields.size() - 1) {
            result->data()[index + it.size_] = ',';
            index += it.size_ + 1;
        } else {
            index += it.size_;
        }
    }
    if (index != resultSize) {
        debug_error("value size error %d v.s. %d\n", index, resultSize);
    }
    return true;
}

// All the operands are raw deltas (without headers)
bool KDSepFieldUpdateMergeOperator::PartialMerge(const vector<string>& operands, vector<string>& finalOperandList)
{
    unordered_map<int, string> operandMap;
    vector<string> rawOperandListVec;
    for (auto& it : operands) {
        StringSplit(it, ",", rawOperandListVec);
    }
    // cerr << "[Partial] rawOperandListVec size = " << rawOperandListVec.size() << endl;
    for (auto it = 0; it < rawOperandListVec.size(); it += 2) {
        // cerr << "[Partial] rawOperandListVec[" << it << "] = " << rawOperandListVec[it] << endl;
        int index = stoi(rawOperandListVec[it]);
        if (operandMap.find(index) != operandMap.end()) {
            operandMap.at(index).assign(rawOperandListVec[it + 1]);
        } else {
            operandMap.insert(make_pair(index, rawOperandListVec[it + 1]));
        }
    }
    string finalOperator = "";
    for (auto it : operandMap) {
        finalOperator.append(to_string(it.first) + "," + it.second + ",");
    }
    finalOperator = finalOperator.substr(0, finalOperator.size() - 1);
    finalOperandList.push_back(finalOperator);
    return true;
}

// All the operands are raw deltas (without headers)
bool KDSepFieldUpdateMergeOperator::PartialMerge(const vector<str_t>& operands, str_t& result)
{
    unordered_map<int, str_t> operandMap;
    vector<str_t> rawOperandListVec;
    for (auto& it : operands) {
        StringSplit(it, ',', rawOperandListVec);
    }
    for (auto it = 0; it < rawOperandListVec.size(); it += 2) {
        int index = str_t_stoi(rawOperandListVec[it]);
        if (operandMap.find(index) != operandMap.end()) {
            operandMap[index] = rawOperandListVec[it + 1];
        } else {
            operandMap.insert(make_pair(index, rawOperandListVec[it + 1]));
        }
    }
    bool first = true;
    result.size_ = 0;

    for (auto& it : operandMap) {
        if (first) {
            result.size_ += IntToStringSize(it.first) + it.second.size_ + 2;
            first = false;
        } else {
            result.size_ += IntToStringSize(it.first) + it.second.size_ + 2;
        }
    }

    // TODO may allocate a 1-byte buffer if the operands are empty
    result.data_ = new char[result.size_];
    first = true;
    int resultIndex = 0;

    for (auto& it : operandMap) {
        if (first) {
            sprintf(result.data_ + resultIndex, "%d,%.*s", it.first, it.second.size_, it.second.data_); 
            first = false;
        } else {
            sprintf(result.data_ + resultIndex, ",%d,%.*s", it.first, it.second.size_, it.second.data_); 
        }
        // replace strlen()
        resultIndex += it.second.size_ + 1; 
        while (result.data_[resultIndex]) resultIndex++;
    }
    result.size_ = resultIndex;
    return true;
}

string KDSepFieldUpdateMergeOperator::kClassName()
{
    return "KDSepFieldUpdateMergeOperator";
}

inline bool RocksDBInternalMergeOperator::ExtractDeltas(bool value_separated,
        str_t& operand, uint64_t& delta_off, vector<str_t>& deltas, str_t&
        raw_index, int& leading_index) const {
    str_t operand_delta;
    size_t header_sz = sizeof(KvHeader);
    int index_sz = sizeof(externalIndexInfo);

    // slighltly similar to PartialMerge()
    // difference: the value index does not include the header (no need) 
    while (delta_off < operand.size_) {
        KvHeader header;
       
        // increase delta_off
        header = GetKVHeaderVarint(operand.data_ + delta_off, header_sz);

        // extract the oprand
        if (header.mergeFlag_ == true) {
            // index update
            if (use_varint_index == false) {
                assert(header.valueSeparatedFlag_ == true && delta_off + 
                        header_sz + index_sz <= operand.size_);
                raw_index = str_t(operand.data_ + delta_off +
                        header_sz, index_sz);
                delta_off += header_sz + index_sz;
            } else {
                assert(header.valueSeparatedFlag_ == true &&
                        delta_off + header_sz <= operand.size_);
                char* buf = operand.data_ + delta_off + header_sz;
                index_sz = GetVlogIndexVarintSize(buf);
                raw_index = str_t(buf, index_sz);
                delta_off += header_sz + index_sz; 
            }
        } else {
            // Check whether we need to collect raw deltas for merging.
            // 1. The value should be not separated (i.e., should be raw value)
            // 2. The previous deltas (if exists) should also be raw deltas
            // 3. The current deltas should be a raw delta

            if (header.valueSeparatedFlag_ == false) {
                // raw delta
                auto& delta_sz = header.rawValueSize_;
                assert(delta_off + header_sz + delta_sz <= operand.size_);
                deltas.push_back(str_t(operand.data_ + delta_off, 
                            header_sz + delta_sz));
                delta_off += header_sz + delta_sz;

                if (!value_separated && (int)deltas.size() == leading_index + 1) {
                    // Extract the raw delta, prepare for field updates
                    leading_index++;
                }
            } else {
                // separated delta
                assert(delta_off + header_sz <= operand.size_);
                deltas.push_back(str_t(operand.data_ + delta_off, header_sz));
                delta_off += header_sz;
            }

        }
    }
    return true;
}

bool RocksDBInternalMergeOperator::FullMerge(const Slice& key, const Slice* existing_value,
    const std::deque<std::string>& operand_list,
    std::string* new_value, Logger* logger) const
{
    struct timeval tv;
    gettimeofday(&tv, 0);

    // request meRGE Operation when the value is found
//    debug_error("Full merge for key = %s, value size = %lu\n",
//            key.ToString().c_str(), existing_value->size());
//    for (auto& str : operand_list) {
//        debug_error("delta size %lu\n", str.size());
//    }
    str_t raw_index(nullptr, 0);
    size_t vheader_sz = sizeof(KvHeader);
    size_t dheader_sz = sizeof(KvHeader);
    int index_sz = sizeof(externalIndexInfo);

    KvHeader vheader; 

    vheader = GetKVHeaderVarint(existing_value->data(), vheader_sz);

    vector<str_t> leadingRawDeltas;
    vector<str_t> deltas;
    str_t operand;

    bool value_separated = vheader.valueSeparatedFlag_;
    int leading_index = 0;

    // Output format:
    // If value is separated:    
    //      [KvHeader] [externalIndexInfo] [appended deltas if any]
    // If value is not separated:
    //      [KvHeader] [   raw   value   ] [appended deltas if any]

    // Step 1. Scan the deltas in the value 
    {
        uint64_t delta_off = 0;
        if (vheader.valueSeparatedFlag_ == false) {
            delta_off = vheader_sz + vheader.rawValueSize_;
        } else if (use_varint_index == false) {
            delta_off = vheader_sz + index_sz;
        } else {
            delta_off = vheader_sz + GetVlogIndexVarintSize(
                    const_cast<char*>(existing_value->data()) + vheader_sz);
        }
        str_t operand_it(const_cast<char*>(existing_value->data()),
                existing_value->size());

        ExtractDeltas(value_separated, operand_it,
                delta_off, deltas, raw_index, leading_index);
    }

    // Step 2. Scan the deltas in the operand list
    for (auto& operand_list_it : operand_list) {
        uint64_t delta_off = 0;
        str_t operand_it(const_cast<char*>(operand_list_it.data()),
                operand_list_it.size());
        ExtractDeltas(value_separated, operand_it,
                delta_off, deltas, raw_index, leading_index);
    }

    // Step 3. Do full merge on the value
    str_t merged_raw_value(nullptr, 0);
    bool need_free = false;
    str_t raw_value(const_cast<char*>(existing_value->data()) + vheader_sz,
            vheader.rawValueSize_);
    if (leading_index > 0) {
        vector<str_t> raw_deltas;
        for (int i = 0; i < leading_index; i++) {
            dheader_sz = GetKVHeaderVarintSize(deltas[i].data_);
            raw_deltas.push_back(str_t(deltas[i].data_ + dheader_sz,
                        deltas[i].size_ - dheader_sz));
        }
        FullMergeFieldUpdates(raw_value, raw_deltas, &merged_raw_value);
        // need to free the space for full merge later
        need_free = true;

        vheader.rawValueSize_ = merged_raw_value.size_;
    } else if (value_separated) {
        if (raw_index.data_ != nullptr) {
            merged_raw_value = raw_index;
        } else {
            if (use_varint_index) {
                merged_raw_value = str_t(raw_value.data_,
                        GetVlogIndexVarintSize(raw_value.data_));
            } else {
                merged_raw_value = str_t(raw_value.data_, index_sz); 
            }
        }
    } else {
        merged_raw_value = raw_value; 
    }

    // Step 4. Do partial merge on the remaining deltas
    str_t partial_merged_delta(nullptr, 0);
    bool need_free_partial = false;
    if (deltas.size() - leading_index > 0) {
        // There are deltas to merge 
        if (deltas.size() - leading_index == 1) {
            // Only one delta. Directly append
            partial_merged_delta = deltas[leading_index]; 
        } else {
            // TODO manage the sequence numbers
            // copy the headers and the contents to the vector; then perform
            // partial merge
            vector<pair<KvHeader, str_t>> operand_type_vec;
            operand_type_vec.resize(deltas.size() - leading_index);
            uint64_t total_delta_size = 0;
            for (int i = leading_index; i < (int)deltas.size(); i++) {
                KvHeader dheader;
                dheader = GetKVHeaderVarint(deltas[i].data_, dheader_sz);
                operand_type_vec[i - leading_index] = 
                    make_pair(dheader, str_t(deltas[i].data_ + dheader_sz,
                                deltas[i].size_ - dheader_sz)); 
                total_delta_size += deltas[i].size_;
            }

            // partial merged delta include the header
            PartialMergeFieldUpdatesWithHeader(operand_type_vec, 
                    partial_merged_delta);

            debug_info("After partial merge: num deltas %lu, tot sz %lu, "
                    "merged delta size %u\n",
                    operand_type_vec.size(), total_delta_size, 
                    partial_merged_delta.size_);

            // need to free the partial merged delta later
            need_free_partial = true;
        }
    }

    // Step 5. Update header
    // Reuse vheader as an output
    if (partial_merged_delta.size_ > 0) {
        vheader.mergeFlag_ = true;
    }

    char header_buf[16];
    vheader_sz = PutKVHeaderVarint(header_buf, vheader);

    // Prepare space for header, value, and deltas
    new_value->resize(vheader_sz + merged_raw_value.size_ +
            partial_merged_delta.size_);
    char* buffer = new_value->data();
    // write the header. Buffer pointer moved by copyIncBuf()
    copyIncBuf(buffer, &header_buf, vheader_sz);

    // write the value
    if (merged_raw_value.size_ > 0) {
        copyIncBuf(buffer, merged_raw_value.data_, merged_raw_value.size_);
        if (need_free) {
            delete[] merged_raw_value.data_;
        }
    }

    // write the delta
    if (partial_merged_delta.size_ > 0) {
        memcpy(buffer, partial_merged_delta.data_, partial_merged_delta.size_);
        if (need_free_partial) {
            delete[] partial_merged_delta.data_;
        }
    }

    debug_info("Full merge finished for key = %s, value size = %lu, "
            "number of deltas = %lu, num of leading %u, final size = %lu\n", 
            key.ToString().c_str(), existing_value->size(), 
            deltas.size(), leading_index, new_value->size());
    StatsRecorder::getInstance()->timeProcess(StatsType::LSM_FLUSH_ROCKSDB_FULLMERGE, tv);
    return true;
}

bool RocksDBInternalMergeOperator::PartialMerge(
        const Slice& key, const Slice& left_operand, const Slice&
        right_operand, std::string* new_value, Logger* logger) const
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    size_t header_sz = sizeof(KvHeader);
    int index_sz = sizeof(externalIndexInfo);
    vector<str_t> operandStrs;
    operandStrs.push_back(str_t(const_cast<char*>(left_operand.data()), left_operand.size()));
    operandStrs.push_back(str_t(const_cast<char*>(right_operand.data()), right_operand.size()));
    str_t index(nullptr, 0);
    vector<pair<KvHeader, str_t>> headers_deltas;

    // A slightly different version of ExtractDeltas()
    // difference: the new value index includes the header here.
    for (auto& it : operandStrs) {
        uint64_t delta_off = 0;
        while (delta_off < it.size_) {
            KvHeader header;

            // increase delta_off. Extract the header
            header = GetKVHeaderVarint(it.data_ + delta_off, header_sz);
                
            // extract the operand
            if (header.mergeFlag_ == true) {
                // index update
                if (use_varint_index == false) {
                    assert(header.valueSeparatedFlag_ == true && 
                            delta_off + header_sz + index_sz <=
                            it.size_);
                    // includes the header
                    index = str_t(it.data_ + delta_off, header_sz +
                            index_sz);
                    delta_off += header_sz + index_sz;
                } else {
                    assert(header.valueSeparatedFlag_ == true && 
                            delta_off + header_sz < it.size_);
                    char* buf = it.data_ + delta_off;
                    index_sz = GetVlogIndexVarintSize(buf);
                    // includes the header
                    index = str_t(buf, header_sz + index_sz);
                    delta_off += header_sz + index_sz; 
                }
            } else {
                if (header.valueSeparatedFlag_ == false) {
                    // raw delta
                    auto& delta_sz = header.rawValueSize_;
                    assert(delta_off + header_sz + delta_sz <= it.size_);
                    // separate the header
                    headers_deltas.push_back(make_pair(header, 
                                str_t(it.data_ + delta_off + header_sz,
                                    delta_sz)));
                    delta_off += header_sz + delta_sz;
                } else {
                    // separated delta
                    assert(delta_off + header_sz <= it.size_);
                    headers_deltas.push_back(make_pair(header, 
                                str_t(it.data_ + delta_off, 0)));
                    delta_off += header_sz;
                }
            }
        }
    }

    str_t final_deltas;
    // merge with headers. The output has header
    PartialMergeFieldUpdatesWithHeader(headers_deltas, final_deltas);
    if (index.size_ > 0) {
        new_value->resize(index.size_ + final_deltas.size_);
        char* buffer = new_value->data();
        copyIncBuf(buffer, index.data_, index.size_);
        memcpy(buffer, final_deltas.data_, final_deltas.size_);
    } else {
        new_value->assign(final_deltas.data_, final_deltas.size_);
    }
    delete[] final_deltas.data_;
    StatsRecorder::getInstance()->timeProcess(StatsType::LSM_FLUSH_ROCKSDB_PARTIALMERGE, tv);
    return true;
}

// operand_type_vec[i].second do not have the header
// If all of them are raw values, can do the partial merge.
// Otherwise, do the partial merge only when the raw values are continuous.
// Example: [separated] [1,A] [2,B] [1,C] [separated] -> [separated] [2,B] [1,C] [separated]
// The original implementation is incorrect; it keeps only the earliest one, and the sequence is all reversed
inline bool RocksDBInternalMergeOperator::PartialMergeFieldUpdatesWithHeader(
        vector<pair<KvHeader, str_t>>& operand_type_vec, str_t& final_result)
    const {
    vector<KvHeader> separated_headers;
    unordered_map<int, str_t> raw_fields;

    // empty - separated deltas; not empty, raw deltas
    vector<unordered_map<int, str_t>> raw_fields_result; 
    vector<int> raw_delta_sizes;

    int final_size = 0;
    size_t header_sz = sizeof(KvHeader);
    KvHeader header; // tmp header

    // Extract the raw fields 
    for (auto i = 0; i < operand_type_vec.size(); i++) {
        if (operand_type_vec[i].first.valueSeparatedFlag_ == false) {
            // not delta separated
            vector<str_t> operand_list;
            StringSplit(operand_type_vec[i].second, ',', operand_list);

            // map the raw fields to their indices
            for (auto j = 0; j < operand_list.size(); j += 2) {
                int index = str_t_stoi(operand_list[j]);
                raw_fields[index] = operand_list[j+1];
            }
        } else {
            // delta separated
            if (!raw_fields.empty()) {
                // the delta metadata
                raw_fields_result.push_back(raw_fields);
                raw_fields.clear();
            }
            raw_fields_result.push_back({});
            // only the delta metadata needs header information
            separated_headers.push_back(operand_type_vec[i].first);
        }
    }

    if (!raw_fields.empty()) {
        raw_fields_result.push_back(raw_fields);
        raw_fields.clear();
    }

    raw_delta_sizes.resize(raw_fields_result.size());
    int i = 0;

    // Calculate the final delta size
    for (auto& it : raw_fields_result) {
        int raw_delta_size = 0;
        if (it.empty() == false) {
            for (auto& it0 : it) {
                // index, two commas, and the field content
                raw_delta_size += IntToStringSize(it0.first) + it0.second.size_ + 2;
            }
            // delete the last comma
            raw_delta_size--;
        }
        // record the raw delta size in the vector
        header.rawValueSize_ = raw_delta_size; 
        raw_delta_sizes[i++] = raw_delta_size;
        final_size += GetKVHeaderVarintSize(header) + raw_delta_size;
    }

    debug_info("PartialMerge raw delta number %lu, "
            "separated deltas %lu, final result size %d\n", 
            operand_type_vec.size(), separated_headers.size(),
            final_size);

    char* result = new char[final_size + 1];
    int result_i = 0;

    // Build the final delta
    // the index of separated deltas
    // The header variable already has double falses
    i = 0;
    int j = 0;

    for (auto& it : raw_fields_result) {
        if (it.empty() == false) {
            bool first = true;
            int tmp_i = result_i;
            header.rawValueSize_ = raw_delta_sizes[j++];
            header_sz = PutKVHeaderVarint(result + result_i, header);
            result_i += header_sz;
            for (auto& it0 : it) {
                if (first) {
                    sprintf(result + result_i, "%d,%.*s", it0.first,
                            it0.second.size_, it0.second.data_);
                    first = false;
                } else {
                    sprintf(result + result_i, ",%d,%.*s", it0.first,
                            it0.second.size_, it0.second.data_);
                }
                result_i += it0.second.size_ + 1;
                while (result[result_i]) 
                    result_i++;
            }
            if (header.rawValueSize_ != result_i - tmp_i - header_sz) {
                debug_error("Header size error: %d %d\n", 
                        header.rawValueSize_,
                        (int)(result_i - tmp_i - header_sz));
                exit(1);
            }
        } else {
            header_sz = PutKVHeaderVarint(result + result_i,
                    separated_headers[i]);
            result_i += header_sz;
            i++;
        }
    }

    final_result = str_t(result, final_size);
    return true;
}

// Do not include the headers
inline bool RocksDBInternalMergeOperator::FullMergeFieldUpdates(
        str_t& raw_value, vector<str_t>& operands, str_t* result) const
{
    int buffer_size = -1;

    vector<str_t> raw_value_fields;
    StringSplit(raw_value, ',', raw_value_fields);

    vector<str_t> rawOperandsVec;

    for (auto& it : operands) {
        StringSplit(it, ',', rawOperandsVec); 
    }

    for (auto it = 0; it < rawOperandsVec.size(); it += 2) {
        int index = str_t_stoi(rawOperandsVec[it]);
        raw_value_fields[index] = rawOperandsVec[it+1];
    }

    for (auto& it : raw_value_fields) {
        buffer_size += it.size_ + 1;
    }

    char* buffer = new char[buffer_size];
    int buffer_index = 0;

    for (auto i = 0; i < raw_value_fields.size() - 1; i++) {
        memcpy(buffer + buffer_index, raw_value_fields[i].data_, raw_value_fields[i].size_);
        buffer[buffer_index + raw_value_fields[i].size_] = ',';
        buffer_index += raw_value_fields[i].size_ + 1; 
    }
    memcpy(buffer + buffer_index, 
            raw_value_fields[raw_value_fields.size()-1].data_, 
            raw_value_fields[raw_value_fields.size()-1].size_);
    result->data_ = buffer;
    result->size_ = buffer_size;
    return true;
}


} // namespace KDSEP_NAMESPACE
