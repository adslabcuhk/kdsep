#ifndef YCSB_C_CORE_WORKLOAD_H_
#define YCSB_C_CORE_WORKLOAD_H_

#include <string>
#include <vector>

#include "counter_generator.h"
#include "db.h"
#include "discrete_generator.h"
#include "dist_to_key_generator.h"
#include "up2x_generator.h"
#include "generator.h"
#include "properties.h"
#include "utils.h"

namespace ycsbc {

enum Operation {
    INSERT,
    READ,
    UPDATE,
    OVERWRITE,
    SCAN,
    READMODIFYWRITE
};

class CoreWorkload {
   public:
    ///
    /// The name of the database table to run queries against.
    ///
    static const std::string TABLENAME_PROPERTY;
    static const std::string TABLENAME_DEFAULT;

    ///
    /// The name of the property for the number of fields in a record.
    ///
    static const std::string FIELD_COUNT_PROPERTY;
    static const std::string FIELD_COUNT_DEFAULT;

    ///
    /// The name of the property for the field length distribution.
    /// Options are "uniform", "zipfian" (favoring short records), and "constant".
    ///
    static const std::string FIELD_LENGTH_DISTRIBUTION_PROPERTY;
    static const std::string FIELD_LENGTH_DISTRIBUTION_DEFAULT;

    ///
    /// The name of the property for the length of a field in bytes.
    ///
    static const std::string FIELD_LENGTH_PROPERTY;
    static const std::string FIELD_LENGTH_DEFAULT;

    ///
    /// The name of the property for deciding whether to read one field (false)
    /// or all fields (true) of a record.
    ///
    static const std::string READ_ALL_FIELDS_PROPERTY;
    static const std::string READ_ALL_FIELDS_DEFAULT;

    ///
    /// The name of the property for deciding whether to write one field (false)
    /// or all fields (true) of a record.
    ///
    static const std::string WRITE_ALL_FIELDS_PROPERTY;
    static const std::string WRITE_ALL_FIELDS_DEFAULT;

    ///
    /// The name of the property for the proportion of read transactions.
    ///
    static const std::string READ_PROPORTION_PROPERTY;
    static const std::string READ_PROPORTION_DEFAULT;

    ///
    /// The name of the property for the proportion of update transactions.
    ///
    static const std::string UPDATE_PROPORTION_PROPERTY;
    static const std::string UPDATE_PROPORTION_DEFAULT;

    ///
    /// The name of the property for the proportion of overwrite transactions.
    ///
    static const std::string OVERWRITE_PROPORTION_PROPERTY;
    static const std::string OVERWRITE_PROPORTION_DEFAULT;

    ///
    /// The name of the property for the proportion of insert transactions.
    ///
    static const std::string INSERT_PROPORTION_PROPERTY;
    static const std::string INSERT_PROPORTION_DEFAULT;

    ///
    /// The name of the property for the proportion of scan transactions.
    ///
    static const std::string SCAN_PROPORTION_PROPERTY;
    static const std::string SCAN_PROPORTION_DEFAULT;

    ///
    /// The name of the property for the proportion of
    /// read-modify-write transactions.
    ///
    static const std::string READMODIFYWRITE_PROPORTION_PROPERTY;
    static const std::string READMODIFYWRITE_PROPORTION_DEFAULT;

    ///
    /// The name of the property for the the distribution of request keys.
    /// Options are "uniform", "zipfian" and "latest".
    ///
    static const std::string REQUEST_DISTRIBUTION_PROPERTY;
    static const std::string REQUEST_DISTRIBUTION_DEFAULT;

    ///
    /// The name of the property for the max scan length (number of records).
    ///
    static const std::string MAX_SCAN_LENGTH_PROPERTY;
    static const std::string MAX_SCAN_LENGTH_DEFAULT;

    ///
    /// The name of the property for the scan length distribution.
    /// Options are "uniform" and "zipfian" (favoring short scans).
    ///
    static const std::string SCAN_LENGTH_DISTRIBUTION_PROPERTY;
    static const std::string SCAN_LENGTH_DISTRIBUTION_DEFAULT;

    ///
    /// The name of the property for the order to insert records.
    /// Options are "ordered" or "hashed".
    ///
    static const std::string INSERT_ORDER_PROPERTY;
    static const std::string INSERT_ORDER_DEFAULT;

    static const std::string INSERT_START_PROPERTY;
    static const std::string INSERT_START_DEFAULT;

    static const std::string RECORD_COUNT_PROPERTY;
    static const std::string OPERATION_COUNT_PROPERTY;

    static const std::string ZIPFIAN_CONSTANT_PROPERTY;
    static const std::string ZIPFIAN_CONSTANT_DEFAULT;

    static const std::string LARGE_VALUE_PROPORTION_PROPERTY;
    static const std::string LARGE_VALUE_PROPORTION_DEFAULT;

    static const std::string MID_VALUE_PROPORTION_PROPERTY;
    static const std::string SMALL_VALUE_PROPORTION_PROPERTY;

    static const std::string LARGE_VALUE_SIZE_PROPERTY;
    static const std::string MID_VALUE_SIZE_PROPERTY;
    static const std::string SMALL_VALUE_SIZE_PROPERTY;

    static const std::string PARETO_K;
    static const std::string PARETO_THETA;
    static const std::string PARETO_SIGMA;
    ///
    /// Initialize the scenario.
    /// Called once, in the main client thread, before any operations are started.
    ///
    virtual void Init(const utils::Properties& p, bool run_phase = false);

    virtual void BuildValues(std::vector<ycsbc::YCSBDB::KVPair>& values);
    virtual void BuildUpdate(std::vector<ycsbc::YCSBDB::KVPair>& update);
    virtual void BuildValuesWithKey(const std::string& key, std::vector<ycsbc::YCSBDB::KVPair>& values);
    virtual void BuildUpdateWithKey(const std::string& key, std::vector<ycsbc::YCSBDB::KVPair>& update);

    virtual std::string NextTable() { return table_name_; }
    virtual std::string NextSequenceKey();     /// Used for loading data
    virtual std::string NextTransactionKey();  /// Used for transactions
    virtual Operation NextOperation() { return op_chooser_.Next(); }
    virtual std::string NextFieldName();
    virtual size_t NextScanLength() { return scan_len_chooser_->Next(); }

    bool read_all_fields() const { return read_all_fields_; }
    bool write_all_fields() const { return write_all_fields_; }

    CoreWorkload()
        : field_count_(0), read_all_fields_(false), write_all_fields_(false), field_len_generator_(NULL), field_len_with_key_generator_(NULL), field_len_follow_key_(false), key_generator_(NULL), key_chooser_(NULL), field_chooser_(NULL), scan_len_chooser_(NULL), insert_key_sequence_(3), ordered_inserts_(true), record_count_(0), is_run_phase_(false) {
    }

    virtual ~CoreWorkload() {
        if (field_len_generator_)
            delete field_len_generator_;
        if (field_len_with_key_generator_) 
            delete field_len_with_key_generator_;
        if (up2x_generator_)
            delete up2x_generator_;
        if (key_generator_)
            delete key_generator_;
        if (key_chooser_)
            delete key_chooser_;
        if (field_chooser_)
            delete field_chooser_;
        if (scan_len_chooser_)
            delete scan_len_chooser_;
    }

   protected:
    Generator<uint64_t>* GetFieldLenGenerator(const utils::Properties& p);
    std::string BuildKeyName(uint64_t key_num);

    std::string table_name_;
    int field_count_;
    bool read_all_fields_;
    bool write_all_fields_;
    Generator<uint64_t>* field_len_generator_;
    DistToKeyGenerator* field_len_with_key_generator_ = nullptr;
    UP2XGenerator* up2x_generator_ = nullptr;
    bool field_len_follow_key_;
    Generator<uint64_t>* key_generator_;
    DiscreteGenerator<Operation> op_chooser_;
    Generator<uint64_t>* key_chooser_;
    Generator<uint64_t>* field_chooser_;
    Generator<uint64_t>* scan_len_chooser_;
    CounterGenerator insert_key_sequence_;
    bool ordered_inserts_;
    size_t record_count_;
    size_t operation_count_;
    bool is_run_phase_;
};

inline std::string CoreWorkload::NextSequenceKey() {
    uint64_t key_num = key_generator_->Next();
    return BuildKeyName(key_num);
}

inline std::string CoreWorkload::NextTransactionKey() {
    uint64_t key_num;
    do {
        key_num = key_chooser_->Next();
    } while (key_num > insert_key_sequence_.Last());
    return BuildKeyName(key_num);
}

inline std::string CoreWorkload::BuildKeyName(uint64_t key_num) {
    if (!ordered_inserts_) {
        key_num = utils::Hash(key_num);
    }

    std::string key = std::string("user").append(std::to_string(key_num));
    if (up2x_generator_ != nullptr) {
        uint64_t key_len = up2x_generator_->KeyLength(key);
        uint64_t key_size = key.size();
        if (key_len > key_size) {
            return key;
        }
        return key.substr(key_size - key_len);
    }
    return key;
}

inline std::string CoreWorkload::NextFieldName() {
    return std::string("field").append(std::to_string(field_chooser_->Next()));
}

}  // namespace ycsbc

#endif  // YCSB_C_CORE_WORKLOAD_H_
