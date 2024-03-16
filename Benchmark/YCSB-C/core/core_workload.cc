#include "core_workload.h"

#include <string>

#include "const_generator.h"
#include "pareto_generator.h"
#include "ratio_generator.h"
#include "scrambled_zipfian_generator.h"
#include "skewed_latest_generator.h"
#include "uniform_generator.h"
#include "zipfian_generator.h"

#include "up2x_generator.h"

using std::string;
using ycsbc::CoreWorkload;

const string CoreWorkload::TABLENAME_PROPERTY = "table";
const string CoreWorkload::TABLENAME_DEFAULT = "usertable";

const string CoreWorkload::FIELD_COUNT_PROPERTY = "fieldcount";
const string CoreWorkload::FIELD_COUNT_DEFAULT = "10";

const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_PROPERTY =
    "field_len_dist";
const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_DEFAULT = "constant";

const string CoreWorkload::FIELD_LENGTH_PROPERTY = "fieldlength";
const string CoreWorkload::FIELD_LENGTH_DEFAULT = "100";

const string CoreWorkload::READ_ALL_FIELDS_PROPERTY = "readallfields";
const string CoreWorkload::READ_ALL_FIELDS_DEFAULT = "true";

const string CoreWorkload::WRITE_ALL_FIELDS_PROPERTY = "writeallfields";
const string CoreWorkload::WRITE_ALL_FIELDS_DEFAULT = "false";

const string CoreWorkload::READ_PROPORTION_PROPERTY = "readproportion";
const string CoreWorkload::READ_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::UPDATE_PROPORTION_PROPERTY = "updateproportion";
const string CoreWorkload::UPDATE_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::OVERWRITE_PROPORTION_PROPERTY =
    "overwriteproportion";
const string CoreWorkload::OVERWRITE_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::INSERT_PROPORTION_PROPERTY = "insertproportion";
const string CoreWorkload::INSERT_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::SCAN_PROPORTION_PROPERTY = "scanproportion";
const string CoreWorkload::SCAN_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::LARGE_VALUE_PROPORTION_PROPERTY = "largeproportion";
const string CoreWorkload::LARGE_VALUE_PROPORTION_DEFAULT = "1";

const string CoreWorkload::MID_VALUE_PROPORTION_PROPERTY = "midproportion";
const string CoreWorkload::SMALL_VALUE_PROPORTION_PROPERTY = "smallproportion";

const string CoreWorkload::LARGE_VALUE_SIZE_PROPERTY = "largesize";
const string CoreWorkload::MID_VALUE_SIZE_PROPERTY = "midsize";
const string CoreWorkload::SMALL_VALUE_SIZE_PROPERTY = "smallsize";

const string CoreWorkload::PARETO_K = "pareto_k";
const string CoreWorkload::PARETO_THETA = "pareto_theta";
const string CoreWorkload::PARETO_SIGMA = "pareto_sigma";

const string CoreWorkload::READMODIFYWRITE_PROPORTION_PROPERTY =
    "readmodifywriteproportion";
const string CoreWorkload::READMODIFYWRITE_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::REQUEST_DISTRIBUTION_PROPERTY =
    "requestdistribution";
const string CoreWorkload::REQUEST_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
const string CoreWorkload::MAX_SCAN_LENGTH_DEFAULT = "1000";

const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_PROPERTY =
    "scanlengthdistribution";
const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::INSERT_ORDER_PROPERTY = "insertorder";
const string CoreWorkload::INSERT_ORDER_DEFAULT = "hashed";

const string CoreWorkload::INSERT_START_PROPERTY = "insertstart";
const string CoreWorkload::INSERT_START_DEFAULT = "0";

const string CoreWorkload::RECORD_COUNT_PROPERTY = "recordcount";
const string CoreWorkload::OPERATION_COUNT_PROPERTY = "operationcount";

const string CoreWorkload::ZIPFIAN_CONSTANT_PROPERTY = "zipfianconstant";
const string CoreWorkload::ZIPFIAN_CONSTANT_DEFAULT = "0.9";

void CoreWorkload::Init(const utils::Properties& p, bool run_phase) {
    if (run_phase) is_run_phase_ = true;
    table_name_ = p.GetProperty(TABLENAME_PROPERTY, TABLENAME_DEFAULT);

    field_count_ =
        std::stoul(p.GetProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_DEFAULT));
    record_count_ = std::stoul(p.GetProperty(RECORD_COUNT_PROPERTY));
    operation_count_ = std::stoul(p.GetProperty(OPERATION_COUNT_PROPERTY));

    field_len_generator_ = GetFieldLenGenerator(p);

    double read_proportion = std::stod(
        p.GetProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_DEFAULT));
    double update_proportion = std::stod(
        p.GetProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_DEFAULT));
    double overwrite_proportion = std::stod(p.GetProperty(
        OVERWRITE_PROPORTION_PROPERTY, OVERWRITE_PROPORTION_DEFAULT));
    double insert_proportion = std::stod(
        p.GetProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_DEFAULT));
    double scan_proportion = std::stod(
        p.GetProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_DEFAULT));
    double readmodifywrite_proportion = std::stod(p.GetProperty(
        READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_DEFAULT));

    std::string request_dist = p.GetProperty(REQUEST_DISTRIBUTION_PROPERTY,
                                             REQUEST_DISTRIBUTION_DEFAULT);
    int max_scan_len = std::stoi(
        p.GetProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_DEFAULT));
    std::string scan_len_dist = p.GetProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,
                                              SCAN_LENGTH_DISTRIBUTION_DEFAULT);
    int insert_start =
        std::stoi(p.GetProperty(INSERT_START_PROPERTY, INSERT_START_DEFAULT));

    read_all_fields_ = utils::StrToBool(
        p.GetProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_DEFAULT));
    write_all_fields_ = utils::StrToBool(
        p.GetProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_DEFAULT));

    if (p.GetProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_DEFAULT) == "hashed") {
        ordered_inserts_ = false;
    } else {
        std::cerr << "ordered values" << std::endl;
        std::cout << "ordered values" << std::endl;
        ordered_inserts_ = true;
    }

    key_generator_ = new CounterGenerator(insert_start);

    if (read_proportion > 0) {
        op_chooser_.AddValue(READ, read_proportion);
    }
    if (update_proportion > 0) {
        op_chooser_.AddValue(UPDATE, update_proportion);
    }
    if (overwrite_proportion > 0) {
        op_chooser_.AddValue(OVERWRITE, overwrite_proportion);
    }
    if (insert_proportion > 0) {
        op_chooser_.AddValue(INSERT, insert_proportion);
    }
    if (scan_proportion > 0) {
        op_chooser_.AddValue(SCAN, scan_proportion);
    }
    if (readmodifywrite_proportion > 0) {
        op_chooser_.AddValue(READMODIFYWRITE, readmodifywrite_proportion);
    }

    insert_key_sequence_.Set(record_count_);

    if (request_dist == "uniform") {
        key_chooser_ = new UniformGenerator(0, record_count_ - 1);

    } else if (request_dist == "zipfian") {
        // If the number of keys changes, we don't want to change popular keys.
        // So we construct the scrambled zipfian generator with a keyspace
        // that is larger than what exists at the beginning of the test.
        // If the generator picks a key that is not inserted yet, we just ignore it
        // and pick another key.
        int op_count = std::stoi(p.GetProperty(OPERATION_COUNT_PROPERTY));
        int new_keys = (int)(op_count * insert_proportion * 2);  // a fudge factor
        key_chooser_ = new ScrambledZipfianGenerator(0, 
                record_count_ + new_keys - 1, 
                std::stod(p.GetProperty(ZIPFIAN_CONSTANT_PROPERTY,
                        ZIPFIAN_CONSTANT_DEFAULT)));

    } else if (request_dist == "latest") {
        key_chooser_ = new SkewedLatestGenerator(insert_key_sequence_, 
                std::stod(p.GetProperty(ZIPFIAN_CONSTANT_PROPERTY,
                        ZIPFIAN_CONSTANT_DEFAULT)));

    } else {
        throw utils::Exception("Unknown request distribution: " + request_dist);
    }

    field_chooser_ = new UniformGenerator(0, field_count_ - 1);

    if (scan_len_dist == "uniform") {
        scan_len_chooser_ = new UniformGenerator(1, max_scan_len);
    } else if (scan_len_dist == "zipfian") {
        scan_len_chooser_ = new ZipfianGenerator(1, max_scan_len);
    } else if (scan_len_dist == "CONStant") {
        scan_len_chooser_ = new UniformGenerator(max_scan_len, max_scan_len);
    } else {
        throw utils::Exception("Distribution not allowed for scan length: " +
                               scan_len_dist);
    }
}

ycsbc::Generator<uint64_t>* CoreWorkload::GetFieldLenGenerator(
    const utils::Properties& p) {
    string field_len_dist = p.GetProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
                                          FIELD_LENGTH_DISTRIBUTION_DEFAULT);
    int field_len =
        std::stoi(p.GetProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_DEFAULT));
    if (field_len_dist == "constant") {
        return new ConstGenerator(field_len);
    } else if (field_len_dist == "uniform") {
        return new UniformGenerator(1, field_len);
    } else if (field_len_dist == "zipfian") {
        return new ZipfianGenerator(1, field_len);
    } else if (field_len_dist == "ratio") {
        double small_ratio =
            std::stod(p.GetProperty(SMALL_VALUE_PROPORTION_PROPERTY));
        double mid_ratio = std::stod(p.GetProperty(MID_VALUE_PROPORTION_PROPERTY));
        double large_ratio =
            std::stod(p.GetProperty(LARGE_VALUE_PROPORTION_PROPERTY));
        int small_size = std::stoi(p.GetProperty(SMALL_VALUE_SIZE_PROPERTY));
        int mid_size = std::stoi(p.GetProperty(MID_VALUE_SIZE_PROPERTY));
        int large_size = std::stoi(p.GetProperty(LARGE_VALUE_SIZE_PROPERTY));
        return new RatioGenerator(small_ratio, mid_ratio, large_ratio, small_size,
                                  mid_size, large_size);
    } else if (field_len_dist == "pareto") {
        double k = std::stod(p.GetProperty(PARETO_K));
        double theta = std::stod(p.GetProperty(PARETO_THETA));
        double sigma = std::stod(p.GetProperty(PARETO_SIGMA));
        uint64_t num = 0;
        if (is_run_phase_) {
            double update_proportion = std::stod(
                p.GetProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_DEFAULT));
            double insert_proportion = std::stod(
                p.GetProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_DEFAULT));
            double overwrite_proportion = std::stod(p.GetProperty(
                OVERWRITE_PROPORTION_PROPERTY, OVERWRITE_PROPORTION_DEFAULT));
            num = operation_count_ *
                  (update_proportion + insert_proportion + overwrite_proportion);
            num = num == 0 ? record_count_ : num;
        } else {
            num = record_count_;
        }
        return new ParetoGenerator(num, theta, k, sigma);
    } else if (field_len_dist == "paretokey") {
        // TODO
//        double k = std::stod(p.GetProperty(PARETO_K));
//        double theta = std::stod(p.GetProperty(PARETO_THETA));
//        double sigma = std::stod(p.GetProperty(PARETO_SIGMA));
        field_len_with_key_generator_ = new DistToKeyGenerator(field_count_);
        return nullptr;
    } else if (field_len_dist == "up2x") {
        up2x_generator_ = new UP2XGenerator();
        return nullptr;
    } else {
        throw utils::Exception("Unknown field length distribution: " +
                               field_len_dist);
    }
}

void CoreWorkload::BuildValues(std::vector<ycsbc::YCSBDB::KVPair>& values) {
    for (int i = 0; i < field_count_; ++i) {
        ycsbc::YCSBDB::KVPair pair;
        pair.first.append("field").append(std::to_string(i));
        pair.second.append(field_len_generator_->Next(), utils::RandomPrintChar());
        // pair.second.append(",");
        values.push_back(pair);
        // std::cout << "Build Values->field name [" << i << "] = " << pair.first <<
        // std::endl; std::cout << "Build Values->field value [" << i << "] = " <<
        // pair.second << std::endl;
    }
}

void CoreWorkload::BuildUpdate(std::vector<ycsbc::YCSBDB::KVPair>& update) {
    ycsbc::YCSBDB::KVPair pair;
    pair.first.append(NextFieldName());
    pair.second.append(pair.first.substr(5) + ",");
    pair.second.append(field_len_generator_->Next(), utils::RandomPrintChar());
    // std::cout << "Update->Next field name = " << pair.first << std::endl;
    // std::cout << "Update->Next field content p.second = " << pair.second <<
    // std::endl;
    update.push_back(pair);
}

void CoreWorkload::BuildValuesWithKey(const std::string& key,
        std::vector<ycsbc::YCSBDB::KVPair>& values) {
//    static uint64_t cnt = 0;
//    static uint64_t lens = 0;
    int len; 
    if (field_len_with_key_generator_ == nullptr && up2x_generator_ == nullptr)
    {
        // normal
        BuildValues(values);
        return;
    } 
    // pareto with key or up2x workload
    len = (up2x_generator_ == nullptr) ?
        field_len_with_key_generator_->Next(key) :
        up2x_generator_->ValueLength();
//    cnt++;
//    lens += len;

    for (int i = 0; i < field_count_; ++i) {
        ycsbc::YCSBDB::KVPair pair;
        pair.first.append("field").append(std::to_string(i));
        pair.second.append(len, utils::RandomPrintChar());
        values.push_back(pair);
    }

//    if (cnt % 1000000 == 0) {
//        fprintf(stderr, "line %d cnt %lu avglen %.2lf\n", __LINE__, cnt, (double)lens / cnt);
//    }
}

void CoreWorkload::BuildUpdateWithKey(const std::string& key, std::vector<ycsbc::YCSBDB::KVPair>& update) {
//    static uint64_t cnt = 0;
//    static uint64_t lens = 0;
    int len; 
    if (field_len_with_key_generator_ == nullptr && up2x_generator_ == nullptr)
    {
        BuildUpdate(update);
        return;
    }
    len = (up2x_generator_ == nullptr) ?
        field_len_with_key_generator_->Next(key) :
        up2x_generator_->ValueLength();
//    cnt++;
//    lens += len;
    ycsbc::YCSBDB::KVPair pair;
    pair.first.append(NextFieldName());
    pair.second.append(pair.first.substr(5) + ",");
    pair.second.append(len, utils::RandomPrintChar());
    update.push_back(pair);
//    if (cnt % 100000 == 0) {
//        fprintf(stderr, "line %d cnt %lu avglen %.2lf\n", __LINE__, cnt, (double)lens / cnt);
//    }
}
