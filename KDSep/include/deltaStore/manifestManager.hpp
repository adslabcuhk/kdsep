#pragma once

#include "common/dataStructure.hpp"

namespace KDSEP_NAMESPACE {

class ManifestManager {
public:
    ManifestManager();
    ~ManifestManager();
    ManifestManager(const string& working_dir);
    bool retrieve(bool& should_recover, unordered_map<uint64_t, string>&
            id2prefixes); 

    void InitialSnapshot(BucketHandler* bucket); 
    void FlushSnapshot();
    void UpdateGCMetadata(
	const vector<BucketHandler*>& old_buckets,
	const vector<BucketHandler*>& new_buckets); 
    void UpdateGCMetadata(const vector<uint64_t>& old_ids,
	const vector<string>& old_prefixes,
	const vector<uint64_t>& new_ids,
	const vector<string>& new_prefixes); 
    void UpdateGCMetadata(const uint64_t old_id, const string& old_prefix,
	    const uint64_t new_id, const string& new_prefix); 
    bool CreateManifestIfNotExist();

private:
    bool enable_pointer_ = false;
    uint64_t pointer_int_ = 0;
    vector<pair<string, uint64_t>> snapshots_;
    shared_mutex mtx_;
    ofstream manifest_fs_;
    ofstream pointer_fs_;
    string working_dir_;
};
}
