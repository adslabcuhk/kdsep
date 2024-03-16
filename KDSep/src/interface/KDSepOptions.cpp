#include "interface/KDSepOptions.hpp"

namespace KDSEP_NAMESPACE {

bool KDSepOptions::dumpOptions(string dumpPath)
{
    ofstream dumpOptionsOutStream;
    dumpOptionsOutStream.open(dumpPath, ios::out);
    if (!dumpOptionsOutStream.is_open()) {
        debug_error("[ERROR] Can not open target file = %s for dump\n", dumpPath.c_str());
        return false;
    }
    dumpOptionsOutStream << "Common options:" << endl;
    dumpOptionsOutStream << "\tKDSep_thread_number_limit = " << KDSep_thread_number_limit << endl;
    dumpOptionsOutStream << "\tdeltaStore_max_bucket_number_ = " << deltaStore_max_bucket_number_ << endl;
    dumpOptionsOutStream << "\tKDSep_merge_operation_ptr = " << KDSep_merge_operation_ptr->kClassName() << endl;
    if (enable_deltaStore == true) {
        dumpOptionsOutStream << "DeltaStore options:" << endl;
        dumpOptionsOutStream << "\tenable_deltaStore = " << enable_deltaStore << endl;
        dumpOptionsOutStream << "\tenable_deltaStore_KDLevel_cache = " << enable_deltaStore_KDLevel_cache << endl;
        dumpOptionsOutStream << "\tenable_bucket_gc = " << enable_bucket_gc << endl;
        dumpOptionsOutStream << "\tdeltaStore_base_cache_mode = " << static_cast<typename std::underlying_type<contentCacheMode>::type>(deltaStore_base_cache_mode) << endl;
        dumpOptionsOutStream << "\tdeltaStore_base_store_mode = " << static_cast<typename std::underlying_type<contentStoreMode>::type>(deltaStore_base_store_mode) << endl;
        dumpOptionsOutStream << "\tdeltaStore_KDCache_item_number_ = " << deltaStore_KDCache_item_number_ << endl;
        dumpOptionsOutStream << "\tdeltaStore_KDLevel_cache_peritem_value_number = " << deltaStore_KDLevel_cache_peritem_value_number << endl;
        dumpOptionsOutStream << "\textract_to_deltaStore_size_lower_bound = " << extract_to_deltaStore_size_lower_bound << endl;
        dumpOptionsOutStream << "\textract_to_deltaStore_size_upper_bound = " << extract_to_deltaStore_size_upper_bound << endl;
        dumpOptionsOutStream << "\tdeltaStore_bucket_size_ = " << deltaStore_bucket_size_ << endl;
        dumpOptionsOutStream << "\tdeltaStore_total_storage_maximum_size = " << deltaStore_total_storage_maximum_size << endl;
        dumpOptionsOutStream << "\tdeltaStore_worker_thread_number_limit = " << deltaStore_op_worker_thread_number_limit_ << endl;
        dumpOptionsOutStream << "\tdeltaStore_gc_thread_number_limit = " << deltaStore_gc_worker_thread_number_limit_ << endl;
        dumpOptionsOutStream << "\tdeltaStore_gc_threshold = " << deltaStore_gc_threshold << endl;
    }
    if (enable_valueStore == true) {
        dumpOptionsOutStream << "ValueStore options:" << endl;
        dumpOptionsOutStream << "\tenable_valueStore = " << enable_valueStore << endl;
        dumpOptionsOutStream << "\tenable_valueStore_fileLvel_cache = " << enable_valueStore_fileLvel_cache << endl;
        dumpOptionsOutStream << "\tenable_valueStore_KDLevel_cache = " << enable_valueStore_KDLevel_cache << endl;
        dumpOptionsOutStream << "\tenable_valueStore_garbage_collection = " << enable_valueStore_garbage_collection << endl;
        dumpOptionsOutStream << "\tvalueStore_base_cache_mode = " << static_cast<typename std::underlying_type<contentCacheMode>::type>(valueStore_base_cache_mode) << endl;
        dumpOptionsOutStream << "\tvalueStore_base_store_mode = " << static_cast<typename std::underlying_type<contentStoreMode>::type>(valueStore_base_store_mode) << endl;
        dumpOptionsOutStream << "\textract_to_valueStore_size_lower_bound = " << extract_to_valueStore_size_lower_bound << endl;
        dumpOptionsOutStream << "\textract_to_valueStore_size_upper_bound = " << extract_to_valueStore_size_upper_bound << endl;
        dumpOptionsOutStream << "\tvalueStore_single_file_maximum_size = " << valueStore_single_file_maximum_size << endl;
        dumpOptionsOutStream << "\tvalueStore_total_storage_maximum_size = " << valueStore_total_storage_maximum_size << endl;
        dumpOptionsOutStream << "\tvalueStore_thread_number_limit = " << valueStore_thread_number_limit << endl;
    }

    dumpOptionsOutStream.flush();
    dumpOptionsOutStream.close();
    return true;
}

bool KDSepOptions::dumpDataStructureInfo(string dumpPath)
{
    ofstream dumpStructureSizeOutStream;
    dumpStructureSizeOutStream.open(dumpPath, ios::out);
    if (!dumpStructureSizeOutStream.is_open()) {
        debug_error("[ERROR] Can not open target file = %s for dump\n", dumpPath.c_str());
        return false;
    }
    // write content
    dumpStructureSizeOutStream << "Size of KvHeader = " << sizeof(KvHeader) << endl;
    dumpStructureSizeOutStream << "Size of externalIndexInfo = " << sizeof(externalIndexInfo) << endl;
    dumpStructureSizeOutStream << "Size of BucketHandler = " << sizeof(BucketHandler) << endl;
    dumpStructureSizeOutStream << "Size of hashStoreWriteOperationHandler = " << sizeof(hashStoreWriteOperationHandler) << endl;
    dumpStructureSizeOutStream << "Size of hashStoreReadOperationHandler = " << sizeof(hashStoreMultiGetOperationHandler) << endl;
    dumpStructureSizeOutStream << "Size of deltaStoreOpHandler = " << sizeof(deltaStoreOpHandler) << endl;
    dumpStructureSizeOutStream << "Size of KDRecordHeader = " << sizeof(KDRecordHeader) << endl;

    dumpStructureSizeOutStream.flush();
    dumpStructureSizeOutStream.close();
    return true;
}

} // namespace KDSEP_NAMESPACE
