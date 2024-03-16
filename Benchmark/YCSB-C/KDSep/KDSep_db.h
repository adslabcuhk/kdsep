#ifndef YCSB_C_KDSepDB_DB_H
#define YCSB_C_KDSepDB_DB_H

#include <assert.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

#include "core/db.h"
#include "core/properties.h"
#include "interface/KDSepInterface.hpp"
#include "interface/KDSepOptions.hpp"
#include "interface/mergeOperation.hpp"

namespace ycsbc {
class KDSepDB : public YCSBDB {
   public:
    KDSepDB(const char *dbfilename, const std::string &config_file_path);

    int Read(const std::string &table, const std::string &key,
             const std::vector<std::string> *fields,
             std::vector<KVPair> &result);

    int Scan(const std::string &table, const std::string &key,
             int len, const std::vector<std::string> *fields,
             std::vector<std::vector<KVPair>> &result);

    int Insert(const std::string &table, const std::string &key,
               std::vector<KVPair> &values);

    int Update(const std::string &table, const std::string &key,
               std::vector<KVPair> &values);

    int OverWrite(const std::string &table, const std::string &key,
                  std::vector<KVPair> &values);

    int Delete(const std::string &table, const std::string &key);

    void printStats();

    ~KDSepDB();

   private:
    std::ofstream outputStream_;
    KDSEP_NAMESPACE::KDSep db_;
    KDSEP_NAMESPACE::KDSepOptions options_;
    struct timeval tv_;
};
}  // namespace ycsbc
#endif  // YCSB_C_KDSepDB_DB_H
