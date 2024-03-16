#include "db/db_factory.h"

#include <string>

#include "KDSep/KDSep_db.h"
// #include "RocksDB/rocksdb_db.h"

using namespace std;
using ycsbc::DBFactory;
using ycsbc::YCSBDB;

YCSBDB *DBFactory::CreateDB(utils::Properties &props) {
    if (props["dbname"] == "rocksdb") {
        return new KDSepDB(props["dbfilename"].c_str(), props["configpath"]);
        // cerr << props["dbfilename"].c_str() << props["configpath"] << endl;
    } else {
        return NULL;
    }
}
