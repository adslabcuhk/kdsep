#ifndef YCSB_C_DB_FACTORY_H
#define YCSB_C_DB_FACTORY_H

#include "core/db.h"
#include "core/properties.h"

namespace ycsbc {

class DBFactory {
   public:
    static YCSBDB* CreateDB(utils::Properties& props);
};

}  // namespace ycsbc

#endif  // YCSB_C_DB_FACTORY_H
