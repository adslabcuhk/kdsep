#include "utils/utils.hpp"

using namespace std;

namespace KDSEP_NAMESPACE {

size_t getRss() {
    bool dump_memory_usage = true;
    double vm_usage = 0.0;
    double resident_set = 0.0;
    if (dump_memory_usage) {
        malloc_trim(0);

        // 'file' stat seems to give the most reliable results
        //
        ifstream stat_stream("/proc/self/stat", ios_base::in);

        // dummy vars for leading entries in stat that we don't care about
        //
        string pid, comm, state, ppid, pgrp, session, tty_nr;
        string tpgid, flags, minflt, cminflt, majflt, cmajflt;
        string utime, stime, cutime, cstime, priority, nice;
        string O, itrealvalue, starttime;

        // the two fields we want
        //
        unsigned long vsize;
        long rss;

        // don't care about the rest
        stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >>
            tty_nr >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
            >> utime >> stime >> cutime >> cstime >> priority >> nice >> O >>
            itrealvalue >> starttime >> vsize >> rss;  

        stat_stream.close();

        long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024;  // in case x86-64 is configured to use 2MB pages
        vm_usage = vsize / 1024.0;
        resident_set = rss * page_size_kb;
//        std::cerr << "vm_usage " << vm_usage << std::endl;
//        std::cerr << "resident " << resident_set << std::endl;
    }
    return resident_set;
}

size_t getRssNoTrim() {
    bool dump_memory_usage = true;
    double vm_usage = 0.0;
    double resident_set = 0.0;
    if (dump_memory_usage) {
        // 'file' stat seems to give the most reliable results
        //
        ifstream stat_stream("/proc/self/stat", ios_base::in);

        // dummy vars for leading entries in stat that we don't care about
        //
        string pid, comm, state, ppid, pgrp, session, tty_nr;
        string tpgid, flags, minflt, cminflt, majflt, cmajflt;
        string utime, stime, cutime, cstime, priority, nice;
        string O, itrealvalue, starttime;

        // the two fields we want
        //
        unsigned long vsize;
        long rss;

        // don't care about the rest
        stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >>
            tty_nr >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
            >> utime >> stime >> cutime >> cstime >> priority >> nice >> O >>
            itrealvalue >> starttime >> vsize >> rss;  

        stat_stream.close();

        long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024;  // in case x86-64 is configured to use 2MB pages
        vm_usage = vsize / 1024.0;
        resident_set = rss * page_size_kb;
//        std::cerr << "vm_usage " << vm_usage << std::endl;
//        std::cerr << "resident " << resident_set << std::endl;
    }
    return resident_set;
}

}
