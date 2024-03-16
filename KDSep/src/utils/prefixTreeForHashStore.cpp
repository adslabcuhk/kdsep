#include "utils/prefixTreeForHashStore.hpp"

namespace KDSEP_NAMESPACE {

PrefixTreeForHashStore::~PrefixTreeForHashStore() {
    size_t rss_before = getRss();
    size_t rss_after;
    //        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
    for (int i = 0; i < (1 << partition_bit_); i++) {
        stack<prefixTreeNode*> stk;
        prefixTreeNode *p = roots_[i], *pre = nullptr;

        // almost a template for post order traversal ...
        while (p != nullptr || !stk.empty()) {
            while (p != nullptr) {
                stk.push(p);
                p = p->left_child; // go down one level
            }

            if (!stk.empty()) {
                p = stk.top(); // its left children are deleted
                stk.pop();
                if (p->right_child == nullptr || pre == p->right_child) {
                    delete p;
                    pre = p;
                    p = nullptr;
                } else {
                    stk.push(p);
                    p = p->right_child;
                }
            }
        }
    }

    rss_after = getRss();
//    debug_error("before targetDeleteVec rss from %lu to %lu "
//            "(diff: %.4lf)\n", 
//           rss_before, rss_after, 
//           (rss_before - rss_after) / 1024.0 / 1024.0); 
//    printf("before targetDeleteVec rss from %lu to %lu "
//            "(diff: %.4lf)\n", 
//           rss_before, rss_after, 
//           (rss_before - rss_after) / 1024.0 / 1024.0); 
    rss_before = rss_after;

    for (long unsigned int i = 0; i < targetDeleteVec.size(); i++) {
        if (targetDeleteVec[i] != nullptr) {
            if (targetDeleteVec[i]->io_ptr != nullptr) {
                delete targetDeleteVec[i]->io_ptr;
            }
            delete targetDeleteVec[i];
        }
    }

    delete[] roots_;
    delete[] rootMtx_;
    rss_after = getRss();
//    debug_error("rss from %lu to %lu (diff: %.4lf)\n", 
//           rss_before, rss_after, 
//           (rss_before - rss_after) / 1024.0 / 1024.0); 
//    fprintf(stdout, "rss from %lu to %lu (diff: %.4lf)\n", 
//           rss_before, rss_after, 
//           (rss_before - rss_after) / 1024.0 / 1024.0); 
}

} // KDSEP_NAMESPACE
