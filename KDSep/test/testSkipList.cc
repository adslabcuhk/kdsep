#include "skipListForHashStore.hpp"
#include <iostream>
#include <cstdio>

using namespace std;
using namespace KDSEP_NAMESPACE;

int main() {
    freopen("input.txt", "r", stdin);
    SkipListForBuckets sb(16);
    uint64_t id;
    string str;
    string op;
    BucketHandler* bh = new BucketHandler;
    bh->file_id = 0;
    sb.insert("", bh);

    while (cin >> op >> str >> id) {
        fprintf(stderr, "-- Command: %s %s %ld --\n", op.c_str(), str.c_str(), id);
        if (op == "insert") {
            BucketHandler* bh = new BucketHandler;
            bh->file_id = id;
            sb.insert(str, bh);
        } else if (op == "print") {
            sb.printNodeMap();
        } else if (op == "get") {
            BucketHandler* bh;
            bool p = sb.get(str, bh);
            if (p) {
                cout << "found: " << bh->file_id << endl;
            } else {
                cout << "not found" << endl;
            }
        } else if (op == "delete") {
            bool status = sb.remove(str);
        } else if (op == "update") {
            BucketHandler* bh = new BucketHandler;
            bh->file_id = id;
            bool status = sb.update(str, bh);
            if (status == false) {
                cout << "cannot update " << str << " to " << id << endl;
            }
        }
    }

    return 0;
}
