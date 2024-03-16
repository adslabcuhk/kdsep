#pragma once

//#include "common/dataStructure.hpp"
//#include "utils/debug.hpp"
#include <bits/stdc++.h>
#include <shared_mutex>
#include <map>
using namespace std;

namespace KDSEP_NAMESPACE {
#define SKIPLIST_P 0.5f
#define MAX_LEVEL 32

//class SkipList {
//
//
//    public Node find(int value) {
//        Node p = head;
//        for (int i = levelCount - 1; i >= 0; --i) {
//            while (p.forwards[i] != null && p.forwards[i].data < value) {
//                p = p.forwards[i];
//            }
//        }
//
//        if (p.forwards[0] != null && p.forwards[0].data == value) {
//            return p.forwards[0];
//        } else {
//            return null;
//        }
//    }
//
//    public void insert(int value) {
//        int level = randomLevel();
//        Node newNode = new Node();
//        newNode.data = value;
//        newNode.maxLevel = level;
//        Node update[] = new Node[level];
//        for (int i = 0; i < level; ++i) {
//            update[i] = head;
//        }
//
//        // record every level largest value which smaller than insert value in update[]
//        Node p = head;
//        for (int i = level - 1; i >= 0; --i) {
//            while (p.forwards[i] != null && p.forwards[i].data < value) {
//                p = p.forwards[i];
//            }
//            update[i] = p;// use update save node in search path
//        }
//
//        // in search path node next node become new node forwords(next)
//        for (int i = 0; i < level; ++i) {
//            newNode.forwards[i] = update[i].forwards[i];
//            update[i].forwards[i] = newNode;
//        }
//
//        // update node hight
//        if (levelCount < level) levelCount = level;
//    }
//
//    public void delete(int value) {
//        Node[] update = new Node[levelCount];
//        Node p = head;
//        for (int i = levelCount - 1; i >= 0; --i) {
//            while (p.forwards[i] != null && p.forwards[i].data < value) {
//                p = p.forwards[i];
//            }
//            update[i] = p;
//        }
//
//        if (p.forwards[0] != null && p.forwards[0].data == value) {
//            for (int i = levelCount - 1; i >= 0; --i) {
//                if (update[i].forwards[i] != null && update[i].forwards[i].data == value) {
//                    update[i].forwards[i] = update[i].forwards[i].forwards[i];
//                }
//            }
//        }
//
//        while (levelCount>1&&head.forwards[levelCount]==null){
//            levelCount--;
//        }
//
//    }
//
//    // 理论来讲，一级索引中元素个数应该占原始数据的 50%，二级索引中元素个数占 25%，三级索引12.5% ，一直到最顶层。
//    // 因为这里每一层的晋升概率是 50%。对于每一个新插入的节点，都需要调用 randomLevel 生成一个合理的层数。
//    // 该 randomLevel 方法会随机生成 1~MAX_LEVEL 之间的数，且 ：
//    //        50%的概率返回 1
//    //        25%的概率返回 2
//    //      12.5%的概率返回 3 ...
//    private int randomLevel() {
//        int level = 1;
//
//        while (Math.random() < SKIPLIST_P && level < MAX_LEVEL)
//            level += 1;
//        return level;
//    }
//
//    public void printAll() {
//        Node p = head;
//        while (p.forwards[0] != null) {
//            System.out.print(p.forwards[0] + " ");
//            p = p.forwards[0];
//        }
//        System.out.println();
//    }
//
//    public class Node {
//        private: 
//        string key = -1;
//        forwards[] = new Node[MAX_LEVEL];
//        int maxLevel = 0;
//    }
//    private int levelCount = 1;
//
//    private Node head = new Node();  // 带头链表
//};

struct BucketHandler {
    uint64_t file_id = 0;
};

class SkipListWithMap {
public:
    void init() {
        st.clear();
    }

    bool insert(const string& key, BucketHandler*& newData) {
//        std::scoped_lock<std::shared_mutex> w_lock(mtx_);
        st[key] = newData;
        return true;
    }

    bool updateNoLargerThan(const string& key, BucketHandler* newData) {
//        std::scoped_lock<std::shared_mutex> w_lock(mtx_);
        auto it = st.upper_bound(key);
        if (it != st.begin()) {
            --it;
//        delete it->second;
            it->second = newData;
            return true;
        } else {
            return false;
        }
    }

    bool deleteNoLargerThan(const string& key) {
//        std::scoped_lock<std::shared_mutex> w_lock(mtx_);
        auto it = st.upper_bound(key);
        if (it != st.begin()) {
            --it;
            st.erase(it);
            return true;
        } else {
            return false;
        }
    }

    // useless
    BucketHandler* find(const string& key) {
//        std::shared_lock<std::shared_mutex> r_lock(mtx_);
        auto it = st.find(key);
        if (it != st.end()) {
            return it->second;
        }
        return nullptr;
    }

    BucketHandler* findNoLargerThan(const string& key) {
        // find the largest key that is not larger than key
//        std::shared_lock<std::shared_mutex> r_lock(mtx_);
        auto it = st.upper_bound(key);
        if (it != st.begin()) {
            return (--it)->second;
        }
        return nullptr;
    }

    // for split
    BucketHandler* findNext(const string& key) {
//        std::shared_lock<std::shared_mutex> r_lock(mtx_);
        auto it = st.upper_bound(key);
        if (it != st.end()) {
            return it->second;
        }
        return nullptr;
    }

    // for merge
    bool deleteNext(const string& key) {
//        std::scoped_lock<std::shared_mutex> w_lock(mtx_);
        
        auto it = st.upper_bound(key);
        if (it != st.end()) {
            st.erase(it);
            return true;
        }
        return false;
    }

    void getAllNodes(vector<pair<string, BucketHandler*>>& validObjectList)
    {
        for (auto& it : st) {
            validObjectList.push_back(it);
        }
    }

    int size() {
//        std::shared_lock<std::shared_mutex> r_lock(mtx_);
        return st.size();
    }

private:
    std::map<string, BucketHandler*> st;
    std::shared_mutex mtx_;
};

class SkipListForBuckets {

public:
    SkipListForBuckets(uint64_t maxFileNumber)
    {
        init(maxFileNumber);
    }

    SkipListForBuckets()
    {
    }

    ~SkipListForBuckets() {}

    void init(uint64_t maxFileNumber)
    {
//        debug_error("init %lu\n", maxFileNumber);
        max_file_num_ = maxFileNumber;
        list_.init();
    }

    uint64_t getRemainFileNumber()
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        int current_file_num = list_.size();
        if (max_file_num_ + 10 < current_file_num) {
//            debug_error("[ERROR] too many files! %lu v.s. %lu\n", 
//                    max_file_num_, current_file_num); 
            exit(1);
        }
        return max_file_num_ - current_file_num;
    }

    bool insert(const string& key, BucketHandler*& newData)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);

        int current_file_num = list_.size();

        if (current_file_num >= max_file_num_) {
//            debug_error("[ERROR] Could note insert new node, since there are "
//                    "too many files, number = %lu, threshold = %lu\n",
//                    current_file_num, max_file_num_);
	    exit(1);
            printNodeMap();
            return 0;
        }
        bool status = list_.insert(key, newData); 
        if (status == true) {
            return true;
        } else {
//            debug_error("[ERROR] Insert new node fail for prefix = %lx\n",
//                    prefix_u64);
            printNodeMap();
            return false;
        }
    }

    bool get(const string& key, BucketHandler*& newData)
    {
        std::scoped_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        newData = list_.findNoLargerThan(key);
        return true;
    }

    bool find(const string& key)
    {
        std::scoped_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        BucketHandler* newData = list_.findNoLargerThan(key);
        return newData == nullptr ? false : true;
    }

    bool remove(const string& key)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        return list_.deleteNoLargerThan(key);
    }

    bool update(const string& key, BucketHandler* newDataObj)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        return list_.updateNoLargerThan(key, newDataObj);
    }

    bool getCurrentValidNodes(vector<pair<string, BucketHandler*>>& validObjectList)
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        list_.getAllNodes(validObjectList);
        return true;
    }

    void printNodeMap()
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        vector<pair<string, BucketHandler*>> validObjectList;
        list_.getAllNodes(validObjectList);
        for (auto& it : validObjectList) {
            fprintf(stderr, "key = %s, value = %ld\n", it.first.c_str(), it.second->file_id);
        }
        return;
    }

private:
//    vector<BucketHandler*> targetDeleteVec;
    uint64_t max_file_num_ = 0;
    uint64_t current_file_num_ = 0;
    SkipListWithMap list_;
    std::shared_mutex nodeOperationMtx_;
};

}
