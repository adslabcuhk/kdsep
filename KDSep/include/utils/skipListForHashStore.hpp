#pragma once

#include "common/dataStructure.hpp"
//#include "utils/debug.hpp"
#include <bits/stdc++.h>
#include <shared_mutex>
#include <map>

using namespace std;

namespace KDSEP_NAMESPACE {
#define SKIPLIST_P 0.5f
#define MAX_LEVEL 32


//struct BucketHandler {
//    uint64_t file_id = 0;
//};

class SkipListWithMap {
public:
    virtual void init() {
        st.clear();
    }

    virtual bool insert(const string& key, BucketHandler* newData) {
//        std::scoped_lock<std::shared_mutex> w_lock(mtx_);

        st[key] = newData;
        return true;
    }

    virtual bool updateNoLargerThan(const string& key, BucketHandler* newData) {
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

    virtual bool deleteNoLargerThan(const string& key) {
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

    virtual BucketHandler* findNoLargerThan(const string& key) {
        // find the largest key that is not larger than key
//        std::shared_lock<std::shared_mutex> r_lock(mtx_);
        auto it = st.upper_bound(key);
        if (it != st.begin()) {
            return (--it)->second;
        }
        return nullptr;
    }

    // for merge 
    virtual BucketHandler* findNext(const string& key) {
//        std::shared_lock<std::shared_mutex> r_lock(mtx_);
        auto it = st.upper_bound(key);
        if (it != st.end()) {
            return it->second;
        }
        return nullptr;
    }

    virtual void getAllNodes(vector<pair<string, BucketHandler*>>& validObjectList)
    {
        for (auto& it : st) {
            validObjectList.push_back(it);
        }
    }

    virtual void getAllValues(vector<BucketHandler*>& validList)
    {
        for (auto& it : st) {
            validList.push_back(it.second);
        }
    }

    virtual int size() {
//        std::shared_lock<std::shared_mutex> r_lock(mtx_);
        return st.size();
    }

private:
    std::map<string, BucketHandler*> st;
    std::shared_mutex mtx_;
};

class SkipList : public SkipListWithMap {
public: 
    ~SkipList() {
        Node* p = head;
        while (p->forwards[0] != nullptr) {
            Node* tmp = p;
            p = p->forwards[0];
            delete[] tmp->forwards;
            delete tmp;
        }
        delete[] p->forwards;
        delete p;
    }

    virtual void init() {
        head = new Node(MAX_LEVEL);
        srand(time(nullptr));
    }

    virtual bool insert(const string& key, BucketHandler* newData) {
        int level = randomLevel();
        for (int i = 0; i < level; ++i) {
            update[i] = head;
        }
        cached_ = nullptr;

        // record every level largest value which smaller than insert value in update[]
        Node* p = head;
        for (int i = level - 1; i >= 0; --i) {
            while (p->forwards[i] != nullptr && p->forwards[i]->key < key) {
                p = p->forwards[i];
            }
            update[i] = p;// use update save node in search path
        }

        // insert the new node in this level 
        // update[i] -> newNode -> update[i]->forwards[i] (original)
        if (p->forwards[0] != nullptr && p->forwards[0]->key == key) {
            // check if the key already exists
//            newData = p->forwards[0]->data;
            fprintf(stderr, "Error: key %s already exists!\n", key.c_str());
            exit(1);
        } else {
            Node* newNode = new Node(level);
            newNode->key = key;
            newNode->data = newData;
            newNode->maxLevel = level;
            for (int i = 0; i < level; ++i) {
                newNode->forwards[i] = update[i]->forwards[i];
                update[i]->forwards[i] = newNode;
            }

            // update node hight
            if (num_levels_ < level) num_levels_ = level;
            size_++;
        }

        return true;
    }

    virtual bool updateNoLargerThan(const string& key, BucketHandler* newData) {
        Node* p = head;
        cached_ = nullptr;
        for (int i = num_levels_ - 1; i >= 0; --i) {
            while (p->forwards[i] != nullptr && key >= p->forwards[i]->key) {
                p = p->forwards[i];
            }
        }

        if (p == head) {
            return false;
        } else {
            p->data = newData;
            return true;
        }
    }

    virtual bool deleteNoLargerThan(const string& key) {
        Node* p = head;
        Node* target = nullptr;
        cached_ = nullptr;

        // search for the first time, find the target
        for (int i = num_levels_ - 1; i >= 0; --i) {
            while (p->forwards[i] != nullptr && key >= p->forwards[i]->key) {
                p = p->forwards[i];
            }
        }

        target = p;
        if (target == head) {
            return false;
        }

        p = head;
        // update: the nodes that are before the target node
        // search for the second time, find the update nodes
        for (int i = num_levels_ - 1; i >= 0; --i) {
            while (p->forwards[i] != nullptr && p->forwards[i]->key < target->key) {
                p = p->forwards[i];
            }
            update[i] = p;
        }

        // the next one is the target node
        if (p->forwards[0] != nullptr) {
            target = p->forwards[0];
            for (int i = num_levels_ - 1; i >= 0; --i) {
                if (update[i]->forwards[i] != nullptr) {
                    update[i]->forwards[i] =
                        update[i]->forwards[i]->forwards[i];
                }
            }
        } else {
            fprintf(stderr, "Error: target key %s not exist!\n",
                    target->key.c_str());
            exit(1);
        }

        while (num_levels_ > 1 && head->forwards[num_levels_ - 1] == nullptr){
            num_levels_--;
        }

        delete[] target->forwards;
        delete target; 
        size_--;
        return true;
    }

    virtual BucketHandler* findNoLargerThan(const string& key) {
        Node* p = head;
        if (cached_ != nullptr) {
            // cache hit
//            fprintf(stdout, "cached key %s\n", cached_->key.c_str());
//            fprintf(stdout, "cached_->forwards[0] %p\n", cached_->forwards[0]);
//            if (cached_->forwards[0] != nullptr) {
//                fprintf(stdout, "cached_->forwards[0]->key %s\n",
//                        cached_->forwards[0]->key.c_str());
//            }
            if (cached_->key <= key && (cached_->forwards[0] == nullptr ||
                    cached_->forwards[0]->key > key)) {
                return cached_->data;
            } 
        }

        for (int i = num_levels_ - 1; i >= 0; --i) {
            while (p->forwards[i] != nullptr && p->forwards[i]->key <= key) {
                p = p->forwards[i];
            }
        }

        // now the key of p is the largest that are no larger than the key 
        if (p == head) {
            return nullptr;
        } else {
//            cached_ = p;
            return p->data;
        }
    }

    virtual BucketHandler* findNext(const string& key) {
        Node* p = head;
        
        // check whether it is cached yet. If so, directly return the next node
        if (cached_ != nullptr) {
            // cache hit
            if (cached_->key <= key && (cached_->forwards[0] == nullptr ||
                    cached_->forwards[0]->key > key)) {
                cached_ = cached_->forwards[0];
                return cached_->data;
            }
        }

        for (int i = num_levels_ - 1; i >= 0; --i) {
            while (p->forwards[i] != nullptr && p->forwards[i]->key <= key) {
                p = p->forwards[i];
            }
        }

        Node* iter = head;
        while (iter->forwards[0] != nullptr) {
            iter = iter->forwards[0];
        }

        if (p->forwards[0] != nullptr) {
//            cached_ = p->forwards[0];
            return p->forwards[0]->data;
        } else {
            return nullptr;
        }
    }

    int randomLevel() {
        int level = 1;
        while (((double)rand() / RAND_MAX) < SKIPLIST_P && level < MAX_LEVEL) {
            level += 1;
        }
        return level;
    }

    virtual void getAllNodes(vector<pair<string, BucketHandler*>>& list) {
        Node* p = head;
        while (p->forwards[0]) {
            list.push_back(make_pair(p->forwards[0]->key,
                        p->forwards[0]->data));
            p = p->forwards[0];
        }
    }

    virtual void getAllValues(vector<BucketHandler*>& valid_list) {
        Node* p = head;
        while (p->forwards[0]) {
            valid_list.push_back(p->forwards[0]->data);
            p = p->forwards[0];
        }
    }

    virtual int size() {
        return size_;
    }

private:
    struct Node {
        string key = "";
        BucketHandler* data = nullptr;
        Node** forwards = nullptr;
        int maxLevel = 0;

        Node(int level) {
            maxLevel = level;
            forwards = new Node*[level];
            memset(forwards, 0, sizeof(Node*) * level);
        }
    };

    Node* head = nullptr;
    Node* update[MAX_LEVEL]; // temp array for iterating
    Node* cached_ = nullptr;
    int num_levels_ = 1;
    int size_ = 0;
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

    uint64_t size()
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        return list_.size();
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

    bool insert(const string& key, BucketHandler* newData)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);

        int current_file_num = list_.size();

        if (current_file_num >= max_file_num_ + 10) {
            fprintf(stderr, 
                    "[ERROR] Could note insert new node, since there are "
                    "too many files, number = %u, threshold = %lu\n",
                    current_file_num, max_file_num_);
	    exit(1);
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

    bool batchInsertAndUpdate(
            const vector<pair<string, BucketHandler*>>& insertList,
          const string& updateKey, BucketHandler* updateData) {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);

        int current_file_num = list_.size();
        if (current_file_num + insertList.size() >= max_file_num_ + 10) {
            fprintf(stderr, 
                    "[ERROR] Could note insert new node, since there are "
                    "too many files, number = %u, threshold = %lu\n",
                    current_file_num, max_file_num_);
            exit(1);
        }

        for (auto& it : insertList) {
            bool status = list_.insert(it.first, it.second); 
            if (status == false) {
                fprintf(stderr, 
                        "[ERROR] Insert new node fail for prefix = %s\n",
                        it.first.c_str());
                printNodeMap();
                return false;
            }
        }

        bool status = list_.updateNoLargerThan(updateKey, updateData);
        if (status == false) {
            fprintf(stderr, 
                    "[ERROR] Update node fail for prefix = %s\n",
                    updateKey.c_str());
            printNodeMap();
            return false;
        }

        return true;
    }

    bool get(const string& key, BucketHandler*& newData)
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        newData = list_.findNoLargerThan(key);
        return (newData != nullptr);
    }

    bool find(const string& key)
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        BucketHandler* newData = list_.findNoLargerThan(key);
        return (newData != nullptr);
    }

    bool getNext(const string& key, BucketHandler*& newData) {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        newData = list_.findNext(key);
        return (newData != nullptr);
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

    bool getCurrentValidNodesNoKey(vector<BucketHandler*>& validList) {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        list_.getAllValues(validList);
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
//    SkipListWithMap list_;
    SkipList list_;
    std::shared_mutex nodeOperationMtx_;
};

}
