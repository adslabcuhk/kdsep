#include "vlog/ds/lru.hh"

namespace KDSEP_NAMESPACE {

LruList::LruList()
{
    this->init(DEFAULT_LRU_SIZE);
}

LruList::LruList(size_t listSize)
{
    this->init(listSize);
}

LruList::~LruList()
{
    if (_slots) {
        for (size_t i = 0; i < _listSize; i++) {
            if (_slots[i].key)
                free(_slots[i].key);
            if (_slots[i].value)
                free(_slots[i].value);
        }
        delete[] _slots;
    }
}

void LruList::insert(unsigned char* key, unsigned char* value, len_t valueSize)
{
    struct LruListRecord* target = 0;
    // key_len_t keySize;

    if (key == 0) {
        assert(key != 0);
    }

    _lock.lock();

    auto it = _existingRecords.find(key);

    if (it != _existingRecords.end()) {
        // reuse existing record
        target = it->second;
        list_move(&target->listPtr, &_lruList);
    } else if (_freeRecords.next != &_freeRecords) {
        // allocate a new record
        target = segment_of(_freeRecords.next, LruListRecord, listPtr);
        list_move(&target->listPtr, &_lruList);
    } else {
        // replace the oldest record
        target = segment_of(_lruList.prev, LruListRecord, listPtr);
        list_move(&target->listPtr, &_lruList);
        _existingRecords.erase(target->key);
    }

    // copy value
    updateValue(target, value, valueSize);

    // mark the metadata as exists in LRU list
    if (it == _existingRecords.end()) {
        // copy the key if it is new
        updateKey(target, key);

        std::pair<unsigned char*, LruListRecord*> record(target->key, target);
        _existingRecords.insert(record);
    }

    _lock.unlock();
}

std::string LruList::get(unsigned char* key)
{
    std::string ret = "";

    _lock.lock_shared();

    auto it = _existingRecords.find(key);

    if (it != _existingRecords.end()) {
        ret = getValue(it->second);
        list_move(&it->second->listPtr, &_lruList);
    }

    _lock.unlock_shared();

    return ret;
}

bool LruList::update(unsigned char* key, unsigned char* value, len_t valueSize)
{
    bool found = false;

    if (key == 0) {
        assert(key != 0);
    }

    _lock.lock();

    auto it = _existingRecords.find(key);
    if (it != _existingRecords.end()) {
        updateValue(it->second, value, valueSize);
        found = true;
    }

    _lock.unlock();

    return found;
}

std::vector<std::string> LruList::getItems()
{
    std::vector<std::string> list;

    _lock.lock_shared();

    struct list_head* rec;
    unsigned char* key;
    key_len_t keySize;
    printf("Items %d\n", (int)_existingRecords.size());
    list_for_each(rec, &_lruList)
    {
        key = segment_of(rec, LruListRecord, listPtr)->key;
        memcpy(&keySize, key, sizeof(key_len_t));

        list.push_back(std::string((char*)key + sizeof(key_len_t), keySize));
    }

    _lock.unlock_shared();

    return list;
}

std::vector<std::string> LruList::getTopNItems(size_t n)
{
    std::vector<std::string> list;

    _lock.lock();

    struct list_head* rec;
    unsigned char* key;
    key_len_t keySize;

    list_for_each(rec, &_lruList)
    {
        key = segment_of(rec, LruListRecord, listPtr)->key;
        memcpy(&keySize, key, sizeof(key_len_t));

        list.push_back(std::string((char*)key + sizeof(key_len_t), keySize));
        if (list.size() > n)
            break;
    }

    _lock.unlock();

    return list;
}

bool LruList::removeItem(unsigned char* key)
{
    bool exist = false;
    _lock.lock();

    if (key == 0) {
        assert(key != 0);
        return false;
    }

    auto it = _existingRecords.find(key);
    exist = it != _existingRecords.end();
    if (exist) {
        list_move(&it->second->listPtr, &_freeRecords);
        _existingRecords.erase(it);
    }
    _lock.unlock();

    return exist;
}

size_t LruList::getItemCount()
{
    size_t count = 0;

    _lock.lock_shared();

    count = _existingRecords.size();

    _lock.unlock_shared();

    return count;
}

size_t LruList::getFreeItemCount()
{
    size_t count = 0;

    _lock.lock_shared();

    count = _listSize - _existingRecords.size();

    _lock.unlock_shared();

    return count;
}

size_t LruList::getAndReset(std::vector<std::string>& dest, size_t n)
{

    _lock.lock();

    struct list_head *rec, *savePtr;
    list_for_each_safe(rec, savePtr, &_lruList)
    {
        unsigned char* key = segment_of(rec, LruListRecord, listPtr)->key;
        key_len_t keySize;
        memcpy(&keySize, key, sizeof(key_len_t));

        std::string keyStr = std::string((char*)key + sizeof(key_len_t), keySize);
        if (n == 0 || dest.size() < n) {
            dest.push_back(keyStr);
        }
        _existingRecords.erase(key);
        list_move(rec, &_freeRecords);
    }

    _lock.unlock();

    return dest.size();
}

void LruList::reset()
{
    _lock.lock();

    struct list_head *rec, *savePtr;

    list_for_each_safe(rec, savePtr, &_lruList)
    {
        _existingRecords.erase(segment_of(rec, LruListRecord, listPtr)->key);
        list_move(rec, &_freeRecords);
    }

    _lock.unlock();
}

void LruList::init(size_t listSize)
{
    _listSize = listSize;

    // init record
    INIT_LIST_HEAD(&_freeRecords);
    INIT_LIST_HEAD(&_lruList);
    _slots = new struct LruListRecord[_listSize];
    for (size_t i = 0; i < _listSize; i++) {
        _slots[i].key = _slots[i].value = nullptr;
        _slots[i].keySize = _slots[i].valueSize = 0;
        INIT_LIST_HEAD(&_slots[i].listPtr);
        list_add(&_slots[i].listPtr, &_freeRecords);
    }
}

bool LruList::updateKey(LruListRecord* rec, unsigned char* key)
{
    key_len_t keySize;
    memcpy(&keySize, key, sizeof(key_len_t));

    if (keySize == 0) {
        assert(0);
        exit(-1);
        return false;
    }

    // The buffer may shrink
    if (rec->keySize < keySize) {
        if (rec->key) {
            free(rec->key);
        }
        rec->key = (unsigned char*)malloc(sizeof(keySize) + keySize + 1);
    }
    memcpy(rec->key, key, keySize + sizeof(key_len_t));
    rec->keySize = keySize;

    return true;
}

bool LruList::updateValue(LruListRecord* rec, unsigned char* value, len_t valueSize)
{
    if (valueSize == 0) {
        assert(0);
        exit(-1);
        return false;
    }
    // The buffer may shrink
    if (rec->valueSize < valueSize) {
        if (rec->value) {
            free(rec->value);
        }
        rec->value = (unsigned char*)malloc(valueSize);
    }
    memcpy(rec->value, value, valueSize);
    rec->valueSize = valueSize;
    return true;
}

std::string LruList::getValue(LruListRecord* rec)
{
    return std::string((char*)rec->value, rec->valueSize);
}

void LruList::print(FILE* output, bool countOnly)
{
    struct list_head* rec;
    size_t i = 0;
    unsigned char* key;

    if (countOnly)
        fprintf(output, "    ");

    fprintf(output, "Free: %lu; Used: %lu\n", this->getFreeItemCount(), this->getItemCount());

    if (countOnly)
        return;

    _lock.lock();

    key_len_t keySize;

    list_for_each(rec, &_lruList)
    {
        key = segment_of(rec, LruListRecord, listPtr)->key;
        memcpy(&keySize, key, sizeof(key_len_t));

        fprintf(output, "Record [%lu]: key = %.*s\n",
            i, keySize, (char*)key + sizeof(key_len_t));
        i++;
    }

    _lock.unlock();
}

}
