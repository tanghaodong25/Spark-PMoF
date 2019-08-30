#ifndef PMPOOL_FREELIST_H
#define PMPOOL_FREELIST_H

#include <memory>
#include <unordered_map>

class DataEntry;
class DataEntries;

class FreeList {
  public:
    FreeList() = default;
    int put(uint64_t key, DataEntry* entry);
    DataEntries* get(uint64_t key);
    int remove(uint64_t key);
  private:
    std::unordered_map<uint64_t, DataEntries*> dataEntryHashBable;
};

#endif //PMPOOL_FREELIST_H
