#include "FreeList.h"
#include "Digest.h"

int FreeList::put(uint64_t key, DataEntry *entry) {
  if (dataEntryHashBable.count(key)) {
    DataEntries *dataEntries = dataEntryHashBable[key];
    DataEntry *dataEntry = dataEntries->last;
    dataEntry->next = entry;
    dataEntries->last = entry;
    dataEntries->size++;
  } else {
    DataEntries *dataEntries = new DataEntries();
    dataEntries->first = entry;
    dataEntries->last = entry;
    dataEntries->size = 1;
    dataEntryHashBable[key] = dataEntries;
  }
  return 0;
}

DataEntries* FreeList::get(uint64_t key) {
  DataEntries* entries;
  if (dataEntryHashBable.count(key)) {
    entries = dataEntryHashBable[key];
  } else {
    entries = nullptr;
  }
  return entries;
}

int FreeList::remove(uint64_t key) {
  if (dataEntryHashBable.count(key)) {
    dataEntryHashBable.erase(key);
  }
  return 0;
}
