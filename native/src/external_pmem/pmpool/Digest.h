#ifndef PMPOOL_DIGEST_H
#define PMPOOL_DIGEST_H

#include <cstdint>
#include <HPNL/ChunkMgr.h>

struct KeyEntry {
  uint16_t shuffle_id;
  uint16_t map_id;
  uint16_t reduce_id;
};

struct DataEntry {
  uint64_t address;
  uint64_t size;
  uint64_t rkey;
  int buffer_id;
  Chunk* ck;
  DataEntry *next;
};

struct DataEntries {
  uint16_t size;
  DataEntry *first;
  DataEntry *last;
};

class Digest {
  public:
    Digest() = default;
    static void computeKeyHash(KeyEntry* keyEntry, uint64_t* hash);
};

#endif
