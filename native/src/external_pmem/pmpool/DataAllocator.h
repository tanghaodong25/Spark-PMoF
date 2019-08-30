#ifndef PMPOOL_DATAALLOCATOR_H
#define PMPOOL_DATAALLOCATOR_H

#include <memory>
#include <atomic>
#include <cstdint>
#include <unordered_map>
#include <vector>
#include <mutex>

#include <memkind.h>

using namespace std;

class DataEntry;
class DataEntries;
class ChunkPool;
class Server;
class PmemPool;
class Chunk;

/// fixed-size persistent memory allocator
class DataAllocator {
  public:
    DataAllocator(vector<string>& pool_paths, uint64_t buffer_size, Server* server);
    ~DataAllocator();
    int init();
    DataEntry* allocate();
    int deallocate(DataEntry* dataEntry);
    Chunk* get(int buffer_id);
  private:
    Server* server_;
    unordered_map<int, DataEntry*> allocated_memory;
    vector<PmemPool*> pmemPools;
    vector<string> pool_paths_;
    int pool_size_;
    atomic<uint64_t> buffer_id_;
    uint64_t buffer_size_;
    mutex mtx;
    bool enable_system_malloc = false;
};

#endif
