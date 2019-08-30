#ifndef PMEMPOOL_PMEMPOOL_H
#define PMEMPOOL_PMEMPOOL_H

#include "HPNL/ChunkMgr.h"
#include "HPNL/Server.h"

class PmemAllocator;

class PmemPoolAllocator {
  public:
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;

    static char* malloc(size_type bytes);
    static void free(char* block);

    static int buffer_size;
    static fid_domain *domain;
    static int id;
    static std::map<Chunk*, int> chunk_to_id_map;
    static std::map<int, std::pair<Chunk*, fid_mr*>> id_to_chunk_map;
    static std::mutex mtx;
    static PmemAllocator* pmemAllocator;
};

class PmemPool : public boost::pool<PmemPoolAllocator> {
  public:
    PmemPool(FabricService*, int request_buffer_size,
              int next_request_buffer_number, PmemAllocator* pmemAllocator_);
    ~PmemPool();
    void *malloc();
    void free(void * const ck);

    Chunk* get();
    void reclaim(Chunk* ck);
    Chunk* get(int id);
    int free_size();
  private:
    void *system_malloc();
  private:
    uint64_t buffer_size;
    uint64_t used_buffers;
};


#endif
