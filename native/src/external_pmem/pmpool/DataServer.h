#ifndef PMPOOL_DATASERVER_H
#define PMPOOL_DATASERVER_H

#include <memory>

#include <HPNL/Server.h>
#include <HPNL/ChunkMgr.h>

class Config;
class Protocol;
class Digest;
class FreeList;
class DataAllocator;

class DataServer {
  public:
    DataServer(Config* config);
    int init();
    int start();
    void wait();
    uint64_t register_rma_buffer(char* rma_buffer, uint64_t size, int buffer_id);
    void unregister_rma_buffer(int buffer_id);
    Chunk* get_rma_buffer(int buffer_id);
    ChunkMgr* get_chunk_mgr();
  private:
    Config* config_;
    std::shared_ptr<FreeList> freeList_;
    std::shared_ptr<DataAllocator> dataAllocator_;
    std::shared_ptr<Server> server_;
    std::shared_ptr<ChunkMgr> chunkMgr_;
    std::shared_ptr<Protocol> protocol_;
  };

#endif //PMPOOL_DATASERVER_H
