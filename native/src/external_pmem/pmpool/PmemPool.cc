#include "PmemPool.h"
#include "PmemAllocator.h"
#include "HPNL/ChunkMgr.h"
#include "HPNL/Connection.h"
#include <rdma/fi_domain.h>

char* PmemPoolAllocator::malloc(const size_type bytes) {
  int chunk_size = buffer_size+sizeof(Chunk);
  int buffer_num = bytes/chunk_size;
  char* memory = nullptr;
  if (pmemAllocator) {
    memory = static_cast<char*>(pmemAllocator->allocate(bytes));
  } else {
    memory = static_cast<char*>(std::malloc(bytes));
  }
  fid_mr* mr = nullptr;
  if (domain) {
    std::cout << "trying to register." << std::endl;
    int res = fi_mr_reg(domain, memory, bytes, FI_RECV | FI_SEND | FI_REMOTE_READ, 0, 0, 0, &mr, nullptr);
    if (res) {
      std::cout << "rdma registration error." << std::endl;
      perror("fi_mr_reg");
      return nullptr; 
    }
    std::cout << "finished registration." << std::endl;
  }

  auto first = reinterpret_cast<Chunk*>(memory);
  auto memory_ptr = first;
  for (int i = 0; i < buffer_num; i++) {
    chunk_to_id_map[memory_ptr] = id;
    id_to_chunk_map[id] = std::pair<Chunk*, fid_mr*>(memory_ptr, mr);
    id++;
    memory_ptr = reinterpret_cast<Chunk*>(reinterpret_cast<char*>(memory_ptr)+chunk_size);
  }
  return reinterpret_cast<char *>(first);
}

void PmemPoolAllocator::free(char* const block) {
  auto memory = reinterpret_cast<Chunk*>(block);
  if (pmemAllocator) {
    pmemAllocator->deallocate(memory);
  } else {
    std::free(memory);
  }
}

int PmemPoolAllocator::buffer_size = 0;
fid_domain* PmemPoolAllocator::domain = nullptr;
int PmemPoolAllocator::id = 0;
std::map<int, std::pair<Chunk*, fid_mr*>> PmemPoolAllocator::id_to_chunk_map;
std::map<Chunk*, int> PmemPoolAllocator::chunk_to_id_map;
std::mutex PmemPoolAllocator::mtx;
PmemAllocator* PmemPoolAllocator::pmemAllocator = nullptr;

PmemPool::PmemPool(FabricService* service_, const int request_buffer_size,
  const int next_request_buffer_number, PmemAllocator* pmemAllocator_) :
    pool(request_buffer_size+sizeof(Chunk), next_request_buffer_number, 0),
    buffer_size(request_buffer_size), used_buffers(0) {

  PmemPoolAllocator::buffer_size = buffer_size;
  if (service_) {
    PmemPoolAllocator::domain = service_->get_domain();
  }
  PmemPoolAllocator::id = 0;
  PmemPoolAllocator::pmemAllocator = pmemAllocator_;
}

PmemPool::~PmemPool() {
  for (auto ck : PmemPoolAllocator::id_to_chunk_map) {
    if (ck.second.second) {
      fi_close(&((fid_mr*)ck.second.second)->fid);
    }
  }
  PmemPoolAllocator::id_to_chunk_map.clear();
  PmemPoolAllocator::chunk_to_id_map.clear();
}

void* PmemPool::malloc() {
  if (!store().empty()) {
    return (store().malloc());
  }
  return system_malloc();
}

void PmemPool::free(void * const ck) {
  (store().free)(ck);
}

Chunk* PmemPool::get(int id) {
  std::lock_guard<std::mutex> l(PmemPoolAllocator::mtx);
  if (!PmemPoolAllocator::id_to_chunk_map.count(id)) {
    return nullptr;
  }
  return PmemPoolAllocator::id_to_chunk_map[id].first;
}

Chunk* PmemPool::get() {
  std::lock_guard<std::mutex> l(PmemPoolAllocator::mtx);
  auto ck = reinterpret_cast<Chunk*>(pool::malloc());
  used_buffers++;
  ck->capacity = buffer_size;
  ck->buffer_id = PmemPoolAllocator::chunk_to_id_map[ck];
  ck->mr = PmemPoolAllocator::id_to_chunk_map[ck->buffer_id].second;
  ck->buffer = ck->data;
  ck->size = 0;
  ck->ctx.internal[4] = ck;
  return ck;
}

void PmemPool::reclaim(Chunk* ck) {
  std::lock_guard<std::mutex> l(PmemPoolAllocator::mtx);
  pool::free(ck);
  used_buffers--;
}

int PmemPool::free_size() {
  return INT_MAX;
}

void* PmemPool::system_malloc() {
  return boost::pool<PmemPoolAllocator>::malloc();
}
