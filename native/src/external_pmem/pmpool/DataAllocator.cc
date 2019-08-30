#include "DataAllocator.h"
#include "DataServer.h"
#include "Digest.h"
#include "HPNL/ChunkMgr.h"
#include "HPNL/Server.h"
#include "PmemAllocator.h"
#include "PmemPool.h"

#include <iostream>

DataAllocator::DataAllocator(vector<string> &pool_paths, uint64_t buffer_size,
                             Server *server)
    : pool_paths_(pool_paths), pool_size_(pool_paths_.size()),
      buffer_size_(buffer_size), buffer_id_(0), server_(server) {}

DataAllocator::~DataAllocator() {}

int DataAllocator::init() {
  for (int i = 0; i < pool_size_; i++) {
    auto pmemAllocator = new PmemAllocator(pool_paths_[i]);
    pmemAllocator->init();
    auto pool =
        new  PmemPool(server_, buffer_size_, 1024*4, nullptr);
    pmemPools.push_back(pool);
  }
  return 0;
}

DataEntry *DataAllocator::allocate() {
  uint64_t local_buffer_id = buffer_id_++;
  int index = local_buffer_id % pool_size_;
  /// create DataEntry
  auto ck = pmemPools[index]->get();
  DataEntry *entry = new DataEntry();
  entry->address = reinterpret_cast<uint64_t>(ck->buffer);
  if (ck->mr) {
    entry->rkey = ck->mr->key;
  }
  entry->buffer_id = local_buffer_id;
  entry->ck = ck;
  entry->next = nullptr;
  return entry;
}

int DataAllocator::deallocate(DataEntry *entry) {
  uint64_t address = entry->address;
  int buffer_id = entry->buffer_id;
  char *buffer = *(char **)&address;
  /// free allocated pmem memory
  int index = buffer_id % pool_size_;
  pmemPools[index]->free(entry->ck);
  delete entry;
  return 0;
}

Chunk* DataAllocator::get(int buffer_id) {
  int index = buffer_id % pool_size_;
  return pmemPools[index]->get(buffer_id);
}
