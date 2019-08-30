#include "PmemAllocator.h"

PmemAllocator::PmemAllocator(string& pool_path) : pool_path_(pool_path), kind(nullptr) {}

int PmemAllocator::init() {
  if (memkind_create_pmem(pool_path_.c_str(), 0, &kind)) {
    return -1; 
  }
  return 0;
}

void* PmemAllocator::allocate(size_t size) {
  return memkind_malloc(kind, size);
}

int PmemAllocator::deallocate(void* buffer) {
  memkind_free(kind, buffer);
  return 0;
}
