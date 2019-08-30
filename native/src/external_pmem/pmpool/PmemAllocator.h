#ifndef PMPOOL_PMEMSTORE_H
#define PMPOOL_PMEMSTORE_H

#include <string>
#include <memkind.h>

using namespace std;

class PmemAllocator {
  public:
    PmemAllocator(string& pool_path);
    int init();
    void* allocate(size_t size);
    int deallocate(void*);
  private:
    string pool_path_;
    memkind* kind;
};

#endif
