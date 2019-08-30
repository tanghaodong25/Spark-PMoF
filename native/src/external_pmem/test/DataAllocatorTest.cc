#include <memory>

#include "../pmpool/DataAllocator.h"
#include "../pmpool/Digest.h"
#include "gtest/gtest.h"
#include <iostream>
#include <cstring>
#include <vector>
#include <thread>
#include <chrono>

DataEntry* dataEntries[1024];

TEST(dataallocator, single_threaded_allocate) {
  std::vector<std::string> vec;
  vec.push_back("/mnt/mem");
  vec.push_back("/mnt/mem1");
  std::shared_ptr<DataAllocator> dataAllocator = std::make_shared<DataAllocator>(vec, 1024*1024*2, nullptr);
  ASSERT_TRUE(dataAllocator->init() == 0);
  for (int i = 0; i < 1024; i++) {
    dataEntries[i] = dataAllocator->allocate();
  }
  for (int i = 0; i < 1024; i++) {
    dataAllocator->deallocate(dataEntries[i]);
  }
}

void alloc(DataAllocator* dataAllocator, int index) {
  dataEntries[index] = dataAllocator->allocate();
  //sleep(1);
  dataAllocator->deallocate(dataEntries[index]);
}

TEST(dataallocator, multi_threaded_allocate) {
  std::vector<std::string> vec;
  vec.push_back("/mnt/mem");
  vec.push_back("/mnt/mem1");
  std::shared_ptr<DataAllocator> dataAllocator = std::make_shared<DataAllocator>(vec, 1024*1024*2, nullptr);
  ASSERT_TRUE(dataAllocator->init() == 0);
  std::vector<std::thread> threads;
  for (int i = 0; i < 1024; i++) {
    threads.push_back(std::thread(alloc, dataAllocator.get(), i));
  }
  for (int i = 0; i < 1024; i++) {
    threads[i].join();
  }
}
