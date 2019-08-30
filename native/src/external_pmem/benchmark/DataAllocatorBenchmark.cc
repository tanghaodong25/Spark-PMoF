#include "../pmpool/DataAllocator.h"
#include "../pmpool/Digest.h"
#include "../pmpool/queue/blockingconcurrentqueue.h"

#include <vector>
#include <cassert>
#include <unistd.h>
#include <string.h>
#include <cstring>
#include <thread>
#include <mutex>
#include <iostream>

moodycamel::BlockingConcurrentQueue<int> q;
int total_memory = 8*1024;
int buffer_size = 2*1024*1024;
int thread_num = 10;
char str[1024*1024*2];
std::vector<DataEntry*> dataEntries(total_memory);
std::atomic<bool> stop{false};
std::atomic<int> total_num{0};
std::mutex mtx;

uint64_t timestamp_now() {
  return std::chrono::high_resolution_clock::now().time_since_epoch() /
         std::chrono::milliseconds(1);
}

void alloc(DataAllocator* dataAllocator) {
  while (!stop) {
    int index;
    bool res = q.wait_dequeue_timed(index, std::chrono::milliseconds(4));
    if (res) {
      dataEntries[index] = dataAllocator->allocate();
      std::memcpy(*(char**)&dataEntries[index]->address, str, buffer_size);
      total_num++;
      if (total_num >= total_memory) {
        stop = true; 
      }
    }
  }
}

int main() {
  std::vector<string> pool_paths;
  pool_paths.push_back("/mnt/mem");
  pool_paths.push_back("/mnt/mem1");
  //pool_paths.push_back("/tmp/");

  std::shared_ptr<DataAllocator> dataAllocator =
      std::make_shared<DataAllocator>(pool_paths, buffer_size, nullptr);
  dataAllocator->init();
 
  memset(str, '0', buffer_size);

  for (int i = 0; i < 12; i++) {
    std::vector<std::thread> threads;
    stop = false;
    total_num = 0;
    for (int i = 0; i < thread_num; i++) {
      threads.push_back(std::thread(alloc, dataAllocator.get())); 
    }

    printf("started.\n");
    uint64_t start = timestamp_now();
    for (int i = 0; i < total_memory; i++) {
      q.enqueue(i);
    }

    for (int i = 0; i < thread_num; i++) {
      threads[i].join();
    }

    uint64_t end = timestamp_now();
    printf("finished, allocate bandwidth is %f GB/s.\n", 16.0/((end - start)/1000.0));

    for (int i = 0; i < total_memory; i++) {
      dataAllocator->deallocate(dataEntries[i]);
    }
    threads.clear();
  }
  return 0;
}
