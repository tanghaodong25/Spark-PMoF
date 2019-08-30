#include <memory>

#include "../pmpool/FreeList.h"
#include "../pmpool/Digest.h"
#include "gtest/gtest.h"

TEST(freelist, first_to_last) {
  std::shared_ptr<FreeList> freeList = std::make_shared<FreeList>();
  std::shared_ptr<DataEntry> dataEntry_1 = std::make_shared<DataEntry>();
  std::shared_ptr<DataEntry> dataEntry_2 = std::make_shared<DataEntry>();
  std::shared_ptr<DataEntry> dataEntry_3 = std::make_shared<DataEntry>();
  dataEntry_1->rkey = 1;
  dataEntry_2->rkey = 2;
  dataEntry_3->rkey = 3;

  freeList->put(1, dataEntry_1.get());
  freeList->put(1, dataEntry_2.get());
  freeList->put(1, dataEntry_3.get());

  DataEntries *dataEntries = freeList->get(1);

  ASSERT_TRUE(dataEntries->size == 3);

  DataEntry *first = dataEntries->first;
  DataEntry *last = dataEntries->last;
  DataEntry *ptr = first;
  int i = 1;
  while (ptr) {
    ASSERT_TRUE(ptr->rkey == i);
    ptr = ptr->next;
    i++;
  }
}

TEST(freelist, last_to_first) {
  std::shared_ptr<FreeList> dataEngine = std::make_shared<FreeList>();
  std::shared_ptr<DataEntry> dataEntry_1 = std::make_shared<DataEntry>();
  std::shared_ptr<DataEntry> dataEntry_2 = std::make_shared<DataEntry>();
  std::shared_ptr<DataEntry> dataEntry_3 = std::make_shared<DataEntry>();
  dataEntry_1->rkey = 1;
  dataEntry_2->rkey = 2;
  dataEntry_3->rkey = 3;

  dataEngine->put(1, dataEntry_1.get());
  dataEngine->put(1, dataEntry_2.get());
  dataEngine->put(1, dataEntry_3.get());

  DataEntries *dataEntries = dataEngine->get(1);

  ASSERT_TRUE(dataEntries->size == 3);

  DataEntry *first = dataEntries->first;
  DataEntry *last = dataEntries->last;
  DataEntry *ptr = last;
  int i = 3;
  while (ptr) {
    ASSERT_TRUE(ptr->rkey == i);
    ptr = ptr->next;
    i--;
  }
}
