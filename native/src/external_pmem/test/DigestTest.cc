#include <iostream>
#include <memory>

#include "../pmpool/Digest.h"
#include "gtest/gtest.h"

TEST(digest, compute) {
  std::shared_ptr<KeyEntry> keyEntry = std::make_shared<KeyEntry>();
  keyEntry->shuffle_id = 0;
  keyEntry->map_id = 10;
  keyEntry->reduce_id = 20;
  uint64_t hash_value_1;
  uint64_t hash_value_2;
  Digest::computeKeyHash(keyEntry.get(), &hash_value_1);
  Digest::computeKeyHash(keyEntry.get(), &hash_value_2);
  ASSERT_TRUE(hash_value_1 == hash_value_2);
}
