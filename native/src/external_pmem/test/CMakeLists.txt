add_executable(unit_tests DigestTest.cc DataEngineTest.cc DataAllocatorTest.cc)

target_link_libraries(unit_tests gtest_main pmpool)

add_test(NAME unit_tests COMMAND unit_tests)
