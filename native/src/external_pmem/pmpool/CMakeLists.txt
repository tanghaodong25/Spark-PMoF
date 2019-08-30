add_library(pmpool SHARED DataServer.cc FreeList.cc DataAllocator.cc Digest.cc Protocol.cc PmemAllocator.cc PmemPool.cc hash/xxhash.cc)
target_link_libraries(pmpool LINK_PUBLIC ${Boost_LIBRARIES} hpnl memkind)
set_target_properties(pmpool PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
