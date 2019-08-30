// automatically generated by the FlatBuffers compiler, do not modify


#ifndef FLATBUFFERS_GENERATED_GETBLOCK_H_
#define FLATBUFFERS_GENERATED_GETBLOCK_H_

#include "flatbuffers/flatbuffers.h"

struct GetBlock;

struct GetBlock FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_SHUFFLEID = 4,
    VT_MAPID = 6,
    VT_STARTPARTITION = 8,
    VT_ENDPARTITION = 10
  };
  uint16_t shuffleId() const {
    return GetField<uint16_t>(VT_SHUFFLEID, 0);
  }
  uint16_t mapId() const {
    return GetField<uint16_t>(VT_MAPID, 0);
  }
  uint16_t startPartition() const {
    return GetField<uint16_t>(VT_STARTPARTITION, 0);
  }
  uint16_t endPartition() const {
    return GetField<uint16_t>(VT_ENDPARTITION, 0);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyField<uint16_t>(verifier, VT_SHUFFLEID) &&
           VerifyField<uint16_t>(verifier, VT_MAPID) &&
           VerifyField<uint16_t>(verifier, VT_STARTPARTITION) &&
           VerifyField<uint16_t>(verifier, VT_ENDPARTITION) &&
           verifier.EndTable();
  }
};

struct GetBlockBuilder {
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_shuffleId(uint16_t shuffleId) {
    fbb_.AddElement<uint16_t>(GetBlock::VT_SHUFFLEID, shuffleId, 0);
  }
  void add_mapId(uint16_t mapId) {
    fbb_.AddElement<uint16_t>(GetBlock::VT_MAPID, mapId, 0);
  }
  void add_startPartition(uint16_t startPartition) {
    fbb_.AddElement<uint16_t>(GetBlock::VT_STARTPARTITION, startPartition, 0);
  }
  void add_endPartition(uint16_t endPartition) {
    fbb_.AddElement<uint16_t>(GetBlock::VT_ENDPARTITION, endPartition, 0);
  }
  explicit GetBlockBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  GetBlockBuilder &operator=(const GetBlockBuilder &);
  flatbuffers::Offset<GetBlock> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<GetBlock>(end);
    return o;
  }
};

inline flatbuffers::Offset<GetBlock> CreateGetBlock(
    flatbuffers::FlatBufferBuilder &_fbb,
    uint16_t shuffleId = 0,
    uint16_t mapId = 0,
    uint16_t startPartition = 0,
    uint16_t endPartition = 0) {
  GetBlockBuilder builder_(_fbb);
  builder_.add_endPartition(endPartition);
  builder_.add_startPartition(startPartition);
  builder_.add_mapId(mapId);
  builder_.add_shuffleId(shuffleId);
  return builder_.Finish();
}

#endif  // FLATBUFFERS_GENERATED_GETBLOCK_H_
