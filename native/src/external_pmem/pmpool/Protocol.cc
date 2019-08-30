#include "Protocol.h"
#include "DataAllocator.h"
#include "Digest.h"
#include "FreeList.h"
#include "DataServer.h"
#include "fbs/block_metadata_generated.h"
#include "fbs/get_block_generated.h"
#include "fbs/push_block_generated.h"

Protocol::Protocol(FreeList *freeList, DataAllocator *dataAllocator,
                   DataServer* dataServer)
    : freeList_(freeList), dataAllocator_(dataAllocator), dataServer_(dataServer) {
  recvCallback_ = std::make_shared<RecvCallback>(this, dataServer_->get_chunk_mgr());
  sendCallback_ = std::make_shared<SendCallback>(dataServer_->get_chunk_mgr());
  readCallback_ = std::make_shared<ReadCallback>(this, dataAllocator_);
  for (int i = 0; i < default_worker_num; i++) {
    auto recvWorker = new RecvWorker(this, dataServer_->get_chunk_mgr());
    recvWorker->start();
    recvWorkers.push_back(recvWorker);
    auto readWorker = new ReadWorker(this);
    readWorker->start();
    readWorkers.push_back(readWorker);
  }
}

Protocol::RecvCallback::RecvCallback(Protocol *protocol, ChunkMgr *chunkMgr)
    : protocol_(protocol), chunkMgr_(chunkMgr) {}

void Protocol::RecvCallback::operator()(void *buffer_id, void *buffer_size) {
  auto buffer_id_ = *static_cast<int *>(buffer_id);
  Chunk *ck = chunkMgr_->get(buffer_id_);
  assert(*static_cast<uint64_t *>(buffer_size) == ck->size);
  protocol_->recvMsgQueue_.enqueue(ck);
}

Protocol::SendCallback::SendCallback(ChunkMgr *chunkMgr)
    : chunkMgr_(chunkMgr) {}

void Protocol::SendCallback::operator()(void *buffer_id, void *buffer_size) {
  auto buffer_id_ = *static_cast<int *>(buffer_id);
  auto ck = chunkMgr_->get(buffer_id_);
  chunkMgr_->reclaim(ck, static_cast<Connection *>(ck->con));
}

Protocol::ReadCallback::ReadCallback(Protocol* protocol, DataAllocator* dataAllocator)
    : protocol_(protocol), dataAllocator_(dataAllocator) {}

void Protocol::ReadCallback::operator()(void *buffer_id, void *buffer_size) {
  int buffer_id_ = *static_cast<int *>(buffer_id);
  Chunk *ck = dataAllocator_->get(buffer_id_);
  auto buffer = ck->buffer;
  protocol_->readMsgQueue_.enqueue(ck);
}

Protocol::RecvWorker::RecvWorker(Protocol *protocol, ChunkMgr* chunkMgr) : protocol_(protocol), chunkMgr_(chunkMgr) {}

int Protocol::RecvWorker::entry() {
  Chunk *ck;
  bool res = protocol_->recvMsgQueue_.wait_dequeue_timed(
      ck, std::chrono::milliseconds(1000));
  if (res) {
    protocol_->handle_receive_msg(ck);
  }
  return 0;
}

Protocol::ReadWorker::ReadWorker(Protocol *protocol) : protocol_(protocol) {}

int Protocol::ReadWorker::entry() {
  Chunk *ck;
  bool res = protocol_->readMsgQueue_.wait_dequeue_timed(
      ck, std::chrono::milliseconds(1000));
  if (res) {
    protocol_->handle_remote_read_msg(ck);
  }
  return 0;
}

void Protocol::decode(char *buffer, int buffer_size, MessageHeader *msg_header,
                      char **msg_content, int *content_size) {
  msg_header->msg_size = buffer_size;
  std::memcpy((void *)&msg_header->msg_type_, buffer, 1);
  std::memcpy((void *)&msg_header->sequence_id_, buffer + 1, 8);
  *msg_content = buffer + 9;
  *content_size = buffer_size - 9;
}

void Protocol::encode(char *buffer, int buffer_capacity,
                      MessageHeader *msg_header, char *msg_content,
                      int content_size) {
  assert(buffer_capacity >= content_size + 9);
  std::memcpy(buffer, (void *)&msg_header->msg_type_, 1);
  std::memcpy(buffer + 1, (void *)&msg_header->sequence_id_, 8);
  if (content_size > 0) {
    std::memcpy(buffer + 9, msg_content, content_size);
  }
  msg_header->msg_size = content_size + 9;
}

std::shared_ptr<Protocol::RecvCallback> Protocol::get_recv_callback() {
  return recvCallback_;
}
std::shared_ptr<Protocol::SendCallback> Protocol::get_send_callback() {
  return sendCallback_;
}

std::shared_ptr<Protocol::ReadCallback> Protocol::get_read_callback() {
  return readCallback_;
}

int Protocol::handle_receive_msg(Chunk *ck) {
  char *msg_content = nullptr;
  int content_size;
  std::shared_ptr<MessageHeader> msg_header = std::make_shared<MessageHeader>();
  Protocol::decode((char *)ck->buffer, ck->size, msg_header.get(), &msg_content,
                   &content_size);
  assert(msg_content != nullptr);

  auto con = (Connection *)ck->con;
  if (msg_header->msg_type_ == 0) {
    flatbuffers::FlatBufferBuilder builder;
    builder.PushFlatBuffer(reinterpret_cast<unsigned char *>(msg_content),
                           content_size);
    auto msg = flatbuffers::GetRoot<PushBlock>(builder.GetBufferPointer());
    /// allocate pmem buffer
    DataEntry *dataEntry = dataAllocator_->allocate();
    dataEntry->size = msg->size();
    KeyEntry keyEntry = {msg->shuffleId(), msg->mapId(), msg->reduceId()};
    uint64_t key = 0;
    Digest::computeKeyHash(&keyEntry, &key);
    freeList_->put(key, dataEntry);
    /// remote read data
    auto read_chunk = dataEntry->ck;
    read_chunk->ptr = msg_header.get();
    con->read(read_chunk, 0, msg->size(), msg->address(),
              msg->rkey());
  } else if (msg_header->msg_type_ == 1) {
    flatbuffers::FlatBufferBuilder builder;
    builder.PushFlatBuffer(reinterpret_cast<unsigned char *>(msg_content),
                           content_size);
    auto msg = flatbuffers::GetRoot<GetBlock>(builder.GetBufferPointer());
    /// search pmem buffer
    uint16_t shuffleId = msg->shuffleId();
    uint16_t mapId = msg->mapId();
    uint16_t startPartition = msg->startPartition();
    uint16_t endPartition = msg->endPartition();

    for (uint16_t i = startPartition; i < endPartition; i++) {
      KeyEntry keyEntry = {shuffleId, mapId, i};
      uint64_t key = 0;
      Digest::computeKeyHash(&keyEntry, &key);
      DataEntries *dataEntries = freeList_->get(key);
      if (dataEntries) {
      }
    }
  } else {
  }
  dataServer_->get_chunk_mgr()->reclaim(ck, con);
  return 0;
}

int Protocol::handle_remote_read_msg(Chunk *ck) {
  //// read_msg_queue.enqueue(ck);
  auto con = (Connection *)ck->con;
  auto msg_header = (MessageHeader *)ck->ptr;
  auto send_chunk = dataServer_->get_chunk_mgr()->get(con);
  Protocol::encode((char *)send_chunk->buffer, ck->capacity, msg_header,
                   nullptr, 0);
  con->send(send_chunk);
  return 0;
}
