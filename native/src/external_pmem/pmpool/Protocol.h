#ifndef PMPOOL_PROTOCOL_H
#define PMPOOL_PROTOCOL_H

#include <memory>
#include <cassert>
#include <cstring>
#include <chrono>

#include "flatbuffers/flatbuffers.h"
#include <HPNL/Callback.h>
#include <HPNL/Connection.h>
#include <HPNL/ChunkMgr.h>

#include "queue/blockingconcurrentqueue.h"
#include "queue/concurrentqueue.h"
#include "ThreadWrapper.h"

class Digest;
class FreeList;
class DataAllocator;
class DataServer;

using namespace moodycamel;

struct MessageHeader {
  MessageHeader() {}
  MessageHeader(uint8_t msg_type, uint64_t sequence_id) {
    msg_type_ = msg_type;
    sequence_id_ = sequence_id;
  }
  uint8_t msg_type_;
  uint64_t sequence_id_;
  int msg_size;
};

class Protocol {
  public:
    class RecvCallback : public Callback {
      public:
        explicit RecvCallback(Protocol *protocol, ChunkMgr* chunkMgr);
        ~RecvCallback() override = default;
        void operator()(void* buffer_id, void* buffer_size) override;
      private:
        Protocol* protocol_;
        ChunkMgr* chunkMgr_;
    };

    class SendCallback : public Callback {
      public:
        explicit SendCallback(ChunkMgr* chunkMgr);
        ~SendCallback() override = default;
        void operator()(void* buffer_id, void* buffer_size) override;
      private:
        ChunkMgr* chunkMgr_;
    };

    class ReadCallback : public Callback {
      public:
        explicit ReadCallback(Protocol *protocol, DataAllocator* dataAllocator);
        ~ReadCallback() override = default;
        void operator()(void* buffer_id, void* buffer_size) override;
      private:
        Protocol* protocol_;
        DataAllocator* dataAllocator_;
    };

    class RecvWorker : public ThreadWrapper {
      public:
        RecvWorker(Protocol* protocol, ChunkMgr* chunkMgr);
        ~RecvWorker() override = default;
        int entry() override;
        void abort() override {}
      private:
        Protocol* protocol_;
        ChunkMgr* chunkMgr_;
    };

    class ReadWorker : public ThreadWrapper {
      public:
        ReadWorker(Protocol* protocol);
        ~ReadWorker() override = default;
        int entry() override;
        void abort() override {}
      private:
        Protocol* protocol_;
    };

    Protocol(FreeList* FreeList, DataAllocator* dataAllocator, DataServer* dataServer);

    friend class RecvCallback;
    friend class ReadCallback;
    friend class ReceiveWorker;
    friend class ReadWorker;

    static void decode(char* buffer, int buffer_size, MessageHeader* msg_header, char** msg_content, int* content_size);
    static void encode(char* buffer, int buffer_capacity, MessageHeader* msg_header, char* msg_content, int content_size);

    std::shared_ptr<RecvCallback> get_recv_callback();
    std::shared_ptr<SendCallback> get_send_callback();
    std::shared_ptr<ReadCallback> get_read_callback();

    int handle_receive_msg(Chunk* ck);
    int handle_remote_read_msg(Chunk* ck);
    int handle_send_msg();
  private:
    FreeList* freeList_;
    DataAllocator* dataAllocator_;
    DataServer* dataServer_;
    std::shared_ptr<RecvCallback> recvCallback_;
    std::shared_ptr<SendCallback> sendCallback_;
    std::shared_ptr<ReadCallback> readCallback_;
    BlockingConcurrentQueue<Chunk*> recvMsgQueue_;
    BlockingConcurrentQueue<Chunk*> readMsgQueue_;
    std::vector<RecvWorker*> recvWorkers;
    std::vector<ReadWorker*> readWorkers;
    int default_worker_num = 3;
};

#endif //PMPOOL_PROTOCOL_H
