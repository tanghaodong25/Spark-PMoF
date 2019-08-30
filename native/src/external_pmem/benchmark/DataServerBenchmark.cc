#include <cstring>

#include "HPNL/Callback.h"
#include "HPNL/ChunkMgr.h"
#include "HPNL/Client.h"
#include "HPNL/Connection.h"

#include "../pmpool/Protocol.h"
#include "../pmpool/fbs/push_block_generated.h"

#include <iostream>

#define BUFFER_SIZE 65536
#define BUFFER_NUM 128
#define RMA_BUFFER_SIZE 2097152
//#define RMA_BUFFER_SIZE 65536

int count = 0;
int total_num = 2*1024;
uint64_t start, end = 0;
std::mutex mtx;
char rma_buffer[RMA_BUFFER_SIZE];
uint64_t rkey;
MessageHeader* messageHeader;
uint8_t* msg_buf;
int msg_size;

uint64_t timestamp_now() {
  return std::chrono::high_resolution_clock::now().time_since_epoch() /
         std::chrono::milliseconds(1);
}

class ShutdownCallback : public Callback {
 public:
  explicit ShutdownCallback(Client* _clt) : clt(_clt) {}
  ~ShutdownCallback() override = default;
  void operator()(void* param_1, void* param_2) override {
    std::cout << "connection shutdown..." << std::endl;
    clt->shutdown();
  }

 private:
  Client* clt;
};

class ConnectedCallback : public Callback {
 public:
  explicit ConnectedCallback(ChunkMgr* bufMgr_) : bufMgr(bufMgr_) {}
  ~ConnectedCallback() override = default;
  void operator()(void* param_1, void* param_2) override {
    auto con = static_cast<Connection*>(param_1);
    Chunk* ck = bufMgr->get(con);
    Protocol::encode((char*)ck->buffer, BUFFER_SIZE, messageHeader, (char*)msg_buf, msg_size);
    ck->size = messageHeader->msg_size;
    con->send(ck);
  }

 private:
  ChunkMgr* bufMgr;
};

class RecvCallback : public Callback {
 public:
  RecvCallback(Client* client_, ChunkMgr* bufMgr_) : client(client_), bufMgr(bufMgr_) {}
  ~RecvCallback() override = default;
  void operator()(void* param_1, void* param_2) override {
    int mid = *static_cast<int*>(param_1);
    Chunk* ck = bufMgr->get(mid);
    auto con = static_cast<Connection*>(ck->con);
    bufMgr->reclaim(ck, con);

    if (count++ >= total_num) {
      std::cout << "finished." << std::endl;
      end = timestamp_now();
      printf("finished, totally consumes %f s, bandwidth %f GB/s.\n",
             (end - start) / 1000.0, 4/((end - start)/1000.0));
      return; 
    }
    if (count == 1) {
      printf("start test.\n");
      start = timestamp_now();
    }
    //printf("count: %d.\n", count);

    Chunk* ck1 = bufMgr->get(con);
    Protocol::encode((char*)ck1->buffer, BUFFER_SIZE, messageHeader, (char*)msg_buf, msg_size);
    ck1->size = messageHeader->msg_size;
    con->send(ck1);
  }

 private:
  Client* client;
  ChunkMgr* bufMgr;
};

class SendCallback : public Callback {
 public:
  explicit SendCallback(ChunkMgr* bufMgr_) : bufMgr(bufMgr_) {}
  ~SendCallback() override = default;
  void operator()(void* param_1, void* param_2) override {
    int mid = *static_cast<int*>(param_1);
    Chunk* ck = bufMgr->get(mid);
    auto con = static_cast<Connection*>(ck->con);
    bufMgr->reclaim(ck, con);
  }

 private:
  ChunkMgr* bufMgr;
};

int main(int argc, char* argv[]) {
  auto client = new Client(1, 16);
  client->init();

  ChunkMgr* bufMgr = new ChunkPool(client, BUFFER_SIZE, BUFFER_NUM);
  client->set_chunk_mgr(bufMgr);

  auto recvCallback = new RecvCallback(client, bufMgr);
  auto sendCallback = new SendCallback(bufMgr);
  auto connectedCallback = new ConnectedCallback(bufMgr);
  auto shutdownCallback = new ShutdownCallback(client);

  client->set_recv_callback(recvCallback);
  client->set_send_callback(sendCallback);
  client->set_connected_callback(connectedCallback);
  client->set_shutdown_callback(shutdownCallback);

  client->start();
  /// initialize rma buffer
  memset(rma_buffer, '0', RMA_BUFFER_SIZE);
  rkey = client->reg_rma_buffer(rma_buffer, RMA_BUFFER_SIZE, 0);

  messageHeader = new MessageHeader(0, 0);
  flatbuffers::FlatBufferBuilder builder;
  auto msg = CreatePushBlock(builder, 0, 0, 0, reinterpret_cast<uint64_t>(rma_buffer), RMA_BUFFER_SIZE, rkey);
  builder.Finish(msg);
  msg_buf = builder.GetBufferPointer();
  msg_size = builder.GetSize();

  client->connect("172.168.2.106", "12345");
  client->wait();

  delete shutdownCallback;
  delete connectedCallback;
  delete sendCallback;
  delete recvCallback;
  delete client;
  delete bufMgr;
  return 0;
}
