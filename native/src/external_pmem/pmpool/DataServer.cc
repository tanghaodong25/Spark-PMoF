#include "DataServer.h"
#include "Config.h"
#include "DataAllocator.h"
#include "Digest.h"
#include "FreeList.h"
#include "Protocol.h"

DataServer::DataServer(Config *config) : config_(config) {}

int DataServer::init() {
  server_ = std::make_shared<Server>(config_->get_network_worker_num(),
                                     config_->get_network_buffer_num());
  if ((server_->init()) != 0) {
    return -1;
  }

  dataAllocator_ = std::make_shared<DataAllocator>(config_->get_pool_paths(),
                                                   2048 * 1024, server_.get());
  dataAllocator_->init();

  chunkMgr_ = std::make_shared<ChunkPool>(server_.get(),
                                          config_->get_network_buffer_size(),
                                          config_->get_network_buffer_num());

  freeList_ = std::make_shared<FreeList>();
  protocol_ =
      std::make_shared<Protocol>(freeList_.get(), dataAllocator_.get(), this);

  server_->set_chunk_mgr(chunkMgr_.get());
  server_->set_recv_callback(protocol_->get_recv_callback().get());
  server_->set_send_callback(protocol_->get_send_callback().get());
  server_->set_read_callback(protocol_->get_read_callback().get());

  return 0;
}

int DataServer::start() {
  /// start network service
  server_->start();
  int ret =
      server_->listen(config_->get_ip().c_str(), config_->get_port().c_str());
  return ret;
}

void DataServer::wait() { server_->wait(); }

uint64_t DataServer::register_rma_buffer(char *rma_buffer, uint64_t size,
                                         int buffer_id) {
  return server_->reg_rma_buffer(rma_buffer, size, buffer_id);
}

void DataServer::unregister_rma_buffer(int buffer_id) {
  server_->unreg_rma_buffer(buffer_id);
}

Chunk *DataServer::get_rma_buffer(int buffer_id) {
  return server_->get_rma_buffer(buffer_id);
}

ChunkMgr *DataServer::get_chunk_mgr() { return chunkMgr_.get(); }
