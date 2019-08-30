#include <memory>

#include "pmpool/Config.h"
#include "pmpool/DataServer.h"

int ServerMain(int argc, char** argv) {
  std::shared_ptr<Config> config = std::make_shared<Config>(argc, argv);
  std::shared_ptr<DataServer> dataServer = std::make_shared<DataServer>(config.get());
  dataServer->init();
  dataServer->start();
  dataServer->wait();
  return 0;
}

int main(int argc, char** argv) {
  ServerMain(argc, argv);
  return 0;
}
