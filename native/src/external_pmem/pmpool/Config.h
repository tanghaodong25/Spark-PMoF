#ifndef PMPOOL_CONFIG_H
#define PMPOOL_CONFIG_H
#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>

using namespace boost::program_options;
using namespace std;

class Config {
public:
  Config(int argc, char **argv) {
    try {
      options_description desc{"Options"};
      desc.add_options()("help,h", "Help screen")(
          "address,a", value<string>()->default_value("172.168.2.106"),
          "set the rdma server address")(
          "port,p", value<string>()->default_value("12345"),
          "set the rdma server port")(
          "network_buffer_size,nbs", value<int>()->default_value(65536), "set network buffer size")(
          "network_buffer_num,nbn", value<int>()->default_value(16), "set network buffer number")(
          "network_worker,nw", value<int>()->default_value(1), "set network wroker number")(
          "paths,ps",value<vector<string>>()->default_value(std::vector<string>(), "/mnt/mem"), "set memory pool path");

      variables_map vm;
      store(parse_command_line(argc, argv, desc), vm);
      notify(vm);

      if (vm.count("help")) {
        std::cout << desc << '\n';
      }
      set_ip(vm["address"].as<string>());
      set_port(vm["port"].as<string>());
      set_network_buffer_size(vm["network_buffer_size"].as<int>());
      set_network_buffer_num(vm["network_buffer_num"].as<int>());
      set_network_worker_num(vm["network_worker"].as<int>());

      pool_paths_.push_back("/mnt/mem");
    } catch (const error &ex) {
      std::cerr << ex.what() << '\n';
    }
  }
  string get_ip() { return ip_; }
  void set_ip(string ip) { ip_ = ip; }

  string get_port() { return port_; }
  void set_port(string port) { port_ = port; }

  int get_network_buffer_size() { return network_buffer_size_; }
  void set_network_buffer_size(int network_buffer_size) { network_buffer_size_ = network_buffer_size; }

  int get_network_buffer_num() { return network_buffer_num_; }
  void set_network_buffer_num(int network_buffer_num) { network_buffer_num_ = network_buffer_num; }

  int get_network_worker_num() { return network_worker_num_; }
  void set_network_worker_num(int network_worker_num) { network_worker_num_ = network_worker_num; }

  std::vector<string>& get_pool_paths() { return pool_paths_; }
  void set_pool_paths(std::vector<string> &pool_paths) { pool_paths_ = pool_paths; }
private:
  string ip_;
  string port_;
  int network_buffer_size_;
  int network_buffer_num_;
  int network_worker_num_;
  std::vector<string> pool_paths_;
};

#endif // PMPOOL_CONFIG_H
