#ifndef PS_OPTIONS_H
#define PS_OPTIONS_H

#include <string>
#include <vector>

class Server;
struct PSOptions;

class Server {
 public:
  std::string ip;
  int port;

  // colon separated ip:port
  Server(const std::string& str);
  Server(const std::string& _ip, const int& _port) : ip(_ip), port(_port) {}

  Server(const Server& server)
      : ip(server.ip),
      port(server.port) {}

  Server& operator=(const Server& server) {
    ip = server.ip;
    port = server.port;
    return *this;
  }

 private:
};

struct PSOptions {
  std::string local_ip;
  int local_port;

  int worker_num;
  int file_size;
  int load_interval;

  // TODO session timeout
  int64_t timeout;

  std::string data_path;
  std::string log_path;
  std::string load_script;
  std::string conf_script;

  //std::vector<Server> servers;

  PSOptions();

  PSOptions(const PSOptions& options);

  void Dump();
};

#endif
