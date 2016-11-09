#ifndef PS_OPTIONS_H
#define PS_OPTIONS_H

#include <string>
#include <vector>

// configuration item name
const std::string LOCAL_IP = "local_ip";
const std::string LOCAL_PORT = "local_port";
const std::string WORKER_NUM = "worker_num";
const std::string FILE_SIZE = "file_size";
const std::string LOAD_INTERVAL = "load_interval";
const std::string FLUSH_INTERVAL = "flush_interval";
const std::string TIMEOUT = "timeout";
const std::string DATA_PATH = "data_path";
const std::string LOG_PATH = "log_path";
const std::string LOAD_SCRIPT = "load_script";
const std::string CONF_SCRIPT = "conf_script";
const std::string DAEMON_MODE = "daemon_mode";
const std::string PASSWD = "passwd";

// Greenplum configuration item name
const std::string GP_USER = "gp_user";
const std::string GP_HOST = "gp_host";
const std::string GP_PORT = "gp_port";
const std::string GPD_HOST = "gpd_host";
const std::string GPD_PORT= "gpd_port";
const std::string ERROR_LIMIT = "error_limit";

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
  int flush_interval;

  // TODO session timeout
  int64_t timeout;

  std::string data_path;
  std::string log_path;
  std::string load_script;
  std::string conf_script;
  std::string passwd;
  bool daemon_mode;

  // greenplum conf
  std::string gp_user;
  std::string gp_host;
  int gp_port;

  // gpfdist conf
  std::string gpd_host;
  int gpd_port;
  int error_limit;
  
  //std::vector<Server> servers;

  PSOptions();

  PSOptions(const PSOptions& options);

  void Dump();
};

#endif
