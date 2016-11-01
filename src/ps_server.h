#ifndef PS_SERVER_H
#define PS_SERVER_H

#include <string>
#include <memory>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>

#include "ps_options.h"
#include "ps_consts.h"
#include "ps_logger.h"

#include "bg_thread.h"
#include "holy_thread.h"

#include "slash_status.h"
#include "slash_mutex.h"

using slash::Status;

class PSServer;
class PSServerConn;
class PSServerThread;

class PSWorkerThread;
class PSDispatchThread;

class PSServer {
 public:

  explicit PSServer(const PSOptions& option);
  virtual ~PSServer();
  Status Start();
  
  std::string local_ip() {
    return options_.local_ip;
  }
  int local_port() {
    return options_.local_port;
  }

  void Exit() {
    //printf ("Server will exit!\n");
    should_exit_ = true;
  }

  Logger* GetLogger(const std::string &database, const std::string &table, const std::string &header);

  slash::Mutex mutex_files_;

 private:

  PSOptions options_;

  // Server related
  int worker_num_;
  PSWorkerThread* ps_worker_thread_[kMaxWorkerThread];
  PSDispatchThread* ps_dispatch_thread_;

  std::atomic<bool> should_exit_;

  // logger related
  std::unordered_map<std::string, Logger*> files_;

  void DoTimingTask();
  // Background thread
};

#endif
