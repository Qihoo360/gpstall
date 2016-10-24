#ifndef PS_SERVER_H
#define PS_SERVER_H

#include <string>
#include <memory>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>

#include "ps_options.h"
#include "ps_consts.h"

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
    should_exit_ = true;
  }

 private:

  PSOptions options_;

  // Server related
  int worker_num_;
  PSWorkerThread* ps_worker_thread_[kMaxWorkerThread];
  PSDispatchThread* ps_dispatch_thread_;

  std::atomic<bool> should_exit_;

  // Background thread
};

#endif
