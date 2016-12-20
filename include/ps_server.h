#ifndef PS_SERVER_H
#define PS_SERVER_H

#include <string>
#include <memory>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>

#include "ps_monitor_thread.h"
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
  
  struct CalcExecTime {
    uint64_t *ptimeused;
    struct timeval st, ed;
    explicit CalcExecTime(uint64_t *timep) {
      gettimeofday(&st, NULL);
      ptimeused = timep;
    }
    ~CalcExecTime() {
      gettimeofday(&ed, NULL);
      *ptimeused = (ed.tv_sec - st.tv_sec) * 1000 + (ed.tv_usec - st.tv_usec) / 1000;
    }
  };

  // gpload err info
  int gpload_failed_num;
  std::string latest_failed_time;
  int failed_files_num;
  std::string failed_files_name;
  int failed_files_size; // KB
  uint64_t gpload_longest_timeused; // ms
  uint64_t gpload_latest_timeused; // ms
  uint64_t gpload_average_timeused; // ms
 
  bool is_stall() {
    return is_stall_;
  }
  void set_is_stall(bool v) {
    is_stall_ = v;
  }
  std::string local_ip() {
    return options_.local_ip;
  }
  int local_port() {
    return options_.local_port;
  }
  std::string gp_user() {
    return options_.gp_user;
  }
  std::string passwd() {
    return options_.passwd;
  }

  void Exit() {
    //printf ("Server will exit!\n");
    should_exit_ = true;
  }

  Logger* GetLogger(const std::string &database, const std::string &table, const std::string &header);

  slash::Mutex mutex_files_;

 private:
  PSOptions options_;

  // Used in monitor
  bool is_stall_;
  uint64_t gpload_total_timeused_;
  uint64_t gpload_counts_;

  // Server related
  int worker_num_;
  PSWorkerThread* ps_worker_thread_[kMaxWorkerThread];
  PSDispatchThread* ps_dispatch_thread_;
  PSMonitorThread* ps_monitor_thread_;

  std::atomic<bool> should_exit_;

  // logger related
  std::unordered_map<std::string, Logger*> files_;

  // Background thread
  void DoTimingTask();
  void MaybeFlushLog();

  void CollectGploadErrInfo(int ret);
};

#endif
