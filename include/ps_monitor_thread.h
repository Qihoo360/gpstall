#ifndef PS_MONITOR_THREAD_H_
#define PS_MONITOR_THREAD_H_

#include "holy_thread.h"
#include <google/protobuf/message.h>
#include "pb_conn.h"

#include <string>

#include "monitor.pb.h"
#include "ps_dispatch_thread.h"

class PSMonitorThread;

class MonitorConn : public pink::PbConn {
 public:
  MonitorConn(int fd, std::string &ip_port, pink::Thread *thread);
  
  virtual int DealMessage();

 private:
  gpstall::Command command_;
  gpstall::ServiceStatus service_status_;

  PSMonitorThread *ps_monitor_thread_;
  PSDispatchThread *ps_dispatch_thread_;

  void GetGpstallStatus();
  void GetGploadStatus();
};


class PSMonitorThread : public pink::HolyThread<MonitorConn> {
 public:
  PSMonitorThread(int port, PSDispatchThread *ps_dispatch_thread);

  PSDispatchThread *ps_dispatch_thread;
  std::string start_time;
};

#endif  // PS_MONITOR_THREAD_H_
