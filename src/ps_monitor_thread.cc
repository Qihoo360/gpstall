#include "pink_define.h"
#include <glog/logging.h>

#include <string>

#include "monitor.pb.h"
#include "ps_server.h"
#include "ps_monitor_thread.h"

extern PSServer *ps_server;

MonitorConn::MonitorConn(int fd, std::string &ip_port, pink::Thread *thread)
  : PbConn(fd, ip_port) {
  ps_monitor_thread_ = reinterpret_cast<PSMonitorThread *>(thread);
  res_ = dynamic_cast<google::protobuf::Message*>(&service_status_);
  ps_dispatch_thread_ = ps_monitor_thread_->ps_dispatch_thread;
  set_is_reply(false);
}

int MonitorConn::DealMessage() {
  // Receive commmand
  command_.ParseFromArray(rbuf_ + COMMAND_HEADER_LENGTH, header_len_);
  std::string cmd = command_.command();
  if (cmd != "gpstall_info") {
    return -1;
  }
  
  // Build reply info
  uint32_t length = htonl(service_status_.ByteSize());
  memcpy(static_cast<void *>(wbuf_), static_cast<void *>(&length), COMMAND_HEADER_LENGTH);

  GetGpstallStatus();
  GetGploadStatus();

  service_status_.SerializeToArray(wbuf_ + COMMAND_HEADER_LENGTH, PB_MAX_MESSAGE);

  set_is_reply(true);
  return 0;
}

void MonitorConn::GetGpstallStatus() {
  // gpstall status
  service_status_.set_start_time(ps_monitor_thread_->start_time);
  gpstall::ServiceStatus_Status s;
  s = ps_server->is_stall() ? gpstall::ServiceStatus::STALL : gpstall::ServiceStatus::ONLINE;
  service_status_.set_service_status(s);
  service_status_.set_conn_num(ps_dispatch_thread_->ClientNum());
  service_status_.set_qps(ps_dispatch_thread_->Qps());
}

void MonitorConn::GetGploadStatus() {
  // gpload status
  service_status_.set_gpload_failed_num(ps_server->gpload_failed_num);
  service_status_.set_lastest_failed_time(ps_server->latest_failed_time);
  service_status_.set_failed_files_num(ps_server->failed_files_num);
  service_status_.set_failed_files_name(ps_server->failed_files_name);
  service_status_.set_failed_files_size(ps_server->failed_files_size);
  service_status_.set_gpload_longest_timeused(ps_server->gpload_longest_timeused);
  service_status_.set_gpload_latest_timeused(ps_server->gpload_latest_timeused);
  service_status_.set_gpload_average_timeused(ps_server->gpload_average_timeused);
}

PSMonitorThread::PSMonitorThread(int port, PSDispatchThread *thread)
  : pink::HolyThread<MonitorConn>::HolyThread(port, 0),
    ps_dispatch_thread(thread) {
    time_t now;
    time(&now);
    start_time.assign(ctime(&now));
    start_time.resize(start_time.size() - 1);
}
