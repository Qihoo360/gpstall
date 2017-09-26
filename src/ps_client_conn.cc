#include "ps_client_conn.h"

#include <vector>
#include <algorithm>
#include <glog/logging.h>
#include <cstdint>

#include "ps_worker_thread.h"
#include "ps_server.h"
#include "ps_monitor_thread.h"
#include "ps_dispatch_thread.h"
extern PSServer* ps_server;

////// PSClientConn //////
PSClientConn::PSClientConn(int fd, std::string ip_port, pink::Thread* thread) :
  PGConn(fd, ip_port) {
  self_thread_ = dynamic_cast<PSWorkerThread*>(thread);
}

PSClientConn::~PSClientConn() {
}

// Msg is  [ length (int32) | pb_msg (length bytes) ]
int PSClientConn::DealMessage() {
  self_thread_->PlusThreadQuerynum();

  DLOG(INFO) << "DealMessage statement=(" << statement_ << ") dbname=("
      << dbname_ << ") table=(" << parser_.table_ << ") rows: size="
      << parser_.rows_.size() << std::endl;

  {
    slash::MutexLock l(&(ps_server->mutex_files_));
    Logger* log = ps_server->GetLogger(dbname_, parser_.table_, parser_.header_);
    for (size_t i = 0; i < parser_.rows_.size(); i++) {
      log->Put(parser_.rows_[i] + "\n");
    }
  }

  // we simple return ok
  set_is_reply(true);

  return 0;
}

void PSClientConn::StallStatus(std::string  &ss) const {
  ss.clear();
  PSDispatchThread* psd = ps_server->DispatchThread();
  PSMonitorThread* psm = ps_server->MonitorThread();

  char inf[10240];
  std::string status = "STALL";
  if (!ps_server->is_stall()) {
    status = "ONLINE";
  }

  snprintf(inf, 10240, "  start_time: %s\n"
                       "  service_status: %s\n"  
                       "  conn_num: %u\n"
                       "  QPS: %lu\n"
                       "  gpload_failed_num: %u\n"
                       "  latest_failed_time: %s\n"
                       "  failed_files_num: %u\n"
                       "  failed_files_size: %lu\n"
                       "  gpload_longest_timeused: %lu\n"
                       "  gpload_latest_timeused: %lu\n"
                       "  gpload_average_timeused: %lu\n",
                        psm->start_time.data(),
                        status.data(),
                        psd->ClientNum(),
                        psd->Qps(),

                        ps_server->gpload_failed_num,
                        ps_server->latest_failed_time.data(),
                        ps_server->failed_files_num,
                        ps_server->failed_files_size,
                        ps_server->gpload_longest_timeused,
                        ps_server->gpload_latest_timeused,
                        ps_server->gpload_average_timeused);

 ss = inf;
 DLOG(INFO) << ss <<std::endl;
}

pink::Status PSClientConn::AppendWelcome() {
  pink::PacketBuf* packet = self_thread_->welcome_msg();

  // TODO maybe add cancel key
  //packet.WriteParameterStatus("is_superuser", "on");

  return AppendObuf(packet->buf, packet->write_pos);
}

// TODO use passwd from conf file
bool PSClientConn::CheckUser(const std::string &user) {
  DLOG(INFO) << "Check User user=" << user;
  return user == ps_server->gp_user();
}
bool PSClientConn::CheckPasswd(const std::string &passwd) {
  DLOG(INFO) << "Check Passwd passwd=" << passwd_;
  return passwd == ps_server->passwd();
}

bool PSClientConn::Glog(const std::string &msg) {
  LOG(ERROR) << msg;
  return true;
}
