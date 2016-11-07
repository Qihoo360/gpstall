#include "ps_client_conn.h"

#include <vector>
#include <algorithm>
#include <glog/logging.h>

#include "ps_worker_thread.h"
#include "ps_server.h"

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

pink::Status PSClientConn::AppendWelcome() {
  pink::PacketBuf* packet = self_thread_->welcome_msg();

  // TODO maybe add cancel key
  //packet.WriteParameterStatus("is_superuser", "on");

  return AppendObuf(packet->buf, packet->write_pos);
}

// TODO use passwd from conf file
bool PSClientConn::Login() {
  DLOG(INFO) << "passwd_=" << passwd_;
  return passwd_.compare("abc") == 0;
}
