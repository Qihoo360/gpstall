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

  printf ("DealMessage statement: (%s) len=%d\n", statement_.c_str(), statement_.size());

  set_is_reply(true);
  //std::string raw_msg(rbuf_ + cur_pos_ - header_len_ - 4, header_len_ + 4);

  return 0;
}

pink::Status PSClientConn::AppendWelcome() {
  pink::PacketBuf* packet = self_thread_->welcome_msg();

  // TODO maybe add cancel key
  //packet.WriteParameterStatus("is_superuser", "on");

  return AppendObuf(packet->buf, packet->write_pos);
}
