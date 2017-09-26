#ifndef PS_CLIENT_CONN_H
#define PS_CLIENT_CONN_H

#include <string>

#include "pg_conn.h"
#include "pink_thread.h"

class PSWorkerThread;

class PSClientConn : public pink::PGConn {
 public:
  PSClientConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~PSClientConn();

  virtual int DealMessage();
  PSWorkerThread* self_thread() {
    return self_thread_;
  }

 private:

  //client::CmdRequest request_;
  //client::CmdResponse response_;

  PSWorkerThread* self_thread_;

  virtual pink::Status AppendWelcome();
  virtual bool CheckUser(const std::string &user);
  virtual bool CheckPasswd(const std::string &passwd);
  virtual bool Glog(const std::string &msg);
  virtual void StallStatus(std::string &ss) const;
};

#endif
