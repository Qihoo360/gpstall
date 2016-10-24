#ifndef PS_DISPATCH_THREAD_H
#define PS_DISPATCH_THREAD_H

#include "ps_worker_thread.h"
#include "ps_client_conn.h"

#include "dispatch_thread.h"

class PSDispatchThread : public pink::DispatchThread<PSClientConn> {
 public:
  PSDispatchThread(int port, int work_num, PSWorkerThread** worker_thread, int cron_interval);
  virtual ~PSDispatchThread();
  virtual void CronHandle();

  int ClientNum();

 private:
};

#endif
