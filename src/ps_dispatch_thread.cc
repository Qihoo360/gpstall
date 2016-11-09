#include "ps_dispatch_thread.h"

#include <glog/logging.h>

#include "ps_client_conn.h"
#include "ps_server.h"

extern PSServer* ps_server;

PSDispatchThread::PSDispatchThread(int port, int work_num, PSWorkerThread** worker_thread, int cron_interval)
  : DispatchThread::DispatchThread(port, work_num,
                                   reinterpret_cast<pink::WorkerThread<PSClientConn>**>(worker_thread),
                                   cron_interval) {
}

PSDispatchThread::~PSDispatchThread() {
  LOG(INFO) << "Dispatch thread " << thread_id() << " exit!!!";
}

void PSDispatchThread::CronHandle() {
  uint64_t server_querynum = 0;
  uint64_t server_current_qps = 0;
  for (int i = 0; i < work_num(); i++) {
    slash::RWLock(&(((PSWorkerThread**)worker_thread())[i]->rwlock_), false);
    server_querynum += ((PSWorkerThread**)worker_thread())[i]->thread_querynum();
    server_current_qps += ((PSWorkerThread**)worker_thread())[i]->last_sec_thread_querynum();
  }

  LOG(INFO) << "ClientNum: " << ClientNum() << " ClientQueryNum: " << server_querynum
      << " ServerCurrentQps: " << server_current_qps;
}

int PSDispatchThread::ClientNum() {
  int num = 0;
  for (int i = 0; i < work_num(); i++) {
    num += ((PSWorkerThread**)worker_thread())[i]->ThreadClientNum();
  }
  return num;
}
