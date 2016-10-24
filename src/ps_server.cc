#include "ps_server.h"

#include <glog/logging.h>

#include "ps_worker_thread.h"
#include "ps_dispatch_thread.h"

PSServer::PSServer(const PSOptions& options)
  : options_(options),
  should_exit_(false) {

    // Create thread
    worker_num_ = 2; 
    for (int i = 0; i < worker_num_; i++) {
      ps_worker_thread_[i] = new PSWorkerThread(kWorkerCronInterval);
    }

    ps_dispatch_thread_ = new PSDispatchThread(options_.local_port, worker_num_, ps_worker_thread_, kDispatchCronInterval);
    // TODO rm
    //LOG(INFO) << "local_host " << options_.local_ip << ":" << options.local_port;
    DLOG(INFO) << "PSServer cstor";
  }

PSServer::~PSServer() {
  DLOG(INFO) << "~PSServer dstor";

  delete ps_dispatch_thread_;
  for (int i = 0; i < worker_num_; i++) {
    delete ps_worker_thread_[i];
  }

  LOG(INFO) << "PSServerThread " << pthread_self() << " exit!!!";
}

Status PSServer::Start() {
  ps_dispatch_thread_->StartThread();

  // TEST 
  LOG(INFO) << "PSServer started on port:" <<  options_.local_port;

  while (!should_exit_) {
    //DoTimingTask();
    sleep(3);
  }
  return Status::OK();
}

//void PSServer::DoTimingTask() {
//}
