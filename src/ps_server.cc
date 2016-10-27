#include "ps_server.h"

#include <glog/logging.h>

#include "ps_worker_thread.h"
#include "ps_dispatch_thread.h"

PSServer::PSServer(const PSOptions& options)
  : options_(options),
  should_exit_(false) {

    options_.Dump();
    // Create thread
    worker_num_ = options_.worker_num; 
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

  //slash::MutexLock l(&mutex_files_); 
  std::unordered_map<std::string, Logger *>::iterator it = files_.begin();
  for (; it != files_.end(); it++) {
    delete it->second;
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

// Mutex should be held
Logger* PSServer::GetLogger(const std::string &database, const std::string &table) {
  std::string key = database + "/" + table; 

  std::unordered_map<std::string, Logger *>::const_iterator it = files_.find(key);
  if (it == files_.end()) {
    Logger *log = new Logger(options_.data_path + key, options_.file_size);
    files_[key] = log;
    return log;
  } else {
    return it->second;
  }
}

//void PSServer::DoTimingTask() {
//}
