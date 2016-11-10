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

static int GCD(int a, int b) {
  return b == 0 ? a : GCD(b, a % b);
}

Status PSServer::Start() {
  ps_dispatch_thread_->StartThread();

  // TEST 
  LOG(INFO) << "PSServer started on port:" <<  options_.local_port;

  int gcd = GCD(options_.load_interval, options_.flush_interval);
  uint64_t common_check_interval = (uint64_t)options_.load_interval / gcd * options_.flush_interval;
  while (!should_exit_) {
    uint64_t try_num = 0;
    while (!should_exit_ && ++try_num <= common_check_interval) {
      //DLOG(INFO) << "should_exit_ " << should_exit_;
      sleep(1);
      if (try_num % options_.flush_interval == 0) {
        MaybeFlushLog();
      }
      if (try_num % options_.load_interval == 0) {
        DoTimingTask();
      }
    }
  }
  return Status::OK();
}

// Mutex should be held
Logger* PSServer::GetLogger(const std::string &database, const std::string &table, const std::string &header) {
  std::string key = database + "/" + table; 

  std::unordered_map<std::string, Logger *>::const_iterator it = files_.find(key);
  if (it == files_.end()) {
    Logger *log = new Logger(options_.data_path + key, options_.file_size, header);
    files_[key] = log;
    return log;
  } else {
    return it->second;
  }
}

void PSServer::MaybeFlushLog() {
  struct timeval now;
  gettimeofday(&now, NULL);

  slash::MutexLock l(&mutex_files_); 
  DLOG(INFO) << "MaybeFlushLog start";
  std::unordered_map<std::string, Logger *>::iterator it = files_.begin();
  for (; it != files_.end(); it++) {
    Logger* log = it->second;
    uint32_t filenum;
    uint64_t offset;
    log->GetProducerStatus(&filenum, &offset);
    //LOG(INFO) << " FlushLog " << log->filename << filenum << " at offset " << offset;
    if (log->Flush().ok()) {
      LOG(INFO) << " FlushLog " << log->filename << filenum << " at offset " << offset;
    }
  }
  DLOG(INFO) << "MaybeFlushLog end ";
}

void PSServer::DoTimingTask() {
  // std::string cmd = "flock -xn /tmp/gpstall.lock -c \"sh " + options_.load_script + " " + options_.data_path + " " + options_.conf_script + "\"";
  const char *cmd_format = "flock -xn /tmp/gpstall.lock -c \"sh %s %s %s %s %s %s %s %d %s %d %d\"";
  char cmd[2048] = {0};
  sprintf(cmd, cmd_format, options_.load_script.c_str(), options_.data_path.c_str(), options_.log_path.c_str(),
                          options_.conf_script.c_str(),
                          options_.gp_user.c_str(), options_.passwd.c_str(), options_.gp_host.c_str(), options_.gp_port,
                          options_.gpd_host.c_str(), options_.gpd_port, options_.error_limit);

  DLOG(INFO) << "Cron Load: " << cmd;

  int ret = system(cmd);
  DLOG(INFO) << "Cron return: " << ret;
  if (WIFSIGNALED(ret) && (WTERMSIG(ret) == SIGINT || WTERMSIG(ret) == SIGQUIT)) {
    Exit();
  }
}
