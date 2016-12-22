#include "env.h"
#include <glog/logging.h>

#include <fstream>

#include "ps_server.h"
#include "ps_monitor_thread.h"
#include "ps_worker_thread.h"
#include "ps_dispatch_thread.h"

const int TMP_BUF_SIZE = 256;

PSServer::PSServer(const PSOptions& options)
  : gpload_failed_num(0),
    latest_failed_time(""),
    failed_files_num(0),
    failed_files_name(""),
    failed_files_size(0),
    gpload_longest_timeused(0),
    gpload_latest_timeused(0),
    gpload_average_timeused(0),
    options_(options),
    is_stall_(false),
    gpload_total_timeused_(0),
    gpload_counts_(0),
    should_exit_(false) {

    options_.Dump();
    // Create thread
    worker_num_ = options_.worker_num; 
    for (int i = 0; i < worker_num_; i++) {
      ps_worker_thread_[i] = new PSWorkerThread(kWorkerCronInterval);
    }

    ps_dispatch_thread_ = new PSDispatchThread(options_.local_port, worker_num_, ps_worker_thread_, kDispatchCronInterval);

		ps_monitor_thread_ = new PSMonitorThread(options_.monitor_port, ps_dispatch_thread_);

    DLOG(INFO) << "PSServer cstor";
  }

PSServer::~PSServer() {
  DLOG(INFO) << "~PSServer dstor";

  delete ps_dispatch_thread_;
  for (int i = 0; i < worker_num_; i++) {
    delete ps_worker_thread_[i];
  }
  delete ps_monitor_thread_;

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
  LOG(INFO) << "PSServer started on port:" <<  options_.local_port;

	ps_monitor_thread_->StartThread();
  LOG(INFO) << "PSMonitor started on port:" <<  options_.monitor_port;

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
  const char *cmd_format = "sh %s %s %s %s %s %s %s %d %s \'%s\' %d %d";
  char cmd[2048] = {0};
  sprintf(cmd, cmd_format,
          options_.load_script.c_str(),
          options_.data_path.c_str(),
          options_.log_path.c_str(),
          options_.conf_script.c_str(),
          options_.gp_user.c_str(),
          options_.passwd.c_str(),
          options_.gp_host.c_str(),
          options_.gp_port,
          options_.gpd_host.c_str(),
          options_.gpd_port_range.c_str(),
          options_.error_limit,
          getpid());

  DLOG(INFO) << "Cron Load: " << cmd;

  int ret = 0;
  {
    CalcExecTime t(&gpload_latest_timeused);
    set_is_stall(true);
    ret = system(cmd);
    set_is_stall(false);
  }
  CollectGploadErrInfo(ret);
}

void PSServer::CollectGploadErrInfo(int ret) {
  if (WIFSIGNALED(ret) && (WTERMSIG(ret) == SIGINT || WTERMSIG(ret) == SIGQUIT))
    Exit();
  int load_ret = WEXITSTATUS(ret);
  DLOG(INFO) << "Cron return: " << load_ret;
  DLOG(INFO) << "timeused: " << gpload_latest_timeused;

  if (load_ret != 0) {
    // gpload error occured
    gpload_failed_num += 1;
    time_t now;
    time(&now);
    latest_failed_time.assign(ctime(&now));
    latest_failed_time.resize(latest_failed_time.size() - 1);
  }

  // gpload timeuesd statistics
  // avoid empty gpload timeused
  if (gpload_latest_timeused > 20) {
    gpload_counts_ += 1;
    gpload_total_timeused_ += gpload_latest_timeused;
    gpload_longest_timeused = std::max(gpload_longest_timeused,
                                  gpload_latest_timeused);
    gpload_average_timeused = gpload_total_timeused_ / gpload_counts_;
  }

  // gpload failed files info
  // failed_files_num, failed_file_name, failed_files_size
  failed_files_size = 0;
  failed_files_num = 0;
  std::vector<std::string> files;
  slash::GetDescendant(options_.data_path, files);
  for (auto iter = files.begin(); iter != files.end(); iter++) {
    if (iter->find("failed") != std::string::npos) {
      failed_files_num += 1;
      std::ifstream f(*iter, std::ios::binary | std::ios::ate);
      failed_files_size += static_cast<uint64_t>(f.tellg());
    }
  }

  char line[TMP_BUF_SIZE] = {0};
  std::string latest_failed_file = options_.log_path + "/latest_failed_file";
  failed_files_name.clear();
  if (slash::FileExists(latest_failed_file)) {
    slash::SequentialFile *sequential_file;
    slash::NewSequentialFile(latest_failed_file, &sequential_file);

    char *ret = NULL;
    do {
      memset(line, 0, TMP_BUF_SIZE);
      ret = sequential_file->ReadLine(line, TMP_BUF_SIZE);
      failed_files_name.append(line);
    } while (ret != NULL);

    delete sequential_file;
  }
}
