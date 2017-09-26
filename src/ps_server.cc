#include "env.h"
#include <glog/logging.h>

#include <fstream>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <sys/select.h>
#include <sys/wait.h>

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
    worker_num_ = (options_.worker_num < kMaxWorkerThread ? options_.worker_num : kMaxWorkerThread); 
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
      if (header != it->second->Header()) {
        //when the insert sentence changed the header, the log should be flushed
        //resetHeader && it->second->Flush();
        bool header_changed = true;
        uint32_t filenum;
        uint64_t offset;
        it->second->GetProducerStatus(&filenum, &offset);
        it->second->SetHeader(header);
        if (it->second->Flush(header_changed).ok()) {
          LOG(INFO) << " header had changed, " << " FlushLog " << it->second->filename << filenum << " at offset " << offset;
        }
        DoTimingTask();
      }
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
    bool header_changed = false;
    uint32_t filenum;
    uint64_t offset;
    log->GetProducerStatus(&filenum, &offset);
    //LOG(INFO) << " FlushLog " << log->filename << filenum << " at offset " << offset;
    if (log->Flush(header_changed).ok()) {
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

  DLOG(INFO) << "Cron Load: " << cmd << std::endl;

  int ret = 0;
  {
    CalcExecTime t(&gpload_latest_timeused);
    set_is_stall(true);
    ret = system(cmd);
    /*
    std::string cmd_string(cmd);
    long timeout_ms = options_.timeout;
    ret = ExecuteScript(cmd_string, timeout_ms);
    */
    set_is_stall(false);
  }
  CollectGploadErrInfo(ret);
}

void PSServer::CollectGploadErrInfo(int ret) {
   
  if (WIFSIGNALED(ret) && (WTERMSIG(ret) == SIGINT || WTERMSIG(ret) == SIGQUIT))
    Exit();
  
  int load_ret = WEXITSTATUS(ret);
  

  //int load_ret = ret;
  DLOG(INFO) << "Cron return: " << load_ret;
  DLOG(INFO) << "timeused: " << gpload_latest_timeused << std::endl;
  
  if (load_ret != 0) {
    // gpload error occured
    gpload_failed_num += 1;
    time_t now = time(NULL);
    char buf[128] = {0};
    strftime(buf, 128, "%Y%m%d%H%M%S", localtime(&now));
    latest_failed_time.assign(buf);
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
  failed_files_num = 0;
  std::vector<std::string> files;
  slash::GetDescendant(options_.data_path, files);
  for (auto iter = files.begin(); iter != files.end(); iter++) {
    if (iter->find("failed") != std::string::npos) {
      failed_files_num += 1;
    }
  }

  failed_files_size = 0;
  char line[TMP_BUF_SIZE] = {0};
  std::string latest_failed_file = options_.log_path + "/latest_failed_file";
  failed_files_name.clear();
  if (slash::FileExists(latest_failed_file)) {
    slash::SequentialFile *sequential_file;
    slash::NewSequentialFile(latest_failed_file, &sequential_file);

    char *ret_p = NULL;
    do {
      memset(line, 0, TMP_BUF_SIZE);
      ret_p = sequential_file->ReadLine(line, TMP_BUF_SIZE);
      std::string fname(line);
      if (!fname.empty()) {
        failed_files_name.append(fname);
        std::ifstream f(fname.substr(0, fname.size() - 1), std::ios::binary | std::ios::ate);
        failed_files_size += static_cast<uint64_t>(f.tellg());
      }
    } while (ret_p != NULL);

    for (int i = 0; i < failed_files_name.size(); i++) {
      if (i == failed_files_name.size() - 1)
        failed_files_name[i] = 0;
      else if (failed_files_name[i] == '\n')
        failed_files_name[i] = ',';
    }

    DLOG(INFO) << "file size: " << failed_files_size << std::endl;

    delete sequential_file;
  }
}

int PSServer::ExecuteScript(const std::string &script, const long timeout_ms) {
  if (script.empty()) {
    return -1;
  }
  
  int pfd[2];
  int result;
  pid_t pid;
  fd_set read_set;
  struct timeval timeout;
  timeout.tv_sec = timeout_ms / 1000;
  timeout.tv_usec = timeout_ms % 1000 * 1000;

  struct sigaction act,oact;
  sigemptyset(&act.sa_mask);
  act.sa_handler = PSServer::SigChild;
  act.sa_flags = SA_RESTART;
  sigaction(SIGCHLD, &act, &oact);

  if (pipe(pfd) < 0) {
    LOG(ERROR) << "Failed to create pipe! error: " << errno << std::endl;
    return -1;
  }

  if ((pid=fork()) < 0) {
    LOG(ERROR) << "Failed to fork process! error: " << errno << std::endl;
    return -1;
  }

  else if ( pid == 0) {
    setpgrp();
    close(pfd[0]);
    execl("/bin/sh", "sh", "-c", script.c_str(), (char *)NULL);
    _exit(127);
  }

  close(pfd[1]);
  do {
    FD_ZERO(&read_set);
    FD_SET(pfd[0], &read_set);
    result = select(pfd[0] + 1, &read_set, NULL, NULL, &timeout);
    if (result == -1) {
      if (errno ==EINTR) {
        continue;
      }
      else {
        LOG(ERROR) << "Failed to select from pipe, error: " << errno << std::endl;
        break;
      }
    }
    else if (result == 0) {
      LOG(WARNING) << "Script execute timeout! script: " << script.c_str() << std::endl;
      break;
    }
    /*script execute successfully**/
    else {
      LOG(INFO) << script << "execute successfully!" << std::endl;
      close(pfd[0]);
      return 0;
    }
  
  } while(true);

  close(pfd[0]);

  LOG(ERROR) << "kill start!" << std::endl;
  kill(-pid, SIGKILL);
  LOG(ERROR) << "kill end!" << std::endl;

  /*execute timeout!*/
  if (result == 0 ) {
    LOG(ERROR) << script << "script execute timeout!" << std::endl;
    return -2;
  }

  return -3;

}

void PSServer::SigChild(int sig) {
  pid_t pid;
  int status;
  while ((pid = waitpid(-1, &status, WNOHANG)) >0) {
  /*
    if (WIFSIGNALED(status) && (WTERMSIG(status) == SIGINT || WTERMSIG(status) == SIGQUIT))
      Exit();
  */
    if (WIFEXITED(status)){
      if (WEXITSTATUS(status)) {
        LOG(ERROR) << "Script not exit with success, ret: " << WEXITSTATUS(status) << std::endl;
      }
    }
      else {
        LOG(ERROR) << "Child process timeout, pid:" << pid << std::endl;
      }
  } 
}
