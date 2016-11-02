#include "ps_worker_thread.h"

#include <glog/logging.h>

PSWorkerThread::PSWorkerThread(int cron_interval)
  : WorkerThread::WorkerThread(cron_interval),
    thread_querynum_(0),
    last_thread_querynum_(0),
    last_time_us_(slash::NowMicros()),
    last_sec_thread_querynum_(0) {
      welcome_msg_ = pink::NewWelcomeMsg();
    }

PSWorkerThread::~PSWorkerThread() {
  should_exit_ = true;
  pthread_join(thread_id(), NULL);

  delete welcome_msg_; 
  LOG(INFO) << "A worker thread " << thread_id() << " exit!!!";
}

void PSWorkerThread::CronHandle() {
  ResetLastSecQuerynum();

  {
    struct timeval now;
    gettimeofday(&now, NULL);
    slash::RWLock l(&rwlock_, false); // Use ReadLock to iterate the conns_
    std::map<int, void*>::iterator iter = conns_.begin();

    while (iter != conns_.end()) {

      // TODO simple 300s
      if (now.tv_sec - static_cast<PSClientConn*>(iter->second)->last_interaction().tv_sec > 300) {
        LOG(INFO) << "Find Timeout Client: " << static_cast<PSClientConn*>(iter->second)->ip_port();
        AddCronTask(WorkerCronTask{TASK_KILL, static_cast<PSClientConn*>(iter->second)->ip_port()});
      }
      iter++;
    }
  }

  {
    slash::MutexLock l(&mutex_);
    while (!cron_tasks_.empty()) {
      WorkerCronTask t = cron_tasks_.front();
      cron_tasks_.pop();
      mutex_.Unlock();
      LOG(INFO) << "PSWorkerThread, Got a WorkerCronTask";
      switch (t.task) {
        case TASK_KILL:
          ClientKill(t.ip_port);
          break;
        case TASK_KILLALL:
          ClientKillAll();
          break;
      }
      mutex_.Lock();
    }
  }
}

bool PSWorkerThread::ThreadClientKill(std::string ip_port) {

  if (ip_port == "") {
    AddCronTask(WorkerCronTask{TASK_KILLALL, ""});
  } else {
    if (!FindClient(ip_port)) {
      return false;
    }
    AddCronTask(WorkerCronTask{TASK_KILL, ip_port});
  }
  return true;
}

int PSWorkerThread::ThreadClientNum() {
  slash::RWLock l(&rwlock_, false);
  return conns_.size();
}

void PSWorkerThread::AddCronTask(WorkerCronTask task) {
  slash::MutexLock l(&mutex_);
  cron_tasks_.push(task);
}

bool PSWorkerThread::FindClient(std::string ip_port) {
  slash::RWLock l(&rwlock_, false);
  std::map<int, void*>::iterator iter;
  for (iter = conns_.begin(); iter != conns_.end(); iter++) {
    if (static_cast<PSClientConn*>(iter->second)->ip_port() == ip_port) {
      return true;
    }
  }
  return false;
}

void PSWorkerThread::ClientKill(std::string ip_port) {
  slash::RWLock l(&rwlock_, true);
  std::map<int, void*>::iterator iter;
  for (iter = conns_.begin(); iter != conns_.end(); iter++) {
    if (static_cast<PSClientConn*>(iter->second)->ip_port() != ip_port) {
      continue;
    }
    LOG(INFO) << "==========Kill Client==============";
    close(iter->first);
    delete(static_cast<PSClientConn*>(iter->second));
    conns_.erase(iter);
    break;
  }
}

void PSWorkerThread::ClientKillAll() {
  slash::RWLock l(&rwlock_, true);
  std::map<int, void*>::iterator iter = conns_.begin();
  while (iter != conns_.end()) {
    LOG(INFO) << "==========Kill Client==============";
    close(iter->first);
    delete(static_cast<PSClientConn*>(iter->second));
    iter = conns_.erase(iter);
  }
}
