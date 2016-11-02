#ifndef PS_LOGGER_H
#define PS_LOGGER_H

#include <cstdio>
#include <list>
#include <string>
#include <deque>
#include <pthread.h>
#include <sys/time.h>

#ifndef __STDC_FORMAT_MACROS
# define __STDC_FORMAT_MACROS
# include <inttypes.h>
#endif 

#include "env.h"
#include "slash_status.h"
#include "slash_mutex.h"

using slash::Status;
using slash::Slice;

std::string NewFileName(const std::string name, const uint32_t current);

class Version;

class Logger {
 public:
  Logger(const std::string& log_path, const int file_size = 100 * 1024 * 1024, const std::string& header = "");
  ~Logger();

  void Lock()         { mutex_.Lock(); }
  void Unlock()       { mutex_.Unlock(); }

  Status Put(const std::string& item);
  Status Flush();

  Status GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset);

  //Status SetProducerStatus(uint32_t filenum, uint64_t pro_offset);

  slash::WritableFile *queue() { return queue_; }


  uint64_t file_size() {
    return file_size_;
  }

  std::string filename;
  struct timeval last_action_;

 private:

  std::string header_;

  Version* version_;
  slash::WritableFile *queue_;
  slash::RWFile *versionfile_;

  slash::Mutex mutex_;

  uint32_t pro_num_;
  std::string log_path_;

  uint64_t file_size_;

  bool empty_file_;

  // Not use
  //int32_t retry_;

  // No copying allowed
  Logger(const Logger&);
  void operator=(const Logger&);
};

// We have to reserve the useless con_offset_, con_num_ and item_num,
// to be compatable with version 1.x .
class Version {
 public:
  Version(slash::RWFile *save);
  ~Version();

  Status Init();

  // RWLock should be held when access members.
  Status StableSave();

  uint64_t pro_offset_;
  uint32_t pro_num_;
  pthread_rwlock_t rwlock_;

  void debug() {
    slash::RWLock(&rwlock_, false);
    printf ("Current pro_num %u\n", pro_num_);
  }

 private:

  slash::RWFile *save_;

  // No copying allowed;
  Version(const Version&);
  void operator=(const Version&);
};

#endif
