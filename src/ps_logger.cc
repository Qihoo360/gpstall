#include "ps_logger.h"

#include <iostream>
#include <string>
#include <glog/logging.h>

#include "ps_consts.h"
#include "slash_mutex.h"

using slash::RWLock;

std::string NewFileName(const std::string name, const uint32_t current) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s%u", name.c_str(), current);
  return std::string(buf);
}

/*
 * Version
 */
Version::Version(slash::RWFile *save)
  : pro_offset_(0),
    pro_num_(0),
    save_(save) {
  assert(save_ != NULL);
  pthread_rwlock_init(&rwlock_, NULL);
}

Version::~Version() {
  StableSave();
  pthread_rwlock_destroy(&rwlock_);
}

Status Version::StableSave() {
  char *p = save_->GetData();
  memcpy(p, &pro_num_, sizeof(uint32_t));
  p += sizeof(uint32_t);
  memcpy(p, &pro_offset_, sizeof(uint64_t));

  return Status::OK();
}

Status Version::Init() {
  Status s;
  if (save_->GetData() != NULL) {
    memcpy((char*)(&pro_num_), save_->GetData(), sizeof(uint32_t));
    memcpy((char*)(&pro_offset_), save_->GetData() + 4, sizeof(uint64_t));
    // DLOG(INFO) << "Version Init pro_offset "<< pro_offset_ << " itemnum " << item_num << " pro_num " << pro_num_ << " con_num " << con_num_;
    return Status::OK();
  } else {
    return Status::Corruption("version init error");
  }
}

/*
 * Logger
 */
Logger::Logger(const std::string& log_path, const int file_size, const std::string &header)
  : header_(header),
    version_(NULL),
    queue_(NULL),
    versionfile_(NULL),
    pro_num_(0),
    log_path_(log_path),
    file_size_(file_size) {

  // To intergrate with old version, we don't set mmap file size to 100M;
  //slash::SetMmapBoundSize(file_size);
  //slash::kMmapBoundSize = 1024 * 1024 * 100;

  DLOG(INFO) << "Logger file_size=" << file_size_;
  Status s;

  if (log_path_.back() != '/') {
    log_path_.append(1, '/');
  }

  slash::CreatePath(log_path_);

  filename = log_path_;
  //filename = log_path_ + kLoggerPrefix;
  const std::string manifest = log_path_ + kManifest;
  std::string profile;

  if (!slash::FileExists(manifest)) {
    DLOG(INFO) << "Logger: Manifest file not exist, we create a new one.";

    profile = NewFileName(filename, pro_num_);
    s = slash::NewWritableFile(profile, &queue_);
    if (!s.ok()) {
      LOG(INFO) << "Logger: new " << filename << " " << s.ToString();
    }

    s = slash::NewRWFile(manifest, &versionfile_);
    if (!s.ok()) {
      LOG(WARNING) << "Logger: new versionfile error " << s.ToString();
    }

    version_ = new Version(versionfile_);
    version_->StableSave();

    if (!header_.empty()) {
      Put(header_);
    }
    empty_file_ = true;
  } else {
    DLOG(INFO) << "Logger: Find the exist file.";

    s = slash::NewRWFile(manifest, &versionfile_);
    if (s.ok()) {
      version_ = new Version(versionfile_);
      version_->Init();
      pro_num_ = version_->pro_num_;

      // Debug
      //version_->debug();
    } else {
      LOG(WARNING) << "Logger: open versionfile error";
    }

    profile = NewFileName(filename, pro_num_);
    DLOG(INFO) << "Logger: open profile " << profile;
    slash::AppendWritableFile(profile, &queue_, version_->pro_offset_);
    
    uint64_t filesize = queue_->Filesize();
    empty_file_ = filesize > 0 ? false : true;
  }
  //gettimeofday(&last_action_, NULL);
}

Logger::~Logger() {
  delete version_;
  delete versionfile_;

  delete queue_;
}

Status Logger::GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset) {
  slash::RWLock(&(version_->rwlock_), false);

  *filenum = version_->pro_num_;
  *pro_offset = version_->pro_offset_;

  return Status::OK();
}

// Note: mutex lock should be held
Status Logger::Put(const std::string &item) {
  Status s;

  /* Check to roll log file */
  uint64_t filesize = queue_->Filesize();
  if (filesize > file_size_) {
    delete queue_;
    queue_ = NULL;
    std::string old_filename = NewFileName(filename, pro_num_);
    slash::RenameFile(old_filename, old_filename + kLoggerSuffix);

    pro_num_++;
    std::string profile = NewFileName(filename, pro_num_);
    slash::NewWritableFile(profile, &queue_);

    {
      slash::RWLock(&(version_->rwlock_), true);
      version_->pro_num_ = pro_num_;
      version_->pro_offset_ = 0;

      if (!header_.empty()) {
        s = queue_->Append(header_);
        if (s.ok()) {
          version_->pro_offset_ += header_.size();
        }
        empty_file_ = true;
      }

      version_->StableSave();
    }
  }

  s = queue_->Append(item);
  if (s.ok()) {
    slash::RWLock(&(version_->rwlock_), true);
    version_->pro_offset_ += item.size();
    version_->StableSave();
  }
  empty_file_ = false;

  //gettimeofday(&last_action_, NULL);
  return s;
}

Status Logger::Flush() {
  Status s;

  if (!empty_file_) {
    delete queue_;
    queue_ = NULL;
    std::string old_filename = NewFileName(filename, pro_num_);
    slash::RenameFile(old_filename, old_filename + kLoggerSuffix);

    pro_num_++;
    std::string profile = NewFileName(filename, pro_num_);
    slash::NewWritableFile(profile, &queue_);

    {
      slash::RWLock(&(version_->rwlock_), true);
      version_->pro_num_ = pro_num_;
      version_->pro_offset_ = 0;

      if (!header_.empty()) {
        s = queue_->Append(header_);
        if (s.ok()) {
          version_->pro_offset_ += header_.size();
        }
        empty_file_ = true;
      }

      version_->StableSave();
    }
    //gettimeofday(&last_action_, NULL);
    return Status::OK();
  } else {
    return Status::NotFound("");
  }
}
