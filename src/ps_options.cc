#include "ps_options.h"

#include <unistd.h>
#include <glog/logging.h>

#include "ps_consts.h"

////// PSOptions //////
PSOptions::PSOptions()
  : local_ip("127.0.0.1"),
    local_port(8001),
    worker_num(8),
    file_size(kFileSize),
    load_interval(kLoadCronInterval),
    data_path("./data"),
    log_path("./log") {
  if (data_path.back() != '/') {
    data_path.append(1, '/');
  }
  if (log_path.back() != '/') {
    log_path.append(1, '/');
  }

  char path[1024];
  char dest[1024];
  pid_t pid = getpid();
  sprintf(path, "/proc/%d/exe", pid);
  if (readlink(path, dest, 1024) == -1) {
    LOG(ERROR) << "Readlink error (" << strerror(errno) << "), path is " << path;
  }
  conf_script = dest;
  size_t pos = conf_script.find_last_of('/');
  if (pos != std::string::npos) {
    std::string bin_path = conf_script.substr(0, pos);
    conf_script = bin_path + "/gpload.yaml.ori";
    load_script = bin_path + "/load.sh";
    //conf_script.replace(conf_script.begin() + pos + 1, conf_script.end(), "gpload.yaml.ori");
  }
}


PSOptions::PSOptions(const PSOptions& options)
  : local_ip(options.local_ip),
    local_port(options.local_port),
    worker_num(options.worker_num),
    file_size(options.file_size),
    load_interval(options.load_interval),
    data_path(options.data_path),
    log_path(options.log_path),
    load_script(options.load_script),
    conf_script(options.conf_script) {
  if (data_path.back() != '/') {
    data_path.append(1, '/');
  }
  if (log_path.back() != '/') {
    log_path.append(1, '/');
  }
  if (worker_num > 64 || worker_num < 1) {
    worker_num = 8;
  }
  if (file_size > (1<<30) || file_size < 64) {
    file_size = kFileSize;
  }
}

void PSOptions::Dump() {
  //fprintf (stderr, "    Options.local_ip    : %s\n", local_ip.c_str());
  //fprintf (stderr, "    Options.local_port  : %d\n", local_port);
  //fprintf (stderr, "    Options.data_path   : %s\n", data_path.c_str());
  //fprintf (stderr, "    Options.log_path    : %s\n", log_path.c_str());
  //fprintf (stderr, "    Options.worker_num  : %d\n", worker_num);
  //fprintf (stderr, "    Options.file_size   : %d Bytes\n", file_size);
  //fprintf (stderr, "    Options.load_script : %s\n", log_path.c_str());

  char cwd[1024];
  getcwd(cwd, sizeof(cwd));

  LOG(INFO) << "    Current directory     : " << cwd;
  LOG(INFO) << "    Options.local_ip      : " << local_ip;
  LOG(INFO) << "    Options.local_port    : " << local_port;
  LOG(INFO) << "    Options.data_path     : " << data_path;
  LOG(INFO) << "    Options.log_path      : " << log_path;
  LOG(INFO) << "    Options.worker_num    : " << worker_num;
  LOG(INFO) << "    Options.file_size     : " << file_size << " Bytes";
  LOG(INFO) << "    Options.load_interval : " << load_interval << " Seconds";
  LOG(INFO) << "    Options.load_script   : " << load_script;
  LOG(INFO) << "    Options.conf_script   : " << conf_script;
}
