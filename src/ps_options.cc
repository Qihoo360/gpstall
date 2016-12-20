#include "base_conf.h"

#include <unistd.h>
#include <glog/logging.h>

#include "ps_consts.h"
#include "ps_options.h"

////// PSOptions //////
PSOptions::PSOptions()
  : local_ip("127.0.0.1"),
    local_port(8001),
    monitor_port(18001),
    worker_num(8),
    file_size(kFileSize),
    load_interval(kLoadCronInterval),
    flush_interval(kFlushCronInterval),
    data_path("./data"),
    log_path("./log"),
    minloglevel(0),
    maxlogsize(1800),
    passwd("passwd"),
    daemon_mode(false),
    gp_user("gp_user"),
    gp_host("127.0.0.1"),
    gp_port(15432),
    gpd_host("127.0.0.1"),
    gpd_port(8081),
    error_limit(50000) {
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
    monitor_port(options.monitor_port),
    worker_num(options.worker_num),
    file_size(options.file_size),
    load_interval(options.load_interval),
    flush_interval(options.flush_interval),
    data_path(options.data_path),
    log_path(options.log_path),
    minloglevel(options.minloglevel),
    maxlogsize(options.maxlogsize),
    load_script(options.load_script),
    conf_script(options.conf_script),
    passwd(options.passwd),
    daemon_mode(options.daemon_mode),
    gp_user(options.gp_user),
    gp_host(options.gp_host),
    gp_port(options.gp_port),
    gpd_host(options.gpd_host),
    gpd_port(options.gpd_port),
    error_limit(options.error_limit) {
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
  char* ret = getcwd(cwd, sizeof(cwd));
  if (!ret) {
    LOG(INFO) << " Getcwd failed: " << strerror(errno);
  } else {
    LOG(INFO) << "    Current directory     : " << cwd;
  }
  LOG(INFO) << "    Options.local_ip      : " << local_ip;
  LOG(INFO) << "    Options.local_port    : " << local_port;
  LOG(INFO) << "    Options.monitor_port  : " << monitor_port;
  LOG(INFO) << "    Options.data_path     : " << data_path;
  LOG(INFO) << "    Options.log_path      : " << log_path;
  LOG(INFO) << "    Options.worker_num    : " << worker_num;
  LOG(INFO) << "    Options.file_size     : " << file_size << " Bytes";
  LOG(INFO) << "    Options.load_interval : " << load_interval << " Seconds";
  LOG(INFO) << "    Options.flush_interval: " << flush_interval << " Seconds";
  LOG(INFO) << "    Options.load_script   : " << load_script;
  LOG(INFO) << "    Options.conf_script   : " << conf_script;
  LOG(INFO) << "    Options.daemon_mode   : " << daemon_mode;
  LOG(INFO) << "    Options.gp_user       : " << gp_user;
  LOG(INFO) << "    Options.gp_host       : " << gp_host;
  LOG(INFO) << "    Options.gp_port       : " << gp_port;
  LOG(INFO) << "    Options.gpd_host      : " << gpd_host;
  LOG(INFO) << "    Options.gpd_port      : " << gpd_port;
  LOG(INFO) << "    Options.error_limit   : " << error_limit;
  DLOG(INFO) << "    Options.passwd        : " << passwd;
}

int PSOptions::GetOptionFromFile(const std::string &configuration_file) {
  slash::BaseConf b(configuration_file);
  if (b.LoadConf() != 0)
    return -1;

  // gpstall conf
  b.GetConfStr(LOCAL_IP, &local_ip);
  b.GetConfInt(LOCAL_PORT, &local_port);
  b.GetConfInt(MONITOR_PORT, &monitor_port);
  b.GetConfInt(WORKER_NUM, &worker_num);
  b.GetConfInt(FILE_SIZE, &file_size);
  b.GetConfInt(LOAD_INTERVAL, &load_interval);
  b.GetConfInt(FLUSH_INTERVAL, &flush_interval);
  // TODO timeout
  b.GetConfStr(DATA_PATH, &data_path);
  b.GetConfStr(LOG_PATH, &log_path);
  b.GetConfInt(MINLOGLEVEL, &minloglevel);
  b.GetConfInt(MAXLOGSIZE, &maxlogsize);
  b.GetConfStr(LOAD_SCRIPT, &load_script);
  b.GetConfStr(CONF_SCRIPT, &conf_script);
  b.GetConfBool(DAEMON_MODE, &daemon_mode);

  // greenplum conf
  b.GetConfStr(GP_USER, &gp_user);
  b.GetConfStr(PASSWD, &passwd);
  b.GetConfStr(GP_HOST, &gp_host);
  b.GetConfInt(GP_PORT, &gp_port);

  // gpfdist conf
  b.GetConfStr(GPD_HOST, &gpd_host);
  b.GetConfInt(GPD_PORT, &gpd_port);
  b.GetConfInt(ERROR_LIMIT, &error_limit);
  return 0;
}
