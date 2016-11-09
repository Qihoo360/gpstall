#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <iostream>
#include <sstream>
#include <glog/logging.h>

#include "ps_server.h"
#include "ps_options.h"

#include "base_conf.h"
#include "env.h"

PSServer* ps_server;

void Usage();
void ParseArgs(int argc, char* argv[], PSOptions& options);

static void GlogInit(const PSOptions& options) {
  if (!slash::FileExists(options.log_path)) {
    slash::CreatePath(options.log_path); 
  }

  FLAGS_alsologtostderr = true;

  FLAGS_log_dir = options.log_path;
  FLAGS_minloglevel = options.minloglevel;
  FLAGS_max_log_size = options.maxlogsize;
  // TODO rm
  FLAGS_logbufsecs = 0;

  ::google::InitGoogleLogging("ps");
}

static void IntSigHandle(const int sig) {
  LOG(INFO) << "Catch Signal " << sig << ", cleanup...";
  //ps_data_server->server_mutex_.Unlock();
  ps_server->Exit();
}

static void PSSignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

static void close_std() {
  int fd;
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
      dup2(fd, STDIN_FILENO);
      dup2(fd, STDOUT_FILENO);
      dup2(fd, STDERR_FILENO);
      close(fd);
    }
}

static int daemonize() {
  pid_t pid;
  umask(0);

  if ((pid = fork()) < 0) {
    LOG(FATAL) << "can't fork" << std::endl;
    return -1;
  } else if (pid != 0)
    exit(0);
  setsid();

  close_std();

  // create pid file
  FILE *fp = fopen(kPidFile.c_str(), "w");
  if (fp) {
    fprintf(fp, "%d\n", (int)getpid());
    fclose(fp);
  } else {
    LOG(FATAL) << "can't create pid file" << std::endl;
    return -1;
  }

  return 0;
}

int main(int argc, char** argv) {
  PSOptions options;

  ParseArgs(argc, argv, options);

  GlogInit(options);

  if (options.daemon_mode) {
    std::cout << "Running in daemon mode." << std::endl;
    if (daemonize() != 0)
      return -1;
  }
  PSSignalSetup();

  ps_server = new PSServer(options);
  ps_server->Start();

  //printf ("Exit\n");
  delete ps_server;

  ::google::ShutdownGoogleLogging();
  return 0;
}

void Usage() {
  printf ("Usage:\n"
          "  ./gpstall [-h] [-c conf/file]\n"
          "    -h            -- show this help\n"
          "    -c conf/file  -- config file\n"
          );
}

int GetOptionFromFile(const std::string &configuration_file, PSOptions& options) {
  slash::BaseConf b(configuration_file);
  if (b.LoadConf() != 0)
    return -1;

  // gpstall conf
  b.GetConfStr(LOCAL_IP, &options.local_ip);
  b.GetConfInt(LOCAL_PORT, &options.local_port);
  b.GetConfInt(WORKER_NUM, &options.worker_num);
  b.GetConfInt(FILE_SIZE, &options.file_size);
  b.GetConfInt(LOAD_INTERVAL, &options.load_interval);
  b.GetConfInt(FLUSH_INTERVAL, &options.flush_interval);
  // TODO timeout
  b.GetConfStr(DATA_PATH, &options.data_path);
  b.GetConfStr(LOG_PATH, &options.log_path);
  b.GetConfInt(MINLOGLEVEL, &options.minloglevel);
  b.GetConfInt(MAXLOGSIZE, &options.maxlogsize);
  b.GetConfStr(LOAD_SCRIPT, &options.load_script);
  b.GetConfStr(CONF_SCRIPT, &options.conf_script);
  b.GetConfBool(DAEMON_MODE, &options.daemon_mode);

  // greenplum conf
  b.GetConfStr(GP_USER, &options.gp_user);
  b.GetConfStr(PASSWD, &options.passwd);
  b.GetConfStr(GP_HOST, &options.gp_host);
  b.GetConfInt(GP_PORT, &options.gp_port);

  // gpfdist conf
  b.GetConfStr(GPD_HOST, &options.gpd_host);
  b.GetConfInt(GPD_PORT, &options.gpd_port);
  b.GetConfInt(ERROR_LIMIT, &options.error_limit);
  return 0;
}

void ParseArgs(int argc, char* argv[], PSOptions& options) {
  if (argc < 2) {
    Usage();
    exit(-1);
  }

  const struct option long_options[] = {
    {"conf_file", required_argument, NULL, 'c'},
    {"help", no_argument, NULL, 'h'},
    {NULL, 0, NULL, 0}, };

  const char* short_options = "h:c:x";

  int ch, longindex;
  while ((ch = getopt_long(argc, argv, short_options, long_options,
                           &longindex)) >= 0) {
    switch (ch) {
      case 'x':
        options.daemon_mode = true;
        break;
      case 'c':
        if (GetOptionFromFile(optarg, options) != 0) {
          Usage();
          exit(-1);
        }
        break;
      case 'h':
      default:
        Usage();
        exit(0);
        break;
    }
  }
}
