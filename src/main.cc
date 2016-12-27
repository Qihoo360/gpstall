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

static int close_std(const std::string &errlog) {
  int fd;
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    close(fd);
  } else return fd;

  if ((fd = open(errlog.c_str(), O_CREAT | O_RDWR, 0644)) != -1) {
    dup2(fd, STDERR_FILENO);
    close(fd);
  } else return fd;
  return 0;
}

static int daemonize(const PSOptions &options) {
  pid_t pid;
  umask(0);

  if ((pid = fork()) < 0) {
    LOG(FATAL) << "can't fork" << std::endl;
    return -1;
  } else if (pid != 0)
    exit(0);
  setsid();

  std::string errlog = options.log_path + "/error_log";
  if (close_std(errlog) != 0) {
    LOG(FATAL) << "can't close std" << std::endl;
    return -1;
  }

  // Don't print log to stderr
  // becase some error info maybe print to stderr in daemon mode
  FLAGS_alsologtostderr = false;

  return 0;
}

int main(int argc, char** argv) {
  PSOptions options;
  ParseArgs(argc, argv, options);

  GlogInit(options);

  if (options.daemon_mode) {
    std::cout << "Will running in daemon mode." << std::endl;
    if (daemonize(options) != 0) {
      std::cout << "Daemon mode failed." << std::endl;
      return -1;
    }
  }
  PSSignalSetup();

  ps_server = new PSServer(options);
  ps_server->Start();

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
        if (options.GetOptionFromFile(optarg) != 0) {
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
