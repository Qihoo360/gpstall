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
  FLAGS_minloglevel = 0;
  FLAGS_max_log_size = 1800;
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

int main(int argc, char** argv) {
  PSOptions options;

  ParseArgs(argc, argv, options);

  //options.Dump();
  GlogInit(options);
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
          "  ./pgstall [argument]\n"
          "  ./pgstall -c configuration file\n"
          "Arguments(Optional):\n"
          "  -n, --local_ip=IP\n"
          "  -p, --local_port=NUM\n"
          "  -w, --worker_num=NUM\n"
          "  -d, --data_path=PATH_TO_DATA\n"
          "  -l, --log_path=PATH_TO_LOGS\n"
          "  -f, --file_size=NUM\n"
          "  -i, --load_interval=NUM(Second)\n"
          "  -s, --flush_interval=NUM(Second)\n"
          "  -x  enable daemon mode\n"
          "  -c, --conf_file=CONFIGURATION_FILE\n"
          );
}

int GetOptionFromFile(const std::string &configuration_file, PSOptions& options) {
  slash::BaseConf b(configuration_file);
  if (b.LoadConf() != 0)
    return -1;

  b.GetConfStr(LOCAL_IP, &options.local_ip);
  b.GetConfInt(LOCAL_PORT, &options.local_port);
  b.GetConfInt(WORKER_NUM, &options.worker_num);
  b.GetConfInt(FILE_SIZE, &options.file_size);
  b.GetConfInt(LOAD_INTERVAL, &options.load_interval);
  b.GetConfInt(FLUSH_INTERVAL, &options.flush_interval);
  // TODO timeout
  b.GetConfStr(DATA_PATH, &options.data_path);
  b.GetConfStr(LOG_PATH, &options.log_path);
  b.GetConfStr(LOAD_SCRIPT, &options.load_script);
  b.GetConfStr(CONF_SCRIPT, &options.conf_script);
  b.GetConfBool(DAEMON_MODE, &options.daemon_mode);

  return 0;
}

void ParseArgs(int argc, char* argv[], PSOptions& options) {
  if (argc < 2) {
    if (GetOptionFromFile(DEFAULT_CONFIGURATION_FILE, options) != 0) {
      Usage();
      exit(-1);
    } else
      return;
  }

  const struct option long_options[] = {
    {"local_ip", required_argument, NULL, 'n'},
    {"local_port", required_argument, NULL, 'p'},
    {"worker_num", required_argument, NULL, 'w'},
    {"file_size", required_argument, NULL, 'f'},
    {"load_interval", required_argument, NULL, 'i'},
    {"flush_interval", required_argument, NULL, 's'},
    {"data_path", required_argument, NULL, 'd'},
    {"log_path", required_argument, NULL, 'l'},
    {"conf_file", required_argument, NULL, 'c'},
    {"help", no_argument, NULL, 'h'},
    {NULL, 0, NULL, 0}, };

  const char* short_options = "n:p:w:f:i:s:d:l:h:c:x";

  int ch, longindex;
  while ((ch = getopt_long(argc, argv, short_options, long_options,
                           &longindex)) >= 0) {
    switch (ch) {
      case 'n':
        options.local_ip = optarg;
        break;
      case 'p':
        options.local_port = atoi(optarg);
        break;
      case 'w':
        options.worker_num = atoi(optarg);
        break;
      case 'f':
        options.file_size = atoi(optarg);
        break;
      case 'i':
        options.load_interval = atoi(optarg);
        break;
      case 's':
        options.flush_interval = atoi(optarg);
        break;
      case 'd':
        options.data_path = optarg;
        break;
      case 'l':
        options.log_path = optarg;
        break;
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
