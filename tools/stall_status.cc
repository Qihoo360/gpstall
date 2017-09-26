#include "status.h"
#include "pb_cli.h"
#include "pink_cli.h"
#include "status.h"
#include <google/protobuf/message.h>

#include <iostream>
#include <string>

#include "monitor.pb.h"

class Pcli : public pink::PbCli {
 public:
   Pcli();
   pink::Status SendCommand(const std::string &command);
   pink::Status SendCommand(const std::string &user,  const std::string &password, const std::string &command);
   pink::Status RecvStatus();
};

Pcli::Pcli() {
}

pink::Status Pcli::SendCommand(const std::string &command) {
  pink::Status s;
  gpstall::Command req;
  req.set_command(command);

  s = Send(&req);

  return s;
}

pink::Status Pcli::SendCommand(const std::string &user,  const std::string &password, const std::string &command) {
  pink::Status s;
  gpstall::Command req;
  req.set_command(command);
  req.set_user(user);
  req.set_password(password);
  s = Send(&req);

  return s;
}


pink::Status Pcli::RecvStatus() {
  pink::Status s;
  gpstall::ServiceStatus service_status_;
  s = Recv(&service_status_);
  if (!s.ok())
    return s;

  std::cout << "start_time: " << service_status_.start_time() << std::endl;
  switch (service_status_.service_status()) {
    case gpstall::ServiceStatus::ONLINE:
      std::cout << "service_status: ONLINE" << std::endl;
      break;
    case gpstall::ServiceStatus::STALL:
      std::cout << "service_status: STALL" << std::endl;
      break;
    case gpstall::ServiceStatus::UNKNOW:
    default:
      std::cout << "service_status: UNKNOW" << std::endl;
  }
  std::cout << "conn_num: " << service_status_.conn_num() << std::endl;
  std::cout << "qps: " << service_status_.qps() << std::endl;
  std::cout << "gpload_failed_count: " << service_status_.gpload_failed_num() << std::endl;
  std::cout << "lastest_failed_time: " << service_status_.lastest_failed_time() << std::endl;
  std::cout << "failed_files_num: " << service_status_.failed_files_num() << std::endl;
  //std::cout << "failed_files_name: " << service_status_.failed_files_name() << std::endl;
  std::cout << "failed_files_size: " << service_status_.failed_files_size() << std::endl;
  std::cout << "longest_gpload_timeused: " << service_status_.gpload_longest_timeused() / 1000 << std::endl;
  std::cout << "latest_gpload_timeused: " << service_status_.gpload_latest_timeused() / 1000 << std::endl;
  std::cout << "gpload_average_timeused: " << service_status_.gpload_average_timeused() / 1000 << std::endl;

  return s;
}

void Usage() {
  std::cout << "Usage: ./stall_status ip port username password" << std::endl;
}

int main(int argc, char *argv[]) {
  if (argc < 5) {
    Usage();
    exit(1);
  }

  Pcli *pcli = new Pcli();
  pcli->Connect(argv[1], atoi(argv[2])); 
  pink::Status s = pcli->SendCommand(argv[3], argv[4], "");
  //pink::Status s = pcli->SendCommand("user" ,"password", "gpstall_info");
  
  if(s.ok()){
    std::cout << "connected!" << std::endl;
    std::string command = "gpstall_info";
    pink::Status ss =  pcli->SendCommand(command);
    
    pcli->RecvStatus();
  }
  else {
    std::cout << "connecting error" << std::endl;
  }
  delete pcli;

  return 0;
}
