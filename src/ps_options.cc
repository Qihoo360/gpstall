#include "ps_options.h"

#include "ps_consts.h"

////// PSOptions //////
PSOptions::PSOptions()
  : local_ip("127.0.0.1"),
    local_port(8001),
    worker_num(8),
    file_size(kFileSize),
    data_path("./data"),
    log_path("./log") {
  if (data_path.back() != '/') {
    data_path.append(1, '/');
  }
  if (log_path.back() != '/') {
    log_path.append(1, '/');
  }
}


PSOptions::PSOptions(const PSOptions& options)
  : local_ip(options.local_ip),
    local_port(options.local_port),
    worker_num(options.worker_num),
    file_size(options.file_size),
    data_path(options.data_path),
    log_path(options.log_path) {
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
  fprintf (stderr, "    Options.local_ip    : %s\n", local_ip.c_str());
  fprintf (stderr, "    Options.local_port  : %d\n", local_port);
  fprintf (stderr, "    Options.data_path   : %s\n", data_path.c_str());
  fprintf (stderr, "    Options.log_path    : %s\n", log_path.c_str());
  fprintf (stderr, "    Options.worker_num  : %d\n", worker_num);
  fprintf (stderr, "    Options.file_size   : %d Bytes\n", file_size);
}
