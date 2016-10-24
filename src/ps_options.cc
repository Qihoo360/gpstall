#include "ps_options.h"

////// PSOptions //////
PSOptions::PSOptions()
  : local_ip("127.0.0.1"),
    local_port(8001),
    data_path("./data"),
    log_path("./log") {
    }


PSOptions::PSOptions(const PSOptions& options)
  : local_ip(options.local_ip),
    local_port(options.local_port),
    data_path(options.data_path),
    log_path(options.log_path) {
}

void PSOptions::Dump() {
  fprintf (stderr, "    Options.local_ip    : %s\n", local_ip.c_str());
  fprintf (stderr, "    Options.local_port  : %d\n", local_port);
  fprintf (stderr, "    Options.data_path   : %s\n", data_path.c_str());
  fprintf (stderr, "    Options.log_path    : %s\n", log_path.c_str());
}
