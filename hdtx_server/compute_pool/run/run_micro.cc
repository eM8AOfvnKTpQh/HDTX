 

#include "worker/handler.h"

// Entrance to run threads that spawn coroutines as coordinators to run distributed transactions
int main(int argc, char* argv[]) {
  // e.g. ./run_micro s-100 means run HDTX with skewed access and write ratio 100%
  Handler* handler = new Handler();
  handler->ConfigureComputeNodeForMICRO(argc, argv);
  handler->GenThreadsForMICRO();
  handler->OutputResult("MICRO", "HDTX");
}
