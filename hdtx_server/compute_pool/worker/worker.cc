#include "worker/worker.h"

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>

#include "allocator/buffer_allocator.h"
#include "allocator/log_allocator.h"
#include "connection/qp_manager.h"
#include "dtx/dtx.h"
#include "micro/micro_txn.h"
#include "smallbank/smallbank_txn.h"
#include "tatp/tatp_txn.h"
#include "tpcc/tpcc_txn.h"
#include "util/latency.h"
#include "util/zipf.h"

#define WAIT 1
#define SAMPLE_PRIORITY 0
#define STAT_ABORT 1

using namespace std::placeholders;

// All the functions are executed in each thread
std::mutex mux;

extern std::atomic<uint64_t> tx_id_generator;
extern std::atomic<uint64_t> connected_t_num;
extern std::vector<double> lock_durations;
extern std::vector<t_id_t> tid_vec;
extern std::vector<double> attemp_tp_vec;
extern std::vector<double> tp_vec;
extern std::vector<double> medianlat_vec;
extern std::vector<double> taillat_vec;
extern std::vector<double> avglat_vec;
extern std::vector<int> sla_vec;
extern std::vector<int> commit_vec;
extern std::vector<double> sample_taillat_vec;
extern std::vector<double> sample_avglat_vec;
extern std::vector<int> sample_commit_vec;

extern std::vector<uint64_t> total_try_times;
extern std::vector<uint64_t> total_commit_times;

__thread size_t ATTEMPTED_NUM;
__thread uint64_t wait_time;
__thread uint64_t seed;                        // Thread-global random seed
__thread uint64_t lock_seed;                   // Random seed for lock
__thread FastRandom* random_generator = NULL;  // Per coroutine random generator
__thread t_id_t thread_gid;
__thread t_id_t thread_local_id;
__thread t_id_t thread_num;

__thread TATP* tatp_client = nullptr;
__thread SmallBank* smallbank_client = nullptr;
__thread TPCC* tpcc_client = nullptr;

__thread MetaManager* meta_man;
__thread QPManager* qp_man;

__thread VersionCache* status;
__thread LockCache* lock_table;

__thread RDMABufferAllocator* rdma_buffer_allocator;
__thread LogOffsetAllocator* log_offset_allocator;
__thread AddrCache* addr_cache;

__thread TATPTxType* tatp_workgen_arr;
__thread SmallBankTxType* smallbank_workgen_arr;
__thread TPCCTxType* tpcc_workgen_arr;

__thread coro_id_t coro_num;
__thread CoroutineScheduler*
    coro_sched;  // Each transaction thread has a coroutine scheduler
__thread bool stop_run;

// Performance measurement (thread granularity)
__thread struct timespec msr_start, msr_end;
__thread double* timer;
__thread uint64_t stat_attempted_tx_total = 0;  // Issued transaction number
__thread uint64_t stat_committed_tx_total = 0;  // Committed transaction number
const coro_id_t POLL_ROUTINE_ID = 0;            // The poll coroutine ID


// For MICRO benchmark
std::vector<ZipfGen*> zipf_gens;
__thread ZipfGen* zipf_gen = nullptr;
__thread double zipf_theta;
__thread bool is_skewed;
__thread uint64_t data_set_size;
__thread uint64_t num_keys_global;
__thread uint64_t write_ratio;

// Stat the commit rate
__thread uint64_t* thread_local_try_times;
__thread uint64_t* thread_local_commit_times;

// Coroutine 0 in each thread does polling
void PollCompletion(coro_yield_t& yield) {
  while (true) {
    coro_sched->PollCompletion();
    Coroutine* next = coro_sched->coro_head->next_coro;
    if (next->coro_id != POLL_ROUTINE_ID) {
      // RDMA_LOG(DBG) << "Coro 0 yields to coro " << next->coro_id;
      coro_sched->RunCoroutine(yield, next);
    }
    if (stop_run) break;
  }
}

void RecordTpLat(double msr_sec) {
  double attemp_tput = (double)stat_attempted_tx_total / msr_sec;
  double tx_tput = (double)stat_committed_tx_total / msr_sec;

  std::sort(timer, timer + stat_committed_tx_total);
  double percentile_50 = timer[stat_committed_tx_total / 2];
  double percentile_99 = timer[stat_committed_tx_total * 99 / 100];

  double sum = 0;
  int sla = 0;
  for (int i = 0; i < stat_committed_tx_total; i++) {
    sum += timer[i];
    if (timer[i] > 1300) sla++;
  }
  double average_latency = sum / stat_committed_tx_total;

  mux.lock();
  tid_vec.push_back(thread_gid);
  attemp_tp_vec.push_back(attemp_tput);
  tp_vec.push_back(tx_tput);
  medianlat_vec.push_back(percentile_50);
  taillat_vec.push_back(percentile_99);
  avglat_vec.push_back(average_latency);
  sla_vec.push_back(sla);
  commit_vec.push_back(stat_committed_tx_total);

  for (size_t i = 0; i < total_try_times.size(); i++) {
    total_try_times[i] += thread_local_try_times[i];
    total_commit_times[i] += thread_local_commit_times[i];
  }

  mux.unlock();
}

// Run actual transactions
void RunTATP(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a dtx: Each coroutine is a coordinator
  DTX* dtx = new DTX(meta_man, qp_man, status, lock_table, thread_gid, coro_id,
                     coro_sched, rdma_buffer_allocator, log_offset_allocator,
                     addr_cache);
  struct timespec tx_start_time, tx_end_time;
  uint64_t tot_time = 0;
  uint64_t tx_id;      // global atomic transaction id
  TATPTxType tx_type;  // transaction type
  int tx_committed = 0;
  long acc_time = 0;
	int commit = 0;


  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    // Guarantee that each coroutine has a different seed
    tx_type = tatp_workgen_arr[FastRand(&seed) % 100];
    stat_attempted_tx_total++;
    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    while (true) {
      tx_id = ++tx_id_generator;
      switch (tx_type) {
        case TATPTxType::kGetSubsciberData: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed =
              TxGetSubsciberData(tatp_client, seed, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case TATPTxType::kGetNewDestination: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed =
              TxGetNewDestination(tatp_client, seed, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case TATPTxType::kGetAccessData: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = TxGetAccessData(tatp_client, seed, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case TATPTxType::kUpdateSubscriberData: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed =
              TxUpdateSubscriberData(tatp_client, seed, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case TATPTxType::kUpdateLocation: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = TxUpdateLocation(tatp_client, seed, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case TATPTxType::kInsertCallForwarding: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed =
              TxInsertCallForwarding(tatp_client, seed, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case TATPTxType::kDeleteCallForwarding: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed =
              TxDeleteCallForwarding(tatp_client, seed, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        default:
          printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
          abort();
      }
      if (tx_committed) {commit = 1;break;}
      if (dtx->tx_error == ITEM_NOT_FOUND) { commit = 0;break;}  // don't record
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec =
          (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 +
          (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      if (tx_usec > wait_time) {
        tx_committed = 1;
		commit = 0;
        break;
      }
//		dtx->lock_mode = PRIORITY;
    }

    /**************************** Stat begin ****************************/
    // Stat after one transaction finishes
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      // RDMA_LOG(INFO) << "commit: " << iter;
      double tx_usec =
          (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000L +
          (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;

      acc_time += tx_usec;
      if (commit == 1) {
        timer[stat_committed_tx_total++] = acc_time;
        acc_time = 0;
      }

    //  tot_time += tx_usec;  // Exclude the influence of non-existent keys
    }
    if (stat_attempted_tx_total >= ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 +
      // (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
       double msr_sec =
           (msr_end.tv_sec - msr_start.tv_sec) +
           (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
      //double msr_sec = (double)tot_time / 1000000;
      RecordTpLat(msr_sec);
      break;
    }
    /**************************** Stat end ****************************/
  }

  delete dtx;
}

void RunSmallBank(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a dtx: Each coroutine is a coordinator
  DTX* dtx = new DTX(meta_man, qp_man, status, lock_table, thread_gid, coro_id,
                     coro_sched, rdma_buffer_allocator, log_offset_allocator,
                     addr_cache);
  struct timespec tx_start_time, tx_end_time;
  uint64_t tx_id;           // global atomic transaction id
  SmallBankTxType tx_type;  // transaction type
  int tx_committed = 0;
	char commit = 0;
  long acc_time = 0;
	
	int retry_count = 1;
  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    tx_type = smallbank_workgen_arr[FastRand(&seed) % 100];
    stat_attempted_tx_total++;
    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    while (true) 
	{
      tx_id = ++tx_id_generator;
      switch (tx_type) {
        case SmallBankTxType::kAmalgamate: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed =
              TxAmalgamate(smallbank_client, seed, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case SmallBankTxType::kBalance: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = TxBalance(smallbank_client, seed, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case SmallBankTxType::kDepositChecking: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed =
              TxDepositChecking(smallbank_client, seed, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case SmallBankTxType::kSendPayment: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed =
              TxSendPayment(smallbank_client, seed, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case SmallBankTxType::kTransactSaving: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed =
              TxTransactSaving(smallbank_client, seed, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case SmallBankTxType::kWriteCheck: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed =
              TxWriteCheck(smallbank_client, seed, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        default:
          printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
          abort();
      }
      // RDMA_LOG(INFO) << "try: " << (uint64_t)tx_type;
      if (tx_committed) {commit = 1;break;}
      if (dtx->tx_error == ITEM_NOT_FOUND)  { commit = 0;break;}
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec =
          (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 +
          (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;

      if (tx_usec > wait_time)
		{
        tx_committed = 1;
		commit = 0;
        break;
      }
if(rand()%55 == 1)		
dtx->lock_mode = PRIORITY;

//#define new_one
#ifdef new_one
	if(retry_count < 500)
		retry_count = retry_count *2;
	if(tx_usec + retry_count > wait_time){
		tx_committed = 2;
		break;
	}
	int back_off = rand()%retry_count;
		usleep(back_off);
#endif
    }

    /**************************** Stat begin ****************************/
    // Stat after one transaction finishes
    if (tx_committed) {
#ifdef new_one
	retry_count = 1;
#endif
      // RDMA_LOG(INFO) << "commit: " << tx_id;
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec =
          (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 +
          (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;

       acc_time += tx_usec;
       if (commit == 1) {
         timer[stat_committed_tx_total++] = acc_time;//tx_usec;//acc_time;
         acc_time = 0;
       }
     // timer[stat_committed_tx_total++] = tx_usec;
    }
    if (stat_attempted_tx_total >= ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 +
      // (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec =
          (msr_end.tv_sec - msr_start.tv_sec) +
          (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
		int sla = 0;
       for (int i = 0; i < stat_committed_tx_total; i++) {
         if (timer[i] < 5000) sla++;
       }
       printf("%d\n",sla);
      RecordTpLat(msr_sec);
      break;
    }
    /**************************** Stat end ****************************/
  }

  delete dtx;
}

void RunTPCC(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a dtx: Each coroutine is a coordinator
  DTX* dtx = new DTX(meta_man, qp_man, status, lock_table, thread_gid, coro_id,
                     coro_sched, rdma_buffer_allocator, log_offset_allocator,
                     addr_cache);
  struct timespec tx_start_time, tx_end_time;
  uint64_t tot_time = 0;
  uint64_t tx_id;      // global atomic transaction id
  TPCCTxType tx_type;  // transaction type
  int tx_committed = 0;
  long acc_time = 0;
int commit =0;
  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    // Guarantee that each coroutine has a different seed
    tx_type = tpcc_workgen_arr[FastRand(&seed) % 100];
    stat_attempted_tx_total++;
    uint64_t last_seed = random_generator[coro_id].GetSeed();

    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    while (true) {
      tx_id = ++tx_id_generator;
      random_generator[coro_id].SetSeed(last_seed);
      switch (tx_type) {
        case TPCCTxType::kDelivery: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed =
              TxDelivery(tpcc_client, random_generator, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        } break;
        case TPCCTxType::kNewOrder: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed =
              TxNewOrder(tpcc_client, random_generator, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;

        } break;
        case TPCCTxType::kOrderStatus: {
          thread_local_try_times[uint64_t(tx_type)]++;

          tx_committed =
              TxOrderStatus(tpcc_client, random_generator, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;

        } break;
        case TPCCTxType::kPayment: {
          thread_local_try_times[uint64_t(tx_type)]++;

          tx_committed =
              TxPayment(tpcc_client, random_generator, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;

        } break;
        case TPCCTxType::kStockLevel: {
          thread_local_try_times[uint64_t(tx_type)]++;

          tx_committed =
              TxStockLevel(tpcc_client, random_generator, yield, tx_id, dtx);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;

        } break;
        default:
          printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
          abort();
      }
      if (tx_committed) {commit = 1;break;}
      if (dtx->tx_error == ITEM_NOT_FOUND) {commit = 0;break;}
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec =
          (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 +
          (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      if (tx_usec > wait_time) {
        tx_committed = 1;
		commit = 0;
        break;
      }
//		dtx->lock_mode = PRIORITY;
    }

    /**************************** Stat begin ****************************/
    // Stat after one transaction finishes
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      // RDMA_LOG(INFO) << "commit: " << tx_id;
      double tx_usec =
          (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000L +
          (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;

      acc_time += tx_usec;
      if (commit == 1) {
        timer[stat_committed_tx_total++] = acc_time;
        acc_time = 0;
      }

//      tot_time += tx_usec;  // Exclude the influence of non-existent keys
    }
    // RDMA_LOG(INFO) << "num: " << stat_attempted_tx_total;
    if (stat_attempted_tx_total >= ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 +
      // (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
       double msr_sec =
           (msr_end.tv_sec - msr_start.tv_sec) +
           (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000L;
      //double msr_sec = (double)tot_time / 1000000;
      RecordTpLat(msr_sec);
      break;
    }
    /**************************** Stat end ****************************/
  }

  delete dtx;
}

void RunMICRO(coro_yield_t& yield, coro_id_t coro_id) {
  double total_msr_us = 0;
  // Each coroutine has a dtx: Each coroutine is a coordinator
  DTX* dtx = new DTX(meta_man, qp_man, status, lock_table, thread_gid, coro_id,
                     coro_sched, rdma_buffer_allocator, log_offset_allocator,
                     addr_cache);
  struct timespec tx_start_time, tx_end_time;
  uint64_t tx_id;  // Global atomic transaction id
  int tx_committed = 0;
	char commit = 0;

  bool is_sample = false;
  int sample_count = 0;
  double* sample_timer = new double[ATTEMPTED_NUM];

  long acc_time = 0;

  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    stat_attempted_tx_total++;
    if (acc_time == 0) {
      if (FastRand(&lock_seed) % 100 < 80) {
        is_sample = false;
      } else {
        is_sample = true;
#if SAMPLE_PRIORITY
	dtx->lock_mode = PRIORITY;
#else
	dtx->lock_mode = NORMAL;
#endif
      }
    }
    // Populate data set
    DataItemPtr micro_objs[data_set_size];
    std::set<uint64_t> uni_set;
    for (uint64_t i = 0; i < data_set_size; i++) {
      micro_key_t micro_key;
      if (is_skewed) {
        // Skewed distribution
        // Random select a zipf_gen
        // int index = FastRand(&seed) % zipf_gens.size();
        // ZipfGen* gen = zipf_gens[index];
        // micro_key.micro_id = (itemkey_t)(gen->next());
        micro_key.micro_id = (itemkey_t)(zipf_gen->next());

        // micro_key.micro_id =
        //     (micro_key.micro_id + num_keys_global / thread_num * thread_gid)
        //     % num_keys_global;

        // RDMA_LOG(INFO) << micro_key.micro_id;
      } else {
        // Uniformed distribution
        micro_key.micro_id = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
      }

      assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

      // if (uni_set.count(micro_key.item_key)) {
      //   continue;
      // } else {
      //   uni_set.insert(micro_key.item_key);
      // }

      if (!uni_set.count(micro_key.item_key)) {
        uni_set.insert(micro_key.item_key);
        micro_objs[uni_set.size() - 1] = std::make_shared<DataItem>(
            (table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
      }
    }

    std::sort(micro_objs, micro_objs + uni_set.size());

    clock_gettime(CLOCK_REALTIME, &tx_start_time);
#if WAIT
    while (true) {
      tx_id = ++tx_id_generator;
       //RDMA_LOG(INFO) << "data size: " << uni_set.size();
      tx_committed = TxMicroTest(seed, yield, tx_id, dtx, micro_objs,
                                 uni_set.size(), write_ratio);

      if (tx_committed) {commit = 1;break;}
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec =
          (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 +
          (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      if (tx_usec > wait_time) {
        tx_committed = 2;
		commit = 0;
        // RDMA_LOG(INFO) << "break";
        break;
      }
      // RDMA_LOG(INFO) << "Retry";
      dtx->lock_mode = PRIORITY;
    }
#else
    tx_id = ++tx_id_generator;
    // RDMA_LOG(INFO) << "data size: " << uni_set.size();
    tx_committed = TxMicroTest(seed, yield, tx_id, dtx, micro_objs,
                               uni_set.size(), write_ratio);
#endif
    /**************************** Stat begin ****************************/
    // Stat after one transaction finishes
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec =
          (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 +
          (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;

#if STAT_ABORT
      acc_time += tx_usec;
      if (tx_committed == 1) {
        timer[stat_committed_tx_total++] = acc_time;
        if (is_sample) {
          sample_timer[sample_count++] = acc_time;
        }
        acc_time = 0;
      }
#else
      if (tx_committed) {
        timer[stat_committed_tx_total++] = tx_usec;
        if (is_sample) {
          sample_timer[sample_count++] = tx_usec;
        }
      }
#endif
    }
    if (stat_attempted_tx_total >= ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 +
      // (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec =
          (msr_end.tv_sec - msr_start.tv_sec) +
          (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;

      total_msr_us = msr_sec * 1000000;

      double attemp_tput = (double)stat_attempted_tx_total / msr_sec;
      double tx_tput = (double)stat_committed_tx_total / msr_sec;

      std::string thread_num_coro_num;
      if (coro_num < 10) {
        thread_num_coro_num =
            std::to_string(thread_num) + "_0" + std::to_string(coro_num);
      } else {
        thread_num_coro_num =
            std::to_string(thread_num) + "_" + std::to_string(coro_num);
      }
      std::string log_file_path =
          "../../../bench_results/MICRO/" + thread_num_coro_num + "/output.txt";

      std::ofstream output_of;
      output_of.open(log_file_path, std::ios::app);

      std::sort(timer, timer + stat_committed_tx_total);
      double percentile_50 = timer[stat_committed_tx_total / 2];
      double percentile_99 = timer[stat_committed_tx_total * 99 / 100];

      double sum = 0;
      int sla = 0;
      for (int i = 0; i < stat_committed_tx_total; i++) {
        sum += timer[i];
        if (timer[i] < 1200) sla++;
      }
      double average_latency = sum / stat_committed_tx_total;

      std::sort(sample_timer, sample_timer + sample_count);
      double sample_tail_latency = sample_timer[sample_count * 99 / 100];

      sum = 0;
      for (int i = 0; i < sample_count; i++) {
        sum += sample_timer[i];
      }
      double sample_avg_latency = sum / sample_count;

      mux.lock();
      tid_vec.push_back(thread_gid);
      attemp_tp_vec.push_back(attemp_tput);
      tp_vec.push_back(tx_tput);
      medianlat_vec.push_back(percentile_50);
      taillat_vec.push_back(percentile_99);
      avglat_vec.push_back(average_latency);
      sla_vec.push_back(sla);
      commit_vec.push_back(stat_committed_tx_total);
      sample_taillat_vec.push_back(sample_tail_latency);
      sample_avglat_vec.push_back(sample_avg_latency);
      sample_commit_vec.push_back(sample_count);
      mux.unlock();
      output_of << tx_tput << " " << percentile_50 << " " << percentile_99
                << " " << average_latency << " " << sla << " "
                << stat_committed_tx_total << std::endl;
      output_of.close();
      // std::cout << tx_tput << " " << percentile_50 << " " << percentile_99 <<
      // std::endl;

      // Output the local addr cache miss rate
      log_file_path = "../../../bench_results/MICRO/" + thread_num_coro_num +
                      "/miss_rate.txt";
      output_of.open(log_file_path, std::ios::app);
      output_of << double(dtx->miss_local_cache_times) /
                       (dtx->hit_local_cache_times +
                        dtx->miss_local_cache_times)
                << std::endl;
      output_of.close();

      log_file_path = "../../../bench_results/MICRO/" + thread_num_coro_num +
                      "/cache_size.txt";
      output_of.open(log_file_path, std::ios::app);
      output_of << dtx->GetAddrCacheSize() << std::endl;
      output_of.close();

      break;
    }
  }

  std::string thread_num_coro_num;
  if (coro_num < 10) {
    thread_num_coro_num =
        std::to_string(thread_num) + "_0" + std::to_string(coro_num);
  } else {
    thread_num_coro_num =
        std::to_string(thread_num) + "_" + std::to_string(coro_num);
  }
  uint64_t total_duration = 0;
  double average_lock_duration = 0;

  // only for test
#if LOCK_WAIT

  for (auto duration : dtx->lock_durations) {
    total_duration += duration;
  }

  std::string total_lock_duration_file = "../../../bench_results/MICRO/" +
                                         thread_num_coro_num +
                                         "/total_lock_duration.txt";
  std::ofstream of;
  of.open(total_lock_duration_file, std::ios::app);
  std::sort(dtx->lock_durations.begin(), dtx->lock_durations.end());
  auto min_lock_duration =
      dtx->lock_durations.empty() ? 0 : dtx->lock_durations[0];
  auto max_lock_duration =
      dtx->lock_durations.empty()
          ? 0
          : dtx->lock_durations[dtx->lock_durations.size() - 1];
  average_lock_duration =
      dtx->lock_durations.empty()
          ? 0
          : (double)total_duration / dtx->lock_durations.size();
  lock_durations[thread_local_id] = average_lock_duration;
  of << thread_gid << " " << average_lock_duration << " " << max_lock_duration
     << std::endl;
  of.close();
#endif

  // only for test
#if INV_BUSY_WAIT
  total_duration = 0;
  for (auto duration : dtx->invisible_durations) {
    total_duration += duration;
  }
  std::string total_inv_duration_file = "../../../bench_results/MICRO/" +
                                        thread_num_coro_num +
                                        "/total_inv_duration.txt";
  std::ofstream ofs;
  ofs.open(total_inv_duration_file, std::ios::app);
  std::sort(dtx->invisible_durations.begin(), dtx->invisible_durations.end());
  auto min_inv_duration =
      dtx->invisible_durations.empty() ? 0 : dtx->invisible_durations[0];
  auto max_inv_duration =
      dtx->invisible_durations.empty()
          ? 0
          : dtx->invisible_durations[dtx->invisible_durations.size() - 1];
  auto average_inv_duration =
      dtx->invisible_durations.empty()
          ? 0
          : (double)total_duration / dtx->invisible_durations.size();

  double total_execution_time = 0;
  for (uint64_t i = 0; i < stat_committed_tx_total; i++) {
    total_execution_time += timer[i];
  }

  uint64_t re_read_times = 0;
  for (uint64_t i = 0; i < dtx->invisible_reread.size(); i++) {
    re_read_times += dtx->invisible_reread[i];
  }

  uint64_t avg_re_read_times =
      dtx->invisible_reread.empty()
          ? 0
          : re_read_times / dtx->invisible_reread.size();

  auto average_execution_time =
      (total_execution_time / stat_committed_tx_total) * 1000000;  // us

  ofs << thread_gid << " " << average_inv_duration << " " << max_inv_duration
      << " " << average_execution_time << " " << avg_re_read_times << " "
      << double(total_duration / total_execution_time) << " "
      << (double)(total_duration / total_msr_us) << std::endl;

  ofs.close();
#endif

  /**************************** Stat end ****************************/

  delete dtx;
  delete[] sample_timer;
}

void run_thread(thread_params* params, TATP* tatp_cli, SmallBank* smallbank_cli,
                TPCC* tpcc_cli) {
  auto bench_name = params->bench_name;
  std::string config_filepath =
      "../../../config/" + bench_name + "_config.json";

  auto json_config = JsonConfig::load_file(config_filepath);
  auto conf = json_config.get(bench_name);
  ATTEMPTED_NUM = conf.get("attempted_num").get_uint64();
  wait_time = conf.get("wait_time").get_uint64();

  if (bench_name == "tatp") {
    tatp_client = tatp_cli;
    tatp_workgen_arr = tatp_client->CreateWorkgenArray();
    thread_local_try_times = new uint64_t[TATP_TX_TYPES]();
    thread_local_commit_times = new uint64_t[TATP_TX_TYPES]();
  } else if (bench_name == "smallbank") {
    smallbank_client = smallbank_cli;
    smallbank_workgen_arr = smallbank_client->CreateWorkgenArray();
    thread_local_try_times = new uint64_t[SmallBank_TX_TYPES]();
    thread_local_commit_times = new uint64_t[SmallBank_TX_TYPES]();
  } else if (bench_name == "tpcc") {
    tpcc_client = tpcc_cli;
    tpcc_workgen_arr = tpcc_client->CreateWorkgenArray();
    thread_local_try_times = new uint64_t[TPCC_TX_TYPES]();
    thread_local_commit_times = new uint64_t[TPCC_TX_TYPES]();
  }

  stop_run = false;
  thread_gid = params->thread_global_id;
  thread_local_id = params->thread_local_id;
  thread_num = params->thread_num_per_machine;
  meta_man = params->global_meta_man;
  status = params->global_status;
  lock_table = params->global_lcache;
  coro_num = (coro_id_t)params->coro_num;
  coro_sched = new CoroutineScheduler(thread_gid, coro_num);

  auto alloc_rdma_region_range =
      params->global_rdma_region->GetThreadLocalRegion(thread_local_id);
  addr_cache = new AddrCache();
  rdma_buffer_allocator = new RDMABufferAllocator(
      alloc_rdma_region_range.first, alloc_rdma_region_range.second);
  log_offset_allocator =
      new LogOffsetAllocator(thread_gid, params->total_thread_num);
  timer = new double[ATTEMPTED_NUM]();

  // Initialize Zipf generator for MICRO benchmark
  if (bench_name == "micro") {
    std::string micro_config_filepath = "../../../config/micro_config.json";
    auto json_config = JsonConfig::load_file(micro_config_filepath);
    auto micro_conf = json_config.get("micro");
    num_keys_global = align_pow2(micro_conf.get("num_keys").get_int64());
    is_skewed = micro_conf.get("is_skewed").get_bool();
    zipf_theta = micro_conf.get("zipf_theta").get_double();
    data_set_size = micro_conf.get("data_set_size").get_uint64();
    write_ratio = micro_conf.get("write_ratio").get_uint64();

    uint64_t zipf_seed = 2 * thread_gid * GetCPUCycle();  // TODO
    uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
    zipf_gen =
        new ZipfGen(num_keys_global, zipf_theta, zipf_seed & zipf_seed_mask);
  }

  // Init coroutine random gens specialized for TPCC benchmark
  random_generator = new FastRandom[coro_num];

  // Guarantee that each thread has a global different initial seed
  seed = 0xdeadbeef + thread_gid;
  lock_seed = 0xdeadbeef + thread_gid;

  // Init coroutines
  for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++) {
    uint64_t coro_seed =
        static_cast<uint64_t>((static_cast<uint64_t>(thread_gid) << 32) |
                              static_cast<uint64_t>(coro_i));
    random_generator[coro_i].SetSeed(coro_seed);
    coro_sched->coro_array[coro_i].coro_id = coro_i;

    // if (bench_name == "micro") {
    //   uint64_t zipf_seed = coro_seed;
    //   uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
    //   auto zipf_gen_ =
    //       new ZipfGen(num_keys_global, zipf_theta, zipf_seed &
    //       zipf_seed_mask);
    //   mux.lock();
    //   zipf_gens.push_back(zipf_gen_); // TODO
    //   mux.unlock();
    // }

    // Bind workload to coroutine
    if (coro_i == POLL_ROUTINE_ID) {
      coro_sched->coro_array[coro_i].func =
          coro_call_t(bind(PollCompletion, _1));
    } else {
      if (bench_name == "tatp") {
        coro_sched->coro_array[coro_i].func =
            coro_call_t(bind(RunTATP, _1, coro_i));
      } else if (bench_name == "smallbank") {
        coro_sched->coro_array[coro_i].func =
            coro_call_t(bind(RunSmallBank, _1, coro_i));
      } else if (bench_name == "tpcc") {
        coro_sched->coro_array[coro_i].func =
            coro_call_t(bind(RunTPCC, _1, coro_i));
      } else if (bench_name == "micro") {
        coro_sched->coro_array[coro_i].func =
            coro_call_t(bind(RunMICRO, _1, coro_i));
      }
    }
  }

  // Link all coroutines via pointers in a loop manner
  coro_sched->LoopLinkCoroutine(coro_num);

  // Build qp connection in thread granularity
  qp_man = new QPManager(thread_gid);
  qp_man->BuildQPConnection(meta_man);

  // Sync qp connections in one compute node before running transactions
  connected_t_num += 1;
  while (connected_t_num != thread_num) {
    usleep(2000);  // wait for all threads connections
  }

  // Start the first coroutine
  coro_sched->coro_array[0].func();

  // Stop running
  stop_run = true;

  // RDMA_LOG(DBG) << "Thread: " << thread_gid << ". Loop RDMA alloc times: " <<
  // rdma_buffer_allocator->loop_times;

  // Clean
  delete[] timer;
  delete addr_cache;
  if (tatp_workgen_arr) delete[] tatp_workgen_arr;
  if (smallbank_workgen_arr) delete[] smallbank_workgen_arr;
  if (tpcc_workgen_arr) delete[] tpcc_workgen_arr;
  if (random_generator) delete[] random_generator;
  if (zipf_gen) delete zipf_gen;
  delete coro_sched;
  delete thread_local_try_times;
  delete thread_local_commit_times;
}
