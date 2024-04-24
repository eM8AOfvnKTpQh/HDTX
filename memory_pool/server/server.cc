#include "server.h"

#include <stdlib.h>
#include <unistd.h>

#include <thread>

#include "util/json_config.h"

#define AUTO_ENABLE 0

#if AUTO_ENABLE
const int post_num = 1000;
#else
const int post_num = 4000;
#endif
char* lock_buffer;

void Server::AllocMem() {
  RDMA_LOG(INFO) << "Start allocating memory...";
  // allocate hash buffer
  if (use_pm) {
    pm_file_fd = open(pm_file.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (pm_file_fd < 0) {
      printf("open file failed, %s\n", strerror(errno));
    }
    // 0x80003 = MAP_SHARED_VALIDATE | MAP_SYNC. Some old kernel does not have
    // linux/mman.h: MAP_SHARED_VALIDATE, MAP_SYNC
    hash_buffer = (char*)mmap(0, hash_buf_size, PROT_READ | PROT_WRITE, 0x80003,
                              pm_file_fd, 0);
    assert(hash_buffer);
    RDMA_LOG(INFO) << "Alloc PM data region success!";
  } else {
    hash_buffer = (char*)malloc(hash_buf_size);
    assert(hash_buffer);
    RDMA_LOG(INFO) << "Alloc DRAM data region success!";
  }
  // Reserve 1/4 for hash conflict in case of full bucket.
  size_t reserve_start = hash_buf_size * 0.75;
  hash_reserve_buffer = hash_buffer + reserve_start;
  // Allocate log buffer.
  log_buffer = (char*)malloc(log_buf_size);
  assert(log_buffer);
  // Reserve buffer for lock.
  reserve_start = log_buf_size * 0.90;
  lock_buffer = log_buffer + reserve_start;
}

void Server::InitMem() {
  RDMA_LOG(INFO) << "Start initializing memory...";
  memset(hash_buffer, 0, hash_buf_size);
  memset(log_buffer, 0, log_buf_size);
  RDMA_LOG(INFO) << "Initialize memory success!";
}

/**
 * This method requires manual posting enable.
 */
void post_memcpy(RCQP* recv_qp, RCQP* wait_qp) {
  // Offload operations.
  uint64_t write_wr_id = wait_qp->next_wr_id(true);
  // Setting the SGE.
  struct ibv_sge sge1 {
    .addr = 0, .length = DataItemSize - sizeof(uint64_t),
    .lkey = wait_qp->local_mr_.key
  };
  struct ibv_exp_send_wr write_sr =
      wait_qp->create_exp_wr(IBV_EXP_WR_RDMA_WRITE, 1, &sge1, 0, 0, 0,
                             IBV_EXP_SEND_SIGNALED, write_wr_id);

  uint64_t faa_wr_id = wait_qp->next_wr_id(true);
  struct ibv_sge sge2 {
    .addr = (uint64_t)lock_buffer, .length = sizeof(uint64_t),
    .lkey = wait_qp->local_mr_.key
  };
  struct ibv_exp_send_wr faa_sr =
      wait_qp->create_exp_wr(IBV_EXP_WR_ATOMIC_FETCH_AND_ADD, 1, &sge2, 0, 0, 0,
                             IBV_EXP_SEND_SIGNALED, faa_wr_id);

  write_sr.next = &faa_sr;

  struct ibv_exp_send_wr* bad_sr;
  wait_qp->ibv_exp_post_send_wrapper(wait_qp->qp_, &write_sr, &bad_sr);

  // Get the wqe address.
  // Write wqe: ctrl seg + raddr seg + data seg
  struct wqe_ctrl_seg* sr_ctrl = wait_qp->get_wqe_by_wr_id(write_wr_id);
  // Shift 16 bytes to get the start addr of raddr seg.
  char* seg = ((char*)sr_ctrl) + sizeof(struct wqe_ctrl_seg);
  struct ibv_sge sg_list[4];
  // Data addr
  struct wqe_raddr_seg* sr_raddr = (struct wqe_raddr_seg*)seg;
  sg_list[0].addr = (uint64_t)&sr_raddr->raddr;
  sg_list[0].length = sizeof(uint64_t);
  sg_list[0].lkey = wait_qp->wq_mr_->lkey;
  // Log addr
  seg += sizeof(struct wqe_raddr_seg);
  struct wqe_data_seg* sr_data = (struct wqe_data_seg*)seg;
  sg_list[1].addr = (uint64_t)&sr_data->addr;
  sg_list[1].length = sizeof(uint64_t);
  sg_list[1].lkey = wait_qp->wq_mr_->lkey;
  // FAA wqe: ctrl seg + raddr seg + atmoic seg
  sr_ctrl = wait_qp->get_wqe_by_wr_id(faa_wr_id);
  seg = ((char*)sr_ctrl) + sizeof(struct wqe_ctrl_seg);
  // Data addr
  sr_raddr = (struct wqe_raddr_seg*)seg;
  sg_list[2].addr = (uint64_t)&sr_raddr->raddr;
  sg_list[2].length = sizeof(uint64_t);
  sg_list[2].lkey = wait_qp->wq_mr_->lkey;
  // Add value
  seg += sizeof(struct wqe_raddr_seg);
  struct wqe_atomic_seg* sr_atomic = (struct wqe_atomic_seg*)seg;
  sg_list[3].addr = (uint64_t)&sr_atomic->swap_add;
  sg_list[3].length = sizeof(uint64_t);
  sg_list[3].lkey = wait_qp->wq_mr_->lkey;
  recv_qp->post_recv_sg(4, sg_list, recv_qp->next_wr_id(false));
}

/**
 * This method will auto post enable.
 */
void post_memcpy_v2(RCQP* recv_qp, RCQP* wait_qp) {
  // Offload operations.
  struct ibv_exp_send_wr wait_sr =
      wait_qp->create_exp_wait_wr(recv_qp, 0, wait_qp->next_wr_id(true), true);
  uint64_t enable_wr_id = wait_qp->next_wr_id(true);
  struct ibv_exp_send_wr enable_sr =
      wait_qp->create_exp_enable_wr(wait_qp, 0, 0, enable_wr_id);

  uint64_t write_wr_id = wait_qp->next_wr_id(true);
  // Setting the SGE.
  struct ibv_sge sge1 {
    .addr = 0, .length = DataItemSize - sizeof(uint64_t),
    .lkey = wait_qp->local_mr_.key
  };
  struct ibv_exp_send_wr write_sr = wait_qp->create_exp_wr(
      IBV_EXP_WR_RDMA_WRITE, 1, &sge1, 0, 0, 0, 0, write_wr_id);

  uint64_t faa_wr_id = wait_qp->next_wr_id(true);
  struct ibv_sge sge2 {
    .addr = (uint64_t)lock_buffer, .length = sizeof(uint64_t),
    .lkey = wait_qp->local_mr_.key
  };
  struct ibv_exp_send_wr faa_sr = wait_qp->create_exp_wr(
      IBV_EXP_WR_ATOMIC_FETCH_AND_ADD, 1, &sge2, 0, 0, 0, 0, faa_wr_id);

  wait_sr.next = &enable_sr;
  enable_sr.next = &write_sr;
  write_sr.next = &faa_sr;

  struct ibv_exp_send_wr* bad_sr;
  wait_qp->ibv_exp_post_send_wrapper(wait_qp->qp_, &wait_sr, &bad_sr);

  // Set PI.
  struct wqe_ctrl_seg* sr_ctrl = wait_qp->get_wqe_by_wr_id(enable_wr_id);
  char* seg = ((char*)sr_ctrl) + sizeof(struct wqe_ctrl_seg);
  struct wqe_wait_en_seg* sr_en_wait = (struct wqe_wait_en_seg*)seg;
  sr_en_wait->pi = htonl(wait_qp->scur_post + 2);

  // Get the wqe address.
  // Write wqe: ctrl seg + raddr seg + data seg
  sr_ctrl = wait_qp->get_wqe_by_wr_id(write_wr_id);
  // Shift 16 bytes to get the start addr of raddr seg.
  seg = ((char*)sr_ctrl) + sizeof(struct wqe_ctrl_seg);
  struct ibv_sge sg_list[4];
  // Data addr
  struct wqe_raddr_seg* sr_raddr = (struct wqe_raddr_seg*)seg;
  sg_list[0].addr = (uint64_t)&sr_raddr->raddr;
  sg_list[0].length = sizeof(uint64_t);
  sg_list[0].lkey = wait_qp->wq_mr_->lkey;
  // Log addr
  seg += sizeof(struct wqe_raddr_seg);
  struct wqe_data_seg* sr_data = (struct wqe_data_seg*)seg;
  // RDMA_LOG(INFO) << ntohl(sr_data->byte_count);
  sg_list[1].addr = (uint64_t)&sr_data->addr;
  sg_list[1].length = sizeof(uint64_t);
  sg_list[1].lkey = wait_qp->wq_mr_->lkey;
  // FAA wqe: ctrl seg + raddr seg + atmoic seg
  sr_ctrl = wait_qp->get_wqe_by_wr_id(faa_wr_id);
  seg = ((char*)sr_ctrl) + sizeof(struct wqe_ctrl_seg);
  // Data addr
  sr_raddr = (struct wqe_raddr_seg*)seg;
  sg_list[2].addr = (uint64_t)&sr_raddr->raddr;
  sg_list[2].length = sizeof(uint64_t);
  sg_list[2].lkey = wait_qp->wq_mr_->lkey;
  // Add value
  seg += sizeof(struct wqe_raddr_seg);
  struct wqe_atomic_seg* sr_atomic = (struct wqe_atomic_seg*)seg;
  // RDMA_LOG(INFO) << ntohll(sr_atomic->swap_add);
  sg_list[3].addr = (uint64_t)&sr_atomic->swap_add;
  sg_list[3].length = sizeof(uint64_t);
  sg_list[3].lkey = wait_qp->wq_mr_->lkey;
  recv_qp->post_recv_sg(4, sg_list, recv_qp->next_wr_id(false));
}

void* poll_cq_loop(void* data) {
  pthread_detach(pthread_self());
  RCQP* qp = (RCQP*)data;
  struct ibv_wc wc {};
  qp->running = true;
  do {
    auto rc = qp->poll_till_completion(wc, no_timeout);
    if (rc == SUCC) {
      if (wc.opcode == IBV_WC_RECV) {
        // RDMA_LOG(INFO) << "Post memcpy.";
#if !AUTO_ENABLE
        // Enable first two wrs.
        qp->post_enable(qp->wait_qp, 0, 2, false, qp->next_wr_id(true), true);
        post_memcpy(qp, qp->wait_qp);
#else
        post_memcpy_v2(qp, qp->wait_qp);
#endif
      } else if (wc.opcode == IBV_WC_FETCH_ADD) {
        RDMA_LOG(INFO) << "wr_id: " << wc.wr_id
                       << " index: " << qp->sq_index[wc.wr_id];
        struct wqe_ctrl_seg* sr_ctrl = qp->get_wqe_by_wr_id(wc.wr_id);
        char* seg = ((char*)sr_ctrl) + sizeof(struct wqe_ctrl_seg);
        struct wqe_raddr_seg* sr_raddr = (struct wqe_raddr_seg*)seg;
        RDMA_LOG(INFO) << "raddr: " << ntohll(sr_raddr->raddr);
        seg += sizeof(struct wqe_raddr_seg);
        struct wqe_atomic_seg* sr_atomic = (struct wqe_atomic_seg*)seg;
        RDMA_LOG(INFO) << "add: " << ntohll(sr_atomic->swap_add);
      } else if (wc.opcode == IBV_WC_SEND) {
        RDMA_LOG(INFO) << "Send completion.";
      }
    } else {
      RDMA_LOG(WARNING) << "Failed to poll completion event.";
    }
  } while (qp->running);
  return nullptr;
}

void Server::InitRDMA() {
  /************* RDMA+PM Initialization *************/
  RDMA_LOG(INFO) << "Start initializing RDMA...";

  rdma_ctrl = std::make_shared<RdmaCtrl>(server_node_id, local_port);
  RdmaCtrl::DevIdx idx{.dev_id = 0,
                       .port_id = 1};  // using the first RNIC's first port
  rdma_ctrl->open_thread_local_device(idx);
  RDMA_ASSERT(rdma_ctrl->register_memory(SERVER_HASH_BUFF_ID, hash_buffer,
                                         hash_buf_size,
                                         rdma_ctrl->get_device()) == true);
  RDMA_ASSERT(rdma_ctrl->register_memory(SERVER_LOG_BUFF_ID, log_buffer,
                                         log_buf_size,
                                         rdma_ctrl->get_device()) == true);
  RDMA_LOG(INFO) << "Register memory success!";

  // This function will be called after establishing the connection.
  auto app_on_connect_cb = [=](QP* qp) {
    RCQP* rc_qp = dynamic_cast<RCQP*>(qp);
    if (rc_qp->use_offload) {  // Use offload.
      RDMA_LOG(INFO) << "Use offload.";
      RCQP* wait_qp = rc_qp->wait_qp;
      // Bind mr for wait_qp.
      wait_qp->bind_local_mr(rdma_ctrl->get_local_mr(SERVER_LOG_BUFF_ID));
      wait_qp->bind_remote_mr(rdma_ctrl->get_local_mr(SERVER_HASH_BUFF_ID));
      // Create thread for polling completion event.
      pthread_create(&rc_qp->cq_poll_thread, nullptr, poll_cq_loop, rc_qp);
      // pthread_create(&wait_qp->cq_poll_thread, nullptr, poll_cq_loop,
      // wait_qp);
      //  Pre post wrs.
      for (int i = 0; i < post_num; i++) {
#if !AUTO_ENABLE
        post_memcpy(rc_qp, wait_qp);
#else
        post_memcpy_v2(rc_qp, wait_qp);
#endif
      }
#if AUTO_ENABLE
      // Enable first two wrs.
      rc_qp->post_enable(wait_qp, 0, 2, false, rc_qp->next_wr_id(true));
#endif
    }
  };

  rdma_ctrl->register_qp_callback(app_on_connect_cb);
}

// All servers need to load data
void Server::LoadData(node_id_t machine_id,
                      node_id_t machine_num,  // number of memory nodes
                      std::string& workload) {
  /************* Load Data *************/
  RDMA_LOG(INFO) << "Start loading database data...";
  // Init tables
  MemStoreAllocParam mem_store_alloc_param(hash_buffer, hash_buffer, 0,
                                           hash_reserve_buffer);
  MemStoreReserveParam mem_store_reserve_param(hash_reserve_buffer, 0,
                                               hash_buffer + hash_buf_size);
  if (workload == "TATP") {
    tatp_server = new TATP();
    tatp_server->LoadTable(machine_id, machine_num, &mem_store_alloc_param,
                           &mem_store_reserve_param);
  } else if (workload == "SmallBank") {
    smallbank_server = new SmallBank();
    smallbank_server->LoadTable(machine_id, machine_num, &mem_store_alloc_param,
                                &mem_store_reserve_param);
  } else if (workload == "TPCC") {
    tpcc_server = new TPCC();
    tpcc_server->LoadTable(machine_id, machine_num, &mem_store_alloc_param,
                           &mem_store_reserve_param);
  } else if (workload == "MICRO") {
    micro_server = new MICRO();
    micro_server->LoadTable(machine_id, machine_num, &mem_store_alloc_param,
                            &mem_store_reserve_param);
  }
  RDMA_LOG(INFO) << "Loading table successfully!";
}

void Server::CleanTable() {
  if (tatp_server) {
    delete tatp_server;
    RDMA_LOG(INFO) << "delete tatp tables";
  }

  if (smallbank_server) {
    delete smallbank_server;
    RDMA_LOG(INFO) << "delete smallbank tables";
  }

  if (tpcc_server) {
    delete tpcc_server;
    RDMA_LOG(INFO) << "delete tpcc tables";
  }
}

void Server::CleanQP() { rdma_ctrl->destroy_rc_qp(); }

void Server::SendMeta(node_id_t machine_id, std::string& workload,
                      size_t compute_node_num) {
  // Prepare hash meta
  char* hash_meta_buffer = nullptr;
  size_t total_meta_size = 0;
  PrepareHashMeta(machine_id, workload, &hash_meta_buffer, total_meta_size);
  assert(hash_meta_buffer != nullptr);
  assert(total_meta_size != 0);

  // Send memory store meta to all the compute nodes via TCP
  for (size_t index = 0; index < compute_node_num; index++) {
    SendHashMeta(hash_meta_buffer, total_meta_size);
  }
  free(hash_meta_buffer);
}

void Server::PrepareHashMeta(node_id_t machine_id, std::string& workload,
                             char** hash_meta_buffer, size_t& total_meta_size) {
  // Get all hash meta
  std::vector<HashMeta*> primary_hash_meta_vec;
  std::vector<HashMeta*> backup_hash_meta_vec;
  std::vector<HashStore*> all_priamry_tables;
  std::vector<HashStore*> all_backup_tables;

  if (workload == "TATP") {
    all_priamry_tables = tatp_server->GetPrimaryHashStore();
    all_backup_tables = tatp_server->GetBackupHashStore();
  } else if (workload == "SmallBank") {
    all_priamry_tables = smallbank_server->GetPrimaryHashStore();
    all_backup_tables = smallbank_server->GetBackupHashStore();
  } else if (workload == "TPCC") {
    all_priamry_tables = tpcc_server->GetPrimaryHashStore();
    all_backup_tables = tpcc_server->GetBackupHashStore();
  } else if (workload == "MICRO") {
    all_priamry_tables = micro_server->GetPrimaryHashStore();
    all_backup_tables = micro_server->GetBackupHashStore();
  }

  for (auto& hash_table : all_priamry_tables) {
    auto* hash_meta = new HashMeta(
        hash_table->GetTableID(), (uint64_t)hash_table->GetDataPtr(),
        hash_table->GetBucketNum(), hash_table->GetHashNodeSize(),
        hash_table->GetBaseOff());
    primary_hash_meta_vec.emplace_back(hash_meta);
  }
  for (auto& hash_table : all_backup_tables) {
    auto* hash_meta = new HashMeta(
        hash_table->GetTableID(), (uint64_t)hash_table->GetDataPtr(),
        hash_table->GetBucketNum(), hash_table->GetHashNodeSize(),
        hash_table->GetBaseOff());
    backup_hash_meta_vec.emplace_back(hash_meta);
  }

  int hash_meta_len = sizeof(HashMeta);
  size_t primary_hash_meta_num = primary_hash_meta_vec.size();
  RDMA_LOG(INFO) << "primary hash meta num: " << primary_hash_meta_num;
  size_t backup_hash_meta_num = backup_hash_meta_vec.size();
  RDMA_LOG(INFO) << "backup hash meta num: " << backup_hash_meta_num;
  total_meta_size =
      sizeof(primary_hash_meta_num) + sizeof(backup_hash_meta_num) +
      sizeof(machine_id) + primary_hash_meta_num * hash_meta_len +
      backup_hash_meta_num * hash_meta_len + sizeof(MEM_STORE_META_END);
  *hash_meta_buffer = (char*)malloc(total_meta_size);

  char* local_buf = *hash_meta_buffer;

  // Fill primary hash meta
  *((size_t*)local_buf) = primary_hash_meta_num;
  local_buf += sizeof(primary_hash_meta_num);
  *((size_t*)local_buf) = backup_hash_meta_num;
  local_buf += sizeof(backup_hash_meta_num);
  *((node_id_t*)local_buf) = machine_id;
  local_buf += sizeof(machine_id);
  for (size_t i = 0; i < primary_hash_meta_num; i++) {
    memcpy(local_buf + i * hash_meta_len, (char*)primary_hash_meta_vec[i],
           hash_meta_len);
  }
  local_buf += primary_hash_meta_num * hash_meta_len;
  // Fill backup hash meta
  for (size_t i = 0; i < backup_hash_meta_num; i++) {
    memcpy(local_buf + i * hash_meta_len, (char*)backup_hash_meta_vec[i],
           hash_meta_len);
  }
  local_buf += backup_hash_meta_num * hash_meta_len;
  // EOF
  *((uint64_t*)local_buf) = MEM_STORE_META_END;
}

void Server::SendHashMeta(char* hash_meta_buffer, size_t& total_meta_size) {
  // Using TCP to send hash meta
  /* --------------- Initialize socket --------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port =
      htons(local_meta_port);  // change host little endian to big endian
  server_addr.sin_addr.s_addr =
      htonl(INADDR_ANY);  // change host "0.0.0.0" to big endian
  int listen_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (listen_socket < 0) {
    RDMA_LOG(ERROR) << "Server creates socket error: " << strerror(errno);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server creates socket success";
  if (bind(listen_socket, (const struct sockaddr*)&server_addr,
           sizeof(server_addr)) < 0) {
    RDMA_LOG(ERROR) << "Server binds socket error: " << strerror(errno);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server binds socket success";
  int max_listen_num = 10;
  if (listen(listen_socket, max_listen_num) < 0) {
    RDMA_LOG(ERROR) << "Server listens error: " << strerror(errno);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server listens success";
  int from_client_socket = accept(listen_socket, NULL, NULL);
  // int from_client_socket = accept(listen_socket, (struct sockaddr*)
  // &client_addr, &client_socket_length);
  if (from_client_socket < 0) {
    RDMA_LOG(ERROR) << "Server accepts error: " << strerror(errno);
    close(from_client_socket);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server accepts success";

  /* --------------- Sending hash metadata --------------- */
  auto retlen = send(from_client_socket, hash_meta_buffer, total_meta_size, 0);
  if (retlen < 0) {
    RDMA_LOG(ERROR) << "Server sends hash meta error: " << strerror(errno);
    close(from_client_socket);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server sends hash meta success";
  size_t recv_ack_size = 100;
  char* recv_buf = (char*)malloc(recv_ack_size);
  recv(from_client_socket, recv_buf, recv_ack_size, 0);
  if (strcmp(recv_buf, "[ACK]hash_meta_received_from_client") != 0) {
    std::string ack(recv_buf);
    RDMA_LOG(ERROR) << "Client receives hash meta error. Received ack is: "
                    << ack;
  }

  free(recv_buf);
  close(from_client_socket);
  close(listen_socket);
}

bool Server::Run() {
  // Now server just waits for user typing quit to finish
  // Server's CPU is not used during one-sided RDMA requests from clients
  printf(
      "======================================================================"
      "=="
      "============================\n");
  printf(
      "Server now runs as a disaggregated mode. No CPU involvement during "
      "RDMA-based transaction processing\n"
      "Type c to run another round, type q if you want to exit :)\n");
  while (true) {
    char ch;
    scanf("%c", &ch);
    if (ch == 'q') {
      return false;
    } else if (ch == 'c') {
      return true;
    } else {
      printf("Type c to run another round, type q if you want to exit :)\n");
    }
    usleep(2000);
  }
}

int main(int argc, char* argv[]) {
  // Configure of this server
  std::string config_filepath = "../../../config/memory_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);

  auto local_node = json_config.get("local_memory_node");
  node_id_t machine_num = (node_id_t)local_node.get("machine_num").get_int64();
  node_id_t machine_id = (node_id_t)local_node.get("machine_id").get_int64();
  assert(machine_id >= 0 && machine_id < machine_num);
  int local_port = (int)local_node.get("local_port").get_int64();
  int local_meta_port = (int)local_node.get("local_meta_port").get_int64();
  int use_pm = (int)local_node.get("use_pm").get_int64();
  std::string pm_root = local_node.get("pm_root").get_str();
  std::string workload = local_node.get("workload").get_str();
  auto mem_size_GB = local_node.get("mem_size_GB").get_uint64();
  auto log_buf_size_GB = local_node.get("log_buf_size_GB").get_uint64();

  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto compute_node_ips = compute_nodes.get("compute_node_ips");  // Array
  size_t compute_node_num = compute_node_ips.size();

  // std::string pm_file = pm_root + "pm_node" + std::to_string(machine_id);
  // // Use fsdax
  std::string pm_file = pm_root;  // Use devdax
  size_t mem_size = (size_t)1024 * 1024 * 1024 * mem_size_GB;
  size_t hash_buf_size = mem_size;  // Currently, we support the hash structure
  size_t log_buf_size = (size_t)1024 * 1024 * 1024 * log_buf_size_GB;

  auto server = std::make_shared<Server>(
      machine_id, local_port, local_meta_port, hash_buf_size, log_buf_size,
      use_pm, pm_file, mem_size);
  server->AllocMem();
  server->InitMem();
  server->LoadData(machine_id, machine_num, workload);
  server->SendMeta(machine_id, workload, compute_node_num);
  server->InitRDMA();
  bool run_next_round = server->Run();

  // Continue to run the next round. RDMA does not need to be inited twice
  while (run_next_round) {
    server->InitMem();
    server->CleanTable();
    server->CleanQP();
    server->LoadData(machine_id, machine_num, workload);
    server->SendMeta(machine_id, workload, compute_node_num);
    run_next_round = server->Run();
  }

  // Stat the cpu utilization
  auto pid = getpid();
  std::string copy_cmd = "cp /proc/" + std::to_string(pid) + "/stat ./";
  system(copy_cmd.c_str());

  copy_cmd = "cp /proc/uptime ./";
  system(copy_cmd.c_str());
  return 0;
}