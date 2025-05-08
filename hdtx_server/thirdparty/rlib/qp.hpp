#pragma once

#include "common.hpp"
#include "include/mlx4.h"
#include "qp_impl.hpp"  // hide the implementation

namespace rdmaio {

/**
 * The QP managed by RLib is identified by the QPIdx
 * Basically it identifies which worker(thread) is using the QP,
 * and which machine this QP is connected to.
 */
typedef struct {
  int node_id;    // the node QP connect to
  int worker_id;  // the thread/task QP belongs
  int index;      // mutliple QP may is needed to connect to the node
} QPIdx;

// some macros for easy computer QP idx, since some use default values
constexpr QPIdx create_rc_idx(int nid, int wid) {
  return QPIdx{.node_id = nid, .worker_id = wid, .index = 0};
}

constexpr QPIdx create_ud_idx(int worker_id, int idx = 0) {
  return QPIdx{.node_id = 0,  // a UD qp can connect to multiple machine
               .worker_id = worker_id,
               .index = idx};
}

/**
 * Wrappers over ibv_qp & ibv_cq
 * For easy use, and connect
 */
class QP {
 public:
  QP(RNicHandler* rnic, QPIdx idx) : idx_(idx), rnic_(rnic) {}

  ~QP() {
    if (qp_ != nullptr) ibv_destroy_qp(qp_);
    if (cq_ != nullptr) ibv_destroy_cq(cq_);
    running = false;
  }
  /**
   * GetMachineMeta to remote QP
   * Note, we leverage TCP for a pre connect.
   * So the IP/Hostname and a TCP port must be given.
   *
   * WARNING:
   * This function actually should contains two functions, connect + change QP
   * status maybe split to connect + change status for more flexibility?
   */
  /**
   * connect to the specific QP at remote, specificed by the nid and wid
   * return SUCC if QP are ready.
   * return TIMEOUT if there is network error.
   * return NOT_READY if remote server fails to find the connected QP
   */
  virtual ConnStatus connect(std::string ip, int port, QPIdx idx,
                             bool use_offload) = 0;

  // return until the completion events
  // this call will block until a timeout
  virtual ConnStatus poll_till_completion(
      ibv_wc& wc, struct timeval timeout = default_timeout) {
    return QPImpl::poll_till_completion(cq_, wc, timeout);
  }

  void bind_local_mr(MemoryAttr attr) { local_mr_ = attr; }

  QPAttr get_attr() const {
    QPAttr res = {.addr = rnic_->query_addr(),  // gid
                  .lid = rnic_->lid,
                  .qpn = (qp_ != nullptr) ? qp_->qp_num : 0,
                  .psn = DEFAULT_PSN,  // TODO! this may be filled later
                  .node_id = 0,        // a place holder
                  .port_id = rnic_->port_id};
    return res;
  }

  /**
   * Get remote MR attribute
   */
  static ConnStatus get_remote_mr(std::string ip, int port, int mr_id,
                                  MemoryAttr* attr) {
    return QPImpl::get_remote_mr(ip, port, mr_id, attr);
  }

  // increments last work request id for a specified connection
  // send == false --> wr type is receive
  // send == true  --> wr type is send
  inline uint64_t next_wr_id(bool send) {
    // we maintain seperate wr_ids for send/recv queues since
    // there is no ordering between their work requests
    if (send) {
      return last_send++;
    } else {
      return last_recv++;
    }
    return 0;
  }

  // QP identifiers
  const QPIdx idx_;

 public:
  // internal verbs structure
  struct ibv_qp* qp_ = NULL;
  struct ibv_cq* cq_ = NULL;

  pthread_t cq_poll_thread;
  bool running = false;

  // local MR used to post reqs
  MemoryAttr local_mr_;
  RNicHandler* rnic_;

  // synchronization
  uint64_t last_send = 0;
  uint64_t last_send_compl = 0;
  uint64_t last_recv = 0;
  uint64_t last_recv_compl = 0;

 protected:
  ConnStatus get_remote_helper(ConnArg* arg, ConnReply* reply, std::string ip,
                               int port) {
    return QPImpl::get_remote_helper(arg, reply, ip, port);
  }
};

inline constexpr RCConfig default_rc_config() {
  return RCConfig{
      .access_flags = (IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
                       IBV_ACCESS_REMOTE_ATOMIC),
      .max_rd_atomic = 1,
      .max_dest_rd_atomic = 1,
      .rq_psn = DEFAULT_PSN,
      .sq_psn = DEFAULT_PSN,
      .timeout = 20};
}

/**
 * Raw RC QP
 */
template <RCConfig (*F)(void) = default_rc_config>
class RRCQP : public QP {
 public:
  RRCQP(RNicHandler* rnic, QPIdx idx, MemoryAttr local_mr, MemoryAttr remote_mr,
        int create_flags = 0)
      : RRCQP(rnic, idx, create_flags) {
    bind_local_mr(local_mr);
    bind_remote_mr(remote_mr);
  }

  RRCQP(RNicHandler* rnic, QPIdx idx, MemoryAttr local_mr, int create_flags = 0)
      : RRCQP(rnic, idx, create_flags) {
    bind_local_mr(local_mr);
  }

  RRCQP(RNicHandler* rnic, QPIdx idx, int create_flags = 0) : QP(rnic, idx) {
    RCQPImpl::init<F>(qp_, cq_, rnic_, create_flags);
#ifdef EXP_VERBS
    // register wq for offload operations
    register_wq_mr();
#endif
  }

  ConnStatus connect(std::string ip, int port, bool use_offload = false) {
    return connect(ip, port, idx_, use_offload);
  }

  ConnStatus connect(std::string ip, int port, QPIdx idx, bool use_offload) {
    // first check whether QP is finished to connect
    enum ibv_qp_state state;
    if ((state = QPImpl::query_qp_status(qp_)) != IBV_QPS_INIT) {
      if (state != IBV_QPS_RTS)
        RDMA_LOG(WARNING) << "qp not in a correct state to connect!";
      return (state == IBV_QPS_RTS) ? SUCC : UNKNOWN;
    }
    ConnArg arg = {};
    ConnReply reply = {};
    arg.type = ConnArg::QP;
    arg.payload.qp.from_node = idx.node_id;
    arg.payload.qp.from_worker = idx.worker_id;
    arg.payload.qp.qp_type = IBV_QPT_RC;
    arg.payload.qp.qp_attr = get_attr();

    arg.use_offload = use_offload;

    auto ret = QPImpl::get_remote_helper(&arg, &reply, ip, port);
    if (ret == SUCC) {
      // change QP status
      if (!RCQPImpl::ready2rcv<F>(qp_, reply.payload.qp, rnic_)) {
        RDMA_LOG(WARNING) << "change qp status to ready to receive error: "
                          << strerror(errno);
        ret = ERR;
        goto CONN_END;
      }

      if (!RCQPImpl::ready2send<F>(qp_)) {
        RDMA_LOG(WARNING) << "change qp status to ready to send error: "
                          << strerror(errno);
        ret = ERR;
        goto CONN_END;
      }
    }
  CONN_END:
    return ret;
  }

  ConnStatus connect(RRCQP* qp) {
    // change QP status
    ConnStatus ret = SUCC;
    QPAttr attr = qp->get_attr();
    if (!RCQPImpl::ready2rcv<F>(qp_, attr, rnic_)) {
      RDMA_LOG(WARNING) << "change qp status to ready to receive error: "
                        << strerror(errno);
      ret = ERR;
      goto CONN_END;
    }

    if (!RCQPImpl::ready2send<F>(qp_)) {
      RDMA_LOG(WARNING) << "change qp status to ready to send error: "
                        << strerror(errno);
      ret = ERR;
      goto CONN_END;
    }
  CONN_END:
    return ret;
  }

  /**
   * Bind this QP's operation to a remote memory region according to the
   * MemoryAttr. Since usually one QP access *one memory region* almost all the
   * time, so it is more convenient to use a bind-post;bind-post-post fashion.
   */
  void bind_remote_mr(MemoryAttr attr) { remote_mr_ = attr; }

  void register_wq_mr() {
    // get the address info of SQ
    struct mlx4_qp* mqp = to_mqp(qp_);
    sq_start = mqp->sq.buf;
    sq_wqe_cnt = mqp->sq.wqe_cnt;
    sq_wqe_shift = mqp->sq.wqe_shift;
    sq_stride = 1 << mqp->sq.wqe_shift;
    sq_end = sq_start + (sq_wqe_cnt * sq_stride);
    scur_post = 0;
    wq_mr_ = ibv_reg_mr(rnic_->pd, sq_start, sq_wqe_cnt * sq_stride,
                        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                            IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ);

    if (!wq_mr_) {
      RDMA_LOG(ERROR) << "failed to register wq memory, error: "
                      << strerror(errno);
    }
  }

  // record the location of wqe
  void update_scur_post(struct ibv_exp_send_wr* wr) {
    uint32_t idx;
    int size;
    int nreq;
    for (nreq = 0; wr; ++nreq, wr = wr->next) {
      idx = scur_post & (sq_wqe_cnt - 1);
      sq_index[wr->wr_id] = idx;
      size = sizeof(struct wqe_ctrl_seg) / 16;
      switch (wr->exp_opcode) {
        case IBV_EXP_WR_RDMA_READ:
        case IBV_EXP_WR_RDMA_WRITE:
        case IBV_EXP_WR_RDMA_WRITE_WITH_IMM:
          size += sizeof(struct wqe_raddr_seg) / 16;
          break;
        case IBV_EXP_WR_ATOMIC_CMP_AND_SWP:
        case IBV_EXP_WR_ATOMIC_FETCH_AND_ADD:
          size +=
              (sizeof(struct wqe_raddr_seg) + sizeof(struct wqe_atomic_seg)) /
              16;
          break;
        case IBV_EXP_WR_SEND:
          break;
        case IBV_EXP_WR_CQE_WAIT: {
          size += sizeof(struct wqe_wait_en_seg) / 16;
        } break;
        case IBV_EXP_WR_SEND_ENABLE:
        case IBV_EXP_WR_RECV_ENABLE: {
          size += sizeof(struct wqe_wait_en_seg) / 16;
        } break;
        case IBV_EXP_WR_NOP:
          break;
        default:
          break;
      }

      for (int i = 0; i < wr->num_sge; i++) {
        if (wr->sg_list[i].length > 0) {
          size += sizeof(struct wqe_data_seg) / 16;
        }
      }
      scur_post += 1;  // equals sq.head
    }

    // RDMA_LOG(INFO) << "scur_post: " << scur_post;
  }

  inline char* get_send_wqe(int n) { return sq_start + (n << sq_wqe_shift); }

  struct wqe_ctrl_seg* get_wqe_by_wr_id(uint64_t wr_id) {
    struct wqe_ctrl_seg* seg = NULL;
    if (sq_index.count(wr_id)) {
      seg = (struct wqe_ctrl_seg*)get_send_wqe(sq_index[wr_id]);
      return seg;
    } else {
      RDMA_LOG(ERROR) << "wqe not found, wr_id:" << wr_id;
    }

    return seg;
  }

  ConnStatus ibv_post_send_wrapper(struct ibv_qp* qp, struct ibv_send_wr* wr,
                                   struct ibv_send_wr** bad_wr) {
    update_scur_post((struct ibv_exp_send_wr*)wr);
    auto rc = ibv_post_send(qp, wr, bad_wr);
    if (rc != 0) {
      RDMA_LOG(ERROR) << "failed to post send, error: " << strerror(errno);
    }

    return rc == 0 ? SUCC : ERR;
  }

  ConnStatus ibv_exp_post_send_wrapper(struct ibv_qp* qp,
                                       struct ibv_exp_send_wr* wr,
                                       struct ibv_exp_send_wr** bad_wr) {
    update_scur_post(wr);
    auto rc = ibv_exp_post_send(qp, wr, bad_wr);
    if (rc != 0) {
      RDMA_LOG(ERROR) << "failed to post send, error: " << strerror(errno);
    }

    return rc == 0 ? SUCC : ERR;
  }

  ConnStatus post_send_to_mr(MemoryAttr& local_mr, MemoryAttr& remote_mr,
                             ibv_wr_opcode op, char* local_buf, uint32_t len,
                             uint64_t off, int flags, uint64_t wr_id = 0,
                             uint32_t imm = 0) {
    struct ibv_send_wr sr {};
    struct ibv_send_wr* bad_sr;

    // setting the SGE
    struct ibv_sge sge {
      .addr = (uint64_t)local_buf, .length = len, .lkey = local_mr.key
    };

    // setting sr, sr has to be initialized in this style
    sr.wr_id = wr_id;
    sr.opcode = op;
    sr.num_sge = 1;
    sr.sg_list = &sge;
    sr.next = NULL;
    sr.send_flags = flags;
    sr.imm_data = imm;

    sr.wr.rdma.remote_addr = remote_mr.buf + off;
    sr.wr.rdma.rkey = remote_mr.key;

    return ibv_post_send_wrapper(qp_, &sr, &bad_sr);
  }

  /**
   * Post request(s) to the sending QP.
   * This is just a wrapper of ibv_post_send
   */
  ConnStatus post_send(ibv_wr_opcode op, char* local_buf, uint32_t len,
                       uint64_t off, int flags, uint64_t wr_id = 0,
                       uint32_t imm = 0) {
    return post_send_to_mr(local_mr_, remote_mr_, op, local_buf, len, off,
                           flags, wr_id, imm);
  }

  ConnStatus post_send_sg(ibv_wr_opcode op, int num_sge,
                          struct ibv_sge* sg_list, uint64_t off, int flags,
                          uint64_t wr_id = 0, uint32_t imm = 0) {
    struct ibv_send_wr sr {};
    struct ibv_send_wr* bad_sr;

    sr.wr_id = wr_id;
    sr.opcode = op;
    sr.num_sge = num_sge;
    sr.sg_list = sg_list;
    sr.next = NULL;
    sr.send_flags = flags;
    sr.imm_data = imm;

    sr.wr.rdma.remote_addr = remote_mr_.buf + off;
    sr.wr.rdma.rkey = remote_mr_.key;

    return ibv_post_send_wrapper(qp_, &sr, &bad_sr);
  }

  ConnStatus post_recv_sg(int num_sge, struct ibv_sge* sg_list,
                          uint64_t wr_id = 0) {
    struct ibv_recv_wr sr {};
    struct ibv_recv_wr* bad_sr;

    sr.wr_id = wr_id;
    sr.num_sge = num_sge;
    sr.sg_list = sg_list;

    auto rc = ibv_post_recv(qp_, &sr, &bad_sr);
    return rc == 0 ? SUCC : ERR;
  }

  ConnStatus post_read(char* local_buf, uint32_t len, uint64_t off, int flags,
                       uint64_t wr_id = 0, uint32_t imm = 0) {
    return post_send(IBV_WR_RDMA_READ, local_buf, len, off, flags, wr_id, imm);
  }

  ConnStatus post_read_sync(char* local_buf, uint32_t len, uint64_t off,
                            int flags, uint64_t wr_id = 0, uint32_t imm = 0) {
    auto rc = post_read(local_buf, len, off, flags, wr_id, imm);
    if (rc == SUCC) {
      struct ibv_wc wc {};
      do {
        rc = poll_till_completion(wc, no_timeout);
      } while ((rc == SUCC) && (wc.wr_id != wr_id));
    }
    return rc;
  }

  ConnStatus post_write(char* local_buf, uint32_t len, uint64_t off, int flags,
                        uint64_t wr_id = 0, uint32_t imm = 0) {
    return post_send(IBV_WR_RDMA_WRITE, local_buf, len, off, flags, wr_id, imm);
  }

  ConnStatus post_write_sync(char* local_buf, uint32_t len, uint64_t off,
                             int flags, uint64_t wr_id = 0, uint32_t imm = 0) {
    auto rc = post_write(local_buf, len, off, flags, wr_id, imm);
    if (rc == SUCC) {
      struct ibv_wc wc {};
      do {
        rc = poll_till_completion(wc, no_timeout);
      } while ((rc == SUCC) && (wc.wr_id != wr_id));
    }
    return rc;
  }

  template <ibv_wr_opcode type>
  ConnStatus post_atomic(MemoryAttr& local_mr, MemoryAttr& remote_mr,
                         char* local_buf, uint64_t off, uint64_t compare,
                         uint64_t swap, int flags, uint64_t wr_id = 0) {
    static_assert(type == IBV_WR_ATOMIC_CMP_AND_SWP ||
                      type == IBV_WR_ATOMIC_FETCH_AND_ADD,
                  "only two atomic operations are currently supported.");

    // check if address (off) is 8-byte aligned
    if ((off & 0x7) != 0) {
      return WRONG_ARG;
    }

    struct ibv_send_wr sr {};
    struct ibv_send_wr* bad_sr;

    // setting the SGE
    struct ibv_sge sge {
      .addr = (uint64_t)local_buf, .length = sizeof(uint64_t),
      .lkey = local_mr.key
    };

    sr.wr_id = wr_id;
    sr.opcode = type;
    sr.num_sge = 1;
    sr.sg_list = &sge;
    sr.next = NULL;
    sr.send_flags = flags;

    // remote memory
    sr.wr.atomic.remote_addr = remote_mr.buf + off;
    sr.wr.atomic.rkey = remote_mr.key;

    sr.wr.atomic.compare_add = compare;
    sr.wr.atomic.swap = swap;

    return ibv_post_send_wrapper(qp_, &sr, &bad_sr);
  }

  // one-sided atomic operations
  ConnStatus post_cas(char* local_buf, uint64_t off, uint64_t compare,
                      uint64_t swap, int flags, uint64_t wr_id = 0) {
    return post_atomic<IBV_WR_ATOMIC_CMP_AND_SWP>(
        local_mr_, remote_mr_, local_buf, off, compare, swap, flags, wr_id);
  }

  ConnStatus post_cas_sync(char* local_buf, uint64_t off, uint64_t compare,
                           uint64_t swap, int flags, uint64_t wr_id = 0) {
    auto rc = post_cas(local_buf, off, compare, swap, flags, wr_id);
    if (rc == SUCC) {
      struct ibv_wc wc {};
      do {
        rc = poll_till_completion(wc, no_timeout);
      } while ((rc == SUCC) && (wc.wr_id != wr_id));
    }
    return rc;
  }

  ConnStatus post_faa(MemoryAttr& local_mr, MemoryAttr& remote_mr,
                      char* local_buf, uint64_t off, uint64_t add_value,
                      int flags, uint64_t wr_id = 0) {
    return post_atomic<IBV_WR_ATOMIC_FETCH_AND_ADD>(
        local_mr, remote_mr, local_buf, off, add_value, 0, flags, wr_id);
  }

  // one-sided fetch and add
  ConnStatus post_faa(char* local_buf, uint64_t off, uint64_t add_value,
                      int flags, uint64_t wr_id = 0) {
    return post_atomic<IBV_WR_ATOMIC_FETCH_AND_ADD>(
        local_mr_, remote_mr_, local_buf, off, add_value, 0, flags,
        wr_id); /* no swap value is needed*/
  }

  ConnStatus post_faa_sync(char* local_buf, uint64_t off, uint64_t add_value,
                           int flags, uint64_t wr_id = 0) {
    auto rc = post_faa(local_buf, off, add_value, flags, wr_id);
    if (rc = SUCC) {
      struct ibv_wc wc {};
      do {
        rc = poll_till_completion(wc, no_timeout);
      } while ((rc == SUCC) && (wc.wr_id != wr_id));
    }
    return rc;
  }

  ConnStatus post_wait(RRCQP* wait_qp, int flags, uint64_t wr_id = 0,
                       bool last = false) {
    struct ibv_exp_send_wr sr {};
    struct ibv_exp_send_wr* bad_sr;

    sr.wr_id = wr_id;
    sr.exp_opcode = IBV_EXP_WR_CQE_WAIT;
    sr.num_sge = 0;
    sr.sg_list = NULL;
    sr.next = NULL;
    sr.exp_send_flags = flags;
    if (last) {
      sr.exp_send_flags |= IBV_EXP_SEND_WAIT_EN_LAST;
    }

    sr.ex.imm_data = 0;
    sr.task.cqe_wait.cq =
        wait_qp->cq_;  // Completion queue (CQ) that WAIT WR relates to
    sr.task.cqe_wait.cq_count = 1;  // Producer index (PI) of the CQ

    return ibv_exp_post_send_wrapper(qp_, &sr, &bad_sr);
  }

  ConnStatus post_enable(RRCQP* enable_qp, int flags, int count,
                         bool enable_all, uint64_t wr_id = 0,
                         bool last = false) {
    struct ibv_exp_send_wr sr {};
    struct ibv_exp_send_wr* bad_sr;

    sr.wr_id = wr_id;
    sr.exp_opcode = IBV_EXP_WR_SEND_ENABLE;
    sr.num_sge = 0;
    sr.sg_list = NULL;
    sr.next = NULL;
    sr.exp_send_flags = flags;
    if (last) {
      sr.exp_send_flags |= IBV_EXP_SEND_WAIT_EN_LAST;
    }

    sr.ex.imm_data = 0;
    sr.task.wqe_enable.qp =
        enable_qp->qp_;  // Queue pair (QP) that SEND_EN/RECV_EN WR relates to

    // if wqe_count is 0 release all WRs from queue
    sr.task.wqe_enable.wqe_count =
        enable_all ? 0 : count;  // Producer index (PI) of the QP

    auto rc = ibv_exp_post_send_wrapper(qp_, &sr, &bad_sr);

    if (enable_all) {
      struct wqe_ctrl_seg* sr_ctrl = get_wqe_by_wr_id(wr_id);
      char* seg = ((char*)sr_ctrl) + sizeof(struct wqe_ctrl_seg);
      struct wqe_wait_en_seg* sr_en_wait = (struct wqe_wait_en_seg*)seg;
      sr_en_wait->pi = htonl(count);
      // RDMA_LOG(INFO) << enable_qp->qp_->qp_num;
      // RDMA_LOG(INFO) << "qp_num: " << ntohl(sr_en_wait->obj_num);
    }

    return rc;
  }

  ConnStatus post_batch(struct ibv_send_wr* send_sr, ibv_send_wr** bad_sr_addr,
                        int num = 0) {
    return ibv_post_send_wrapper(qp_, send_sr, bad_sr_addr);
  }

  struct ibv_exp_send_wr create_exp_wr(ibv_exp_wr_opcode op, int num_sge,
                                       struct ibv_sge* sg_list, uint64_t off,
                                       uint64_t compare_add, uint64_t swap,
                                       int flags, uint64_t wr_id = 0,
                                       uint32_t imm = 0) {
    struct ibv_exp_send_wr sr {};

    // setting sr, sr has to be initialized in this style
    sr.wr_id = wr_id;
    sr.exp_opcode = op;
    sr.num_sge = num_sge;
    sr.sg_list = sg_list;
    sr.next = NULL;
    sr.exp_send_flags = flags;
    sr.ex.imm_data = imm;

    if (op == IBV_EXP_WR_ATOMIC_CMP_AND_SWP ||
        op == IBV_EXP_WR_ATOMIC_FETCH_AND_ADD) {
      sr.wr.atomic.remote_addr = remote_mr_.buf + off;
      sr.wr.atomic.rkey = remote_mr_.key;

      sr.wr.atomic.compare_add = compare_add;
      sr.wr.atomic.swap = swap;
    } else {
      sr.wr.rdma.remote_addr = remote_mr_.buf + off;
      sr.wr.rdma.rkey = remote_mr_.key;
    }

    return sr;
  }

  struct ibv_exp_send_wr create_exp_wait_wr(RRCQP* wait_qp, int flags,
                                            uint64_t wr_id = 0,
                                            bool last = false) {
    struct ibv_exp_send_wr sr {};

    sr.wr_id = wr_id;
    sr.exp_opcode = IBV_EXP_WR_CQE_WAIT;
    sr.num_sge = 0;
    sr.sg_list = NULL;
    sr.next = NULL;
    sr.exp_send_flags = flags;
    if (last) {
      sr.exp_send_flags |= IBV_EXP_SEND_WAIT_EN_LAST;
    }

    sr.ex.imm_data = 0;
    sr.task.cqe_wait.cq =
        wait_qp->cq_;  // Completion queue (CQ) that WAIT WR relates to
    sr.task.cqe_wait.cq_count = 1;  // Producer index (PI) of the CQ

    return sr;
  }

  struct ibv_exp_send_wr create_exp_enable_wr(RRCQP* enable_qp, int flags,
                                              int count, uint64_t wr_id = 0,
                                              bool last = false) {
    struct ibv_exp_send_wr sr {};

    sr.wr_id = wr_id;
    sr.exp_opcode = IBV_EXP_WR_SEND_ENABLE;
    sr.num_sge = 0;
    sr.sg_list = NULL;
    sr.next = NULL;
    sr.exp_send_flags = flags;
    if (last) {
      sr.exp_send_flags |= IBV_EXP_SEND_WAIT_EN_LAST;
    }

    sr.ex.imm_data = 0;
    sr.task.wqe_enable.qp =
        enable_qp->qp_;  // Queue pair (QP) that SEND_EN/RECV_EN WR relates to

    // if wqe_count is 0 release all WRs from queue
    sr.task.wqe_enable.wqe_count = count;  // Producer index (PI) of the QP

    return sr;
  }

  /**
   * Poll completions. These are just wrappers of ibv_poll_cq
   */
  int poll_send_completion(ibv_wc& wc) { return ibv_poll_cq(cq_, 1, &wc); }

  ConnStatus poll_till_completion(ibv_wc& wc,
                                  struct timeval timeout = default_timeout) {
    auto ret = QP::poll_till_completion(wc, timeout);
    if (ret == SUCC) {
      low_watermark_ = high_watermark_;
    }
    return ret;
  }

  /**
   * Used to count pending reqs
   * XD: current we use 64 as default, but it is rather application defined,
   * which is related to how the QP's send to are created, etc
   */
  bool need_poll(int threshold = (RCQPImpl::RC_MAX_SEND_SIZE / 2)) {
    return (high_watermark_ - low_watermark_) >= threshold;
  }

  uint64_t high_watermark_ = 0;
  uint64_t low_watermark_ = 0;

  MemoryAttr remote_mr_;

  // wq mr
  struct ibv_mr* wq_mr_;

  char* sq_start;     // indicates the base address of the SQ
  size_t sq_wqe_cnt;  // the number of wqe in the SQ
  uint32_t sq_wqe_shift;
  uint32_t sq_stride;                     // 1 << sq_wqe_stride
  std::map<uint64_t, uint32_t> sq_index;  // key: wr_id value: index
  char* sq_end;                           // indicates the end address of the SQ
  uint32_t scur_post;                     // the idx of next wqe

  bool use_offload = false;
  // qp for offload
  RRCQP* wait_qp;
};

inline constexpr UDConfig default_ud_config() {
  return UDConfig{.max_send_size = UDQPImpl::MAX_SEND_SIZE,
                  .max_recv_size = UDQPImpl::MAX_RECV_SIZE,
                  .qkey = DEFAULT_QKEY,
                  .psn = DEFAULT_PSN};
}

/**
 * Raw UD QP
 */
template <UDConfig (*F)(void) = default_ud_config, int MAX_SERVER_NUM = 16>
class RUDQP : public QP {
  // the QKEY is used to identify UD QP requests
  static const int DEFAULT_QKEY = 0xdeadbeaf;

 public:
  RUDQP(RNicHandler* rnic, QPIdx idx, MemoryAttr local_mr) : RUDQP(rnic, idx) {
    bind_local_mr(local_mr);
  }

  RUDQP(RNicHandler* rnic, QPIdx idx) : QP(rnic, idx) {
    UDQPImpl::init<F>(qp_, cq_, recv_cq_, rnic_);
    std::fill_n(ahs_, MAX_SERVER_NUM, nullptr);
  }

  bool queue_empty() { return pendings == 0; }

  bool need_poll(int threshold = UDQPImpl::MAX_SEND_SIZE / 2) {
    return pendings >= threshold;
  }

  /**
   * Simple wrapper to expose underlying QP structures
   */
  inline __attribute__((always_inline)) ibv_cq* recv_queue() {
    return recv_cq_;
  }

  inline __attribute__((always_inline)) ibv_qp* send_qp() { return qp_; }

  ConnStatus connect(std::string ip, int port, bool use_offload = false) {
    // UD QP is not bounded to a mac, so use idx to index
    return connect(ip, port, idx_, use_offload);
  }

  ConnStatus connect(std::string ip, int port, QPIdx idx, bool use_offload) {
    ConnArg arg;
    ConnReply reply;
    arg.type = ConnArg::QP;
    arg.payload.qp.from_node = idx.worker_id;
    arg.payload.qp.from_worker = idx.index;
    arg.payload.qp.qp_type = IBV_QPT_UD;

    arg.use_offload = use_offload;

    auto ret = QPImpl::get_remote_helper(&arg, &reply, ip, port);

    if (ret == SUCC) {
      // create the ah, and store the address handler
      auto ah = UDQPImpl::create_ah(rnic_, reply.payload.qp);
      if (ah == nullptr) {
        RDMA_LOG(WARNING) << "create address handler error: "
                          << strerror(errno);
        ret = ERR;
      } else {
        ahs_[reply.payload.qp.node_id] = ah;
        attrs_[reply.payload.qp.node_id] = reply.payload.qp;
      }
    }
  CONN_END:
    return ret;
  }

  /**
   * whether this UD QP has been post recved
   * a UD QP should be first been post_recved; then it can be connected w others
   */
  bool ready() { return ready_; }

  void set_ready() { ready_ = true; }

  friend class UDAdapter;

 private:
  /**
   * FIXME: curretly we have limited servers, so we use an array.
   * using a map will affect the perfomrance in microbenchmarks.
   * remove it, and merge this in UDAdapter?
   */
  struct ibv_ah* ahs_[MAX_SERVER_NUM];
  struct QPAttr attrs_[MAX_SERVER_NUM];

  // current outstanding requests which have not been polled
  int pendings = 0;

  struct ibv_cq* recv_cq_ = NULL;
  bool ready_ = false;
};

}  // end namespace rdmaio
