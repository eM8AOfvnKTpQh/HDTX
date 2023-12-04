#include "dtx/dtx.h"
#include "util/timer.h"

bool DTX::CheckReadRO(std::vector<DirectRead>& pending_direct_ro,
                      std::vector<HashRead>& pending_hash_ro,
                      std::list<InvisibleRead>& pending_invisible_ro,
                      std::list<HashRead>& pending_next_hash_ro,
                      coro_yield_t& yield) {
  if (!CheckDirectRO(pending_direct_ro, pending_invisible_ro,
                     pending_next_hash_ro)) {
    // RDMA_LOG(ERROR) << "fail";
    return false;
  }
  if (!CheckHashRO(pending_hash_ro, pending_invisible_ro,
                   pending_next_hash_ro)) {
    // RDMA_LOG(ERROR) << "fail";
    return false;
  }

  // During results checking, we may re-read data due to invisibility and hash
  // collisions
  while (!pending_invisible_ro.empty() || !pending_next_hash_ro.empty()) {
    // RDMA_LOG(INFO) << "check";
    coro_sched->Yield(yield, coro_id);
    if (!CheckInvisibleRO(pending_invisible_ro)) {
      // RDMA_LOG(ERROR) << "fail";
      return false;
    }
    if (!CheckNextHashRO(pending_invisible_ro, pending_next_hash_ro)) {
      // RDMA_LOG(ERROR) << "fail";
      return false;
    }
  }

  return true;
}

bool DTX::CheckReadRORW(std::vector<DirectRead>& pending_direct_ro,
                        std::vector<HashRead>& pending_hash_ro,
                        std::vector<HashRead>& pending_hash_rw,
                        std::vector<InsertOffRead>& pending_insert_off_rw,
                        std::vector<LockRead>& pending_lock_rw,
                        std::list<InvisibleRead>& pending_invisible_ro,
                        std::list<HashRead>& pending_next_hash_ro,
                        std::list<HashRead>& pending_next_hash_rw,
                        std::list<InsertOffRead>& pending_next_off_rw,
                        coro_yield_t& yield) {
  // check read-only results
  if (!CheckDirectRO(pending_direct_ro, pending_invisible_ro,
                     pending_next_hash_ro)) {
    // RDMA_LOG(ERROR) << "fail";
    return false;
  }
  if (!CheckHashRO(pending_hash_ro, pending_invisible_ro,
                   pending_next_hash_ro)) {
    // RDMA_LOG(ERROR) << "fail";
    return false;
  }

  // The reason to use separate CheckHashRO and CheckHashRW: We need to compare
  // txid with the fetched id in read-write txn check read-write results
  if (!CheckHashRW(pending_hash_rw, pending_invisible_ro,
                   pending_next_hash_rw)) {
    // RDMA_LOG(ERROR) << "fail";
    return false;
  }

  if (!CheckInsertOffRW(pending_insert_off_rw, pending_invisible_ro,
                        pending_next_off_rw)) {
    // RDMA_LOG(ERROR) << "fail";
    return false;
  }

  std::vector<LockRead> pending_locked_rw;
  char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));

  // During results checking, we may re-read data due to invisibility and hash
  // collisions
  while (!pending_invisible_ro.empty() || !pending_next_hash_ro.empty() ||
         !pending_next_hash_rw.empty() || !pending_next_off_rw.empty() ||
         !pending_lock_rw.empty() || !not_locked_rw_set.empty()) {
    // RDMA_LOG(INFO) << "check rw: "
    //                << "tid: " << t_id;
    coro_sched->Yield(yield, coro_id);

    // Recheck read-only replies
    if (!CheckInvisibleRO(pending_invisible_ro)) {
      return false;
    }

    if (!CheckNextHashRO(pending_invisible_ro, pending_next_hash_ro)) {
      RDMA_LOG(ERROR) << "fail";
      exit(-1);
      return false;
    }

    // Recheck read-write replies
    if (!CheckNextHashRW(pending_invisible_ro, pending_next_hash_rw)) {
      RDMA_LOG(ERROR) << "fail";
      exit(-1);
      return false;
    }

    if (!CheckNextOffRW(pending_invisible_ro, pending_next_off_rw)) {
      RDMA_LOG(ERROR) << "fail";
      exit(-1);
      return false;
    }

    if (!CheckLockRW(pending_lock_rw, pending_locked_rw, cas_buf)) {
      // RDMA_LOG(ERROR) << "fail";
      return false;
    }
  }

  if (!CheckValidRW(pending_locked_rw)) {
    // RDMA_LOG(ERROR) << "fail";
    return false;
  }

  return true;
}

bool DTX::CheckValidate(std::vector<ValidateRead>& pending_validate) {
  // Check version
  for (auto& re : pending_validate) {
    auto it = re.item->item_ptr;
    // Compare version
    if (it->version != *((version_t*)re.version_buf)) {
      // RDMA_LOG(DBG) << "MY VERSION " << it->version;
      // RDMA_LOG(DBG) << "version_buf " << *((version_t*)re.version_buf);
      return false;
    }
  }
  return true;
}

bool DTX::CheckCommitAll(std::vector<CommitWrite>& pending_commit_write,
                         char* cas_buf) {
  // Release: set visible and unlock remote data
  for (auto& re : pending_commit_write) {
    auto* qp = thread_qp_man->GetRemoteDataQPWithNodeID(re.node_id);
    // qp->post_send(IBV_WR_RDMA_WRITE, cas_buf, sizeof(lock_t), re.lock_off,
    //               0);  // Release
    qp->post_cas(cas_buf, re.lock_off,
                 encode_id(t_id, coro_id) | STATE_INVISIBLE, STATE_CLEAN, 0);
  }
  return true;
}