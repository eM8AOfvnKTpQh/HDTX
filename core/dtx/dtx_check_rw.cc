#include "dtx/dtx.h"
#include "util/timer.h"

/*-------------------------------------------------------------------------------------------*/

bool DTX::CheckCasRW(std::list<LockRead>& pending_lock_rw,
                     std::list<HashRead>& pending_next_hash_rw,
                     std::list<InsertOffRead>& pending_next_off_rw) {
  for (auto& re : pending_lock_rw) {
    if (*((lock_t*)re.lock_buf) != STATE_CLEAN) {
      return false;
    }

    auto it = re.item->item_ptr;
    auto* fetched_item = (DataItem*)(re.data_buf);
    if (likely(fetched_item->key == it->key &&
               fetched_item->table_id == it->table_id)) {
      if (it->user_insert) {
        // Insert or update (insert an exsiting key)
        if (it->version < fetched_item->version) return false;
        old_version_for_insert.push_back(
            OldVersionForInsert{.table_id = it->table_id,
                                .key = it->key,
                                .version = fetched_item->version});
      } else {
        // Update or deletion
        if (likely(fetched_item->valid)) {
          if (tx_id < fetched_item->version) return false;
          *it = *fetched_item;  // Get old data
        } else {
          // The item is deleted before, then update the local cache
          addr_cache->Insert(re.primary_node_id, it->table_id, it->key,
                             NOT_FOUND);
          return false;
        }
      }
      // The item must be visible because we can lock it
      re.item->is_fetched = true;
    } else {
      // The cached address is stale

      // 1. Release lock
      *((lock_t*)re.lock_buf) = STATE_CLEAN;
      if (!coro_sched->RDMAWrite(coro_id, re.qp, re.lock_buf,
                                 it->GetRemoteLockAddr(), sizeof(lock_t)))
        return false;

      // TODO: Whether to reacquire lock

      // 2. Read via hash
      const HashMeta& meta =
          global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
      uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
      offset_t node_off = idx * meta.node_size + meta.base_off;
      auto* local_hash_node =
          (HashNode*)thread_rdma_buffer_alloc->Alloc(sizeof(HashNode));
      if (it->user_insert) {
        pending_next_off_rw.emplace_back(
            InsertOffRead{.qp = re.qp,
                          .item = re.item,
                          .buf = (char*)local_hash_node,
                          .remote_node = re.primary_node_id,
                          .meta = meta,
                          .node_off = node_off});
      } else {
        pending_next_hash_rw.emplace_back(
            HashRead{.qp = re.qp,
                     .item = re.item,
                     .buf = (char*)local_hash_node,
                     .remote_node = re.primary_node_id,
                     .meta = meta});
      }
      if (!coro_sched->RDMARead(coro_id, re.qp, (char*)local_hash_node,
                                node_off, sizeof(HashNode)))
        return false;
    }
  }
  return true;
}
/*----------------------------------------------------------------------------------*/

bool DTX::CheckLockRW(std::list<LockRead>& pending_lock_rw,
                      std::list<LockRead>& pending_locked_rw, char* cas_buf) {
  for (auto iter = pending_lock_rw.begin(); iter != pending_lock_rw.end();) {
    auto& set_it = *iter;
    auto it = set_it.item->item_ptr;
#if USE_CAS
    if (*((lock_t*)set_it.lock_buf) != STATE_CLEAN) {
      return false;
    }
#else
    struct priority_lock* lock = (struct priority_lock*)set_it.lock_buf;
    bool high = (set_it.item->lock_mode == PRIORITY) ? true : false;
    if (set_it.client_turn == -1) {  // First initialization
      set_it.client_turn =
          high ? mask(lock->high_ticket_x) : lock->low_ticket_x;
      // Used to check lock state
      set_it.low_turn_x_first = lock->low_turn_x;
      set_it.low_equals_first = (lock->low_turn_x == lock->low_ticket_x);
      set_it.high_equals_first =
          (lock->high_turn_x == mask(lock->high_ticket_x));
      // Used to check conflict
      set_it.low_turn_x_old = lock->low_turn_x;
      set_it.high_turn_x_old = lock->high_turn_x;
    }

    // RDMA_LOG(INFO) << "key: " << set_it.item->item_ptr->key
    //                << " lock: " << lock->low_turn_x << ' '
    //                << lock->low_ticket_x << ' ' << lock->high_turn_x << ' '
    //                << lock->high_ticket_x << " addr: "
    //                << set_it.qp->remote_mr_.buf + it->GetRemoteLockAddr()
    //                << " tx_id: " << tx_id;

    if (!checkLock(set_it)) {     // Check if the lock has been acquired
      if (!checkValid(set_it)) {  // If lock state is invalid, try to reset it
        // reset
        uint64_t compare = *((uint64_t*)set_it.lock_buf);
        uint64_t swap = getValidVal(set_it);
        if (!coro_sched->RDMACAS(coro_id, set_it.qp, cas_buf,
                                 it->GetRemoteLockAddr(), compare, swap)) {
          return false;
        }
      } else if (checkReacquire(set_it)) {  // Check whether the lock needs to
                                            // be reacquired.
        // RDMA_LOG(WARNING) << "Reacquire lock.";
        uint64_t add_value;
        if (set_it.item->lock_mode == PRIORITY) {
          add_value = HIGH_TICKET_X_ADD;
        } else {
          add_value = LOW_TICKET_X_ADD;
          set_it.item->lock_mode = NORMAL;
        }

        // Reset time and client_turn.
        gettimeofday(&set_it.start_time, nullptr);
        set_it.client_turn = -1;
        // Re-read data
        set_it.re_read = true;

        if (!coro_sched->RDMAFAA(coro_id, set_it.qp, set_it.lock_buf,
                                 it->GetRemoteLockAddr(), add_value)) {
          return false;
        }
        iter++;
        continue;
      }
      if (checkConflict(set_it, 2 * LEASE_TIME)) {  // If a conflict occurs, try
                                                    // to reset lock state.
        // RDMA_LOG(WARNING) << "Lock conflict.";

        uint64_t compare = *((uint64_t*)set_it.lock_buf);
        uint64_t swap = getResetVal(set_it);

        // RDMA_LOG(INFO) << "cas"
        //                << " key: " << set_it.item->item_ptr->key;
        if (!coro_sched->RDMACAS(coro_id, set_it.qp, cas_buf,
                                 it->GetRemoteLockAddr(), compare, swap)) {
          return false;
        }

        // RDMA_LOG(INFO) << "key: " << set_it.item->item_ptr->key
        //                << " lock: " << lock->low_turn_x << ' '
        //                << lock->low_ticket_x << ' ' << lock->high_turn_x
        //                << ' ' << lock->high_ticket_x
        //                << " addr: " << it->GetRemoteLockAddr()
        //                << " client_turn: " << set_it.client_turn
        //                << " tx_id: " << tx_id;

        // Release the lock held by the transaction.
        for (auto& lock_it : pending_locked_rw) {
          // RDMA_LOG(INFO)
          //     << "release, key: " << lock_it.item->item_ptr->key
          //     << " addr: " <<
          //     lock_it.item->item_ptr->GetRemoteLockAddr();

          uint64_t add_value = (lock_it.item->lock_mode == PRIORITY)
                                   ? HIGH_TURN_X_ADD
                                   : LOW_TURN_X_ADD;
          if (!coro_sched->RDMAFAA(coro_id, lock_it.qp, lock_it.lock_buf,
                                   lock_it.item->item_ptr->GetRemoteLockAddr(),
                                   add_value)) {
            return false;
          }
        }
        // Abort the transaction.
        return false;
      }
      // Re-read data
      set_it.re_read = true;
      // Re-read lock
      if (!coro_sched->RDMARead(coro_id, set_it.qp, set_it.lock_buf,
                                it->GetRemoteLockAddr(), sizeof(lock_t))) {
        return false;
      }

      iter++;
      continue;
    }
#endif

    pending_locked_rw.emplace_back(set_it);  // Lock successfully.
    iter = pending_lock_rw.erase(iter);      // Point to next item.
  }

  for (auto iter = not_locked_rw_set.begin();
       iter != not_locked_rw_set.end();) {
    auto index = *iter;
    auto& set_it = read_write_set[index];
    if (!set_it.is_fetched) {
      iter++;
      continue;
    }
    // If fetched, try to get the lock.
    char* lock_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
    auto it = set_it.item_ptr;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(set_it.read_which_node);

    struct timeval start_time;
    gettimeofday(&start_time, nullptr);
    pending_lock_rw.emplace_back(
        LockRead{.index = index,
                 .qp = qp,
                 .item = &set_it,
                 .lock_buf = lock_buf,
                 .data_buf = data_buf,
                 .primary_node_id = set_it.read_which_node,
                 .start_time = start_time,
                 .client_turn = -1,
                 .re_read = false});

    std::shared_ptr<LockReadBatch> doorbell = std::make_shared<LockReadBatch>();
    LockType type;
    uint64_t compare_add, swap;

#if USE_CAS
    locked_rw_set.emplace_back(index);
    type = CAS;
    compare_add = STATE_CLEAN;
    swap = u_id;
#else
    type = FAA;
    compare_add =
        (set_it.lock_mode == PRIORITY) ? HIGH_TICKET_X_ADD : LOW_TICKET_X_ADD;
    swap = 0;
#endif

    doorbell->SetLockReq(lock_buf, it->GetRemoteLockAddr(), type, compare_add,
                         swap);
    doorbell->SetReadReq(data_buf, it->remote_offset, DataItemSize);
    if (!doorbell->SendReqs(coro_sched, qp, coro_id)) {
      return false;
    }
    iter = not_locked_rw_set.erase(iter);  // Point to next item.
  }

  return true;
}

bool DTX::CheckValidRW(std::list<LockRead>& pending_locked_rw) {
#if !USE_CAS
  for (auto& set_it : pending_locked_rw) {
    locked_rw_set.emplace_back(set_it.index);
  }

  // for (auto& set_it : pending_locked_rw) {
  //   struct timeval end_time;
  //   gettimeofday(&end_time, nullptr);
  //   uint64_t diff = (end_time.tv_sec - set_it.start_time.tv_sec) * 1000000 +
  //                   (end_time.tv_usec - set_it.start_time.tv_usec);
  //   if (diff > LEASE_TIME) {
  //     return false;
  //   }
  // }
#endif

  // Check valid
  for (auto& set_it : pending_locked_rw) {
    auto* fetched_item = (DataItem*)(set_it.data_buf);
    auto it = set_it.item->item_ptr;
    if (likely(fetched_item->key == it->key &&
               fetched_item->table_id == it->table_id)) {
      if (it->user_insert) {  // Insert or update (insert an exsiting key)
        // TODO: Whether to compare version here
        // if (it->version < fetched_item->version) return false;

        // Used to compare version during validate phase for insertion
        old_version_for_insert.push_back(
            OldVersionForInsert{.table_id = it->table_id,
                                .key = it->key,
                                .version = fetched_item->version});
      } else {
        // RDMA_LOG(WARNING) << "Update or deletion";

        if (likely(fetched_item->valid)) {
          // if (tx_id < fetched_item->version) {
          //   // RDMA_LOG(ERROR) << "Version is too old.";
          //   return false;
          // }
          *it = *fetched_item;  // Update item
        } else {
          // RDMA_LOG(ERROR) << "The item is deleted before.";

          // The item is deleted before, then update the local cache
          addr_cache->Insert(set_it.primary_node_id, it->table_id, it->key,
                             NOT_FOUND);
          return false;
        }
      }
      // The item must be visible because we can lock it
      set_it.item->is_fetched = true;
    } else {  // It's unlikely to happen
      // RDMA_LOG(ERROR) << "The cached address is stale";
      return false;
    }
  }
  return true;
}

int DTX::FindMatchSlot(HashRead& res,
                       std::list<InvisibleRead>& pending_invisible_ro) {
  auto* local_hash_node = (HashNode*)res.buf;
  auto* it = res.item->item_ptr.get();
  bool find = false;

  for (auto& item : local_hash_node->data_items) {
    if (item.valid && item.key == it->key && item.table_id == it->table_id) {
      // if (tx_id < item.version) return VERSION_TOO_OLD;
      *it = item;
      addr_cache->Insert(res.remote_node, it->table_id, it->key,
                         it->remote_offset);
      res.item->is_fetched = true;
      find = true;
      break;
    }
  }
  if (likely(find)) {
#if LOCK_REFUSE_READ_RW
    if (it->lock == u_id) return SLOT_LOCKED;
#else
    if (unlikely((it->lock & STATE_INVISIBLE))) {
#if INV_ABORT
      return SLOT_INV;
#endif
      // This item is invisible, we need re-read
      char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
      uint64_t lock_offset = it->GetRemoteLockAddr(it->remote_offset);
      pending_invisible_ro.emplace_back(
          InvisibleRead{.qp = res.qp, .buf = cas_buf, .off = lock_offset});
      if (!coro_sched->RDMARead(coro_id, res.qp, cas_buf, lock_offset,
                                sizeof(lock_t)))
        return SLOT_NOT_FOUND;
    }
#endif
    return SLOT_FOUND;
  }
  return SLOT_NOT_FOUND;
}

bool DTX::CheckHashRW(std::vector<HashRead>& pending_hash_rw,
                      std::list<InvisibleRead>& pending_invisible_ro,
                      std::list<HashRead>& pending_next_hash_rw) {
  // Check results from hash read
  for (auto& res : pending_hash_rw) {
    auto rc = FindMatchSlot(res, pending_invisible_ro);
    if (rc == SLOT_NOT_FOUND) {
      auto* local_hash_node = (HashNode*)res.buf;
      if (local_hash_node->next == nullptr) {
        tx_error = TXError::ITEM_NOT_FOUND;
        return false;
      }
      // Not found, we need to re-read the next bucket
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr +
                      res.meta.base_off;
      pending_next_hash_rw.emplace_back(HashRead{.qp = res.qp,
                                                 .item = res.item,
                                                 .buf = res.buf,
                                                 .remote_node = res.remote_node,
                                                 .meta = res.meta});
      if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off,
                                sizeof(HashNode)))
        return false;
    } else if (rc == VERSION_TOO_OLD || rc == SLOT_INV || rc == SLOT_LOCKED) {
      return false;
    }
  }
  return true;
}

bool DTX::CheckNextHashRW(std::list<InvisibleRead>& pending_invisible_ro,
                          std::list<HashRead>& pending_next_hash_rw) {
  for (auto iter = pending_next_hash_rw.begin();
       iter != pending_next_hash_rw.end();) {
    auto res = *iter;
    auto rc = FindMatchSlot(res, pending_invisible_ro);
    if (rc == SLOT_FOUND)
      iter = pending_next_hash_rw.erase(iter);
    else if (rc == SLOT_NOT_FOUND) {
      auto* local_hash_node = (HashNode*)res.buf;
      // The item does not exist
      if (local_hash_node->next == nullptr) {
        tx_error = TXError::ITEM_NOT_FOUND;
        return false;
      }
      // Read the next bucket
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr +
                      res.meta.base_off;
      if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off,
                                sizeof(HashNode)))
        return false;
      iter++;
    } else if (rc == VERSION_TOO_OLD || rc == SLOT_INV || rc == SLOT_LOCKED) {
      return false;
    }
  }
  return true;
}

/*------------------------------------------------------------------------------*/
int DTX::FindInsertOff(InsertOffRead& res,
                       std::list<InvisibleRead>& pending_invisible_ro) {
  offset_t possible_insert_position = OFFSET_NOT_FOUND;
  version_t old_version;
  auto* local_hash_node = (HashNode*)res.buf;
  auto it = res.item->item_ptr;
  for (int i = 0; i < ITEM_NUM_PER_NODE; i++) {
    auto& data_item = local_hash_node->data_items[i];
    if (possible_insert_position == OFFSET_NOT_FOUND && !data_item.valid &&
        data_item.lock == STATE_CLEAN) {
      // Within a txn, multiple items cannot insert into one slot
      std::pair<node_id_t, offset_t> new_pos(res.remote_node,
                                             res.node_off + i * DataItemSize);
      if (inserted_pos.find(new_pos) != inserted_pos.end()) {
        continue;
      } else {
        inserted_pos.insert(new_pos);
      }
      // We only need one possible empty and clean slot to insert.
      // This case is entered only once
      possible_insert_position = res.node_off + i * DataItemSize;
      old_version = data_item.version;
    } else if (data_item.valid && data_item.key == it->key &&
               data_item.table_id == it->table_id) {
      // Find an item that matches. It is actually an update
      if (it->version < data_item.version) {
        return VERSION_TOO_OLD;
      }
      possible_insert_position = res.node_off + i * DataItemSize;
      addr_cache->Insert(res.remote_node, it->table_id, it->key,
                         possible_insert_position);
      old_version = data_item.version;
      it->lock = data_item.lock;
      break;
    }
  }
  // After searching the available insert offsets
  if (possible_insert_position != OFFSET_NOT_FOUND) {
    // There is no need to back up the old data for the first time insertion.
    // Therefore, during recovery, if there is no old backups for some data in
    // remote memory pool, it indicates that this is caused by an insertion.
#if LOCK_REFUSE_READ_RW
    if (it->lock == u_id) return SLOT_LOCKED;
    it->remote_offset = possible_insert_position;
    old_version_for_insert.push_back(OldVersionForInsert{
        .table_id = it->table_id, .key = it->key, .version = old_version});
#else
    it->remote_offset = possible_insert_position;
    old_version_for_insert.push_back(OldVersionForInsert{
        .table_id = it->table_id, .key = it->key, .version = old_version});
    if (unlikely((it->lock & STATE_INVISIBLE))) {
#if INV_ABORT
      return SLOT_INV;
#endif
      // This item is invisible, we need re-read
      char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
      uint64_t lock_offset = it->GetRemoteLockAddr(it->remote_offset);
      pending_invisible_ro.emplace_back(
          InvisibleRead{.qp = res.qp, .buf = cas_buf, .off = lock_offset});
      if (!coro_sched->RDMARead(coro_id, res.qp, cas_buf, lock_offset,
                                sizeof(lock_t)))
        return OFFSET_NOT_FOUND;
    }
#endif
    res.item->is_fetched = true;
    return OFFSET_FOUND;
  }
  return OFFSET_NOT_FOUND;
}

bool DTX::CheckInsertOffRW(std::vector<InsertOffRead>& pending_insert_off_rw,
                           std::list<InvisibleRead>& pending_invisible_ro,
                           std::list<InsertOffRead>& pending_next_off_rw) {
  // Check results from offset read
  for (auto& res : pending_insert_off_rw) {
    auto rc = FindInsertOff(res, pending_invisible_ro);
    if (rc == OFFSET_NOT_FOUND) {
      auto* local_hash_node = (HashNode*)res.buf;
      if (local_hash_node->next == nullptr) {
        tx_error = TXError::ITEM_NOT_FOUND;
        return false;
      }
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr +
                      res.meta.base_off;
      pending_next_off_rw.emplace_back(
          InsertOffRead{.qp = res.qp,
                        .item = res.item,
                        .buf = res.buf,
                        .remote_node = res.remote_node,
                        .meta = res.meta,
                        .node_off = res.node_off});
      if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off,
                                sizeof(HashNode)))
        return false;
    } else if (rc == VERSION_TOO_OLD || rc == SLOT_INV || rc == SLOT_LOCKED) {
      return false;
    }
  }
  return true;
}

bool DTX::CheckNextOffRW(std::list<InvisibleRead>& pending_invisible_ro,
                         std::list<InsertOffRead>& pending_next_off_rw) {
  for (auto iter = pending_next_off_rw.begin();
       iter != pending_next_off_rw.end();) {
    auto& res = *iter;
    auto rc = FindInsertOff(res, pending_invisible_ro);
    if (rc == OFFSET_FOUND) {
      iter = pending_next_off_rw.erase(iter);
    } else if (rc == OFFSET_NOT_FOUND) {
      auto* local_hash_node = (HashNode*)res.buf;
      if (local_hash_node->next == nullptr) {
        tx_error = TXError::ITEM_NOT_FOUND;
        return false;
      }
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr +
                      res.meta.base_off;
      if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off,
                                sizeof(HashNode)))
        return false;
      iter++;
    } else if (rc == VERSION_TOO_OLD || rc == SLOT_INV || rc == SLOT_LOCKED) {
      return false;
    }
  }
  return true;
}
