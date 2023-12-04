#include "lock.h"

bool checkLock(LockRead &re) {
  struct priority_lock *lock = (struct priority_lock *)re.lock_buf;
  bool high = (re.item->lock_mode == PRIORITY) ? true : false;
  uint16_t client_turn = re.client_turn;
  uint16_t low_turn_x_first = re.low_turn_x_first;
  bool low_equals_first = re.low_equals_first;
  bool high_equals_first = re.high_equals_first;

  // check whether get the lock
  if (high) {
    bool ret = (client_turn == lock->high_turn_x);
    if (low_equals_first) return ret;
    if (high_equals_first)
      ret = ret && (low_turn_x_first + 1 == lock->low_turn_x);
    return ret;
  } else {
    return (client_turn == lock->low_turn_x) &&
           (lock->high_turn_x == mask(lock->high_ticket_x));
  }
}

bool checkValid(LockRead &re) {
  struct priority_lock *lock = (struct priority_lock *)re.lock_buf;

  // check valid
  if (lock->low_turn_x > lock->low_ticket_x ||
      lock->high_turn_x > mask(lock->high_ticket_x)) {
    // RDMA_LOG(ERROR) << "lock invalid: " << lock->low_turn_x << ' '
    //                 << lock->low_ticket_x << ' ' << lock->high_turn_x << ' '
    //                 << lock->high_ticket_x;
    return false;
  }
  return true;
}

bool checkReacquire(LockRead &re) {
  struct priority_lock *lock = (struct priority_lock *)re.lock_buf;
  bool high = (re.item->lock_mode == PRIORITY) ? true : false;
  uint16_t server_turn = high ? lock->high_turn_x : lock->low_turn_x;
  uint16_t client_turn = re.client_turn;
  return client_turn < server_turn;
}

// check failures and deadlocks
bool checkConflict(LockRead &re, int64_t timeout) {
  struct timeval end_time;
  gettimeofday(&end_time, nullptr);
  int64_t time_diff = (end_time.tv_sec - re.start_time.tv_sec) * 1000000L +
                      (end_time.tv_usec - re.start_time.tv_usec);
  return time_diff > timeout;
}

uint64_t getValidVal(LockRead &re) {
  uint64_t swap = *((uint64_t *)re.lock_buf);
  struct priority_lock *swap_ptr = (struct priority_lock *)&swap;

  if (swap_ptr->low_turn_x > swap_ptr->low_ticket_x) {
    swap_ptr->low_turn_x = swap_ptr->low_ticket_x;
  }

  if (swap_ptr->high_turn_x > mask(swap_ptr->high_ticket_x)) {
    swap_ptr->high_turn_x = mask(swap_ptr->high_ticket_x);
  }

  // set visable
  swap_ptr->high_ticket_x = mask(swap_ptr->high_ticket_x);
}

uint64_t getResetVal(LockRead &re) {
  uint64_t swap = *((uint64_t *)re.lock_buf);
  struct priority_lock *swap_ptr = (struct priority_lock *)&swap;

  swap_ptr->low_turn_x = swap_ptr->low_ticket_x;
  swap_ptr->high_turn_x = mask(swap_ptr->high_ticket_x);

  // set visable
  swap_ptr->high_ticket_x = mask(swap_ptr->high_ticket_x);

  // RDMA_LOG(INFO) << "swap: " << swap_ptr->low_turn_x << ' '
  //                << swap_ptr->low_ticket_x << ' ' << swap_ptr->high_turn_x
  //                << ' ' << swap_ptr->high_ticket_x
  //                << " key: " << re.item->item_ptr->key;

  return swap;
}

int64_t getWaitTime(LockRead &re) {
  struct priority_lock *lock = (struct priority_lock *)re.lock_buf;
  bool high = (re.item->lock_mode == PRIORITY) ? true : false;
  uint16_t client_turn = re.client_turn;
  // wait for a moment
  int64_t turn_diff = high
                          ? (client_turn - lock->high_turn_x)
                          : (client_turn - lock->low_turn_x) +
                                (mask(lock->high_ticket_x) - lock->high_turn_x);

  return turn_diff * 5;
}