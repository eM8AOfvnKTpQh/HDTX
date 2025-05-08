#pragma once

#include "base/common.h"
#include "dtx/structs.h"
#include "scheduler/corotine_scheduler.h"

#define LOW_TURN_X_ADD 0x0000000000000001
#define LOW_TICKET_X_ADD 0x0000000000010000
#define HIGH_TURN_X_ADD 0x0000000100000000
#define HIGH_TICKET_X_ADD 0x0001000000000000

#define HIGH_TICKET_MAX 32768
#define LOW_TICKET_MAX 32768

#define mask(x) (x & 0x7FFF)

enum LockStatus {
  LOCK_SUCCESS,
  LOCK_RETRY,
  LOCK_TIMEOUT,
  LOCK_OVERFLOW,
  LOCK_ERROR
};

struct priority_lock {
  uint16_t low_turn_x;
  uint16_t low_ticket_x;
  uint16_t high_turn_x;
  uint16_t high_ticket_x;
};

bool checkLock(LockRead &re);
bool checkValid(LockRead &re);
bool checkReacquire(LockRead &re);
bool checkConflict(LockRead &re, int64_t timeout);
uint64_t getValidVal(LockRead &re);
uint64_t getResetVal(LockRead &re);
int64_t getWaitTime(LockRead &re);