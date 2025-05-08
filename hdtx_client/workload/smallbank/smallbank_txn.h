// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <memory>

#include "dtx/dtx.h"
#include "smallbank/smallbank_db.h"

/**
 * return:
 *  -1: business fail
 *   0: tx fail
 *   1: tx success
 */

/******************** The business logic (Transaction) start
 * ********************/

int TxAmalgamate(SmallBank* smallbank_client, uint64_t seed,
                 coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
/* Calculate the sum of saving and checking kBalance */
int TxBalance(SmallBank* smallbank_client, uint64_t seed, coro_yield_t& yield,
              tx_id_t tx_id, DTX* dtx);
/* Add $1.3 to acct_id's checking account */
int TxDepositChecking(SmallBank* smallbank_client, uint64_t seed,
                      coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
int TxSendPayment(SmallBank* smallbank_client, uint64_t seed,
                  coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
/* Add $20 to acct_id's saving's account */
int TxTransactSaving(SmallBank* smallbank_client, uint64_t seed,
                     coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
/* Read saving and checking kBalance + update checking kBalance unconditionally
 */
int TxWriteCheck(SmallBank* smallbank_client, uint64_t seed,
                 coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
/******************** The business logic (Transaction) end ********************/