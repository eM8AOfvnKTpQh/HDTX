// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <memory>

#include "dtx/dtx.h"
#include "tpcc/tpcc_db.h"

/**
 * return:
 *  -1: business fail
 *   0: tx fail
 *   1: tx success
 */

/******************** The business logic (Transaction) start
 * ********************/

// The following transaction business logics are referred to the standard TPCC
// specification.

/* TPC BENCHMARK™ C
** Standard Specification
** Revision 5.11
** February 2010
** url: http://tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf
*/

// Note: Remote hash slot limits the insertion number. For a 20-slot bucket, the
// uppper bound is 44744 new order.
int TxNewOrder(TPCC* tpcc_client, FastRandom* random_generator,
               coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
int TxPayment(TPCC* tpcc_client, FastRandom* random_generator,
              coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
int TxDelivery(TPCC* tpcc_client, FastRandom* random_generator,
               coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
int TxOrderStatus(TPCC* tpcc_client, FastRandom* random_generator,
                  coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
int TxStockLevel(TPCC* tpcc_client, FastRandom* random_generator,
                 coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
/******************** The business logic (Transaction) end ********************/