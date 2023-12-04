// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <memory>

#include "dtx/dtx.h"
#include "tatp/tatp_db.h"

/**
 * return:
 *  -1: business fail
 *   0: tx fail
 *   1: tx success
 */

/******************** The business logic (Transaction) start
 * ********************/

// Read 1 SUBSCRIBER row
int TxGetSubsciberData(TATP* tatp_client, uint64_t seed, coro_yield_t& yield,
                       tx_id_t tx_id, DTX* dtx);

// 1. Read 1 SPECIAL_FACILITY row
// 2. Read up to 3 CALL_FORWARDING rows
// 3. Validate up to 4 rows
int TxGetNewDestination(TATP* tatp_client, uint64_t seed, coro_yield_t& yield,
                        tx_id_t tx_id, DTX* dtx);

// Read 1 ACCESS_INFO row
int TxGetAccessData(TATP* tatp_client, uint64_t seed, coro_yield_t& yield,
                    tx_id_t tx_id, DTX* dtx);

// Update 1 SUBSCRIBER row and 1 SPECIAL_FACILTY row
int TxUpdateSubscriberData(TATP* tatp_client, uint64_t seed,
                           coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Update a SUBSCRIBER row
int TxUpdateLocation(TATP* tatp_client, uint64_t seed, coro_yield_t& yield,
                     tx_id_t tx_id, DTX* dtx);

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Read a SPECIAL_FACILTY row
// 3. Insert a CALL_FORWARDING row
int TxInsertCallForwarding(TATP* tatp_client, uint64_t seed,
                           coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Delete a CALL_FORWARDING row
int TxDeleteCallForwarding(TATP* tatp_client, uint64_t seed,
                           coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);

/******************** The business logic (Transaction) end ********************/