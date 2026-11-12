/*
 * Copyright (C) 2026 The pgagroal community
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors may
 * be used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef PGAGROAL_QUEUE_H
#define PGAGROAL_QUEUE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <pgagroal.h>

#include <stdbool.h>

/**
 * Initialize a slot index queue for use.
 *
 * Must be called exactly once on the shared memory instance, before any
 * process pushes or pops (pgagroal_pool_init does this before forking
 * the workers).
 *
 * @param queue The queue
 */
void
pgagroal_queue_init(struct pgagroal_slot_queue* queue);

/**
 * Push a slot index onto the queue (multi-producer safe).
 *
 * Lock-free; never blocks. With the one-entry-per-slot flag protocol
 * maintained by the callers, the queue can never overflow, but the
 * function still returns false if the ring is full (defensive).
 *
 * @param queue The queue
 * @param slot The slot index
 * @returns true if the index was enqueued, false if the queue was full
 */
bool
pgagroal_queue_push(struct pgagroal_slot_queue* queue, int slot);

/**
 * Pop a slot index from the queue (multi-consumer safe).
 *
 * Lock-free; never blocks. Each successfully popped entry is delivered
 * to exactly one caller.
 *
 * @param queue The queue
 * @param slot The popped slot index
 * @returns true if an index was dequeued, false if the queue was empty
 */
bool
pgagroal_queue_pop(struct pgagroal_slot_queue* queue, int* slot);

/**
 * Approximate number of entries currently in the queue.
 *
 * Under concurrency the value is a snapshot; used for logging/metrics only.
 *
 * @param queue The queue
 * @returns the number of entries (head to tail distance)
 */
unsigned int
pgagroal_queue_size(struct pgagroal_slot_queue* queue);

#ifdef __cplusplus
}
#endif

#endif
