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

/* pgagroal */
#include <pgagroal.h>
#include <queue.h>

/*
 * Bounded lock-free MPMC queue of slot indices.
 *
 * This is Dmitry Vyukov's bounded MPMC queue
 * (https://sites.google.com/site/1024cores/home/lock-free-algorithms/queues/bounded-mpmc-queue),
 * adapted to C11 atomics and to a fixed capacity of SLOT_QUEUE_CAPACITY
 * cells so that the instance can live inside the shared memory segment of
 * struct main_configuration and be shared by all forked processes.
 *
 * Properties:
 * - FIFO: slots are handed out in the order they were released, which
 *   gives natural fairness (least recently used first) and avoids the
 *   hot-slot bias of an index scan that always starts at 0.
 * - Lock-free: every push and pop makes progress with a bounded number
 *   of steps; contention resolves through a single CAS on head/tail.
 * - Bounded and overflow-safe: the pool protocol guarantees at most one
 *   live entry per slot (see the free_queued/notinit_queued flags in
 *   struct main_configuration), and SLOT_QUEUE_CAPACITY >=
 *   MAX_NUMBER_OF_CONNECTIONS, so push cannot fail in practice.
 * - Wait-free empty/full detection: a pop on an empty queue and a push on
 *   a full queue each return in O(1) by comparing the cell sequence
 *   number with the position; nobody spins waiting for a producer or a
 *   consumer.
 *
 * Memory ordering: positions (head/tail) are handled with relaxed CAS;
 * the hand-off of the payload is ordered by the release store of the cell
 * sequence by the producer and the acquire load of the sequence by the
 * consumer. The callers additionally use seq_cst operations on
 * states[slot] and the dedup flags, which orders the queue operations
 * with the slot state transitions.
 */


void
pgagroal_queue_init(struct pgagroal_slot_queue* queue)
{
   atomic_init(&queue->head, 0);
   atomic_init(&queue->tail, 0);

   for (unsigned int i = 0; i < SLOT_QUEUE_CAPACITY; i++)
   {
      atomic_init(&queue->cells[i].sequence, i);
      queue->cells[i].slot = -1;
   }
}

bool
pgagroal_queue_push(struct pgagroal_slot_queue* queue, int slot)
{
   unsigned int pos;

   pos = atomic_load_explicit(&queue->tail, memory_order_relaxed);

   for (;;)
   {
      struct pgagroal_queue_cell* cell = &queue->cells[pos & (SLOT_QUEUE_CAPACITY - 1)];
      unsigned int seq = atomic_load_explicit(&cell->sequence, memory_order_acquire);
      /* Modular difference: valid while the drift between seq and pos is
       * below 2^31, which the one-entry-per-slot protocol guarantees.
       * A cell is ready to be WRITTEN when seq == pos and holds an
       * unconsumed entry when seq == pos + 1. */
      int dif = (int)(seq - pos);

      if (dif == 0)
      {
         /* Cell is ready for this position: claim it by advancing tail */
         if (atomic_compare_exchange_weak_explicit(&queue->tail, &pos, pos + 1,
                                                   memory_order_relaxed,
                                                   memory_order_relaxed))
         {
            cell->slot = slot;
            /* Release: makes the payload visible to the dequeueing
             * consumer, which acquire-loads this sequence value */
            atomic_store_explicit(&cell->sequence, pos + 1, memory_order_release);
            return true;
         }
         /* CAS failed: pos holds the latest tail, retry with it */
      }
      else if (dif < 0)
      {
         /* Sequence lags behind: the cell at this position has not been
          * freed by a full ring wrap yet -> queue is full */
         return false;
      }
      else
      {
         /* Another producer already claimed this position */
         pos = atomic_load_explicit(&queue->tail, memory_order_relaxed);
      }
   }
}

bool
pgagroal_queue_pop(struct pgagroal_slot_queue* queue, int* slot)
{
   unsigned int pos;

   pos = atomic_load_explicit(&queue->head, memory_order_relaxed);

   for (;;)
   {
      struct pgagroal_queue_cell* cell = &queue->cells[pos & (SLOT_QUEUE_CAPACITY - 1)];
      unsigned int seq = atomic_load_explicit(&cell->sequence, memory_order_acquire);
      /* A cell holds an unconsumed entry when seq == pos + 1; when
       * seq == pos the cell is still waiting for its producer, i.e. the
       * queue is empty for this position. */
      int dif = (int)(seq - (pos + 1));

      if (dif == 0)
      {
         /* Cell holds an unconsumed entry: claim it by advancing head */
         if (atomic_compare_exchange_weak_explicit(&queue->head, &pos, pos + 1,
                                                   memory_order_relaxed,
                                                   memory_order_relaxed))
         {
            *slot = cell->slot;
            /* Release: the cell becomes reusable for the push that will
             * target this position after a full capacity wrap */
            atomic_store_explicit(&cell->sequence, pos + SLOT_QUEUE_CAPACITY,
                                  memory_order_release);
            return true;
         }
         /* CAS failed: pos holds the latest head, retry with it */
      }
      else if (dif < 0)
      {
         /* The producer of this position has not published the payload
          * yet -> queue is empty */
         return false;
      }
      else
      {
         /* Another consumer already consumed this position */
         pos = atomic_load_explicit(&queue->head, memory_order_relaxed);
      }
   }
}

unsigned int
pgagroal_queue_size(struct pgagroal_slot_queue* queue)
{
   unsigned int tail = atomic_load_explicit(&queue->tail, memory_order_relaxed);
   unsigned int head = atomic_load_explicit(&queue->head, memory_order_relaxed);

   /* Modular arithmetic: correct while the drift is below 2^31 */
   return tail - head;
}
