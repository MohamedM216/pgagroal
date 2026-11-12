/*
 * Copyright (C) 2026 The pgagroal community
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or other
 * materials provided with the distribution.
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
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */
#include <pgagroal.h>
#include <queue.h>
#include <mctf.h>

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>

/*
 * Unit tests for the bounded lock-free MPMC slot queue
 * (pgagroal_queue_push / pgagroal_queue_pop)
 *
 * The queue is a pure shared-memory data structure, so every property can
 * be verified without a PostgreSQL server: FIFO order, capacity, ring
 * wrap-around and multi-producer/multi-consumer integrity under stress.
 */

#define TEST_QUEUE_SLOTS 64
#define TEST_QUEUE_OPS 64000 /* per producer; 2 producers -> each of the 64 slot indices appears 2000 times */

static struct pgagroal_slot_queue stress_queue;
static atomic_int stress_produced;
static atomic_int stress_consumed_sum;
static atomic_bool stress_corrupt;
static int stress_nprod = 2;
static int stress_ncons = 2;

static void*
stress_producer(void* arg)
{
   (void)arg;
   for (int i = 0; i < TEST_QUEUE_OPS; i++)
   {
      int slot = atomic_fetch_add(&stress_produced, 1) % TEST_QUEUE_SLOTS;
      while (!pgagroal_queue_push(&stress_queue, slot))
      {
         /* Full ring: test-only backpressure */
         sched_yield();
      }
   }
   return NULL;
}

static void*
stress_consumer(void* arg)
{
   (void)arg;
   int slot;
   for (int i = 0; i < TEST_QUEUE_OPS * stress_nprod / stress_ncons; i++)
   {
      while (!pgagroal_queue_pop(&stress_queue, &slot))
      {
         sched_yield();
      }
      if (slot < 0 || slot >= TEST_QUEUE_SLOTS)
      {
         /* Flag the corruption and keep consuming so every producer can
          * finish; the test body asserts the flag below. */
         atomic_store(&stress_corrupt, true);
      }
      else
      {
         atomic_fetch_add(&stress_consumed_sum, slot);
      }
   }
   return NULL;
}

/* An empty queue must report empty, not hand out a bogus index. */
MCTF_TEST(test_queue_empty_pop)
{
   struct pgagroal_slot_queue queue;
   int slot = 42;

   pgagroal_queue_init(&queue);

   MCTF_ASSERT(pgagroal_queue_pop(&queue, &slot) == false, cleanup, "pop on an empty queue must return false");
   MCTF_ASSERT_INT_EQ(slot, 42, cleanup, "pop on an empty queue must not overwrite the output");

cleanup:
   MCTF_FINISH();
}

/* Capacity, FIFO order and drain semantics on a single thread. */
MCTF_TEST(test_queue_fifo_capacity)
{
   struct pgagroal_slot_queue queue;
   int slot;

   pgagroal_queue_init(&queue);

   for (int i = 0; i < SLOT_QUEUE_CAPACITY; i++)
   {
      MCTF_ASSERT(pgagroal_queue_push(&queue, i % TEST_QUEUE_SLOTS), cleanup, "push into a non-full ring must succeed");
   }

   MCTF_ASSERT(!pgagroal_queue_push(&queue, 999), cleanup, "push beyond capacity must be rejected");
   MCTF_ASSERT_INT_EQ((int)pgagroal_queue_size(&queue), SLOT_QUEUE_CAPACITY, cleanup, "size must report the ring capacity");

   for (int i = 0; i < SLOT_QUEUE_CAPACITY; i++)
   {
      MCTF_ASSERT(pgagroal_queue_pop(&queue, &slot), cleanup, "pop from a non-empty ring must succeed");
      MCTF_ASSERT_INT_EQ(slot, i % TEST_QUEUE_SLOTS, cleanup, "FIFO order must be preserved");
   }

   MCTF_ASSERT(!pgagroal_queue_pop(&queue, &slot), cleanup, "pop after draining must return false");

cleanup:
   MCTF_FINISH();
}

/* The sequence numbers must survive full ring wrap-around cycles. */
MCTF_TEST(test_queue_wrap_around)
{
   struct pgagroal_slot_queue queue;
   int slot;
   int rounds = 3 * (SLOT_QUEUE_CAPACITY / TEST_QUEUE_SLOTS) + 5;

   pgagroal_queue_init(&queue);

   for (int round = 0; round < rounds; round++)
   {
      for (int i = 0; i < TEST_QUEUE_SLOTS; i++)
      {
         MCTF_ASSERT(pgagroal_queue_push(&queue, i), cleanup, "wrap-around push must succeed");
      }
      for (int i = 0; i < TEST_QUEUE_SLOTS; i++)
      {
         MCTF_ASSERT(pgagroal_queue_pop(&queue, &slot), cleanup, "wrap-around pop must succeed");
         MCTF_ASSERT_INT_EQ(slot, i, cleanup, "wrap-around must preserve FIFO order");
      }
   }

cleanup:
   MCTF_FINISH();
}

/* Multi-producer / multi-consumer integrity: every index enqueued exactly
 * once is dequeued exactly once, so the sum of the consumed indices must
 * match the expected value; no index may ever leave the valid range. */
MCTF_TEST(test_queue_mpmc_stress, 120)
{
   pthread_t producers[8];
   pthread_t consumers[8];
   long long expected_total = 0;
   long long got;
   int slot;

   for (int i = 0; i < TEST_QUEUE_SLOTS; i++)
   {
      expected_total += i;
   }
   expected_total *= (long long)TEST_QUEUE_OPS * stress_nprod / TEST_QUEUE_SLOTS;

   pgagroal_queue_init(&stress_queue);
   atomic_store(&stress_produced, 0);
   atomic_store(&stress_consumed_sum, 0);
   atomic_store(&stress_corrupt, false);

   for (int i = 0; i < stress_ncons; i++)
   {
      MCTF_ASSERT(pthread_create(&consumers[i], NULL, stress_consumer, NULL) == 0, cleanup, "consumer thread creation failed");
   }
   for (int i = 0; i < stress_nprod; i++)
   {
      MCTF_ASSERT(pthread_create(&producers[i], NULL, stress_producer, NULL) == 0, cleanup, "producer thread creation failed");
   }
   for (int i = 0; i < stress_nprod; i++)
   {
      pthread_join(producers[i], NULL);
   }
   for (int i = 0; i < stress_ncons; i++)
   {
      pthread_join(consumers[i], NULL);
   }

   got = atomic_load(&stress_consumed_sum);
   MCTF_ASSERT(got == expected_total, cleanup, "MPMC checksum mismatch: entries lost or duplicated");
   MCTF_ASSERT(!atomic_load(&stress_corrupt), cleanup, "MPMC stress popped an out-of-range slot index");

   MCTF_ASSERT(!pgagroal_queue_pop(&stress_queue, &slot), cleanup, "queue must be drained after the stress run");

cleanup:
   MCTF_FINISH();
}
