/*
 * Copyright (C) 2026 The pgagroal community
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list
 *    of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this
 *    list of conditions and the following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors may
 *    be used to endorse or promote products derived from this software without specific
 *    prior written permission.
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
#include <message.h>
#include <utils.h>
#include <mctf.h>

#include <string.h>

static int
append_message(char* buffer, int offset, char kind, const char* payload, int payload_len)
{
   pgagroal_write_byte(buffer + offset, kind);
   pgagroal_write_int32(buffer + offset + 1, payload_len + 4);

   if (payload_len > 0)
   {
      memcpy(buffer + offset + 5, payload, payload_len);
   }

   return offset + 5 + payload_len;
}

/* Build a Parse ('P') message: statement name + query + zero parameters. */
static int
build_parse_message(char* buffer, const char* statement_name, const char* query)
{
   char payload[1024];
   int payload_len = 0;
   int name_len = (int)strlen(statement_name);
   int query_len = (int)strlen(query);

   memcpy(payload + payload_len, statement_name, name_len + 1);
   payload_len += name_len + 1;

   memcpy(payload + payload_len, query, query_len + 1);
   payload_len += query_len + 1;

   /* No parameter types */
   payload[payload_len++] = 0;
   payload[payload_len++] = 0;

   return append_message(buffer, 0, 'P', payload, payload_len);
}

/* Build a minimal ErrorResponse ('E') message with severity FATAL. */
static int
build_fatal_error_message(char* buffer)
{
   char payload[7];

   payload[0] = 'S';
   memcpy(payload + 1, "FATAL", 5);
   payload[6] = '\0';

   return append_message(buffer, 0, 'E', payload, sizeof(payload));
}

/* Build a ReadyForQuery ('Z') message. */
static int
build_z_message(char* buffer, char tx_state)
{
   return append_message(buffer, 0, 'Z', &tx_state, 1);
}

/* ------------------------------------------------------------------ */
/* P message: split header must not truncate the statement name       */
/* ------------------------------------------------------------------ */

struct parse_result
{
   int called;
   char name[64];
};

static void
parse_message_cb(char kind, char* msg, int msglen __attribute__((unused)), void* arg)
{
   struct parse_result* result = (struct parse_result*)arg;

   if (kind != 'P')
   {
      return;
   }

   result->called++;

   /*
    * This mimics transaction_client_message():
    *
    *    char* ps = pgagroal_read_string(msg + 5);
    *
    * The callback expects to be able to read the prepared statement name.
    */
   char* ps = pgagroal_read_string(msg + 5);

   if (ps != NULL)
   {
      strncpy(result->name, ps, sizeof(result->name) - 1);
      result->name[sizeof(result->name) - 1] = '\0';
   }
}

MCTF_TEST(test_message_split_parse_statement_name)
{
   char buffer[1024];
   struct pgagroal_message_state state;
   struct parse_result result;
   int length;

   length = build_parse_message(buffer, "S1", "SELECT 1");

   memset(&state, 0, sizeof(state));
   memset(&result, 0, sizeof(result));

   /*
    * Split the message header after 3 bytes.
    *
    * The second read contains the remaining header bytes and the full payload.
    * The callback should still be able to see the full statement name "S1".
    */
   pgagroal_parse_message(&state, buffer, 3, parse_message_cb, &result);
   pgagroal_parse_message(&state, buffer + 3, length - 3, parse_message_cb, &result);

   MCTF_ASSERT(result.called == 1, cleanup,
               "Parse callback must be invoked exactly once");

   MCTF_ASSERT(strcmp(result.name, "S1") == 0, cleanup,
               "split Parse message must not truncate the prepared statement name");

cleanup:
   MCTF_FINISH();
}

MCTF_TEST(test_message_split_fatal_error_detection)
{
   char buffer[1024];
   char chunk1[32];
   char chunk2[1024];
   struct pgagroal_message_state state;
   struct pipeline_server_state pipeline_state;
   bool in_tx = false;
   bool fatal = false;
   int length;

   length = build_fatal_error_message(buffer);

   memset(&state, 0, sizeof(state));
   memset(&pipeline_state, 0, sizeof(pipeline_state));

   pipeline_state.in_tx = &in_tx;
   pipeline_state.saw_rfq = NULL;
   pipeline_state.fatal = &fatal;

   /*
    * chunk1 contains:
    *
    *   'E' + 4-byte length + first payload byte 'S'
    *
    * This is exactly the situation where pgagroal_parse_message() fires the
    * callback today: header complete plus first payload byte.
    *
    * The FATAL bytes arrive only in chunk2.
    */
   memset(chunk1, 0, sizeof(chunk1));
   memcpy(chunk1, buffer, 6);

   memcpy(chunk2, buffer + 6, length - 6);

   pgagroal_parse_message(&state, chunk1, 6, pgagroal_pipeline_server_rfq, &pipeline_state);
   pgagroal_parse_message(&state, chunk2, length - 6, pgagroal_pipeline_server_rfq, &pipeline_state);

   MCTF_ASSERT(fatal == true, cleanup,
               "FATAL ErrorResponse split after first payload byte must still be detected");

cleanup:
   MCTF_FINISH();
}

MCTF_TEST(test_message_aligned_fatal_error_detection)
{
   char buffer[1024];
   struct pgagroal_message_state state;
   struct pipeline_server_state pipeline_state;
   bool in_tx = false;
   bool fatal = false;
   int length;

   length = build_fatal_error_message(buffer);

   memset(&state, 0, sizeof(state));
   memset(&pipeline_state, 0, sizeof(pipeline_state));

   pipeline_state.in_tx = &in_tx;
   pipeline_state.saw_rfq = NULL;
   pipeline_state.fatal = &fatal;

   pgagroal_parse_message(&state, buffer, length, pgagroal_pipeline_server_rfq, &pipeline_state);

   MCTF_ASSERT(fatal == true, cleanup,
               "aligned FATAL ErrorResponse must be detected");

cleanup:
   MCTF_FINISH();
}

MCTF_TEST(test_message_split_z_state)
{
   char buffer[16];
   struct pgagroal_message_state state;
   struct pipeline_server_state pipeline_state;
   bool in_tx = true;
   bool saw_rfq = false;
   bool fatal = false;
   int length;

   length = build_z_message(buffer, 'I');

   memset(&state, 0, sizeof(state));
   memset(&pipeline_state, 0, sizeof(pipeline_state));

   pipeline_state.in_tx = &in_tx;
   pipeline_state.saw_rfq = &saw_rfq;
   pipeline_state.fatal = &fatal;

   /* Split between header and the transaction-state byte */
   pgagroal_parse_message(&state, buffer, 5, pgagroal_pipeline_server_rfq, &pipeline_state);
   pgagroal_parse_message(&state, buffer + 5, length - 5, pgagroal_pipeline_server_rfq, &pipeline_state);

   MCTF_ASSERT(in_tx == false, cleanup,
               "split idle ReadyForQuery must clear in_tx");

   MCTF_ASSERT(saw_rfq == true, cleanup,
               "split idle ReadyForQuery must set saw_rfq");

   MCTF_ASSERT(fatal == false, cleanup,
               "split ReadyForQuery must not set fatal");

cleanup:
   MCTF_FINISH();
}
