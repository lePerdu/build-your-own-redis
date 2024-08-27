#ifndef COMMAND_H_
#define COMMAND_H_

#include <stdint.h>
#include <stdio.h>
#include <threads.h>

#include "buffer.h"
#include "protocol.h"
#include "store.h"

#define COMMAND_ARGS_MAX 5

struct command_ctx {
  struct store *store;
  struct req_object *args;
  struct buffer *out_buf;
  thrd_t async_task_thread;
  struct work_queue *async_task_queue;
};

// TODO: Pass as pointer? The object is fairly small, so passing by value should
// be fine and makes for slightly cleaner code (. vs ->)
typedef void (*command_handler)(struct command_ctx ctx);

struct command {
  const char *name;
  uint8_t arg_count;
  command_handler handler;
};

const struct command *lookup_command(enum req_type type);

void print_request(
    FILE *stream, const struct command *cmd, const struct req_object *args);

enum {
  USEC_PER_SEC = 1000000,
  USEC_PER_MSEC = 1000,
  NSEC_PER_USEC = 1000,
};

uint64_t get_monotonic_usec(void);
void store_entry_free_maybe_async(
    struct work_queue *task_queue, struct store_entry *to_delete);

#endif
