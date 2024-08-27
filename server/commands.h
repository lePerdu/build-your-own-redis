#ifndef COMMAND_H_
#define COMMAND_H_

#include <stdint.h>
#include <threads.h>

#include "buffer.h"
#include "store.h"
#include "types.h"

#define COMMAND_ARGS_MAX 6

struct command_ctx {
  struct store *store;
  struct slice *args;
  uint32_t arg_count;
  struct buffer *out_buf;
  thrd_t async_task_thread;
  struct work_queue *async_task_queue;
};

void init_commands(void);
// TODO: Pass as pointer? The object is fairly small, so passing by value should
// be fine and makes for slightly cleaner code (. vs ->)
void run_command(struct command_ctx ctx);

enum {
  USEC_PER_SEC = 1000000,
  USEC_PER_MSEC = 1000,
  NSEC_PER_USEC = 1000,
};

uint64_t get_monotonic_usec(void);
void store_entry_free_maybe_async(
    struct work_queue *task_queue, struct store_entry *to_delete);

#endif
