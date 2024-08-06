#ifndef COMMAND_H_
#define COMMAND_H_

#include <stdint.h>
#include <stdio.h>

#include "buffer.h"
#include "protocol.h"
#include "store.h"

#define COMMAND_ARGS_MAX 3

typedef void (*command_handler)(
    struct store *store, struct req_object *args, struct buffer *res_buf);

struct command {
  const char *name;
  uint8_t arg_count;
  command_handler handler;
};

const struct command *lookup_command(enum req_type type);

void print_request(
    FILE *stream, const struct command *cmd, const struct req_object *args);

#endif
