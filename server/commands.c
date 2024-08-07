#include "commands.h"

#include <assert.h>
#include <stdio.h>

#include "buffer.h"
#include "object.h"
#include "protocol.h"
#include "store.h"
#include "types.h"

static void write_object_response(struct buffer *out, struct object *obj) {
  write_response_header(out, RES_OK);
  write_object(out, obj);
}

static void do_get(
    struct store *store, struct req_object args[1], struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }

  struct const_slice key = to_const_slice(args[0].str_val);
  struct object *found = store_get(store, key);
  if (found == NULL) {
    write_nil_response(out_buf);
    return;
  }

  if (!object_is_scalar(found->type)) {
    write_err_response(out_buf, "not scalar");
    return;
  }

  write_object_response(out_buf, found);
}

static struct object make_object_from_req(struct req_object *req_o) {
  switch (req_o->type) {
    case SER_INT:
      return make_int_object(req_o->int_val);
    case SER_STR: {
      struct object new = make_slice_object(req_o->str_val);
      // Mark as NIL since the value has been moved out
      req_o->type = SER_NIL;
      return new;
    }
    default:
      assert(false);
  }
}

static void do_set(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }

  struct const_slice key = to_const_slice(args[0].str_val);
  struct object val = make_object_from_req(&args[1]);

  store_set(store, key, val);
  write_nil_response(out_buf);
}

static void do_del(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }

  struct const_slice key = to_const_slice(args[0].str_val);
  bool found = store_del(store, key);
  write_bool_response(out_buf, found);
}

static bool append_key_to_response(
    struct const_slice key, struct object *val, void *arg) {
  (void)val;
  struct buffer *out_buf = arg;
  write_str_value(out_buf, key);
  return true;
}

static void do_keys(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  (void)args;
  write_arr_response_header(out_buf, store_size(store));

  store_iter(store, append_key_to_response, out_buf);
}

static void do_hget(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }

  if (args[1].type != SER_STR) {
    write_err_response(out_buf, "invalid field");
    return;
  }

  struct object *outer = store_get(store, to_const_slice(args[0].str_val));
  if (outer == NULL) {
    write_nil_response(out_buf);
    return;
  }

  if (outer->type != OBJ_HMAP) {
    write_err_response(out_buf, "object not a hash map");
    return;
  }

  struct object *inner = hmap_get(outer, to_const_slice(args[1].str_val));
  if (inner == NULL) {
    write_nil_response(out_buf);
    return;
  }

  write_object_response(out_buf, inner);
}

static void do_hset(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }
  struct const_slice key = to_const_slice(args[0].str_val);

  if (args[1].type != SER_STR) {
    write_err_response(out_buf, "invalid field");
    return;
  }
  struct const_slice field = to_const_slice(args[1].str_val);

  struct object *outer = store_get(store, key);
  if (outer == NULL) {
    // Create new hmap
    outer = store_set(store, key, make_hmap_object());
  }

  if (outer->type != OBJ_HMAP) {
    write_err_response(out_buf, "object not a hash map");
    return;
  }

  hmap_set(outer, field, make_object_from_req(&args[2]));
  write_nil_response(out_buf);
}

static void do_hdel(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }
  struct const_slice key = to_const_slice(args[0].str_val);

  if (args[1].type != SER_STR) {
    write_err_response(out_buf, "invalid field");
    return;
  }
  struct const_slice field = to_const_slice(args[1].str_val);

  struct object *outer = store_get(store, key);
  if (outer == NULL) {
    write_bool_response(out_buf, false);
    return;
  }

  if (outer->type != OBJ_HMAP) {
    write_err_response(out_buf, "object not a hash map");
    return;
  }

  bool deleted = hmap_del(outer, field);
  write_bool_response(out_buf, deleted);
}

static void do_hlen(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }
  struct const_slice key = to_const_slice(args[0].str_val);

  struct object *found = store_get(store, key);
  if (found == NULL) {
    write_int_response(out_buf, 0);
    return;
  }

  if (found->type != OBJ_HMAP) {
    write_err_response(out_buf, "object not a hash map");
    return;
  }

  write_int_response(out_buf, hmap_size(found));
}

static void do_hkeys(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }
  struct const_slice key = to_const_slice(args[0].str_val);

  struct object *found = store_get(store, key);
  if (found == NULL) {
    write_arr_response_header(out_buf, 0);
    return;
  }

  if (found->type != OBJ_HMAP) {
    write_err_response(out_buf, "object not a hash map");
    return;
  }

  write_arr_response_header(out_buf, hmap_size(found));
  hmap_iter(found, append_key_to_response, out_buf);
}

static bool append_key_val_to_response(
    struct const_slice key, struct object *val, void *arg) {
  struct buffer *out_buf = arg;
  write_str_value(out_buf, key);
  write_object(out_buf, val);
  return true;
}

static void do_hgetall(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }
  struct const_slice key = to_const_slice(args[0].str_val);

  struct object *found = store_get(store, key);
  if (found == NULL) {
    write_arr_response_header(out_buf, 0);
    return;
  }

  if (found->type != OBJ_HMAP) {
    write_err_response(out_buf, "object not a hash map");
    return;
  }

  write_arr_response_header(out_buf, hmap_size(found) * 2);
  hmap_iter(found, append_key_val_to_response, out_buf);
}

static const struct command all_commands[REQ_MAX_ID] = {
#define CMD(name, arg_count, handler) [REQ_##name] = {#name, arg_count, handler}
    // clang-format off
  CMD(GET, 1, do_get),
  CMD(SET, 2, do_set),
  CMD(DEL, 1, do_del),
  CMD(KEYS, 0, do_keys),

  CMD(HGET, 2, do_hget),
  CMD(HSET, 3, do_hset),
  CMD(HDEL, 2, do_hdel),
  CMD(HLEN, 1, do_hlen),
  CMD(HGETALL, 1, do_hgetall),
  CMD(HKEYS, 1, do_hkeys),
// clang-format on
#undef CMD
};

const struct command *lookup_command(enum req_type type) {
  if (type >= REQ_MAX_ID) {
    return NULL;
  }

  const struct command *cmd = &all_commands[type];
  if (cmd->handler == NULL) {
    return NULL;
  }
  return cmd;
}

void print_request(
    FILE *stream, const struct command *cmd, const struct req_object *args) {
  fprintf(stream, "%s", cmd->name);
  if (cmd->arg_count > 0) {
    for (int i = 0; i < cmd->arg_count; i++) {
      fputc(' ', stream);
      print_req_object(stream, &args[i]);
    }
  }
}
