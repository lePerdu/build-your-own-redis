#include "commands.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

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

static void do_sadd(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }
  struct const_slice key = to_const_slice(args[0].str_val);

  if (args[1].type != SER_STR) {
    write_err_response(out_buf, "invalid member");
    return;
  }
  struct const_slice set_key = to_const_slice(args[1].str_val);

  struct object *found = store_get(store, key);
  if (found == NULL) {
    found = store_set(store, key, make_hset_object());
  }

  if (found->type != OBJ_HSET) {
    write_err_response(out_buf, "object not a set");
    return;
  }

  bool added = hset_add(found, set_key);
  write_bool_response(out_buf, added);
}

static void do_sismember(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }
  struct const_slice key = to_const_slice(args[0].str_val);

  if (args[1].type != SER_STR) {
    write_err_response(out_buf, "invalid member");
    return;
  }
  struct const_slice set_key = to_const_slice(args[1].str_val);

  struct object *found = store_get(store, key);
  if (found == NULL) {
    write_bool_response(out_buf, false);
    return;
  }

  if (found->type != OBJ_HSET) {
    write_err_response(out_buf, "object not a set");
    return;
  }

  bool contains = hset_contains(found, set_key);
  write_bool_response(out_buf, contains);
}

static void do_srem(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }
  struct const_slice key = to_const_slice(args[0].str_val);

  if (args[1].type != SER_STR) {
    write_err_response(out_buf, "invalid member");
    return;
  }
  struct const_slice set_key = to_const_slice(args[1].str_val);

  struct object *found = store_get(store, key);
  if (found == NULL) {
    write_bool_response(out_buf, false);
    return;
  }

  if (found->type != OBJ_HSET) {
    write_err_response(out_buf, "object not a set");
    return;
  }

  bool removed = hset_del(found, set_key);
  write_bool_response(out_buf, removed);
}

static void do_scard(
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

  if (found->type != OBJ_HSET) {
    write_err_response(out_buf, "object not a set");
    return;
  }

  write_int_response(out_buf, hset_size(found));
}

static void do_srandmember(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
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

  if (found->type != OBJ_HSET) {
    write_err_response(out_buf, "object not a set");
    return;
  }

  struct const_slice member;
  bool found_member = hset_peek(found, &member);
  if (found_member) {
    write_str_response(out_buf, member);
  } else {
    write_nil_response(out_buf);
  }
}

static void do_spop(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
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

  if (found->type != OBJ_HSET) {
    write_err_response(out_buf, "object not a set");
    return;
  }

  struct slice member;
  bool found_member = hset_pop(found, &member);
  if (found_member) {
    write_str_response(out_buf, to_const_slice(member));
    free(member.data);
  } else {
    write_nil_response(out_buf);
  }
}

static bool append_set_key_to_response(struct const_slice key, void *arg) {
  struct buffer *out_buf = arg;
  write_str_value(out_buf, key);
  return true;
}

static void do_smembers(
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

  if (found->type != OBJ_HSET) {
    write_err_response(out_buf, "object not a set");
    return;
  }

  write_arr_response_header(out_buf, hset_size(found));
  hset_iter(found, append_set_key_to_response, out_buf);
}

static void do_zscore(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }

  if (args[1].type != SER_STR) {
    write_err_response(out_buf, "invalid member");
    return;
  }

  struct object *outer = store_get(store, to_const_slice(args[0].str_val));
  if (outer == NULL) {
    write_nil_response(out_buf);
    return;
  }

  if (outer->type != OBJ_ZSET) {
    write_err_response(out_buf, "object not a sorted set");
    return;
  }

  double score;
  bool found = zset_score(outer, to_const_slice(args[1].str_val), &score);
  if (found) {
    write_float_response(out_buf, score);
  } else {
    write_nil_response(out_buf);
  }
}

static void do_zadd(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }
  struct const_slice key = to_const_slice(args[0].str_val);

  if (args[1].type != SER_FLOAT) {
    write_err_response(out_buf, "invalid score");
    return;
  }
  double score = args[1].float_val;

  if (args[2].type != SER_STR) {
    write_err_response(out_buf, "invalid member");
    return;
  }
  struct const_slice member = to_const_slice(args[2].str_val);

  struct object *outer = store_get(store, key);
  if (outer == NULL) {
    // Create new set
    outer = store_set(store, key, make_zset_object());
  }

  if (outer->type != OBJ_ZSET) {
    write_err_response(out_buf, "object not a sorted set");
    return;
  }

  bool added = zset_add(outer, member, score);
  write_bool_response(out_buf, added);
}

static void do_zrem(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }
  struct const_slice key = to_const_slice(args[0].str_val);

  if (args[1].type != SER_STR) {
    write_err_response(out_buf, "invalid member");
    return;
  }
  struct const_slice member = to_const_slice(args[1].str_val);

  struct object *outer = store_get(store, key);
  if (outer == NULL) {
    write_bool_response(out_buf, false);
    return;
  }

  if (outer->type != OBJ_ZSET) {
    write_err_response(out_buf, "object not a sorted set");
    return;
  }

  bool deleted = zset_del(outer, member);
  write_bool_response(out_buf, deleted);
}

static void do_zcard(
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

  if (found->type != OBJ_ZSET) {
    write_err_response(out_buf, "object not a sorted set");
    return;
  }

  write_int_response(out_buf, zset_size(found));
}

static void do_zrank(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }
  struct const_slice key = to_const_slice(args[0].str_val);

  if (args[1].type != SER_STR) {
    write_err_response(out_buf, "invalid member");
    return;
  }
  struct const_slice member = to_const_slice(args[1].str_val);

  struct object *outer = store_get(store, key);
  if (outer == NULL) {
    write_nil_response(out_buf);
    return;
  }

  if (outer->type != OBJ_ZSET) {
    write_err_response(out_buf, "object not a sorted set");
    return;
  }

  int_val_t rank = zset_rank(outer, member);
  if (rank < 0) {
    write_nil_response(out_buf);
  } else {
    write_int_response(out_buf, rank);
  }
}

static void do_zquery(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  if (args[0].type != SER_STR) {
    write_err_response(out_buf, "invalid key");
    return;
  }
  struct const_slice key = to_const_slice(args[0].str_val);

  if (args[1].type != SER_FLOAT) {
    write_err_response(out_buf, "invalid score");
    return;
  }
  double score = args[1].float_val;

  if (args[2].type != SER_STR) {
    write_err_response(out_buf, "invalid member");
    return;
  }
  struct const_slice member = to_const_slice(args[2].str_val);

  if (args[3].type != SER_INT) {
    write_err_response(out_buf, "invalid offset");
    return;
  }
  int64_t offset = args[3].int_val;

  if (args[4].type != SER_INT || args[4].int_val < 0) {
    write_err_response(out_buf, "invalid limit");
    return;
  }
  uint64_t limit = args[4].int_val;

  struct object *outer = store_get(store, key);
  if (outer == NULL) {
    write_arr_response_header(out_buf, 0);
    return;
  }

  if (outer->type != OBJ_ZSET) {
    write_err_response(out_buf, "object not a sorted set");
    return;
  }

  struct zset_node *start = zset_query(outer, member, score);
  start = zset_node_offset(start, offset);

  if (start == NULL) {
    write_arr_response_header(out_buf, 0);
    return;
  }

  // The protocol doesn't handle unknown-length arrays, so we have to figure out
  // the count ahead of time
  uint32_t max_count = zset_size(outer) - zset_node_rank(outer, start);
  uint32_t count = limit < max_count ? limit : max_count;

  write_arr_response_header(out_buf, count * 2);
  for (uint32_t i = 0; i < count; i++) {
    assert(start != NULL);
    write_str_value(out_buf, zset_node_key(start));
    write_float_value(out_buf, zset_node_score(start));

    start = zset_node_offset(start, 1);
  }
}

static void do_shutdown(
    struct store *store, struct req_object *args, struct buffer *out_buf) {
  (void)store;
  (void)args;
  (void)out_buf;
  exit(EXIT_SUCCESS);
}

static const struct command all_commands[REQ_MAX_ID] = {
#define CMD(name, arg_count, handler) \
  [REQ_##name] = {#name, (arg_count), (handler)}
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

  CMD(SADD, 2, do_sadd),
  CMD(SISMEMBER, 2, do_sismember),
  CMD(SREM, 2, do_srem),
  CMD(SCARD, 1, do_scard),
  CMD(SRANDMEMBER, 1, do_srandmember),
  CMD(SPOP, 1, do_spop),
  CMD(SMEMBERS, 1, do_smembers),

  CMD(ZSCORE, 2, do_zscore),
  CMD(ZADD, 3, do_zadd),
  CMD(ZREM, 2, do_zrem),
  CMD(ZCARD, 1, do_zcard),
  CMD(ZRANK, 2, do_zrank),
  CMD(ZQUERY, 5, do_zquery),

  CMD(SHUTDOWN, 0, do_shutdown),

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
