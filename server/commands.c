#include "commands.h"

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <threads.h>
#include <time.h>

#include "buffer.h"
#include "hashmap.h"
#include "object.h"
#include "protocol.h"
#include "queue.h"
#include "store.h"
#include "types.h"

enum {
  // Allocation complexity required before async deletion
  ASYNC_DELETE_COMPLEXITY = 1000,
};

uint64_t get_monotonic_usec(void) {
  struct timespec time;
  // CLOCK_MONOTONIC isn't detected properly by clang-tidy for some reason
  // NOLINTNEXTLINE(misc-include-cleaner)
  int res = clock_gettime(CLOCK_MONOTONIC, &time);
  assert(res == 0);

  return time.tv_sec * USEC_PER_SEC + time.tv_nsec / NSEC_PER_USEC;
}

static void store_entry_free_callback(void *arg) { store_entry_free(arg); }

void submit_async_delete(
    struct work_queue *task_queue, struct store_entry *to_delete) {
  struct work_task task = {
      .callback = store_entry_free_callback,
      .arg = to_delete,
  };
  work_queue_push(task_queue, task);
}

void store_entry_free_maybe_async(
    struct work_queue *task_queue, struct store_entry *to_delete) {
  if (object_allocation_complexity(store_entry_object(to_delete)) >=
      ASYNC_DELETE_COMPLEXITY) {
    submit_async_delete(task_queue, to_delete);
  } else {
    store_entry_free(to_delete);
  }
}

static void do_get(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);
  struct object *found = store_get(ctx.store, key);
  if (found == NULL) {
    write_null_value(ctx.out_buf);
    return;
  }

  if (!object_is_scalar(found->type)) {
    write_simple_err_value(ctx.out_buf, "not scalar");
    return;
  }

  write_object(ctx.out_buf, found);
}

static void do_set(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);
  store_set(ctx.store, key, swap_slice_into_object(&ctx.args[2]));
  write_null_value(ctx.out_buf);
}

static void do_del(struct command_ctx ctx) {
  struct store_entry *removed =
      store_detach(ctx.store, to_const_slice(ctx.args[1]));
  if (removed == NULL) {
    write_bool_value(ctx.out_buf, false);
    return;
  }

  store_entry_free_maybe_async(ctx.async_task_queue, removed);
  write_bool_value(ctx.out_buf, true);
}

static bool append_key_to_value(
    struct const_slice key, struct object *val, void *arg) {
  (void)val;
  struct buffer *out_buf = arg;
  write_str_value(out_buf, key);
  return true;
}

static void do_keys(struct command_ctx ctx) {
  write_array_header(ctx.out_buf, store_size(ctx.store));

  store_iter(ctx.store, append_key_to_value, ctx.out_buf);
}

static void do_ttl(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);
  struct object *found = store_get(ctx.store, key);
  if (found == NULL) {
    write_int_value(ctx.out_buf, -2);
    return;
  }

  int64_t expires_at_us = store_object_get_expire(ctx.store, found);
  if (expires_at_us < 0) {
    write_int_value(ctx.out_buf, -1);
    return;
  }

  uint64_t now = get_monotonic_usec();
  uint64_t ttl =
      (uint64_t)expires_at_us > now ? (expires_at_us - now) / USEC_PER_MSEC : 0;
  write_int_value(ctx.out_buf, (int_val_t)ttl);
}

static void do_expire(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  int_val_t ttl_ms;
  if (!parse_int_arg(&ttl_ms, to_const_slice(ctx.args[2]))) {
    write_simple_err_value(ctx.out_buf, "invalid ttl");
    return;
  }

  if (ttl_ms <= 0) {
    // Delegate to the DEL since it might decide to perform async deletion
    // DEL just uses the first argument, so the context can be passed as-is
    do_del(ctx);
    return;
  }

  struct object *found = store_get(ctx.store, key);
  if (found == NULL) {
    write_bool_value(ctx.out_buf, false);
    return;
  }

  uint64_t expires_at_us = get_monotonic_usec() + ttl_ms * USEC_PER_MSEC;
  store_object_set_expire(ctx.store, found, (int64_t)expires_at_us);
  write_bool_value(ctx.out_buf, true);
}

static void do_persist(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);
  struct object *found = store_get(ctx.store, key);
  if (found == NULL) {
    write_bool_value(ctx.out_buf, false);
    return;
  }

  store_object_set_expire(ctx.store, found, -1);
  write_bool_value(ctx.out_buf, true);
}

static void do_hget(struct command_ctx ctx) {
  struct object *outer = store_get(ctx.store, to_const_slice(ctx.args[1]));
  if (outer == NULL) {
    write_null_value(ctx.out_buf);
    return;
  }

  if (outer->type != OBJ_HMAP) {
    write_simple_err_value(ctx.out_buf, "object not a hash map");
    return;
  }

  struct object *inner = hmap_get(outer, to_const_slice(ctx.args[2]));
  if (inner == NULL) {
    write_null_value(ctx.out_buf);
    return;
  }

  write_object(ctx.out_buf, inner);
}

static void do_hset(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  struct const_slice field = to_const_slice(ctx.args[2]);

  struct object *outer = store_get(ctx.store, key);
  if (outer == NULL) {
    // Create new hmap
    outer = store_set(ctx.store, key, make_hmap_object());
  }

  if (outer->type != OBJ_HMAP) {
    write_simple_err_value(ctx.out_buf, "object not a hash map");
    return;
  }

  hmap_set(outer, field, swap_slice_into_object(&ctx.args[3]));
  write_null_value(ctx.out_buf);
}

static void do_hdel(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  struct const_slice field = to_const_slice(ctx.args[2]);

  struct object *outer = store_get(ctx.store, key);
  if (outer == NULL) {
    write_bool_value(ctx.out_buf, false);
    return;
  }

  if (outer->type != OBJ_HMAP) {
    write_simple_err_value(ctx.out_buf, "object not a hash map");
    return;
  }

  bool deleted = hmap_del(outer, field);
  write_bool_value(ctx.out_buf, deleted);
}

static void do_hlen(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  struct object *found = store_get(ctx.store, key);
  if (found == NULL) {
    write_int_value(ctx.out_buf, 0);
    return;
  }

  if (found->type != OBJ_HMAP) {
    write_simple_err_value(ctx.out_buf, "object not a hash map");
    return;
  }

  write_int_value(ctx.out_buf, hmap_size(found));
}

static void do_hkeys(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  struct object *found = store_get(ctx.store, key);
  if (found == NULL) {
    write_array_header(ctx.out_buf, 0);
    return;
  }

  if (found->type != OBJ_HMAP) {
    write_simple_err_value(ctx.out_buf, "object not a hash map");
    return;
  }

  write_array_header(ctx.out_buf, hmap_size(found));
  hmap_iter(found, append_key_to_value, ctx.out_buf);
}

static bool append_key_val_to_value(
    struct const_slice key, struct object *val, void *arg) {
  struct buffer *out_buf = arg;
  write_str_value(out_buf, key);
  write_object(out_buf, val);
  return true;
}

static void do_hgetall(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  struct object *found = store_get(ctx.store, key);
  if (found == NULL) {
    write_array_header(ctx.out_buf, 0);
    return;
  }

  if (found->type != OBJ_HMAP) {
    write_simple_err_value(ctx.out_buf, "object not a hash map");
    return;
  }

  write_array_header(ctx.out_buf, hmap_size(found) * 2);
  hmap_iter(found, append_key_val_to_value, ctx.out_buf);
}

static void do_sadd(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  struct const_slice set_key = to_const_slice(ctx.args[2]);

  struct object *found = store_get(ctx.store, key);
  if (found == NULL) {
    found = store_set(ctx.store, key, make_hset_object());
  }

  if (found->type != OBJ_HSET) {
    write_simple_err_value(ctx.out_buf, "object not a set");
    return;
  }

  bool added = hset_add(found, set_key);
  write_bool_value(ctx.out_buf, added);
}

static void do_sismember(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  struct const_slice set_key = to_const_slice(ctx.args[2]);

  struct object *found = store_get(ctx.store, key);
  if (found == NULL) {
    write_bool_value(ctx.out_buf, false);
    return;
  }

  if (found->type != OBJ_HSET) {
    write_simple_err_value(ctx.out_buf, "object not a set");
    return;
  }

  bool contains = hset_contains(found, set_key);
  write_bool_value(ctx.out_buf, contains);
}

static void do_srem(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  struct const_slice set_key = to_const_slice(ctx.args[2]);

  struct object *found = store_get(ctx.store, key);
  if (found == NULL) {
    write_bool_value(ctx.out_buf, false);
    return;
  }

  if (found->type != OBJ_HSET) {
    write_simple_err_value(ctx.out_buf, "object not a set");
    return;
  }

  bool removed = hset_del(found, set_key);
  write_bool_value(ctx.out_buf, removed);
}

static void do_scard(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  struct object *found = store_get(ctx.store, key);
  if (found == NULL) {
    write_int_value(ctx.out_buf, 0);
    return;
  }

  if (found->type != OBJ_HSET) {
    write_simple_err_value(ctx.out_buf, "object not a set");
    return;
  }

  write_int_value(ctx.out_buf, hset_size(found));
}

static void do_srandmember(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  struct object *found = store_get(ctx.store, key);
  if (found == NULL) {
    write_null_value(ctx.out_buf);
    return;
  }

  if (found->type != OBJ_HSET) {
    write_simple_err_value(ctx.out_buf, "object not a set");
    return;
  }

  struct const_slice member;
  bool found_member = hset_peek(found, &member);
  if (found_member) {
    write_str_value(ctx.out_buf, member);
  } else {
    write_null_value(ctx.out_buf);
  }
}

static void do_spop(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  struct object *found = store_get(ctx.store, key);
  if (found == NULL) {
    write_null_value(ctx.out_buf);
    return;
  }

  if (found->type != OBJ_HSET) {
    write_simple_err_value(ctx.out_buf, "object not a set");
    return;
  }

  struct slice member;
  bool found_member = hset_pop(found, &member);
  if (found_member) {
    write_str_value(ctx.out_buf, to_const_slice(member));
    free(member.data);
  } else {
    write_null_value(ctx.out_buf);
  }
}

static bool append_set_key_to_value(struct const_slice key, void *arg) {
  struct buffer *out_buf = arg;
  write_str_value(out_buf, key);
  return true;
}

static void do_smembers(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  struct object *found = store_get(ctx.store, key);
  if (found == NULL) {
    write_array_header(ctx.out_buf, 0);
    return;
  }

  if (found->type != OBJ_HSET) {
    write_simple_err_value(ctx.out_buf, "object not a set");
    return;
  }

  write_array_header(ctx.out_buf, hset_size(found));
  hset_iter(found, append_set_key_to_value, ctx.out_buf);
}

static void do_zscore(struct command_ctx ctx) {
  struct object *outer = store_get(ctx.store, to_const_slice(ctx.args[1]));
  if (outer == NULL) {
    write_null_value(ctx.out_buf);
    return;
  }

  if (outer->type != OBJ_ZSET) {
    write_simple_err_value(ctx.out_buf, "object not a sorted set");
    return;
  }

  double score;
  bool found = zset_score(outer, to_const_slice(ctx.args[2]), &score);
  if (found) {
    write_float_value(ctx.out_buf, score);
  } else {
    write_null_value(ctx.out_buf);
  }
}

static void do_zadd(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  double score;
  if (!parse_float_arg(&score, to_const_slice(ctx.args[2]))) {
    write_simple_err_value(ctx.out_buf, "invalid score");
    return;
  }

  struct const_slice member = to_const_slice(ctx.args[3]);

  struct object *outer = store_get(ctx.store, key);
  if (outer == NULL) {
    // Create new set
    outer = store_set(ctx.store, key, make_zset_object());
  }

  if (outer->type != OBJ_ZSET) {
    write_simple_err_value(ctx.out_buf, "object not a sorted set");
    return;
  }

  bool added = zset_add(outer, member, score);
  write_bool_value(ctx.out_buf, added);
}

static void do_zrem(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  struct const_slice member = to_const_slice(ctx.args[2]);

  struct object *outer = store_get(ctx.store, key);
  if (outer == NULL) {
    write_bool_value(ctx.out_buf, false);
    return;
  }

  if (outer->type != OBJ_ZSET) {
    write_simple_err_value(ctx.out_buf, "object not a sorted set");
    return;
  }

  bool deleted = zset_del(outer, member);
  write_bool_value(ctx.out_buf, deleted);
}

static void do_zcard(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  struct object *found = store_get(ctx.store, key);
  if (found == NULL) {
    write_int_value(ctx.out_buf, 0);
    return;
  }

  if (found->type != OBJ_ZSET) {
    write_simple_err_value(ctx.out_buf, "object not a sorted set");
    return;
  }

  write_int_value(ctx.out_buf, zset_size(found));
}

static void do_zrank(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  struct const_slice member = to_const_slice(ctx.args[2]);

  struct object *outer = store_get(ctx.store, key);
  if (outer == NULL) {
    write_null_value(ctx.out_buf);
    return;
  }

  if (outer->type != OBJ_ZSET) {
    write_simple_err_value(ctx.out_buf, "object not a sorted set");
    return;
  }

  int_val_t rank = zset_rank(outer, member);
  if (rank < 0) {
    write_null_value(ctx.out_buf);
  } else {
    write_int_value(ctx.out_buf, rank);
  }
}

static void do_zquery(struct command_ctx ctx) {
  struct const_slice key = to_const_slice(ctx.args[1]);

  double score;
  if (!parse_float_arg(&score, to_const_slice(ctx.args[2]))) {
    write_simple_err_value(ctx.out_buf, "invalid score");
    return;
  }

  struct const_slice member = to_const_slice(ctx.args[3]);

  int_val_t offset;
  if (!parse_int_arg(&offset, to_const_slice(ctx.args[4]))) {
    write_simple_err_value(ctx.out_buf, "invalid offset");
    return;
  }

  int_val_t limit;
  // NOLINTNEXTLINE(readability-magic-numbers)
  if (!parse_int_arg(&limit, to_const_slice(ctx.args[5])) || limit < 0) {
    write_simple_err_value(ctx.out_buf, "invalid limit");
    return;
  }

  struct object *outer = store_get(ctx.store, key);
  if (outer == NULL) {
    write_array_header(ctx.out_buf, 0);
    return;
  }

  if (outer->type != OBJ_ZSET) {
    write_simple_err_value(ctx.out_buf, "object not a sorted set");
    return;
  }

  struct zset_node *start = zset_query(outer, member, score);
  start = zset_node_offset(start, offset);

  if (start == NULL) {
    write_array_header(ctx.out_buf, 0);
    return;
  }

  // The protocol doesn't handle unknown-length arrays, so we have to figure out
  // the count ahead of time
  uint32_t max_count = zset_size(outer) - zset_node_rank(outer, start);
  uint32_t count = limit < max_count ? limit : max_count;

  write_array_header(ctx.out_buf, count * 2);
  for (uint32_t i = 0; i < count; i++) {
    assert(start != NULL);
    write_str_value(ctx.out_buf, zset_node_key(start));
    write_float_value(ctx.out_buf, zset_node_score(start));

    start = zset_node_offset(start, 1);
  }
}

static void shutdown_work_thread(void *arg) {
  (void)arg;
  thrd_exit(0);
}

static void do_shutdown(struct command_ctx ctx) {
  // Gracefully exit the worker thread to please ASAN
  work_queue_push_front(
      ctx.async_task_queue, (struct work_task){
                                .callback = shutdown_work_thread,
                                .arg = NULL,
                            });
  int async_res;
  int res = thrd_join(ctx.async_task_thread, &async_res);
  assert(res == thrd_success);
  assert(async_res == 0);
  exit(EXIT_SUCCESS);
}

static void do_command_not_found(struct command_ctx ctx) {
  write_simple_err_value(ctx.out_buf, "invalid command");
}

static void do_not_enough_args(struct command_ctx ctx) {
  write_simple_err_value(ctx.out_buf, "not enough arguments");
}

typedef void (*command_handler)(struct command_ctx ctx);

static struct hash_map commands_map;

struct command_entry {
  struct hash_entry base;
  struct const_slice name;
  uint32_t arg_count;
  command_handler handler;
};

struct command_key {
  struct hash_entry base;
  struct const_slice name;
};

static bool command_entry_compare(
    const struct hash_entry *raw_key, const struct hash_entry *raw_entry) {
  return slice_eq(
      container_of(raw_key, struct command_key, base)->name,
      container_of(raw_entry, struct command_entry, base)->name);
}

struct command_def {
  const char *name;
  uint32_t arg_count;
  command_handler handler;
};

static const struct command_def all_commands[] = {
    {"GET", 1, do_get},
    {"SET", 2, do_set},
    {"DEL", 1, do_del},
    {"KEYS", 0, do_keys},

    {"TTL", 1, do_ttl},
    {"EXPIRE", 2, do_expire},
    {"PERSIST", 1, do_persist},

    {"HGET", 2, do_hget},
    {"HSET", 3, do_hset},
    {"HDEL", 2, do_hdel},
    {"HLEN", 1, do_hlen},
    {"HGETALL", 1, do_hgetall},
    {"HKEYS", 1, do_hkeys},

    {"SADD", 2, do_sadd},
    {"SISMEMBER", 2, do_sismember},
    {"SREM", 2, do_srem},
    {"SCARD", 1, do_scard},
    {"SRANDMEMBER", 1, do_srandmember},
    {"SPOP", 1, do_spop},
    {"SMEMBERS", 1, do_smembers},

    {"ZSCORE", 2, do_zscore},
    {"ZADD", 3, do_zadd},
    {"ZREM", 2, do_zrem},
    {"ZCARD", 1, do_zcard},
    {"ZRANK", 2, do_zrank},
    {"ZQUERY", 5, do_zquery},

    {"SHUTDOWN", 0, do_shutdown},
    {NULL, 0, NULL},
};

// Storage for the hash entries (it's a easier to copy metadata than to
// construct it right from the start)
static struct command_entry
    all_command_entries[sizeof(all_commands) / sizeof(all_commands[0])];

enum { COMMAND_HASH_CAP = 64 };

void init_commands(void) {
  hash_map_init(&commands_map, COMMAND_HASH_CAP);

  for (unsigned i = 0; all_commands[i].name != NULL; i++) {
    struct const_slice name_slice = make_str_slice(all_commands[i].name);
    all_command_entries[i] = (struct command_entry){
        .base.hash_code = slice_hash(name_slice),
        .name = name_slice,
        .arg_count = all_commands[i].arg_count,
        .handler = all_commands[i].handler,
    };

    hash_map_insert(&commands_map, &all_command_entries[i].base);
  }
}

void run_command(struct command_ctx ctx) {
  assert(ctx.arg_count > 0);
  struct const_slice cmd_name = to_const_slice(ctx.args[0]);
  struct command_key key = {
      .base.hash_code = slice_hash(cmd_name),
      .name = cmd_name,
  };
  struct hash_entry *found =
      hash_map_get(&commands_map, &key.base, command_entry_compare);
  if (found == NULL) {
    do_command_not_found(ctx);
    return;
  }

  struct command_entry *cmd = container_of(found, struct command_entry, base);
  if (cmd->arg_count != ctx.arg_count - 1) {
    do_not_enough_args(ctx);
    return;
  }
  cmd->handler(ctx);
}
/**/
/*void print_request(*/
/*    FILE *stream, const struct command *cmd, const struct req_object *args)
 * {*/
/*  fprintf(stream, "%s", cmd->name);*/
/*  if (cmd->arg_count > 0) {*/
/*    for (int i = 0; i < cmd->arg_count; i++) {*/
/*      fputc(' ', stream);*/
/*      print_req_object(stream, &args[i]);*/
/*    }*/
/*  }*/
/*}*/
