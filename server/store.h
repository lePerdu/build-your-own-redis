#ifndef STORE_H_
#define STORE_H_

#include <stdint.h>

#include "hashmap.h"
#include "heap.h"
#include "object.h"
#include "types.h"

struct store {
  struct hash_map map;
  struct heap expires;
};

struct store_entry {
  struct hash_entry entry;
  struct heap_ref ttl_ref;

  // Owned
  struct slice key;
  // Owned
  struct object val;
};

struct store_key {
  struct hash_entry entry;
  struct const_slice key;
};

void store_init(struct store *store);

static inline uint32_t store_size(const struct store *store) {
  return hash_map_size(&store->map);
}

struct object *store_get(struct store *store, struct const_slice key);
struct object *store_set(
    struct store *store, struct const_slice key, struct object val);
bool store_del(struct store *store, struct const_slice key);

typedef bool (*store_iter_fn)(
    struct const_slice key, struct object *val, void *arg);
void store_iter(struct store *store, store_iter_fn iter, void *arg);

int64_t store_object_get_expire(
    const struct store *store, const struct object *obj);
void store_object_set_expire(
    struct store *store, struct object *obj, int64_t timestamp_ms);

void store_delete_expired(struct store *store, uint64_t before_us);

#endif
