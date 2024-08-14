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

void store_init(struct store *store);

static inline uint32_t store_size(const struct store *store) {
  return hash_map_size(&store->map);
}

struct object *store_get(struct store *store, struct const_slice key);
struct object *store_set(
    struct store *store, struct const_slice key, struct object val);

// Delete is 2 steps so the deletion can be async
struct store_entry *store_detach(struct store *store, struct const_slice key);
struct object *store_entry_object(struct store_entry *entry);
void store_entry_free(struct store_entry *entry);

typedef bool (*store_iter_fn)(
    struct const_slice key, struct object *val, void *arg);
void store_iter(struct store *store, store_iter_fn iter, void *arg);

int64_t store_object_get_expire(
    const struct store *store, const struct object *obj);
void store_object_set_expire(
    struct store *store, struct object *obj, int64_t timestamp_ms);

struct store_entry *store_detach_next_expired(
    struct store *store, uint64_t expired_after_us);

#endif
