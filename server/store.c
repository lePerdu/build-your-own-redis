#include "store.h"

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "hashmap.h"
#include "heap.h"
#include "object.h"
#include "types.h"

enum {
  STORE_INIT_CAP = 64,

  // TODO: Make the index signed, or just treat this as a special value?
  TTL_INDEX_NONE = UINT32_MAX,
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

void store_init(struct store *store) {
  hash_map_init(&store->map, STORE_INIT_CAP);
  heap_init(&store->expires);
}

static struct store_entry *store_entry_alloc(
    struct const_slice key, struct object val) {
  struct store_entry *new = malloc(sizeof(*new));
  assert(new != NULL);
  new->ttl_ref.index = TTL_INDEX_NONE;
  new->entry.hash_code = slice_hash(key);
  new->key = slice_dup(key);
  new->val = val;
  return new;
}

void store_entry_free(struct store_entry *ent) {
  free(ent->key.data);
  object_destroy(ent->val);
  free(ent);
}

static bool store_entry_compare(
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    const struct hash_entry *raw_key, const struct hash_entry *raw_ent) {
  const struct store_key *key = container_of(raw_key, struct store_key, entry);
  const struct store_entry *ent =
      container_of(raw_ent, struct store_entry, entry);

  return slice_eq(key->key, to_const_slice(ent->key));
}

struct object *store_get(struct store *store, struct const_slice key) {
  // TODO: Re-structure hashmap API to avoid double hashing when inserting?
  struct store_key key_ent = {
      .entry.hash_code = slice_hash(key),
      .key = key,
  };

  struct hash_entry *found =
      hash_map_get(&store->map, &key_ent.entry, store_entry_compare);
  if (found == NULL) {
    return NULL;
  }

  struct store_entry *existing = container_of(found, struct store_entry, entry);
  return &existing->val;
}

struct object *store_set(
    struct store *store, struct const_slice key, struct object val) {
  // TODO: Re-structure hashmap API to avoid double hashing when inserting?
  struct store_key key_ent = {
      .entry.hash_code = slice_hash(key),
      .key = key,
  };

  struct hash_entry *existing =
      hash_map_get(&store->map, &key_ent.entry, store_entry_compare);
  if (existing == NULL) {
    struct store_entry *new_ent = store_entry_alloc(key, val);
    hash_map_insert(&store->map, &new_ent->entry);
    return &new_ent->val;
  }

  struct store_entry *existing_ent =
      container_of(existing, struct store_entry, entry);
  object_destroy(existing_ent->val);
  existing_ent->val = val;
  return &existing_ent->val;
}

/** Helper for detach functions */
static struct store_entry *do_detach(
    struct store *store, struct store_key *key) {
  struct hash_entry *removed =
      hash_map_delete(&store->map, &key->entry, store_entry_compare);
  if (removed == NULL) {
    return NULL;
  }

  struct store_entry *ent = container_of(removed, struct store_entry, entry);
  if (ent->ttl_ref.index != TTL_INDEX_NONE) {
    heap_pop(&store->expires, ent->ttl_ref.index);
  }
  return ent;
}

struct store_entry *store_detach(struct store *store, struct const_slice key) {
  struct store_key key_ent = {
      .entry.hash_code = slice_hash(key),
      .key = key,
  };

  return do_detach(store, &key_ent);
}

struct object *store_entry_object(struct store_entry *entry) {
  return &entry->val;
}

struct store_iter_ctx {
  store_iter_fn callback;
  void *arg;
};

static bool store_iter_wrapper(struct hash_entry *raw_ent, void *arg) {
  struct store_iter_ctx *ctx = arg;
  struct store_entry *ent = container_of(raw_ent, struct store_entry, entry);
  return ctx->callback(to_const_slice(ent->key), &ent->val, ctx->arg);
}

void store_iter(struct store *store, store_iter_fn iter, void *arg) {
  struct store_iter_ctx ctx = {.callback = iter, .arg = arg};
  hash_map_iter(&store->map, store_iter_wrapper, &ctx);
}

int64_t store_object_get_expire(
    const struct store *store, const struct object *obj) {
  struct store_entry *entry = container_of(obj, struct store_entry, val);
  if (entry->ttl_ref.index == TTL_INDEX_NONE) {
    return -1;
  }

  return (int64_t)store->expires.data[entry->ttl_ref.index].value;
}

void store_object_set_expire(
    struct store *store, struct object *obj, int64_t timestamp_ms) {
  struct store_entry *entry = container_of(obj, struct store_entry, val);
  if (timestamp_ms < 0) {
    if (entry->ttl_ref.index != TTL_INDEX_NONE) {
      heap_pop(&store->expires, entry->ttl_ref.index);
      entry->ttl_ref.index = TTL_INDEX_NONE;
    }
  } else {
    if (entry->ttl_ref.index == TTL_INDEX_NONE) {
      heap_insert(&store->expires, timestamp_ms, &entry->ttl_ref);
    } else {
      heap_update(&store->expires, entry->ttl_ref.index, timestamp_ms);
    }
  }
}

struct store_entry *store_detach_next_expired(
    struct store *store, uint64_t expired_after_us) {
  if (heap_empty(&store->expires)) {
    return NULL;
  }

  struct heap_node next = heap_peek_min(&store->expires);
  if (next.value > expired_after_us) {
    return NULL;
  }
  struct store_entry *to_expire =
      container_of(next.backref, struct store_entry, ttl_ref);

  // TODO: Refactor the hashmap API so that an entry can be deleted by
  // reference (currently this isn't possible since we need the "parent" ref
  // in the hash bucket linked list)
  struct store_key key = {
      .entry.hash_code = to_expire->entry.hash_code,
      .key = to_const_slice(to_expire->key),
  };

  // do_detach handles removing the entry from the heap
  struct store_entry *detached = do_detach(store, &key);
  assert(detached == to_expire);
  return to_expire;
}
