#include "store.h"

#include "hashmap.h"

#define STORE_INIT_CAP 64

void store_init(struct store *s) { hash_map_init(&s->map, STORE_INIT_CAP); }

static struct store_entry *store_entry_alloc(
    struct const_slice key, struct object val) {
  struct store_entry *new = malloc(sizeof(*new));
  assert(new != NULL);
  new->entry.hash_code = slice_hash(key);
  new->key = slice_dup(key);
  new->val = val;
  return new;
}

static void store_entry_free(struct store_entry *ent) {
  free(ent->key.data);
  object_destroy(ent->val);
  free(ent);
}

static bool store_entry_compare(
    const struct hash_entry *raw_a, const struct hash_entry *raw_b) {
  const struct store_entry *a = container_of(raw_a, struct store_entry, entry);
  const struct store_entry *b = container_of(raw_b, struct store_entry, entry);

  return (
      a->key.size == b->key.size &&
      memcmp(a->key.data, b->key.data, a->key.size) == 0);
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
  } else {
    struct store_entry *existing =
        container_of(found, struct store_entry, entry);
    return &existing->val;
  }
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
  } else {
    struct store_entry *existing_ent =
        container_of(existing, struct store_entry, entry);
    object_destroy(existing_ent->val);
    existing_ent->val = val;
    return &existing_ent->val;
  }
}

bool store_del(struct store *store, struct const_slice key) {
  // TODO: Re-structure hashmap API to avoid double hashing when inserting?
  struct store_key key_ent = {
      .entry.hash_code = slice_hash(key),
      .key = key,
  };

  struct hash_entry *removed =
      hash_map_delete(&store->map, &key_ent.entry, store_entry_compare);
  if (removed != NULL) {
    store_entry_free(container_of(removed, struct store_entry, entry));
    return true;
  } else {
    return false;
  }
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

void store_iter(struct store *store, store_iter_fn cb, void *arg) {
  struct store_iter_ctx ctx = {.callback = cb, .arg = arg};
  hash_map_iter(&store->map, store_iter_wrapper, &ctx);
}
