#include "object.h"

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "buffer.h"
#include "hashmap.h"
#include "protocol.h"
#include "types.h"

enum {
  HMAP_INIT_CAP = 8,
  HSET_INIT_CAP = 8,
};

static bool hmap_entry_free_iter(struct hash_entry *raw_ent, void *arg);
static bool hset_entry_free_iter(struct hash_entry *raw_ent, void *arg);

void object_destroy(struct object obj) {
  switch (obj.type) {
    case OBJ_INT:
      break;
    case OBJ_STR:
      free(obj.str_val.data);
      break;
    case OBJ_HMAP:
      hash_map_iter(&obj.hmap_val, hmap_entry_free_iter, NULL);
      hash_map_destroy(&obj.hmap_val);
      break;
    case OBJ_HSET:
      hash_map_iter(&obj.hmap_val, hset_entry_free_iter, NULL);
      hash_map_destroy(&obj.hmap_val);
      break;
    default:
      assert(false);
  }
}

void write_object(struct buffer *out, struct object *obj) {
  switch (obj->type) {
    case OBJ_INT:
      return write_int_value(out, obj->int_val);
    case OBJ_STR:
      return write_str_value(out, to_const_slice(obj->str_val));
    default:
      assert(false);
  }
}

struct hmap_entry {
  struct hash_entry entry;
  struct slice key;
  struct object val;
};

struct hmap_key {
  struct hash_entry entry;
  struct const_slice key;
};

static struct hmap_entry *hmap_entry_alloc(
    struct const_slice key, struct object val) {
  struct hmap_entry *ent = malloc(sizeof(*ent));
  assert(ent != NULL);
  ent->entry.hash_code = slice_hash(key);
  ent->key = slice_dup(key);
  ent->val = val;
  return ent;
}

static bool hmap_entry_compare(
    const struct hash_entry *raw_a, const struct hash_entry *raw_b) {
  return slice_eq(
      container_of(raw_a, struct hmap_key, entry)->key,
      container_of(raw_b, struct hmap_key, entry)->key);
}

static void hmap_entry_free(struct hmap_entry *entry) {
  free(entry->key.data);
  object_destroy(entry->val);
  free(entry);
}

static bool hmap_entry_free_iter(struct hash_entry *raw_ent, void *arg) {
  (void)arg;
  hmap_entry_free(container_of(raw_ent, struct hmap_entry, entry));
  return true;
}

struct object make_hmap_object(void) {
  struct object obj = {.type = OBJ_HMAP};
  hash_map_init(&obj.hmap_val, HMAP_INIT_CAP);
  return obj;
}

struct object *hmap_get(struct object *obj, struct const_slice key) {
  assert(obj->type == OBJ_HMAP);
  struct hash_map *map = &obj->hmap_val;

  // TODO: Re-structure hashmap API to avoid double hashing when inserting?
  struct hmap_key key_ent = {
      .entry.hash_code = slice_hash(key),
      .key = key,
  };

  struct hash_entry *found =
      hash_map_get(map, &key_ent.entry, hmap_entry_compare);
  if (found == NULL) {
    return NULL;
  }
  struct hmap_entry *existing = container_of(found, struct hmap_entry, entry);
  return &existing->val;
}

void hmap_set(struct object *obj, struct const_slice key, struct object val) {
  assert(obj->type == OBJ_HMAP);
  struct hash_map *map = &obj->hmap_val;

  // TODO: Re-structure hashmap API to avoid double hashing when inserting?
  struct hmap_key key_ent = {
      .entry.hash_code = slice_hash(key),
      .key = key,
  };

  struct hash_entry *existing =
      hash_map_get(map, &key_ent.entry, hmap_entry_compare);
  if (existing == NULL) {
    struct hmap_entry *new_ent = hmap_entry_alloc(key, val);
    hash_map_insert(map, &new_ent->entry);
  } else {
    struct hmap_entry *existing_ent =
        container_of(existing, struct hmap_entry, entry);
    object_destroy(existing_ent->val);
    existing_ent->val = val;
  }
}

bool hmap_del(struct object *obj, struct const_slice key) {
  assert(obj->type == OBJ_HMAP);
  struct hash_map *map = &obj->hmap_val;

  // TODO: Re-structure hashmap API to avoid double hashing when inserting?
  struct hmap_key key_ent = {
      .entry.hash_code = slice_hash(key),
      .key = key,
  };

  struct hash_entry *removed =
      hash_map_delete(map, &key_ent.entry, hmap_entry_compare);
  if (removed != NULL) {
    hmap_entry_free(container_of(removed, struct hmap_entry, entry));
    return true;
  }
  return false;
}

int_val_t hmap_size(struct object *obj) {
  assert(obj->type == OBJ_HMAP);
  return hash_map_size(&obj->hmap_val);
}

struct hmap_iter_ctx {
  hmap_iter_fn callback;
  void *arg;
};

static bool hmap_iter_wrapper(struct hash_entry *raw_ent, void *arg) {
  struct hmap_iter_ctx *ctx = arg;
  struct hmap_entry *ent = container_of(raw_ent, struct hmap_entry, entry);
  return ctx->callback(to_const_slice(ent->key), &ent->val, ctx->arg);
}

void hmap_iter(struct object *obj, hmap_iter_fn iter, void *arg) {
  assert(obj->type == OBJ_HMAP);
  struct hmap_iter_ctx ctx = {.callback = iter, .arg = arg};
  hash_map_iter(&obj->hmap_val, hmap_iter_wrapper, &ctx);
}

struct hset_entry {
  struct hash_entry entry;
  struct slice key;
};

struct hset_key {
  struct hash_entry entry;
  struct const_slice key;
};

struct object make_hset_object(void) {
  struct object obj = {.type = OBJ_HSET};
  hash_map_init(&obj.hmap_val, HSET_INIT_CAP);
  return obj;
}

static struct hset_entry *hset_entry_alloc(struct const_slice key) {
  struct hset_entry *ent = malloc(sizeof(*ent));
  assert(ent != NULL);
  ent->entry.hash_code = slice_hash(key);
  ent->key = slice_dup(key);
  return ent;
}

static bool hset_entry_compare(
    const struct hash_entry *raw_a, const struct hash_entry *raw_b) {
  return slice_eq(
      container_of(raw_a, struct hset_key, entry)->key,
      container_of(raw_b, struct hset_key, entry)->key);
}

static void hset_entry_free(struct hset_entry *entry) {
  free(entry->key.data);
  free(entry);
}

static struct slice hset_entry_free_extract_key(struct hset_entry *entry) {
  struct slice key = entry->key;
  free(entry);
  return key;
}

static bool hset_entry_free_iter(struct hash_entry *raw_ent, void *arg) {
  (void)arg;
  hset_entry_free(container_of(raw_ent, struct hset_entry, entry));
  return true;
}

bool hset_add(struct object *obj, struct const_slice key) {
  if (hset_contains(obj, key)) {
    return false;
  }

  struct hset_entry *new = hset_entry_alloc(key);
  hash_map_insert(&obj->hmap_val, &new->entry);
  return true;
}

bool hset_contains(struct object *obj, struct const_slice key) {
  assert(obj->type == OBJ_HSET);
  struct hash_map *set = &obj->hmap_val;

  struct hset_key key_ent = {
      .entry.hash_code = slice_hash(key),
      .key = key,
  };

  struct hash_entry *found =
      hash_map_get(set, &key_ent.entry, hset_entry_compare);
  return found != NULL;
}

bool hset_del(struct object *obj, struct const_slice key) {
  assert(obj->type == OBJ_HSET);
  struct hash_map *set = &obj->hmap_val;

  struct hset_key key_ent = {
      .entry.hash_code = slice_hash(key),
      .key = key,
  };

  struct hash_entry *exists =
      hash_map_delete(set, &key_ent.entry, hset_entry_compare);
  if (exists == NULL) {
    return false;
  }

  hset_entry_free(container_of(exists, struct hset_entry, entry));
  return true;
}

bool hset_pop(struct object *obj, struct slice *out) {
  assert(obj->type == OBJ_HSET);
  struct hash_map *set = &obj->hmap_val;

  struct hash_entry *found = hash_map_pop(set);
  if (found == NULL) {
    return false;
  }
  *out = hset_entry_free_extract_key(
      container_of(found, struct hset_entry, entry));
  return true;
}

bool hset_peek(struct object *obj, struct const_slice *out) {
  assert(obj->type == OBJ_HSET);
  struct hash_map *set = &obj->hmap_val;

  struct hash_entry *found = hash_map_peek(set);
  if (found == NULL) {
    return false;
  }
  *out = to_const_slice(container_of(found, struct hset_entry, entry)->key);
  return true;
}

int_val_t hset_size(struct object *obj) {
  assert(obj->type == OBJ_HSET);
  return hash_map_size(&obj->hmap_val);
}

struct hset_iter_ctx {
  hset_iter_fn callback;
  void *arg;
};

static bool hset_iter_wrapper(struct hash_entry *raw_ent, void *arg) {
  struct hset_iter_ctx *ctx = arg;
  struct hset_entry *ent = container_of(raw_ent, struct hset_entry, entry);
  return ctx->callback(to_const_slice(ent->key), ctx->arg);
}

void hset_iter(struct object *obj, hset_iter_fn iter, void *arg) {
  assert(obj->type == OBJ_HSET);
  struct hset_iter_ctx ctx = {.callback = iter, .arg = arg};
  hash_map_iter(&obj->hmap_val, hset_iter_wrapper, &ctx);
}
