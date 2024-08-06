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
};

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
  const struct hmap_key *ent_a = container_of(raw_a, struct hmap_key, entry);
  const struct hmap_key *ent_b = container_of(raw_b, struct hmap_key, entry);
  return (
      ent_a->key.size == ent_b->key.size &&
      memcmp(ent_a->key.data, ent_b->key.data, ent_a->key.size) == 0);
}

static bool hmap_entry_free(struct hash_entry *raw_ent, void *arg) {
  (void)arg;
  struct hmap_entry *ent = container_of(raw_ent, struct hmap_entry, entry);
  free(ent->key.data);
  object_destroy(ent->val);
  free(ent);
  return true;
}

void object_destroy(struct object obj) {
  switch (obj.type) {
    case OBJ_INT:
      break;
    case OBJ_STR:
      free(obj.str_val.data);
      break;
    case OBJ_HMAP:
      hash_map_iter(&obj.hmap_val, hmap_entry_free, NULL);
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
    hmap_entry_free(removed, NULL);
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
