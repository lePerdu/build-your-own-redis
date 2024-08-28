#include "object.h"

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "avl.h"
#include "hashmap.h"
#include "types.h"

enum {
  HMAP_INIT_CAP = 8,
  HSET_INIT_CAP = 8,
  ZSET_INIT_CAP = 8,
};

static bool hmap_entry_free_iter(struct hash_entry *raw_ent, void *arg);
static bool hset_entry_free_iter(struct hash_entry *raw_ent, void *arg);
static bool zset_hash_entry_free_iter(struct hash_entry *raw_ent, void *arg);

static void free_hmap_part(
    struct hash_map *map, hash_entry_iter_fn free_entry) {
  hash_map_iter(map, free_entry, NULL);
  hash_map_destroy(map);
  free(map);
}

void object_destroy(struct object obj) {
  switch (obj.type) {
    case OBJ_STR:
      string_destroy(&obj.str_val);
      break;
    case OBJ_HMAP:
      free_hmap_part(obj.hmap_val, hmap_entry_free_iter);
      break;
    case OBJ_HSET:
      free_hmap_part(obj.hmap_val, hset_entry_free_iter);
      break;
    case OBJ_ZSET:
      free_hmap_part(obj.hmap_val, zset_hash_entry_free_iter);
      // Don't need to destroy the AVL tree because it uses the same nodes as
      // the hash table and doesn't have extra allocations
      break;
    default:
      assert(false);
  }
}

uint32_t object_allocation_complexity(const struct object *obj) {
  switch (obj->type) {
    case OBJ_STR:
      return 1;
    case OBJ_HSET:
    case OBJ_ZSET:
      // Entry and key for each object
      return hash_map_size(obj->hmap_val);
    case OBJ_HMAP:
      // Entry and value for each object
      return hash_map_size(obj->hmap_val) * 2;
    default:
      assert(false);
  }
}

struct hmap_entry {
  struct hash_entry entry;
  string val;
  struct inline_string key;
};

struct hmap_key {
  struct hash_entry entry;
  struct const_slice key;
};

static struct hmap_entry *hmap_entry_alloc(struct const_slice key, string val) {
  struct hmap_entry *ent = malloc(sizeof(*ent) + key.size);
  assert(ent != NULL);
  ent->entry.hash_code = slice_hash(key);
  inline_string_init_slice(&ent->key, key);
  ent->val = val;
  return ent;
}

static bool hmap_entry_compare(
    const struct hash_entry *raw_key, const struct hash_entry *raw_ent) {
  return slice_eq(
      container_of(raw_key, struct hmap_key, entry)->key,
      inline_string_const_slice(
          &container_of(raw_ent, struct hmap_entry, entry)->key));
}

static void hmap_entry_free(struct hmap_entry *entry) {
  string_destroy(&entry->val);
  free(entry);
}

static bool hmap_entry_free_iter(struct hash_entry *raw_ent, void *arg) {
  (void)arg;
  hmap_entry_free(container_of(raw_ent, struct hmap_entry, entry));
  return true;
}

struct object make_hmap_object(void) {
  struct object obj = {.type = OBJ_HMAP};
  obj.hmap_val = malloc(sizeof(*obj.hmap_val));
  assert(obj.hmap_val != NULL);
  hash_map_init(obj.hmap_val, HMAP_INIT_CAP);
  return obj;
}

bool hmap_get(
    struct object *obj, struct const_slice key, struct const_slice *val) {
  assert(obj->type == OBJ_HMAP);
  struct hash_map *map = obj->hmap_val;

  // TODO: Re-structure hashmap API to avoid double hashing when inserting?
  struct hmap_key key_ent = {
      .entry.hash_code = slice_hash(key),
      .key = key,
  };

  struct hash_entry *found =
      hash_map_get(map, &key_ent.entry, hmap_entry_compare);
  if (found == NULL) {
    return false;
  }
  struct hmap_entry *existing = container_of(found, struct hmap_entry, entry);
  *val = string_const_slice(&existing->val);
  return true;
}

void hmap_set(struct object *obj, struct const_slice key, string val) {
  assert(obj->type == OBJ_HMAP);
  struct hash_map *map = obj->hmap_val;

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
    string_destroy(&existing_ent->val);
    existing_ent->val = val;
  }
}

bool hmap_del(struct object *obj, struct const_slice key) {
  assert(obj->type == OBJ_HMAP);
  struct hash_map *map = obj->hmap_val;

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
  return hash_map_size(obj->hmap_val);
}

struct hmap_iter_ctx {
  hmap_iter_fn callback;
  void *arg;
};

static bool hmap_iter_wrapper(struct hash_entry *raw_ent, void *arg) {
  struct hmap_iter_ctx *ctx = arg;
  struct hmap_entry *ent = container_of(raw_ent, struct hmap_entry, entry);
  return ctx->callback(
      inline_string_const_slice(&ent->key), string_const_slice(&ent->val),
      ctx->arg);
}

void hmap_iter(struct object *obj, hmap_iter_fn iter, void *arg) {
  assert(obj->type == OBJ_HMAP);
  struct hmap_iter_ctx ctx = {.callback = iter, .arg = arg};
  hash_map_iter(obj->hmap_val, hmap_iter_wrapper, &ctx);
}

struct hset_entry {
  struct hash_entry entry;
  struct inline_string key;
};

struct hset_key {
  struct hash_entry entry;
  struct const_slice key;
};

struct object make_hset_object(void) {
  struct object obj = {.type = OBJ_HSET};
  obj.hmap_val = malloc(sizeof(*obj.hmap_val));
  assert(obj.hmap_val != NULL);
  hash_map_init(obj.hmap_val, HSET_INIT_CAP);
  return obj;
}

static struct hset_entry *hset_entry_alloc(struct const_slice key) {
  struct hset_entry *ent = malloc(sizeof(*ent) + key.size);
  assert(ent != NULL);
  ent->entry.hash_code = slice_hash(key);
  inline_string_init_slice(&ent->key, key);
  return ent;
}

static bool hset_entry_compare(
    const struct hash_entry *raw_key, const struct hash_entry *raw_ent) {
  return slice_eq(
      container_of(raw_key, struct hset_key, entry)->key,
      hset_entry_key(container_of(raw_ent, struct hset_entry, entry)));
}

struct const_slice hset_entry_key(const struct hset_entry *entry) {
  return inline_string_const_slice(&entry->key);
}

void hset_entry_free(struct hset_entry *entry) { free(entry); }

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
  hash_map_insert(obj->hmap_val, &new->entry);
  return true;
}

bool hset_contains(struct object *obj, struct const_slice key) {
  assert(obj->type == OBJ_HSET);
  struct hash_map *set = obj->hmap_val;

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
  struct hash_map *set = obj->hmap_val;

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

struct hset_entry *hset_pop(struct object *obj) {
  assert(obj->type == OBJ_HSET);
  struct hash_map *set = obj->hmap_val;

  struct hash_entry *found = hash_map_pop(set);
  if (found == NULL) {
    return NULL;
  }
  return container_of(found, struct hset_entry, entry);
}

const struct hset_entry *hset_peek(struct object *obj) {
  assert(obj->type == OBJ_HSET);
  struct hash_map *set = obj->hmap_val;

  struct hash_entry *found = hash_map_peek(set);
  if (found == NULL) {
    return NULL;
  }
  return container_of(found, struct hset_entry, entry);
}

int_val_t hset_size(struct object *obj) {
  assert(obj->type == OBJ_HSET);
  return hash_map_size(obj->hmap_val);
}

struct hset_iter_ctx {
  hset_iter_fn callback;
  void *arg;
};

static bool hset_iter_wrapper(struct hash_entry *raw_ent, void *arg) {
  struct hset_iter_ctx *ctx = arg;
  struct hset_entry *ent = container_of(raw_ent, struct hset_entry, entry);
  return ctx->callback(inline_string_const_slice(&ent->key), ctx->arg);
}

void hset_iter(struct object *obj, hset_iter_fn iter, void *arg) {
  assert(obj->type == OBJ_HSET);
  struct hset_iter_ctx ctx = {.callback = iter, .arg = arg};
  hash_map_iter(obj->hmap_val, hset_iter_wrapper, &ctx);
}

struct zset_node {
  struct hash_entry hash_base;
  struct avl_node avl_base;
  double score;
  struct inline_string key;
};

struct zset_hash_key {
  struct hash_entry base;
  struct const_slice key;
};

struct zset_tree_key {
  struct const_slice key;
  double score;
};

struct const_slice zset_node_key(const struct zset_node *node) {
  return inline_string_const_slice(&node->key);
}

double zset_node_score(const struct zset_node *node) { return node->score; }

static bool zset_node_eq(
    const struct hash_entry *raw_key, const struct hash_entry *raw_ent) {
  return slice_eq(
      container_of(raw_key, struct zset_hash_key, base)->key,
      inline_string_const_slice(
          &container_of(raw_ent, struct zset_node, hash_base)->key));
}

static int zset_compare_helper(
    struct const_slice key1, double score1, struct const_slice key2,
    double score2) {
  double cmp_score = score1 - score2;
  if (cmp_score < 0.0) {
    return -1;
  }
  if (cmp_score > 0.0) {
    return 1;
  }

  size_t size1 = key1.size;
  size_t size2 = key2.size;
  size_t min_size = size1 <= size2 ? size1 : size2;
  int cmp_prefix = memcmp(key1.data, key2.data, min_size);
  if (cmp_prefix != 0) {
    return cmp_prefix;
  }

  // Subtraction could overflow, so do this to be on the safe side
  if (size1 < size2) {
    return -1;
  }
  if (size1 > size2) {
    return 1;
  }
  return 0;
}

static int zset_node_compare(
    const struct avl_node *raw_a, const struct avl_node *raw_b) {
  const struct zset_node *node_a =
      container_of(raw_a, struct zset_node, avl_base);
  const struct zset_node *node_b =
      container_of(raw_b, struct zset_node, avl_base);
  return zset_compare_helper(
      zset_node_key(node_a), node_a->score, zset_node_key(node_b),
      node_b->score);
}

static int zset_key_compare(
    const void *raw_key, const struct avl_node *raw_node) {
  const struct zset_tree_key *key = raw_key;
  const struct zset_node *node =
      container_of(raw_node, struct zset_node, avl_base);
  return zset_compare_helper(
      key->key, key->score, zset_node_key(node), node->score);
}

static struct zset_node *zset_node_alloc(struct const_slice key, double score) {
  struct zset_node *node = malloc(sizeof(*node) + key.size);
  assert(node != NULL);
  node->hash_base.hash_code = slice_hash(key);
  avl_init(&node->avl_base);
  inline_string_init_slice(&node->key, key);
  node->score = score;
  return node;
}

static void zset_node_free(struct zset_node *node) { free(node); }

static bool zset_hash_entry_free_iter(struct hash_entry *raw_ent, void *arg) {
  (void)arg;
  zset_node_free(container_of(raw_ent, struct zset_node, hash_base));
  return true;
}

struct object make_zset_object(void) {
  struct object obj = {.type = OBJ_ZSET};
  obj.hmap_val = malloc(sizeof(*obj.hmap_val));
  assert(obj.hmap_val != NULL);
  hash_map_init(obj.hmap_val, ZSET_INIT_CAP);
  obj.tree_val = NULL;
  return obj;
}

uint32_t zset_size(struct object *obj) {
  assert(obj->type == OBJ_ZSET);
  uint32_t hash_size = hash_map_size(obj->hmap_val);
  uint32_t tree_size = avl_size(obj->tree_val);
  assert(hash_size == tree_size);
  return hash_size;
}

bool zset_score(struct object *obj, struct const_slice key, double *score) {
  assert(obj->type == OBJ_ZSET);
  struct zset_hash_key hash_key = {
      .base.hash_code = slice_hash(key),
      .key = key,
  };
  struct hash_entry *found =
      hash_map_get(obj->hmap_val, &hash_key.base, zset_node_eq);
  if (found == NULL) {
    return false;
  }

  *score = container_of(found, struct zset_node, hash_base)->score;
  return true;
}

bool zset_add(struct object *obj, struct const_slice key, double score) {
  assert(obj->type == OBJ_ZSET);
  struct hash_map *map = obj->hmap_val;

  struct zset_hash_key key_ent = {
      .base.hash_code = slice_hash(key),
      .key = key,
  };

  struct hash_entry *found = hash_map_get(map, &key_ent.base, zset_node_eq);
  if (found == NULL) {
    struct zset_node *new = zset_node_alloc(key, score);
    hash_map_insert(map, &new->hash_base);
    avl_insert(&obj->tree_val, &new->avl_base, zset_node_compare);
    return true;
  }

  // Delete and re-insert with the new score
  struct zset_node *existing = container_of(found, struct zset_node, hash_base);
  avl_delete(&obj->tree_val, &existing->avl_base);
  existing->score = score;
  avl_insert(&obj->tree_val, &existing->avl_base, zset_node_compare);
  return false;
}

bool zset_del(struct object *obj, struct const_slice key) {
  assert(obj->type == OBJ_ZSET);
  struct hash_map *map = obj->hmap_val;

  struct zset_hash_key key_ent = {
      .base.hash_code = slice_hash(key),
      .key = key,
  };

  struct hash_entry *removed =
      hash_map_delete(map, &key_ent.base, zset_node_eq);
  if (removed == NULL) {
    return false;
  }

  // Delete and re-insert with the new score
  struct zset_node *existing =
      container_of(removed, struct zset_node, hash_base);
  avl_delete(&obj->tree_val, &existing->avl_base);
  zset_node_free(existing);
  return true;
}

int64_t zset_rank(struct object *obj, struct const_slice key) {
  assert(obj->type == OBJ_ZSET);
  struct hash_map *map = obj->hmap_val;

  struct zset_hash_key key_ent = {
      .base.hash_code = slice_hash(key),
      .key = key,
  };

  struct hash_entry *found_raw = hash_map_get(map, &key_ent.base, zset_node_eq);
  if (found_raw == NULL) {
    return -1;
  }

  struct zset_node *found =
      container_of(found_raw, struct zset_node, hash_base);
  return avl_rank(obj->tree_val, &found->avl_base);
}

struct zset_node *zset_query(
    struct object *obj, struct const_slice key, double score) {
  assert(obj->type == OBJ_ZSET);
  struct zset_tree_key key_ent = {
      .key = key,
      .score = score,
  };

  struct avl_node *found =
      avl_search_lte(obj->tree_val, &key_ent, zset_key_compare);
  if (found == NULL) {
    return NULL;
  }

  return container_of(found, struct zset_node, avl_base);
}

uint32_t zset_node_rank(struct object *obj, struct zset_node *node) {
  assert(obj->type == OBJ_ZSET);
  return avl_rank(obj->tree_val, &node->avl_base);
}

struct zset_node *zset_node_offset(struct zset_node *node, int64_t offset) {
  if (node == NULL) {
    return NULL;
  }

  struct avl_node *target = avl_offset(&node->avl_base, offset);
  if (target == NULL) {
    return NULL;
  }

  return container_of(target, struct zset_node, avl_base);
}
