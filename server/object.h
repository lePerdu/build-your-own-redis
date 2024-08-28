#ifndef OBJECT_H_
#define OBJECT_H_

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>

#include "hashmap.h"
#include "types.h"

enum obj_type {
  OBJ_STR,
  OBJ_HMAP,
  OBJ_HSET,
  OBJ_ZSET,
};

struct object {
  enum obj_type type;
  union {
    string str_val;

    // These are both needed for ZSET
    struct {
      struct hash_map *hmap_val;
      struct avl_node *tree_val;
    };
  };
};

/**
 * Give a rough estimate for the number of allocations an object contains.
 *
 * This is used for determining whether to asynchronously free an object.
 */
uint32_t object_allocation_complexity(const struct object *obj);

static inline struct object make_string_object(string str) {
  return (struct object){.type = OBJ_STR, .str_val = str};
}

struct object make_hmap_object(void);
struct object make_hset_object(void);
struct object make_zset_object(void);

/**
 * Destroys the object and all sub-objects.
 */
void object_destroy(struct object obj);

bool hmap_get(
    struct object *obj, struct const_slice key, struct const_slice *val);
void hmap_set(struct object *obj, struct const_slice key, string val);
bool hmap_del(struct object *obj, struct const_slice key);
int_val_t hmap_size(struct object *obj);

typedef bool (*hmap_iter_fn)(
    struct const_slice key, struct const_slice val, void *arg);
void hmap_iter(struct object *obj, hmap_iter_fn iter, void *arg);

/** Returns `true` if the element was added, `false` if it already exists */
bool hset_add(struct object *obj, struct const_slice key);
bool hset_contains(struct object *obj, struct const_slice key);
/** Returns `true` if the element was removed, `false` if did not exist */
bool hset_del(struct object *obj, struct const_slice key);
/** Returns the removed element, NULL if empty */
struct hset_entry *hset_pop(struct object *obj);
/** Returns `true` if the element was found, `false` if empty */
const struct hset_entry *hset_peek(struct object *obj);

/** Key the key of an entry returned by `hset_pop` or `hset_peek` */
struct const_slice hset_entry_key(const struct hset_entry *entry);
/** Free an entry returned by `hset_pop` */
void hset_entry_free(struct hset_entry *entry);

int_val_t hset_size(struct object *obj);

typedef bool (*hset_iter_fn)(struct const_slice key, void *arg);
void hset_iter(struct object *obj, hset_iter_fn iter, void *arg);

uint32_t zset_size(struct object *obj);

/** Get score by name */
bool zset_score(struct object *obj, struct const_slice key, double *score);
/** Get rank by name */
int64_t zset_rank(struct object *obj, struct const_slice key);
/** Get node <= the target by name and score */
struct zset_node *zset_query(
    struct object *obj, struct const_slice key, double score);

struct const_slice zset_node_key(const struct zset_node *node);
double zset_node_score(const struct zset_node *node);
uint32_t zset_node_rank(struct object *obj, struct zset_node *node);
struct zset_node *zset_node_offset(struct zset_node *node, int64_t offset);

/** Add or update score */
bool zset_add(struct object *obj, struct const_slice key, double score);
/** Delete by name */
bool zset_del(struct object *obj, struct const_slice key);

#endif
