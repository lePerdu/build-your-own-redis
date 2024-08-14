#ifndef OBJECT_H_
#define OBJECT_H_

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>

#include "buffer.h"
#include "hashmap.h"
#include "types.h"

enum obj_type {
  OBJ_INT,
  OBJ_FLOAT,
  OBJ_STR,
  OBJ_HMAP,
  OBJ_HSET,
  OBJ_ZSET,
};

struct object {
  enum obj_type type;
  union {
    int_val_t int_val;
    double float_val;
    struct slice str_val;

    // These are both needed for ZSET
    struct {
      struct hash_map *hmap_val;
      struct avl_node *tree_val;
    };
  };
};

static inline bool object_is_scalar(enum obj_type type) {
  switch (type) {
    case OBJ_INT:
    case OBJ_FLOAT:
    case OBJ_STR:
      return true;
    case OBJ_HMAP:
    case OBJ_HSET:
    case OBJ_ZSET:
      return false;
    default:
      assert(false);
  }
}

/**
 * Give a rough estimate for the number of allocations an object contains.
 *
 * This is used for determining whether to asynchronously free an object.
 */
uint32_t object_allocation_complexity(const struct object *obj);

static inline struct object make_slice_object(struct slice slice) {
  return (struct object){.type = OBJ_STR, .str_val = slice};
}

static inline struct object make_int_object(int_val_t n) {
  return (struct object){.type = OBJ_INT, .int_val = n};
}

static inline struct object make_float_object(double val) {
  return (struct object){.type = OBJ_FLOAT, .float_val = val};
}

struct object make_hmap_object(void);
struct object make_hset_object(void);
struct object make_zset_object(void);

/**
 * Destroys the object and all sub-objects.
 */
void object_destroy(struct object obj);

void write_object(struct buffer *out, struct object *obj);

struct object *hmap_get(struct object *obj, struct const_slice key);
void hmap_set(struct object *obj, struct const_slice key, struct object val);
bool hmap_del(struct object *obj, struct const_slice key);
int_val_t hmap_size(struct object *obj);

typedef bool (*hmap_iter_fn)(
    struct const_slice key, struct object *val, void *arg);
void hmap_iter(struct object *obj, hmap_iter_fn iter, void *arg);

/** Returns `true` if the element was added, `false` if it already exists */
bool hset_add(struct object *obj, struct const_slice key);
bool hset_contains(struct object *obj, struct const_slice key);
/** Returns `true` if the element was removed, `false` if did not exist */
bool hset_del(struct object *obj, struct const_slice key);
/** Returns `true` if the element was removed, `false` if empty */
bool hset_pop(struct object *obj, struct slice *out);
/** Returns `true` if the element was found, `false` if empty */
bool hset_peek(struct object *obj, struct const_slice *out);

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
/** Delete by node reference (from zset_query) */
/*void zset_node_delete(struct object *obj, struct zset_node *node);*/

/** Add or update score */
bool zset_add(struct object *obj, struct const_slice key, double score);
/** Delete by name */
bool zset_del(struct object *obj, struct const_slice key);

#endif
