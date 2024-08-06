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
  OBJ_STR,
  OBJ_HMAP,
};

struct object {
  enum obj_type type;
  union {
    int_val_t int_val;
    struct slice str_val;
    struct hash_map hmap_val;
  };
};

static inline bool object_is_scalar(enum obj_type type) {
  switch (type) {
    case OBJ_INT:
    case OBJ_STR:
      return true;
    case OBJ_HMAP:
      return false;
    default:
      assert(false);
  }
}

static inline struct object make_slice_object(struct slice slice) {
  return (struct object){.type = OBJ_STR, .str_val = slice};
}

static inline struct object make_int_object(int_val_t n) {
  return (struct object){.type = OBJ_INT, .int_val = n};
}

struct object make_hmap_object(void);

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

#endif
