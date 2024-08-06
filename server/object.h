#ifndef OBJECT_H_
#define OBJECT_H_

#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>

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

static inline bool object_is_scalar(enum obj_type t) {
  switch (t) {
    case OBJ_INT:
    case OBJ_STR:
      return true;
    case OBJ_HMAP:
      return false;
    default:
      assert(false);
  }
}

static inline struct object make_slice_object(struct slice s) {
  return (struct object){.type = OBJ_STR, .str_val = s};
}

static inline struct object make_int_object(int_val_t n) {
  return (struct object){.type = OBJ_INT, .int_val = n};
}

static inline struct object make_hmap_object(void) {
  struct object o = {.type = OBJ_HMAP};
  hash_map_init(&o.hmap_val, 8);
  return o;
}

/**
 * Destroys the object and all sub-objects.
 */
void object_destroy(struct object o);

void write_object(struct buffer *b, struct object *o);

struct object *hmap_get(struct object *o, struct const_slice key);
void hmap_set(struct object *o, struct const_slice key, struct object val);
bool hmap_del(struct object *o, struct const_slice key);
int_val_t hmap_size(struct object *o);

typedef bool (*hmap_iter_fn)(
    struct const_slice key, struct object *val, void *arg);
void hmap_iter(struct object *o, hmap_iter_fn cb, void *arg);

#endif
