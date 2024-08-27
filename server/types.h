#ifndef TYPES_H_
#define TYPES_H_

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

typedef int64_t int_val_t;

// TODO: Use 32-bit sizes?

struct slice {
  size_t size;
  void *data;
};

struct const_slice {
  size_t size;
  const void *data;
};

/** Macro used for intrusive data types */
#define container_of(ptr, type, member) \
  ((type *)((void *)(ptr) - offsetof(type, member)))

static inline struct slice make_slice(void *data, size_t size) {
  return (struct slice){.size = size, .data = data};
}

static inline struct const_slice make_const_slice(
    const void *data, size_t size) {
  return (struct const_slice){.size = size, .data = data};
}

static inline struct const_slice make_str_slice(const char *str) {
  return make_const_slice(str, strlen(str));
}

static inline struct const_slice to_const_slice(struct slice slice) {
  return make_const_slice(slice.data, slice.size);
}

static inline void slice_set(struct slice slice, size_t index, uint8_t byte) {
  ((uint8_t *)slice.data)[index] = byte;
}

static inline uint8_t const_slice_get(struct const_slice slice, size_t index) {
  return ((const uint8_t *)slice.data)[index];
}

static inline void slice_advance(struct slice *slice, size_t n) {
  assert(n <= slice->size);
  slice->size -= n;
  slice->data += n;
}

static inline void const_slice_advance(struct const_slice *slice, size_t n) {
  assert(n <= slice->size);
  slice->size -= n;
  slice->data += n;
}

static inline struct slice slice_dup(struct const_slice slice) {
  void *new_data = malloc(slice.size);
  memcpy(new_data, slice.data, slice.size);
  return make_slice(new_data, slice.size);
}

static inline struct slice slice_extract(struct slice *slice) {
  struct slice original = *slice;
  slice->size = 0;
  slice->data = NULL;
  return original;
}

static inline bool slice_eq(
    struct const_slice slice_a, struct const_slice slice_b) {
  return (
      slice_a.size == slice_b.size &&
      memcmp(slice_a.data, slice_b.data, slice_a.size) == 0);
}

static inline ssize_t slice_index_of(struct const_slice slice, uint8_t byte) {
  // Can't use strchr because it needs to consider the slice size
  for (size_t i = 0; i < slice.size; i++) {
    if (const_slice_get(slice, i) == byte) {
      return (ssize_t)i;
    }
  }
  return -1;
}

#endif
