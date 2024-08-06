#ifndef TYPES_H_
#define TYPES_H_

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

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

static inline struct const_slice to_const_slice(struct slice s) {
  return make_const_slice(s.data, s.size);
}

static inline void slice_set(struct slice s, size_t index, uint8_t b) {
  ((uint8_t *)s.data)[index] = b;
}

static inline uint8_t const_slice_get(struct const_slice s, size_t index) {
  return ((const uint8_t *)s.data)[index];
}

static inline void slice_advance(struct slice *s, size_t n) {
  assert(n <= s->size);
  s->size -= n;
  s->data += n;
}

static inline void const_slice_advance(struct const_slice *s, size_t n) {
  assert(n <= s->size);
  s->size -= n;
  s->data += n;
}

static inline struct slice slice_dup(struct const_slice s) {
  void *new_data = malloc(s.size);
  memcpy(new_data, s.data, s.size);
  return make_slice(new_data, s.size);
}

#endif
