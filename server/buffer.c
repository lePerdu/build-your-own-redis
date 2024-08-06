#include "buffer.h"

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "types.h"

void buffer_init(struct buffer *buf, uint32_t init_cap) {
  buf->size = 0;
  buf->cap = init_cap;
  buf->data = malloc(init_cap);
  assert(buf->data != NULL);
}

void buffer_destroy(struct buffer *buf) { free(buf->data); }

void buffer_ensure_cap(struct buffer *buf, uint32_t extra_cap) {
  // Use a size up as an easy way to avoid overflow
  assert((uint64_t)buf->size + extra_cap <= UINT32_MAX);
  uint32_t target_cap = buf->size + extra_cap;
  if (buf->cap >= target_cap) {
    return;
  }

  uint64_t new_cap = buf->cap;
  do {
    new_cap *= 2;
  } while (new_cap < target_cap);

  buf->cap = new_cap <= UINT32_MAX ? new_cap : UINT32_MAX;
  void *new_data = realloc(buf->data, buf->cap);
  assert(new_data != NULL);
  buf->data = new_data;
}

void buffer_ensure_cap_exact(struct buffer *buf, uint32_t extra_cap) {
  assert((uint64_t)buf->size + extra_cap <= UINT32_MAX);
  uint32_t target_cap = buf->size + extra_cap;
  if (buf->cap >= target_cap) {
    return;
  }
  void *new_data = realloc(buf->data, target_cap);
  assert(new_data != NULL);
  buf->data = new_data;
}

void buffer_realloc(struct buffer *buf, uint32_t new_cap) {
  assert(new_cap >= buf->size);
  void *new_data = realloc(buf->data, new_cap);
  assert(new_data != NULL);
  buf->data = new_data;
}

void buffer_trunc(struct buffer *buf) { buffer_realloc(buf, buf->size); }

void buffer_append(struct buffer *buf, const void *data, uint32_t size) {
  buffer_ensure_cap(buf, size);
  memcpy(buf->data + buf->size, data, size);
  buf->size += size;
}

void buffer_append_slice(struct buffer *buf, struct const_slice slice) {
  return buffer_append(buf, slice.data, slice.size);
}

void buffer_append_byte(struct buffer *buf, uint8_t byte) {
  buffer_ensure_cap(buf, 1);
  buf->size++;
  buffer_set_byte(buf, buf->size - 1, byte);
}

uint8_t buffer_get_byte(const struct buffer *buf, uint32_t index) {
  assert(index < buf->size);
  return ((uint8_t *)buf->data)[index];
}

void buffer_set_byte(struct buffer *buf, uint32_t index, uint8_t byte) {
  assert(index < buf->size);
  ((uint8_t *)buf->data)[index] = byte;
}
