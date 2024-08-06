#ifndef BUFFER_H_
#define BUFFER_H_

#include <stdint.h>

#include "types.h"

struct buffer {
  uint32_t size;
  uint32_t cap;
  void *data;
};

static inline struct slice buffer_slice(struct buffer *b) {
  return make_slice(b->data, b->size);
}

static inline struct const_slice buffer_const_slice(const struct buffer *b) {
  return make_const_slice(b->data, b->size);
}

static inline void *buffer_tail(struct buffer *b) { return b->data + b->size; }

void buffer_init(struct buffer *b, uint32_t init_cap);
void buffer_destroy(struct buffer *b);

/**
 * Ensure there is at least `extra_cap` bytes of extra capacity.
 *
 * May allocate more space than needed so that it can be called repeatedly.
 */
void buffer_ensure_cap(struct buffer *b, uint32_t extra_cap);

/**
 * Like `buffer_ensure_cap`, but only allocate the exact amount needed.
 */
void buffer_ensure_cap_exact(struct buffer *b, uint32_t extra_cap);

/** `new_cap` must still fit the current buffer size. */
void buffer_realloc(struct buffer *b, uint32_t new_cap);

/** Truncate to exactly fit current data. */
void buffer_trunc(struct buffer *b);

void buffer_append(struct buffer *b, const void *data, uint32_t size);

void buffer_append_slice(struct buffer *b, struct const_slice s);

void buffer_append_byte(struct buffer *b, uint8_t byte);

uint8_t buffer_get_byte(const struct buffer *b, uint32_t index);
void buffer_set_byte(struct buffer *b, uint32_t index, uint8_t byte);

/**
 * Wrapper around `struct buffer` which has a moving `head` pointer.
 *
 * Data can be appended to the (growable) tail and head from the head.
 * Once the head is empty / not needed, the buffer can be reset to reclaim
 * the unused space in the head.
 */
struct offset_buf {
  uint32_t start;
  struct buffer buf;
};

static inline void offset_buf_reset(struct offset_buf *b) {
  b->start = 0;
  b->buf.size = 0;
}

static inline void offset_buf_init(struct offset_buf *b, uint32_t init_cap) {
  b->start = 0;
  buffer_init(&b->buf, init_cap);
}

/**
 * Position where data can be read from the buffer.
 */
static inline const void *offset_buf_head(struct offset_buf *b) {
  return b->buf.data + b->start;
}

/**
 * Size of the data currently available.
 */
static inline size_t offset_buf_remaining(struct offset_buf *b) {
  return b->buf.size - b->start;
}

/**
 * Advance the start of the buffer after parsing data from it.
 */
static inline void offset_buf_advance(struct offset_buf *b, uint32_t incr) {
  b->start += incr;
  assert(b->start <= b->buf.size);
}

static inline struct const_slice offset_buf_head_slice(struct offset_buf *b) {
  return make_const_slice(offset_buf_head(b), offset_buf_remaining(b));
}

/**
 * Position into which more data can be read.
 */
static inline void *offset_buf_tail(struct offset_buf *b) {
  return b->buf.data + b->buf.size;
}

/**
 * Remaining capacity in the buffer.
 */
static inline size_t offset_buf_cap(const struct offset_buf *b) {
  return b->buf.cap - b->buf.size;
}

/**
 * Advance the end of the buffer after reading data into it.
 */
static inline void offset_buf_inc_size(struct offset_buf *b, uint32_t incr) {
  b->buf.size += incr;
  assert(b->buf.size <= b->buf.cap);
}

static inline struct slice offset_buf_tail_slice(struct offset_buf *b) {
  return make_slice(offset_buf_tail(b), offset_buf_cap(b));
}

static inline void offset_buf_reset_start(struct offset_buf *b) {
  uint32_t remaining = offset_buf_remaining(b);
  if (remaining > 0 && b->start > 0) {
    memmove(b->buf.data, offset_buf_head(b), remaining);
  }
  // Always reset this either way
  b->start = 0;
  b->buf.size = remaining;
}

static inline void offset_buf_grow(struct offset_buf *b, uint32_t extra) {
  buffer_ensure_cap(&b->buf, extra);
}

#endif
