#ifndef BUFFER_H_
#define BUFFER_H_

#include <assert.h>
#include <stdint.h>
#include <string.h>

#include "types.h"

struct buffer {
  uint32_t size;
  uint32_t cap;
  void *data;
};

static inline struct slice buffer_slice(struct buffer *buf) {
  return make_slice(buf->data, buf->size);
}

static inline struct const_slice buffer_const_slice(const struct buffer *buf) {
  return make_const_slice(buf->data, buf->size);
}

static inline void *buffer_tail(struct buffer *buf) {
  return buf->data + buf->size;
}

static inline uint32_t buffer_remaining(const struct buffer *buf) {
  return buf->cap - buf->size;
}

static inline void buffer_inc_size(struct buffer *buf, uint32_t inc) {
  assert(buf->size + inc <= buf->cap);
  buf->size += inc;
}

void buffer_init(struct buffer *buf, uint32_t init_cap);
void buffer_destroy(struct buffer *buf);

/**
 * Ensure there is at least `extra_cap` bytes of extra capacity.
 *
 * May allocate more space than needed so that it can be called repeatedly.
 */
void buffer_ensure_cap(struct buffer *buf, uint32_t extra_cap);

/**
 * Like `buffer_ensure_cap`, but only allocate the exact amount needed.
 */
void buffer_ensure_cap_exact(struct buffer *buf, uint32_t extra_cap);

/** `new_cap` must still fit the current buffer size. */
void buffer_realloc(struct buffer *buf, uint32_t new_cap);

/** Truncate to exactly fit current data. */
void buffer_trunc(struct buffer *buf);

void buffer_append(struct buffer *buf, const void *data, uint32_t size);

void buffer_append_slice(struct buffer *buf, struct const_slice slice);

void buffer_append_byte(struct buffer *buf, uint8_t byte);

uint8_t buffer_get_byte(const struct buffer *buf, uint32_t index);
void buffer_set_byte(struct buffer *buf, uint32_t index, uint8_t byte);

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

static inline void offset_buf_reset(struct offset_buf *buf) {
  buf->start = 0;
  buf->buf.size = 0;
}

static inline void offset_buf_init(struct offset_buf *buf, uint32_t init_cap) {
  buf->start = 0;
  buffer_init(&buf->buf, init_cap);
}

static inline void offset_buf_destroy(struct offset_buf *buf) {
  buffer_destroy(&buf->buf);
}

/**
 * Position where data can be read from the buffer.
 */
static inline const void *offset_buf_head(struct offset_buf *buf) {
  return buf->buf.data + buf->start;
}

/**
 * Size of the data currently available.
 */
static inline size_t offset_buf_remaining(struct offset_buf *buf) {
  return buf->buf.size - buf->start;
}

/**
 * Advance the start of the buffer after parsing data from it.
 */
static inline void offset_buf_advance(struct offset_buf *buf, uint32_t incr) {
  buf->start += incr;
  assert(buf->start <= buf->buf.size);
}

static inline struct const_slice offset_buf_head_slice(struct offset_buf *buf) {
  return make_const_slice(offset_buf_head(buf), offset_buf_remaining(buf));
}

/**
 * Position into which more data can be read.
 */
static inline void *offset_buf_tail(struct offset_buf *buf) {
  return buf->buf.data + buf->buf.size;
}

/**
 * Remaining capacity in the buffer.
 */
static inline size_t offset_buf_cap(const struct offset_buf *buf) {
  return buf->buf.cap - buf->buf.size;
}

/**
 * Advance the end of the buffer after reading data into it.
 */
static inline void offset_buf_inc_size(struct offset_buf *buf, uint32_t incr) {
  buf->buf.size += incr;
  assert(buf->buf.size <= buf->buf.cap);
}

static inline struct slice offset_buf_tail_slice(struct offset_buf *buf) {
  return make_slice(offset_buf_tail(buf), offset_buf_cap(buf));
}

static inline void offset_buf_reset_start(struct offset_buf *buf) {
  uint32_t remaining = offset_buf_remaining(buf);
  if (remaining > 0 && buf->start > 0) {
    memmove(buf->buf.data, offset_buf_head(buf), remaining);
  }
  // Always reset this either way
  buf->start = 0;
  buf->buf.size = remaining;
}

static inline void offset_buf_grow(struct offset_buf *buf, uint32_t extra) {
  buffer_ensure_cap(&buf->buf, extra);
}

#endif
