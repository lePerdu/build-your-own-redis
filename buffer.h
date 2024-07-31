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

static inline void *buffer_tail(struct buffer *b) {
	return b->data + b->size;
}

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

#endif
