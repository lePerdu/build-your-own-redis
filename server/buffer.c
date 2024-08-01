#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "buffer.h"

void buffer_init(struct buffer *b, uint32_t init_cap) {
	b->size = 0;
	b->cap = init_cap;
	b->data = malloc(init_cap);
	assert(b->data != NULL);
}

void buffer_destroy(struct buffer *b) {
	free(b->data);
}

void buffer_ensure_cap(struct buffer *b, uint32_t extra_cap) {
	// Use a size up as an easy way to avoid overflow
	assert((uint64_t)b->size + extra_cap <= UINT32_MAX);
	uint32_t target_cap = b->size + extra_cap;
	if (b->cap >= target_cap) {
		return;
	}

	uint64_t new_cap = b->cap;
	do {
		new_cap *= 2;
	} while (new_cap < target_cap);

	b->cap = new_cap <= UINT32_MAX ? new_cap : UINT32_MAX;
	b->data = realloc(b->data, b->cap);
}

void buffer_ensure_cap_exact(struct buffer *b, uint32_t extra_cap) {
	assert((uint64_t)b->size + extra_cap <= UINT32_MAX);
	uint32_t target_cap = b->size + extra_cap;
	if (b->cap >= target_cap) {
		return;
	} else {
		b->data = realloc(b->data, target_cap);
		assert(b->data != NULL);
	}
}

void buffer_realloc(struct buffer *b, uint32_t new_cap) {
	assert(new_cap >= b->size);
	b->data = realloc(b->data, new_cap);
	assert(b->data != NULL);
}

void buffer_trunc(struct buffer *b) {
	buffer_realloc(b, b->size);
}

void buffer_append(struct buffer *b, const void *data, uint32_t size) {
	buffer_ensure_cap(b, size);
	memcpy(b->data + b->size, data, size);
	b->size += size;
}

void buffer_append_slice(struct buffer *b, struct const_slice s) {
	return buffer_append(b, s.data, s.size);
}

void buffer_append_byte(struct buffer *b, uint8_t byte) {
	buffer_ensure_cap(b, 1);
	b->size++;
	buffer_set_byte(b, b->size - 1, byte);
}

uint8_t buffer_get_byte(const struct buffer *b, uint32_t index) {
	assert(index < b->size);
	return ((uint8_t *) b->data)[index];
}

void buffer_set_byte(struct buffer *b, uint32_t index, uint8_t byte) {
	assert(index < b->size);
	((uint8_t *) b->data)[index] = byte;
}
