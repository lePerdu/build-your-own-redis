#ifndef PROTOCOL_H_
#define PROTOCOL_H_

#include <assert.h>
#include <bits/types/struct_iovec.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>

#include "types.h"
#include "buffer.h"

typedef uint32_t proto_size_t;

#define PROTO_SIZE_SIZE (sizeof(proto_size_t))
#define PROTO_HEADER_SIZE PROTO_SIZE_SIZE

enum proto_type {
	SER_NIL = 0,
	SER_INT = 1,
	SER_STR = 2,
	SER_ARR = 3,
};

// Helpers for deserializing

ssize_t parse_obj_type(enum proto_type *t, struct const_slice buffer);
ssize_t parse_int_value(int_val_t *n, struct const_slice buffer);
/**
 * Parses a string ref from the buffer (does not copy/allocate).
 */
ssize_t parse_str_value(struct const_slice *str, struct const_slice buffer);
ssize_t parse_arr_value(struct slice *str);

// Helpers for serializing

void write_obj_type(struct buffer *b, enum proto_type t);
void write_nil_value(struct buffer *b);
void write_int_value(struct buffer *b, int_val_t n);
void write_str_value(struct buffer *b, struct const_slice str);
void write_array_header(struct buffer *b, uint32_t n);

enum req_type {
	// Invalid request type, used for marking invalid/unknown data
	REQ_UNKNOWN = -1,
	REQ_GET = 0,
	REQ_SET = 1,
	REQ_DEL = 2,
	REQ_KEYS = 3,

	REQ_HGET = 16,
	REQ_HSET = 17,
	REQ_HDEL = 18,
	REQ_HKEYS = 19,
	REQ_HGETALL = 20,
	REQ_HLEN = 21,

	REQ_MAX_ID,
};

static_assert(REQ_MAX_ID <= UINT8_MAX, "Too many requests to fit in 1 byte");

/**
 * Subset of `struct object` specialized for requests which only stores
 * non-allocated values.
 */
struct req_object {
	/** Only OBJ_STR or OBJ_INT */
	enum proto_type type;
	union {
		int_val_t int_val;
		struct const_slice str_val;
	};
};

#define REQ_ARGS_MAX 2

struct request {
	enum req_type type;
	struct req_object args[REQ_ARGS_MAX];
};

enum parse_result {
	PARSE_OK = 0,
	PARSE_ERR = -1,
	PARSE_MORE = -2,
};

enum res_type {
	RES_OK = 0,
	RES_ERR = 1,
};

enum write_result {
	WRITE_OK = 0,
	WRITE_ERR = -1,
};

ssize_t parse_request(struct request *req, struct const_slice buffer);
void print_request(FILE *stream, const struct request *req);

/** Write the size at an arbitrary location (for writing it at the start). */
void write_message_size_at(void *buf, proto_size_t size);
void write_response_header(struct buffer *b, enum res_type res_type);

// Helpers for common response types
void write_nil_response(struct buffer *b);
void write_int_response(struct buffer *b, int_val_t n);
void write_str_response(struct buffer *b, struct const_slice str);
void write_arr_response_header(struct buffer *b, uint32_t size);
void write_err_response(struct buffer *b, const char *msg);

struct read_buf {
	uint32_t start;
	struct buffer buf;
};

void read_buf_init(struct read_buf *r, uint32_t init_cap);
void read_buf_reset_start(struct read_buf *r);
void read_buf_grow(struct read_buf *r, uint32_t extra);

/**
 * Position where data can be read from the buffer.
 */
static inline const void *read_buf_head(struct read_buf *r) {
	return r->buf.data + r->start;
}

/**
 * Size of the data currently available.
 */
static inline size_t read_buf_remaining(struct read_buf *r) {
	return r->buf.size - r->start;
}

/**
 * Advance the start of the buffer after parsing data from it.
 */
static inline void read_buf_advance(struct read_buf *r, uint32_t incr) {
	r->start += incr;
	assert(r->start <= r->buf.size);
}

static inline struct const_slice read_buf_head_slice(struct read_buf *r) {
	return make_const_slice(read_buf_head(r), read_buf_remaining(r));
}

/**
 * Position into which more data can be read.
 */
static inline void *read_buf_tail(struct read_buf *r) {
	return r->buf.data + r->buf.size;
}

/**
 * Remaining capacity in the buffer.
 */
static inline size_t read_buf_cap(const struct read_buf *r) {
	return r->buf.cap - r->buf.size;
}

/**
 * Advance the end of the buffer after reading data into it.
 */
static inline void read_buf_inc_size(struct read_buf *r, uint32_t incr) {
	r->buf.size += incr;
	assert(r->buf.size <= r->buf.cap);
}

static inline struct slice read_buf_tail_slice(struct read_buf *r) {
	return make_slice(read_buf_tail(r), read_buf_cap(r));
}

struct write_buf {
	uint32_t buf_sent;
	struct buffer buf;
};

void write_buf_init(struct write_buf *w, uint32_t init_cap);

/**
 * Data remaining to be sent.
 */
static inline const void *write_buf_head(struct write_buf *w) {
	return w->buf.data + w->buf_sent;
}

/**
 * Amount of unsent data.
 */
static inline size_t write_buf_remaining(const struct write_buf *w) {
	assert(w->buf.size >= w->buf_sent);
	return w->buf.size - w->buf_sent;
}

/**
 * Advance the "sent" position after sending data from the buffer.
 */
static inline void write_buf_advance(struct write_buf *w, uint32_t n) {
	assert(w->buf_sent + n <= w->buf.size);
	w->buf_sent += n;
}

static inline struct const_slice write_buf_head_slice(struct write_buf *w) {
	return make_const_slice(write_buf_head(w), write_buf_remaining(w));
}

static inline void *write_buf_tail(struct write_buf *w) {
	return w->buf.data + w->buf.size;
}

/**
 * Available remaining capacity.
 */
static inline size_t write_buf_capacity(const struct write_buf *w) {
	return w->buf.cap - w->buf.size;
}

/**
 * Advance the end of the buffer after writing data into the tail.
 */
static inline void write_buf_inc_size(struct write_buf *w, uint32_t incr) {
	w->buf.size += incr;
	assert(w->buf.size <= w->buf.cap);
}

static inline struct slice write_buf_tail_slice(struct write_buf *w) {
	return make_slice(write_buf_tail(w), write_buf_capacity(w));
}

static inline void write_buf_reset(struct write_buf *w) {
	w->buf_sent = 0;
	w->buf.size = 0;
}

#endif
