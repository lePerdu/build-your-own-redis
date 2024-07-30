#ifndef PROTOCOL_H_
#define PROTOCOL_H_

#include <assert.h>
#include <bits/types/struct_iovec.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>

typedef uint32_t proto_size_t;

#define PROTO_SIZE_SIZE (sizeof(proto_size_t))
#define PROTO_HEADER_SIZE PROTO_SIZE_SIZE
#define PROTO_MAX_PAYLOAD_SIZE 4096
#define PROTO_MAX_MESSAGE_SIZE (PROTO_HEADER_SIZE + PROTO_MAX_PAYLOAD_SIZE)

enum obj_type {
	OBJ_NIL,
	OBJ_INT,
	OBJ_STR,
	OBJ_ARR,
};

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

struct array {
	size_t size;
	struct object *data;
};

struct object {
	enum obj_type type;
	// TODO: Store in type field
	bool owned;
	union {
		int_val_t int_val;

		struct slice str_val; 
		struct const_slice str_ref;
		struct array arr_val;
	};
};

static inline struct slice make_slice(void *data, size_t size) {
	return (struct slice) {.size = size, .data = data};
}

static inline struct const_slice make_const_slice(const void *data, size_t size) {
	return (struct const_slice) {.size = size, .data = data};
}

static inline struct const_slice make_str_slice(const char *str) {
	return make_const_slice(str, strlen(str));
}

static inline struct const_slice to_const_slice(struct slice s) {
	return make_const_slice(s.data, s.size);
}

static inline void slice_set(struct slice s, size_t index, uint8_t b) {
	((uint8_t *) s.data)[index] = b;
}

static inline uint8_t const_slice_get(struct const_slice s, size_t index) {
	return ((const uint8_t *) s.data)[index];
}

static inline struct object make_str_object(const char *s) {
	return (struct object) {
		.type = OBJ_STR,
		.owned = false,
		.str_ref = make_str_slice(s),
	};
}

static inline struct object make_slice_object(struct const_slice s) {
	return (struct object) { .type = OBJ_STR, .owned = false, .str_ref = s };
}

static inline struct object make_owned_slice_object(struct slice s) {
	return (struct object) { .type = OBJ_STR, .owned = true, .str_val = s };
}

static inline struct object make_nil_object(void) {
	return (struct object) {.type = OBJ_NIL, .owned = false};
}

struct object make_arr_object(size_t size);

bool object_to_slice(struct const_slice *s, struct object o);

struct object object_to_ref(struct object o);
struct object object_to_owned(struct object o);

/**
 * Destroys the object and all sub-objects.
 */
void object_destroy(struct object o);

ssize_t parse_int_value(int_val_t *n, struct const_slice buffer);
ssize_t write_int_value(struct slice buffer, int_val_t n);

ssize_t parse_str_value(struct slice *str, struct const_slice buffer);
/**
 * Like `parse_str_value`, but doesn't copy the string data: `str->data`
 * points to data inside `buffer`.
 */
ssize_t parse_str_ref_value(struct const_slice *str, struct const_slice buffer);
ssize_t write_str_value(struct slice buffer, struct const_slice str);

ssize_t parse_array_value(struct array *arr, struct const_slice buffer);
ssize_t write_array_value(struct slice buffer, struct array arr);

// Helpers when manually serializing an array
ssize_t parse_array_size(size_t *n, struct const_slice buffer);
ssize_t write_array_size(struct slice buffer, size_t n);

ssize_t parse_object(struct object *v, struct const_slice buffer);
ssize_t write_object(struct slice buffer, struct object v);

enum req_type {
	// Invalid request type, used for marking invalid/unknown data
	REQ_UNKNOWN = -1,
	REQ_GET = 0,
	REQ_SET = 1,
	REQ_DEL = 2,
	REQ_KEYS = 3,
	REQ_MAX_ID,
};

static_assert(REQ_MAX_ID <= UINT8_MAX, "Too many requests to fit in 1 byte");

#define REQ_ARGS_MAX 2

struct request {
	enum req_type type;
	struct object args[REQ_ARGS_MAX];
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

struct response {
	enum res_type type;
	struct object arg;
};

enum write_result {
	WRITE_OK = 0,
	WRITE_ERR = -1,
};

ssize_t write_request(
	struct slice buffer,
	const struct request *req
);
ssize_t parse_request(struct request *req, struct const_slice buffer);
void print_request(FILE *stream, const struct request *req);

ssize_t write_response(
	struct slice buffer,
	const struct response *res
);
ssize_t parse_response(struct response *res, struct const_slice buffer);
void print_response(FILE *stream, const struct response *res);

struct read_buf {
	size_t buf_start;
	size_t buf_size;
	// Large enough to hold 1 max-sized message
	uint8_t buf[PROTO_MAX_MESSAGE_SIZE];
};

void read_buf_init(struct read_buf *r);
void read_buf_reset_start(struct read_buf *r);

/**
 * Position where data can be read from the buffer.
 */
static inline void *read_buf_start_pos(struct read_buf *r) {
	return &r->buf[r->buf_start];
}

/**
 * Size of the data currently available.
 */
static inline size_t read_buf_size(struct read_buf *r) {
	return r->buf_size;
}

/**
 * Advance the start of the buffer after parsing data from it.
 */
static inline void read_buf_advance(struct read_buf *r, size_t incr) {
	r->buf_start += incr;
	assert(r->buf_start <= sizeof(r->buf));

	assert(incr <= r->buf_size);
	r->buf_size -= incr;
}

static inline struct const_slice read_buf_head_slice(struct read_buf *r) {
	return make_const_slice(read_buf_start_pos(r), read_buf_size(r));
}

/**
 * Position into which more data can be read.
 */
static inline void *read_buf_read_pos(struct read_buf *r) {
	return &r->buf[r->buf_start + r->buf_size];
}

/**
 * Remaining capacity in the buffer.
 */
static inline size_t read_buf_cap(const struct read_buf *r) {
	return sizeof(r->buf) - (r->buf_start + r->buf_size);
}

/**
 * Advance the end of the buffer after reading data into it.
 */
static inline void read_buf_inc_size(struct read_buf *r, size_t incr) {
	r->buf_size += incr;
	assert(r->buf_size <= sizeof(r->buf));
}

static inline struct slice read_buf_tail_slice(struct read_buf *r) {
	return make_slice(read_buf_read_pos(r), read_buf_cap(r));
}

struct write_buf {
	size_t buf_size;
	size_t buf_sent;
	uint8_t buf[PROTO_MAX_MESSAGE_SIZE];
};

void write_buf_init(struct write_buf *w);

/**
 * Data remaining to be sent.
 */
static inline void *write_buf_write_pos(struct write_buf *w) {
	return &w->buf[w->buf_sent];
}

/**
 * Amount of unsent data.
 */
static inline size_t write_buf_remaining(const struct write_buf *w) {
	assert(w->buf_size >= w->buf_sent);
	return w->buf_size - w->buf_sent;
}

/**
 * Advance the "sent" position after sending data from the buffer.
 */
static inline void write_buf_advance(struct write_buf *w, size_t n) {
	assert(w->buf_sent + n <= w->buf_size);
	w->buf_sent += n;
}

static inline struct const_slice write_buf_head_slice(struct write_buf *w) {
	return make_const_slice(write_buf_write_pos(w), write_buf_remaining(w));
}

static inline void *write_buf_tail(struct write_buf *w) {
	return &w->buf[w->buf_size];
}

/**
 * Available remaining capacity.
 */
static inline size_t write_buf_capacity(const struct write_buf *w) {
	return sizeof(w->buf) - w->buf_size;
}

/**
 * Advance the end of the buffer after writing data into the tail.
 */
static inline void write_buf_inc_size(struct write_buf *rw, size_t incr) {
	rw->buf_size += incr;
	assert(rw->buf_size <= sizeof(rw->buf));
}

static inline struct slice write_buf_tail_slice(struct write_buf *w) {
	return make_slice(write_buf_tail(w), write_buf_capacity(w));
}

static inline void write_buf_reset(struct write_buf *w) {
	w->buf_sent = 0;
	w->buf_size = 0;
}

#endif
