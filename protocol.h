#ifndef PROTOCOL_H_
#define PROTOCOL_H_

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>

#define PROTO_HEADER_SIZE 4
#define PROTO_MAX_PAYLOAD_SIZE 4096
#define PROTO_MAX_MESSAGE_SIZE (PROTO_HEADER_SIZE + PROTO_MAX_PAYLOAD_SIZE)

typedef uint32_t message_len_t;
static_assert(
	sizeof(message_len_t) == PROTO_HEADER_SIZE, "incorrect message length type"
);

struct slice {
	size_t size;
	void *data;
};

struct const_slice {
	size_t size;
	const void *data;
};

enum req_type {
	REQ_GET = 0,
	REQ_SET = 1,
	REQ_DEL = 2,
};

struct request {
	enum req_type type;
	struct const_slice key;

	/**
	 * Only used for SET
	 */
	struct const_slice val;
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
	struct const_slice data;
};

enum write_result {
	WRITE_OK = 0,
	WRITE_ERR = -1,
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
