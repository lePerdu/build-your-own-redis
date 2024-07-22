#ifndef PROTOCOL_H_
#define PROTOCOL_H_

#include <assert.h>
#include <bits/types/struct_iovec.h>
#include <stdint.h>
#include <sys/types.h>

#define PROTO_HEADER_SIZE 4
#define PROTO_MAX_PAYLOAD_SIZE 4096
#define PROTO_MAX_MESSAGE_SIZE (PROTO_HEADER_SIZE + PROTO_MAX_PAYLOAD_SIZE)

typedef uint32_t message_len_t;
static_assert(
	sizeof(message_len_t) == PROTO_HEADER_SIZE, "incorrect message length type"
);

enum req_type {
	REQ_GET = 0,
	REQ_SET = 1,
	REQ_DEL = 2,
};

struct slice {
	size_t size;
	void *data;
};

struct request {
	enum req_type type;
	struct slice key;

	/**
	 * Only used for SET
	 */
	struct slice val;
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
	struct slice data;
};

enum write_result {
	WRITE_OK = 0,
	WRITE_ERR = -1,
};

ssize_t write_request(
	struct slice buffer,
	struct request *req
);
ssize_t parse_request(struct request *req, struct slice buffer);

ssize_t write_response(
	struct slice buffer,
	struct response *res
);
ssize_t parse_response(struct response *res, struct slice buffer);

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

/**
 * Try to parse 1 message from the buffer. Can return a `parse_result` in case of
 * error or more data is required. Returns the size of the message otherwise.
 */
ssize_t read_buf_parse(struct read_buf *r, uint8_t buf[PROTO_MAX_PAYLOAD_SIZE]);

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

static inline void write_buf_reset(struct write_buf *w) {
	w->buf_sent = 0;
	w->buf_size = 0;
}

/**
 * Copy data into the write buffer.
 */
void write_buf_set_message(struct write_buf *w, const void *buf, size_t size);

#endif
