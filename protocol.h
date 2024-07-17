#ifndef PROTOCOL_H_
#define PROTOCOL_H_

#include <bits/types/struct_iovec.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#define PROTO_HEADER_SIZE 4
#define PROTO_MAX_MESSAGE_SIZE 4096

ssize_t read_one_message(int fd, void *buf, size_t size);
ssize_t write_one_message(int fd, const void *buf, size_t size);

struct read_buf {
	size_t buf_start;
	size_t buf_size;
	// Large enough to hold 1 max-sized message
	char buf[PROTO_HEADER_SIZE + PROTO_MAX_MESSAGE_SIZE];
};

void read_buf_init(struct read_buf *r);

enum read_result {
	READ_OK = 0,
	READ_IO_ERR = -1,
	READ_EOF = -3,
	READ_MORE = -4,
};

/**
 * Fill read buffer with data from `fd`.
 *
 * The full buffer capacity is requested from `fd`, although this may read less
 * than the full capacity.
 */
enum read_result read_buf_fill(int fd, struct read_buf *r);

enum parse_result {
	PARSE_ERR = -1,
	PARSE_MORE = -2,
};

/**
 * Try to parse 1 message from the buffer. Can return a `parse_result` in case of
 * error or more data is required. Returns the size of the message otherwise.
 */
ssize_t read_buf_parse(struct read_buf *r, uint8_t buf[PROTO_MAX_MESSAGE_SIZE]);

struct write_buf {
	size_t buf_size;
	size_t buf_sent;
	char buf[PROTO_HEADER_SIZE + PROTO_MAX_MESSAGE_SIZE];
};

void write_buf_init(struct write_buf *w);

void write_buf_set_message(struct write_buf *w, const void *buf, size_t size);

enum write_result {
	WRITE_OK = 0,
	WRITE_IO_ERR = -1,
	WRITE_MORE = -2,
};

enum write_result write_buf_flush(int fd, struct write_buf *w);

#endif
