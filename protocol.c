#include <assert.h>
#include <bits/types/struct_iovec.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "protocol.h"

typedef uint32_t message_len_t;
static_assert(
	sizeof(message_len_t) == PROTO_HEADER_SIZE, "incorrect message length type"
);

static ssize_t read_exact(int fd, void *buf, size_t size) {
	size_t remaining = size;
	while (remaining > 0) {
		ssize_t n_read = read(fd, buf, remaining);
		if (n_read <= 0) {
			// TODO Report specific error, like with errno?
			return -1;
		}

		remaining -= n_read;
		buf += n_read;
	}
	return size;
}

ssize_t read_one_message(int fd, void *buf, size_t size) {
	message_len_t message_len;

	if (read_exact(fd, &message_len, PROTO_HEADER_SIZE) == -1) {
		return -1;
	}

	if (message_len > PROTO_MAX_MESSAGE_SIZE) {
		return -1;
	}

	if (message_len > size) {
		// TODO Read rest of the message to "reset" the protocol?
		return -1;
	}

	return read_exact(fd, buf, message_len);
}

static ssize_t write_exact(int fd, const void *buf, size_t size) {
	size_t remaining = size;
	while (remaining > 0) {
		ssize_t n_write = write(fd, buf, remaining);
		if (n_write <= 0) {
			// TODO Report specific error, like with errno?
			return -1;
		}

		remaining -= n_write;
		buf += n_write;
	}
	return size;
}

ssize_t write_one_message(int fd, const void *buf, size_t size) {
	if (size > PROTO_MAX_MESSAGE_SIZE) {
		return -1;
	}

	message_len_t message_len = size;
	if (write_exact(fd, &message_len, PROTO_HEADER_SIZE) == -1) {
		return -1;
	}

	return write_exact(fd, buf, size);
}

void read_buf_init(struct read_buf *r) {
	r->buf_start = 0;
	r->buf_size = 0;
}

enum read_result read_buf_fill(int fd, struct read_buf *r) {
	// Make room for full message.
	// TODO: Better heuristic for when to move data in buffer?
	if (r->buf_size > 0 && r->buf_start > 0) {
		memmove(r->buf, &r->buf[r->buf_start], r->buf_size);
	}
	// Always reset this either way
	r->buf_start = 0;

	size_t cap = sizeof(r->buf) - r->buf_size;
	// TODO: Are there valid scenarios where this is desired?
	assert(cap > 0);

	ssize_t n_read;
	// Repeat for EINTR
	do {
		n_read = read(fd, &r->buf[r->buf_size], cap);
	} while (n_read == -1 && errno == EINTR);

	if (n_read == -1) {
		if (errno == EAGAIN) {
			return READ_MORE;
		} else {
			return READ_IO_ERR;
		}
	} else if (n_read == 0) {
		return READ_EOF;
	} else {
		assert(n_read > 0);
		r->buf_size += n_read;
		return READ_OK;
	}
}

ssize_t read_buf_parse(struct read_buf *r, uint8_t buf[PROTO_MAX_MESSAGE_SIZE]) {
	if (r->buf_size < PROTO_HEADER_SIZE) {
		return PARSE_MORE;
	}

	message_len_t message_len;
	memcpy(&message_len, &r->buf[r->buf_start], PROTO_HEADER_SIZE);

	if (message_len > PROTO_MAX_MESSAGE_SIZE) {
		return PARSE_ERR;
	} else if (message_len + PROTO_HEADER_SIZE > r->buf_size) {
		return PARSE_MORE;
	}

	memcpy(buf, &r->buf[r->buf_start + PROTO_HEADER_SIZE], message_len);
	size_t total_message_size = PROTO_HEADER_SIZE + message_len;
	r->buf_start += total_message_size;
	r->buf_size -= total_message_size;

	return message_len;
}

void write_buf_init(struct write_buf *w) {
	w->buf_size = 0;
	w->buf_sent = 0;
}

void write_buf_set_message(struct write_buf *w, const void *buf, size_t size) {
	assert(size <= PROTO_MAX_MESSAGE_SIZE);
	assert(w->buf_size == 0);
	message_len_t message_len = size;
	memcpy(w->buf, &message_len, PROTO_HEADER_SIZE);
	memcpy(&w->buf[PROTO_HEADER_SIZE], buf, size);
	w->buf_size = size + 4;
}

enum write_result write_buf_flush(int fd, struct write_buf *w) {
	assert(w->buf_size > 0);
	assert(w->buf_sent < w->buf_size);

	ssize_t n_write;
	do {
		n_write = write(fd, &w->buf[w->buf_sent], w->buf_size - w->buf_sent);
	} while (n_write == -1 && errno == EINTR);

	if (n_write == -1) {
		if (errno == EAGAIN) {
			return WRITE_MORE;
		} else {
			return WRITE_IO_ERR;
		}
	}

	assert(n_write > 0);
	w->buf_sent += n_write;
	assert(w->buf_sent <= w->buf_size);
	if (w->buf_sent == w->buf_size) {
		w->buf_size = 0;
		w->buf_sent = 0;
		return WRITE_OK;
	} else {
		return WRITE_MORE;
	}
}

int write_buf_output_full(int fd, struct write_buf *w) {
	ssize_t res = write_exact(fd, w->buf, w->buf_size);
	if (res == -1) {
		return WRITE_IO_ERR;
	}

	w->buf_size = 0;
	return 0;
}

