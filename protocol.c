#include <bits/types/struct_iovec.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>

#include "protocol.h"

static inline void slice_advance(struct slice *s, size_t n) {
	assert(n <= s->size);
	s->size -= n;
	s->data += n;
}

#define STR_LEN_SIZE 4

static ssize_t write_string(struct slice buffer, struct slice s) {
	if (buffer.size < STR_LEN_SIZE + s.size) {
		return -1;
	}

	message_len_t str_len = s.size;
	memcpy(buffer.data, &str_len, STR_LEN_SIZE);
	slice_advance(&buffer, STR_LEN_SIZE);
	memcpy(buffer.data, s.data, s.size);
	slice_advance(&buffer, s.size);

	return STR_LEN_SIZE + s.size;
}

ssize_t write_request(
	struct slice buffer,
	const struct request *req
) {
	void *init_buf_start = buffer.data;
	// Write the total size at the end once it's known
	slice_advance(&buffer, PROTO_HEADER_SIZE);

	((uint8_t *)buffer.data)[0] = req->type;
	slice_advance(&buffer, 1);

	ssize_t str_res;
	switch (req->type) {
		case REQ_GET:
		case REQ_DEL:
			str_res = write_string(buffer, req->key);
			if (str_res < 0) {
				return WRITE_ERR;
			}
			slice_advance(&buffer, str_res);
			break;
		case REQ_SET:
			str_res = write_string(buffer, req->key);
			if (str_res < 0) {
				return WRITE_ERR;
			}
			slice_advance(&buffer, str_res);

			str_res = write_string(buffer, req->val);
			if (str_res < 0) {
				return WRITE_ERR;
			}
			slice_advance(&buffer, str_res);
			break;
		default:
			return WRITE_ERR;
	}

	size_t total_size = buffer.data - init_buf_start;
	message_len_t res_len = total_size - PROTO_HEADER_SIZE;
	memcpy(init_buf_start, &res_len, STR_LEN_SIZE);
	return total_size;
}

ssize_t write_response(
	struct slice buffer,
	const struct response *res
) {
	size_t total_size = PROTO_HEADER_SIZE + 1 + STR_LEN_SIZE + res->data.size;
	if (total_size > buffer.size) {
		return WRITE_ERR;
	}

	message_len_t message_len = total_size - PROTO_HEADER_SIZE;
	memcpy(buffer.data, &message_len, PROTO_HEADER_SIZE);
	slice_advance(&buffer, PROTO_HEADER_SIZE);

	((uint8_t *) buffer.data)[0] = res->type;
	slice_advance(&buffer, 1);

	int str_res = write_string(buffer, res->data);
	if (str_res < 0) {
		return -1;
	}

	return total_size;
}

static ssize_t parse_string(struct slice *s, struct slice buffer) {
	if (buffer.size < STR_LEN_SIZE) {
		return -1;
	}

	message_len_t str_len;
	memcpy(&str_len, buffer.data, STR_LEN_SIZE);
	slice_advance(&buffer, STR_LEN_SIZE);

	if (buffer.size < str_len) {
		return -1;
	}

	s->size = str_len;
	s->data = buffer.data;
	return STR_LEN_SIZE + str_len;
}

ssize_t parse_request(struct request *req, struct slice buffer) {
	void *init_buf_start = buffer.data;

	if (buffer.size < PROTO_HEADER_SIZE) {
		return PARSE_MORE;
	}

	message_len_t message_len;
	memcpy(&message_len, buffer.data, PROTO_HEADER_SIZE);
	slice_advance(&buffer, PROTO_HEADER_SIZE);

	if (message_len > PROTO_MAX_PAYLOAD_SIZE) {
		return PARSE_ERR;
	} else if (message_len > buffer.size) {
		return PARSE_MORE;
	}

	// Limit slice to just the packet to simplify later checks
	buffer.size = message_len;

	req->type = ((const uint8_t *) buffer.data)[0];
	slice_advance(&buffer, 1);
	ssize_t sub_res;
	switch (req->type) {
		case REQ_GET:
		case REQ_DEL:
			sub_res = parse_string(&req->key, buffer);
			if (sub_res < 0) {
				return PARSE_ERR;
			}
			slice_advance(&buffer, sub_res);
			break;
		case REQ_SET:
			sub_res = parse_string(&req->key, buffer);
			if (sub_res < 0) {
				return PARSE_ERR;
			}
			slice_advance(&buffer, sub_res);

			sub_res = parse_string(&req->val, buffer);
			if (sub_res < 0) {
				return PARSE_ERR;
			}
			slice_advance(&buffer, sub_res);
			break;
		default:
			return PARSE_ERR;
	}

	if (buffer.size > 0) {
		// Shouldn't be extra data
		return PARSE_ERR;
	}

	return buffer.data - init_buf_start;
}

ssize_t parse_response(struct response *res, struct slice buffer) {
	void *init_buf_start = buffer.data;

	if (buffer.size < PROTO_HEADER_SIZE) {
		return PARSE_MORE;
	}

	message_len_t message_len;
	memcpy(&message_len, buffer.data, PROTO_HEADER_SIZE);
	slice_advance(&buffer, PROTO_HEADER_SIZE);

	if (message_len > PROTO_MAX_PAYLOAD_SIZE) {
		return PARSE_ERR;
	} else if (message_len > buffer.size) {
		return PARSE_MORE;
	}

	// Limit slice to just the packet to simplify later checks
	buffer.size = message_len;

	res->type = ((const uint8_t *) buffer.data)[0];
	switch (res->type) {
		case RES_OK:
		case RES_ERR:
			break;
		default:
			return PARSE_ERR;
	}
	slice_advance(&buffer, 1);

	ssize_t sub_res = parse_string(&res->data, buffer);
	if (sub_res < 0) {
		return PARSE_ERR;
	}
	slice_advance(&buffer, sub_res);

	if (buffer.size > 0) {
		// Shouldn't be extra data
		return PARSE_ERR;
	}

	return buffer.data - init_buf_start;
}

void print_request(FILE *stream, const struct request *req) {
	switch (req->type) {
        case REQ_GET:
			fprintf(stream, "GET %.*s", (int)req->key.size, (char *)req->key.data);
			break;
        case REQ_SET:
			fprintf(
				stream,
				"SET %.*s %.*s",
				(int)req->key.size,
				(char *)req->key.data,
				(int)req->val.size,
				(char *)req->val.data
			);
			break;
        case REQ_DEL:
			fprintf(stream, "DEL %.*s", (int)req->key.size, (char *)req->key.data);
			break;
		default:
			assert(false);
	}
}

void print_response(FILE *stream, const struct response *res) {
	switch (res->type) {
        case RES_OK:
			fprintf(stream, "OK %.*s", (int)res->data.size, (char *)res->data.data);
			break;
        case RES_ERR:
			fprintf(
				stream,
				"ERR %.*s",
				(int)res->data.size,
				(char *)res->data.data
			);
			break;
		default:
			assert(false);
	}
}

void read_buf_init(struct read_buf *r) {
	r->buf_start = 0;
	r->buf_size = 0;
}

void read_buf_reset_start(struct read_buf *r) {
	if (r->buf_size > 0 && r->buf_start > 0) {
		memmove(r->buf, read_buf_start_pos(r), r->buf_size);
	}
	// Always reset this either way
	r->buf_start = 0;
}

void write_buf_init(struct write_buf *w) {
	w->buf_size = 0;
	w->buf_sent = 0;
}
