#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>

#include "protocol.h"
#include "types.h"

#define INT_VAL_SIZE (sizeof(int_val_t))

static ssize_t parse_size(proto_size_t *ret, struct const_slice buffer) {
	if (buffer.size < PROTO_SIZE_SIZE) {
		return -1;
	}

	memcpy(ret, buffer.data, PROTO_SIZE_SIZE);
	return PROTO_SIZE_SIZE;
}

static ssize_t write_size(struct slice buffer, proto_size_t size) {
	if (buffer.size < PROTO_SIZE_SIZE) {
		return -1;
	}

	memcpy(buffer.data, &size, PROTO_SIZE_SIZE);
	return PROTO_SIZE_SIZE;
}

ssize_t parse_int_value(int_val_t *n, struct const_slice buffer) {
	if (buffer.size < INT_VAL_SIZE) {
		return -1;
	}
	memcpy(n, buffer.data, INT_VAL_SIZE);
	return INT_VAL_SIZE;
}

ssize_t write_int_value(struct slice buffer, int_val_t n) {
	ssize_t res = write_obj_type(buffer, SER_INT);
	if (res < 0) {
		return WRITE_ERR;
	}
	slice_advance(&buffer, res);

	if (buffer.size < INT_VAL_SIZE) {
		return -1;
	}
	memcpy(buffer.data, &n, INT_VAL_SIZE);
	return res + INT_VAL_SIZE;
}

ssize_t parse_str_value(struct const_slice *str, struct const_slice buffer) {
	proto_size_t len;
	ssize_t n = parse_size(&len, buffer);
	if (n < 0) {
		return -1;
	}
	const_slice_advance(&buffer, n);

	if (len > buffer.size) {
		return -1;
	}

	*str = make_const_slice(buffer.data, len);
	return n + str->size;
}

ssize_t write_str_value(struct slice buffer, struct const_slice str) {
	ssize_t res = write_obj_type(buffer, SER_STR);
	if (res < 0) {
		return WRITE_ERR;
	}
	slice_advance(&buffer, res);

	size_t total_size = res + PROTO_SIZE_SIZE + str.size;
	if (buffer.size < total_size) {
		return -1;
	}

	proto_size_t str_len = str.size;
	ssize_t n = write_size(buffer, str_len);
	// Already checked buffer size above
	assert(n > 0);
	slice_advance(&buffer, n);

	memcpy(buffer.data, str.data, str.size);

	return total_size;
}

// Helpers when manually serializing an array
ssize_t parse_array_size(size_t *arr_size, struct const_slice buffer) {
	proto_size_t len;
	ssize_t n = parse_size(&len, buffer);
	if (n < 0) {
		return -1;
	}
	const_slice_advance(&buffer, n);
	*arr_size = len;
	return n;
}

ssize_t write_array_header(struct slice buffer, size_t arr_size) {
	const void *init = buffer.data;
	ssize_t res = write_obj_type(buffer, SER_ARR);
	if (res < 0) {
		return -1;
	}
	slice_advance(&buffer, res);

	res = write_size(buffer, arr_size);
	if (res < 0) {
		return -1;
	}
	slice_advance(&buffer, res);
	return buffer.data - init;
}

ssize_t parse_req_object(struct req_object *o, struct const_slice buffer) {
	if (buffer.size < 1) {
		return -1;
	}

	o->type = const_slice_get(buffer, 0);
	const_slice_advance(&buffer, 1);

	ssize_t val_size;
	switch (o->type) {
		case SER_INT:
			val_size = parse_int_value(&o->int_val, buffer);
			break;
		case SER_STR:
			val_size = parse_str_value(&o->str_val, buffer);
			break;
		default:
			return -1;
	}
	if (val_size < 0) {
		return -1;
	}

	return 1 + val_size;
}

static int request_arg_count(enum req_type t) {
	switch (t) {
		case REQ_GET: return 1;
		case REQ_SET: return 2;
		case REQ_DEL: return 1;
		case REQ_KEYS: return 0;
		default: return -1;
	}
}

ssize_t write_obj_type(struct slice buffer, enum proto_type t) {
	if (buffer.size < 1) {
		return WRITE_ERR;
	}
	slice_set(buffer, 0, t);

	return 1;
}

ssize_t write_message_size(struct slice buffer, proto_size_t size) {
	return write_size(buffer, size);
}

ssize_t write_response_header(struct slice buffer, enum res_type res_type) {
	if (buffer.size < 1) {
		return WRITE_ERR;
	}
	slice_set(buffer, 0, res_type);

	return 1;
}

ssize_t write_nil_response(struct slice buffer) {
	const void *init = buffer.data;
	ssize_t res = write_response_header(buffer, RES_OK);
	if (res < 0) {
		return WRITE_ERR;
	}
	slice_advance(&buffer, res);

	res = write_obj_type(buffer, SER_NIL);
	if (res < 0) {
		return WRITE_ERR;
	}
	slice_advance(&buffer, res);

	return buffer.data - init;
}

ssize_t write_int_response(struct slice buffer, int_val_t n) {
	const void *init = buffer.data;
	ssize_t res = write_response_header(buffer, RES_OK);
	if (res < 0) {
		return WRITE_ERR;
	}
	slice_advance(&buffer, res);

	res = write_int_value(buffer, n);
	if (res < 0) {
		return WRITE_ERR;
	}
	slice_advance(&buffer, res);

	return buffer.data - init;
}

ssize_t write_str_response(struct slice buffer, struct const_slice str) {
	const void *init = buffer.data;
	ssize_t res = write_response_header(buffer, RES_OK);
	if (res < 0) {
		return WRITE_ERR;
	}
	slice_advance(&buffer, res);

	res = write_str_value(buffer, str);
	if (res < 0) {
		return WRITE_ERR;
	}
	slice_advance(&buffer, res);

	return buffer.data - init;
}

ssize_t write_arr_response_header(struct slice buffer, size_t size) {
	const void *init = buffer.data;
	ssize_t res = write_response_header(buffer, RES_OK);
	if (res < 0) {
		return WRITE_ERR;
	}
	slice_advance(&buffer, res);

	res = write_array_header(buffer, size);
	if (res < 0) {
		return WRITE_ERR;
	}
	slice_advance(&buffer, res);

	return buffer.data - init;
}

ssize_t write_err_response(struct slice buffer, const char *msg) {
	const void *init = buffer.data;
	ssize_t res = write_response_header(buffer, RES_ERR);
	if (res < 0) {
		return WRITE_ERR;
	}
	slice_advance(&buffer, res);

	res = write_str_value(buffer, make_str_slice(msg));
	if (res < 0) {
		return WRITE_ERR;
	}
	slice_advance(&buffer, res);

	return buffer.data - init;
}

ssize_t parse_request(struct request *req, struct const_slice buffer) {
	const void *init_buf_start = buffer.data;

	if (buffer.size < PROTO_HEADER_SIZE) {
		return PARSE_MORE;
	}

	proto_size_t message_len;
	memcpy(&message_len, buffer.data, PROTO_HEADER_SIZE);
	const_slice_advance(&buffer, PROTO_HEADER_SIZE);

	if (message_len > PROTO_MAX_PAYLOAD_SIZE) {
		return PARSE_ERR;
	} else if (message_len > buffer.size) {
		return PARSE_MORE;
	}

	// Limit slice to just the packet to simplify later checks
	buffer.size = message_len;

	req->type = const_slice_get(buffer, 0);
	const_slice_advance(&buffer, 1);

	int arg_count = request_arg_count(req->type);
	if (arg_count < 0) {
		return WRITE_ERR;
	}

	for (int i = 0; i < arg_count; i++) {
		ssize_t n = parse_req_object(&req->args[i], buffer);
		if (n < 0) {
			return WRITE_ERR;
		}
		const_slice_advance(&buffer, n);
	}

	if (buffer.size > 0) {
		// Shouldn't be extra data
		return PARSE_ERR;
	}

	return buffer.data - init_buf_start;
}

static int print_object(FILE *stream, const struct req_object *o);

static int print_object(FILE *stream, const struct req_object *o) {
	switch (o->type) {
        case SER_INT:
			return fprintf(stream, "%ld", o->int_val);
        case SER_STR:
			return fprintf(
				stream, "%.*s",
				(int)o->str_val.size,
				(const char *)o->str_val.data
			);
		default:
			return fprintf(stream, "<INVALID>");
	}
}

static const char *request_name(enum req_type t) {
	switch (t) {
        case REQ_GET: return "GET";
        case REQ_SET: return "SET";
        case REQ_DEL: return "DEL";
		case REQ_KEYS: return "KEYS";
		default: return "UNKNOWN";
	}
}

void print_request(FILE *stream, const struct request *req) {
	const char *name = request_name(req->type);
	int arg_count = request_arg_count(req->type);

	fprintf(stream, "%s", name);
	if (arg_count > 0) {
		for (int i = 0; i < arg_count; i++) {
			fputc(' ', stream);
			print_object(stream, &req->args[i]);
		}
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
