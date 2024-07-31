#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>

#include "protocol.h"
#include "buffer.h"
#include "types.h"

#define INT_VAL_SIZE (sizeof(int_val_t))

static ssize_t parse_size(proto_size_t *ret, struct const_slice buffer) {
	if (buffer.size < PROTO_SIZE_SIZE) {
		return -1;
	}

	memcpy(ret, buffer.data, PROTO_SIZE_SIZE);
	return PROTO_SIZE_SIZE;
}

ssize_t parse_int_value(int_val_t *n, struct const_slice buffer) {
	if (buffer.size < INT_VAL_SIZE) {
		return -1;
	}
	memcpy(n, buffer.data, INT_VAL_SIZE);
	return INT_VAL_SIZE;
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

ssize_t parse_request(struct request *req, struct const_slice buffer) {
	const void *init_buf_start = buffer.data;

	if (buffer.size < PROTO_HEADER_SIZE) {
		return PARSE_MORE;
	}

	proto_size_t message_len;
	memcpy(&message_len, buffer.data, PROTO_HEADER_SIZE);
	const_slice_advance(&buffer, PROTO_HEADER_SIZE);

	if (message_len > buffer.size) {
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

static void write_size(struct buffer *b, proto_size_t size) {
	buffer_ensure_cap(b, PROTO_SIZE_SIZE);
	memcpy(buffer_tail(b), &size, PROTO_SIZE_SIZE);
	b->size += PROTO_SIZE_SIZE;
}

void write_message_size_at(void *buf, proto_size_t size) {
	memcpy(buf, &size, PROTO_SIZE_SIZE);
}

void write_obj_type(struct buffer *b, enum proto_type t) {
	buffer_append_byte(b, t);
}

void write_nil_value(struct buffer *b) {
	write_obj_type(b, SER_NIL);
}

void write_int_value(struct buffer *b, int_val_t n) {
	write_obj_type(b, SER_INT);
	buffer_append(b, &n, INT_VAL_SIZE);
}

void write_str_value(struct buffer *b, struct const_slice str) {
	write_obj_type(b, SER_STR);
	write_size(b, str.size);
	buffer_append_slice(b, str);
}

void write_array_header(struct buffer *b, uint32_t arr_size) {
	write_obj_type(b, SER_ARR);
	write_size(b, arr_size);
}

void write_response_header(struct buffer *b, enum res_type res_type) {
	buffer_append_byte(b, res_type);
}

void write_nil_response(struct buffer *b) {
	write_response_header(b, RES_OK);
	write_nil_value(b);
}

void write_int_response(struct buffer *b, int_val_t n) {
	write_response_header(b, RES_OK);
	write_int_value(b, n);
}

void write_str_response(struct buffer *b, struct const_slice str) {
	write_response_header(b, RES_OK);
	write_str_value(b, str);
}

void write_arr_response_header(struct buffer *b, uint32_t size) {
	write_response_header(b, RES_OK);
	write_array_header(b, size);
}

void write_err_response(struct buffer *b, const char *msg) {
	write_response_header(b, RES_OK);
	write_str_value(b, make_str_slice(msg));
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
