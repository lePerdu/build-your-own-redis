#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>

#include "protocol.h"
#include "buffer.h"
#include "types.h"

#define INT_VAL_SIZE (sizeof(int_val_t))

ssize_t parse_int_value(int_val_t *n, struct const_slice buffer) {
	if (buffer.size < INT_VAL_SIZE) {
		return PARSE_MORE;
	}
	memcpy(n, buffer.data, INT_VAL_SIZE);
	return INT_VAL_SIZE;
}

ssize_t parse_str_value(struct const_slice *str, struct const_slice buffer) {
	if (buffer.size < PROTO_SIZE_SIZE) {
		return PARSE_MORE;
	}

	proto_size_t len;
	memcpy(&len, buffer.data, PROTO_SIZE_SIZE);
	const_slice_advance(&buffer, PROTO_SIZE_SIZE);

	if (buffer.size < len) {
		return PARSE_MORE;
	}

	*str = make_const_slice(buffer.data, len);
	return PROTO_SIZE_SIZE + str->size;
}

int request_arg_count(enum req_type t) {
	switch (t) {
		case REQ_GET: return 1;
		case REQ_SET: return 2;
		case REQ_DEL: return 1;
		case REQ_KEYS: return 0;
		default: return -1;
	}
}

void req_object_destroy(struct req_object *o) {
	switch (o->type) {
		case SER_STR:
			free(o->str_val.data);
			break;
		default:
			break;
	}
}

ssize_t parse_req_type(enum req_type *t, struct const_slice buffer) {
	if (buffer.size < 1) {
		return PARSE_MORE;
	}

	*t = const_slice_get(buffer, 0);
	const_slice_advance(&buffer, 1);

	// Use this to check if the type is valid
	int arg_count = request_arg_count(*t);
	if (arg_count < 0) {
		return PARSE_ERR;
	}
	return 1;
}

ssize_t parse_req_object(struct req_object *o, struct const_slice buffer) {
	if (buffer.size < 1) {
		return PARSE_MORE;
	}

	o->type = const_slice_get(buffer, 0);
	const_slice_advance(&buffer, 1);

	ssize_t val_size;
	switch (o->type) {
		case SER_INT:
			val_size = parse_int_value(&o->int_val, buffer);
			break;
		case SER_STR: {
			struct const_slice s;
			val_size = parse_str_value(&s, buffer);
			if (val_size > 0) {
				o->str_val = slice_dup(s);
			}
			break;
		}
		default:
			return PARSE_ERR;
	}
	if (val_size < 0) {
		return val_size;
	}

	return 1 + val_size;
}

static void write_size(struct buffer *b, proto_size_t size) {
	buffer_ensure_cap(b, PROTO_SIZE_SIZE);
	memcpy(buffer_tail(b), &size, PROTO_SIZE_SIZE);
	b->size += PROTO_SIZE_SIZE;
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
	write_response_header(b, RES_ERR);
	write_str_value(b, make_str_slice(msg));
}

static int print_object(FILE *stream, const struct req_object *o);

static int print_object(FILE *stream, const struct req_object *o) {
	switch (o->type) {
		case SER_NIL:
			return fprintf(stream, "<NIL>");
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
