#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "protocol.h"

#define INT_VAL_SIZE (sizeof(int_val_t))

static inline void slice_advance(struct slice *s, size_t n) {
	assert(n <= s->size);
	s->size -= n;
	s->data += n;
}

static inline void const_slice_advance(struct const_slice *s, size_t n) {
	assert(n <= s->size);
	s->size -= n;
	s->data += n;
}

static inline struct slice slice_dup(struct const_slice s) {
	void *new_data = malloc(s.size);
	memcpy(new_data, s.data, s.size);
	return make_slice(new_data, s.size);
}

struct object object_to_ref(struct object o) {
	if (o.type == OBJ_STR && o.owned) {
		return make_slice_object(o.str_ref);
	} else {
		return o;
	}
}

struct object object_to_owned(struct object o) {
	if (o.type == OBJ_STR && !o.owned) {
		return make_owned_slice_object(slice_dup(o.str_ref));
	} else {
		return o;
	}
}

bool object_to_slice(struct const_slice *s, struct object o) {
	if (o.type != OBJ_STR) {
		return false;
	}

	*s = o.str_ref;
	return true;
}

struct object make_arr_object(size_t size) {
	struct object *data = malloc(sizeof(data[0]) * size);
	return (struct object) {
		.type = OBJ_ARR,
		.owned = true,
		.arr_val = {
			.size = size,
			.data = data,
		},
	};
}

void object_destroy(struct object o) {
	if (!o.owned) {
		return;
	}
	switch (o.type) {
		case OBJ_NIL:
		case OBJ_INT:
			break;
		case OBJ_STR:
			free(o.str_val.data);
			break;
		case OBJ_ARR:
			for (size_t i = 0; i < o.arr_val.size; i++) {
				object_destroy(o.arr_val.data[i]);
			}
			free(o.arr_val.data);
			break;
		default:
			assert(false);
	}
}

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
	if (buffer.size < INT_VAL_SIZE) {
		return -1;
	}
	memcpy(buffer.data, &n, INT_VAL_SIZE);
	return INT_VAL_SIZE;
}

ssize_t parse_str_ref_value(struct const_slice *str, struct const_slice buffer) {
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

ssize_t parse_str_value(struct slice *str, struct const_slice buffer) {
	struct const_slice ref;
	ssize_t n = parse_str_ref_value(&ref, buffer);
	if (n < 0) {
		return -1;
	}

	*str = slice_dup(ref);
	return n;
}

ssize_t write_str_value(struct slice buffer, struct const_slice str) {
	size_t total_size = PROTO_SIZE_SIZE + str.size;
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

ssize_t write_array_size(struct slice buffer, size_t arr_size) {
	return write_size(buffer, arr_size);
}

ssize_t parse_array_value(struct array *arr, struct const_slice buffer) {
	const void *init_buf_start = buffer.data;

	ssize_t n = parse_array_size(&arr->size, buffer);
	if (n < 0) {
		return -1;
	}
	const_slice_advance(&buffer, n);

	arr->data = malloc(sizeof(arr->data[0]) * arr->size);
	assert(arr->data != NULL);

	for (size_t i = 0; i < arr->size; i++) {
		n = parse_object(&arr->data[i], buffer);
		if (n < 0) {
			return -1;
		}
		const_slice_advance(&buffer, n);
	}

	return buffer.data - init_buf_start;
}

ssize_t write_array_value(struct slice buffer, struct array arr) {
	const void *init_buf_start = buffer.data;

	ssize_t n = write_array_size(buffer, arr.size);
	if (n < 0) {
		return -1;
	}
	slice_advance(&buffer, n);

	for (size_t i = 0; i < arr.size; i++) {
		n = write_object(buffer, arr.data[i]);
		if (n < 0) {
			return -1;
		}
		slice_advance(&buffer, n);
	}

	return buffer.data - init_buf_start;
}

ssize_t parse_object(struct object *o, struct const_slice buffer) {
	if (buffer.size < 1) {
		return -1;
	}

	o->type = const_slice_get(buffer, 0);
	const_slice_advance(&buffer, 1);

	ssize_t val_size;
	switch (o->type) {
		case OBJ_NIL:
			o->owned = false;
			return 1;
		case OBJ_INT:
			o->owned = false;
			val_size = parse_int_value(&o->int_val, buffer);
			break;
		case OBJ_STR:
			o->owned = false;
			val_size = parse_str_ref_value(&o->str_ref, buffer);
			break;
		case OBJ_ARR:
			o->owned = true;
			val_size = parse_array_value(&o->arr_val, buffer);
			break;
		default:
			return -1;
	}
	if (val_size < 0) {
		return -1;
	}

	return 1 + val_size;
}

ssize_t write_object(struct slice buffer, struct object o) {
	if (buffer.size < 1) {
		return -1;
	}
	slice_set(buffer, 0, o.type);
	slice_advance(&buffer, 1);

	ssize_t val_size;
	switch (o.type) {
		case OBJ_NIL:
			val_size = 0;
			break;
		case OBJ_INT:
			val_size = write_int_value(buffer, o.int_val);
			break;
		case OBJ_STR:
			val_size = write_str_value(buffer, o.str_ref);
			break;
		case OBJ_ARR:
			val_size = write_array_value(buffer, o.arr_val);
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

ssize_t write_request(
	struct slice buffer,
	const struct request *req
) {
	void *init_buf_start = buffer.data;
	// Write the total size at the end once it's known
	slice_advance(&buffer, PROTO_HEADER_SIZE);

	((uint8_t *)buffer.data)[0] = req->type;
	slice_advance(&buffer, 1);

	int arg_count = request_arg_count(req->type);
	if (arg_count < 0) {
		return WRITE_ERR;
	}

	for (int i = 0; i < arg_count; i++) {
		ssize_t n = write_object(buffer, req->args[i]);
		if (n < 0) {
			return WRITE_ERR;
		}
		slice_advance(&buffer, n);
	}

	size_t total_size = buffer.data - init_buf_start;
	proto_size_t res_len = total_size - PROTO_HEADER_SIZE;
	memcpy(init_buf_start, &res_len, PROTO_SIZE_SIZE);
	return total_size;
}

ssize_t write_response(
	struct slice buffer,
	const struct response *res
) {
	void *init_buf_start = buffer.data;
	// Write the total size at the end once it's known
	slice_advance(&buffer, PROTO_HEADER_SIZE);

	((uint8_t *)buffer.data)[0] = res->type;
	slice_advance(&buffer, 1);

	ssize_t n = write_object(buffer, res->arg);
	if (n < 0) {
		return WRITE_ERR;
	}
	slice_advance(&buffer, n);

	size_t total_size = buffer.data - init_buf_start;
	proto_size_t res_len = total_size - PROTO_HEADER_SIZE;
	memcpy(init_buf_start, &res_len, PROTO_SIZE_SIZE);
	return total_size;
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
		ssize_t n = parse_object(&req->args[i], buffer);
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

ssize_t parse_response(struct response *res, struct const_slice buffer) {
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

	res->type = const_slice_get(buffer, 0);
	switch (res->type) {
		case RES_OK:
		case RES_ERR:
			break;
		default:
			return PARSE_ERR;
	}
	const_slice_advance(&buffer, 1);

	ssize_t sub_res = parse_object(&res->arg, buffer);
	if (sub_res < 0) {
		return PARSE_ERR;
	}
	const_slice_advance(&buffer, sub_res);

	if (buffer.size > 0) {
		// Shouldn't be extra data
		return PARSE_ERR;
	}

	return buffer.data - init_buf_start;
}

static int print_object(FILE *stream, const struct object *o);

static int print_array(FILE *stream, const struct array *arr) {
	int res;
	int total = 0;

#define SUM_OR_RETURN(expr) \
	res = expr; \
	if (res < 0) { \
		return res; \
	} \
	total += res;

	SUM_OR_RETURN(fputc('[', stream));

	if (arr->size > 0) {
		SUM_OR_RETURN(print_object(stream, &arr->data[0]));

		for (uint32_t i = 1; i < arr->size; i++) {
			SUM_OR_RETURN(print_object(stream, &arr->data[i]));
		}
	}

	SUM_OR_RETURN(fputc(']', stream));
#undef SUM_OR_RETURN
	return total;
}

static int print_object(FILE *stream, const struct object *o) {
	switch (o->type) {
        case OBJ_NIL:
			return fprintf(stream, "<NIL>");
        case OBJ_INT:
			return fprintf(stream, "%ld", o->int_val);
        case OBJ_STR:
			return fprintf(
				stream, "%.*s",
				(int)o->str_ref.size,
				(const char *)o->str_ref.data
			);
        case OBJ_ARR:
			return print_array(stream, &o->arr_val);
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

static const char *response_name(enum res_type t) {
	switch (t) {
		case RES_OK: return "OK";
		case RES_ERR: return "ERR";
		default: return "UNKNOWN";
	}
}

void print_response(FILE *stream, const struct response *res) {
	const char *name = response_name(res->type);
	fprintf(stream, "%s ", name);
	print_object(stream, &res->arg);
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
