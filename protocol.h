#ifndef PROTOCOL_H_
#define PROTOCOL_H_

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>

#include "types.h"
#include "buffer.h"

typedef uint32_t proto_size_t;

#define PROTO_SIZE_SIZE (sizeof(proto_size_t))
#define PROTO_HEADER_SIZE PROTO_SIZE_SIZE

enum proto_type {
	SER_NIL = 0,
	SER_INT = 1,
	SER_STR = 2,
	SER_ARR = 3,
};

// Helpers for deserializing

ssize_t parse_obj_type(enum proto_type *t, struct const_slice buffer);
ssize_t parse_int_value(int_val_t *n, struct const_slice buffer);
/**
 * Parses a string ref from the buffer (does not copy/allocate).
 */
ssize_t parse_str_value(struct const_slice *str, struct const_slice buffer);
ssize_t parse_arr_value(struct slice *str);

// Helpers for serializing

void write_obj_type(struct buffer *b, enum proto_type t);
void write_nil_value(struct buffer *b);
void write_int_value(struct buffer *b, int_val_t n);
void write_str_value(struct buffer *b, struct const_slice str);
void write_array_header(struct buffer *b, uint32_t n);

enum req_type {
	// Invalid request type, used for marking invalid/unknown data
	REQ_UNKNOWN = -1,
	REQ_GET = 0,
	REQ_SET = 1,
	REQ_DEL = 2,
	REQ_KEYS = 3,

	REQ_HGET = 16,
	REQ_HSET = 17,
	REQ_HDEL = 18,
	REQ_HKEYS = 19,
	REQ_HGETALL = 20,
	REQ_HLEN = 21,

	REQ_MAX_ID,
};

static_assert(REQ_MAX_ID <= UINT8_MAX, "Too many requests to fit in 1 byte");

/**
 * Subset of `struct object` specialized for requests which only stores
 * non-allocated values.
 */
struct req_object {
	/** Only OBJ_STR or OBJ_INT */
	enum proto_type type;
	union {
		int_val_t int_val;
		struct const_slice str_val;
	};
};

#define REQ_ARGS_MAX 2

struct request {
	enum req_type type;
	struct req_object args[REQ_ARGS_MAX];
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

enum write_result {
	WRITE_OK = 0,
	WRITE_ERR = -1,
};

ssize_t parse_request(struct request *req, struct const_slice buffer);
void print_request(FILE *stream, const struct request *req);

void write_response_header(struct buffer *b, enum res_type res_type);

// Helpers for common response types
void write_nil_response(struct buffer *b);
void write_int_response(struct buffer *b, int_val_t n);
void write_str_response(struct buffer *b, struct const_slice str);
void write_arr_response_header(struct buffer *b, uint32_t size);
void write_err_response(struct buffer *b, const char *msg);

#endif
