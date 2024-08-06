#ifndef PROTOCOL_H_
#define PROTOCOL_H_

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>

#include "buffer.h"
#include "types.h"

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

ssize_t parse_int_value(int_val_t *n, struct const_slice buffer);
ssize_t parse_str_value(struct const_slice *str, struct const_slice buffer);

// Helpers for serializing

void write_obj_type(struct buffer *out, enum proto_type type);
void write_nil_value(struct buffer *out);
void write_int_value(struct buffer *out, int_val_t n);
void write_str_value(struct buffer *out, struct const_slice str);
void write_array_header(struct buffer *out, uint32_t arr_size);

enum req_type {
  REQ_GET = 0,
  REQ_SET = 1,
  REQ_DEL = 2,
  REQ_KEYS = 3,

  REQ_HGET = 16,
  REQ_HSET = 17,
  REQ_HDEL = 18,
  REQ_HLEN = 19,
  REQ_HKEYS = 20,
  REQ_HGETALL = 21,

  REQ_MAX_ID,
};

static_assert(REQ_MAX_ID <= UINT8_MAX, "Too many requests to fit in 1 byte");

/**
 * Subset of `struct object` specialized for requests which only stores
 * NIL, STR and INT.
 */
struct req_object {
  enum proto_type type;
  union {
    int_val_t int_val;
    /** Owned string value */
    struct slice str_val;
    // TODO: Support string refs when data is still around
  };
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

void req_object_destroy(struct req_object *obj);
int print_req_object(FILE *stream, const struct req_object *obj);

ssize_t parse_req_type(enum req_type *type, struct const_slice buffer);
ssize_t parse_req_object(struct req_object *obj, struct const_slice buffer);

void write_response_header(struct buffer *out, enum res_type res_type);

// Helpers for common response types
void write_nil_response(struct buffer *out);
void write_int_response(struct buffer *out, int_val_t n);
void write_str_response(struct buffer *out, struct const_slice str);
void write_arr_response_header(struct buffer *out, uint32_t size);
void write_err_response(struct buffer *out, const char *msg);

#endif
