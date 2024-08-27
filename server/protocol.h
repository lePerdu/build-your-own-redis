#ifndef PROTOCOL_H_
#define PROTOCOL_H_

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>

#include "buffer.h"
#include "types.h"

typedef uint32_t proto_size_t;

#define PROTO_SIZE_SIZE (sizeof(proto_size_t))
#define PROTO_HEADER_SIZE PROTO_SIZE_SIZE

enum resp_type {
  RESP_NULL = '_',
  RESP_BOOLEAN = '#',
  RESP_NUMBER = ':',
  RESP_DOUBLE = ',',
  RESP_SIMPLE_STR = '+',
  RESP_SIMPLE_ERR = '-',
  RESP_BLOB_STR = '$',
  RESP_ARRAY = '*',
};

// Helpers for deserializing

enum parse_result {
  PARSE_OK = 0,
  PARSE_ERR = -1,
  PARSE_MORE = -2,
};

ssize_t parse_array_header(uint32_t *size, struct const_slice buffer);
ssize_t parse_blob_str(struct const_slice *str, struct const_slice buffer);

bool parse_int_arg(int_val_t *val, struct const_slice input);
bool parse_float_arg(double *val, struct const_slice input);

// Helpers for serializing

void write_null_value(struct buffer *out);
void write_bool_value(struct buffer *out, bool val);
void write_int_value(struct buffer *out, int_val_t n);
void write_float_value(struct buffer *out, double val);
void write_simple_str_value(struct buffer *out, const char *str);
void write_simple_err_value(struct buffer *out, const char *str);
void write_str_value(struct buffer *out, struct const_slice str);
void write_array_header(struct buffer *out, uint32_t arr_size);

enum req_type {
  REQ_GET = 0,
  REQ_SET = 1,
  REQ_DEL = 2,
  REQ_KEYS = 3,
  REQ_TTL = 4,
  REQ_EXPIRE = 5,
  REQ_PERSIST = 6,

  REQ_HGET = 16,
  REQ_HSET = 17,
  REQ_HDEL = 18,
  REQ_HLEN = 19,
  REQ_HKEYS = 20,
  REQ_HGETALL = 21,

  REQ_SADD = 32,
  REQ_SISMEMBER = 33,
  REQ_SREM = 34,
  REQ_SCARD = 35,
  REQ_SRANDMEMBER = 36,
  REQ_SPOP = 37,
  REQ_SMEMBERS = 38,

  REQ_ZSCORE = 48,
  REQ_ZADD = 49,
  REQ_ZREM = 50,
  REQ_ZCARD = 51,
  REQ_ZRANK = 52,
  REQ_ZQUERY = 53,

  REQ_SHUTDOWN = 255,

  REQ_MAX_ID,
};

static_assert(
    REQ_MAX_ID - 1 <= UINT8_MAX, "Too many requests to fit in 1 byte");

#endif
