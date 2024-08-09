#include "protocol.h"

#include <assert.h>
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "buffer.h"
#include "types.h"

#define INT_VAL_SIZE (sizeof(int_val_t))
#define FLOAT_VAL_SIZE (sizeof(double))

ssize_t parse_int_value(int_val_t *n, struct const_slice buffer) {
  if (buffer.size < INT_VAL_SIZE) {
    return PARSE_MORE;
  }
  memcpy(n, buffer.data, INT_VAL_SIZE);
  return INT_VAL_SIZE;
}

ssize_t parse_float_value(double *val, struct const_slice buffer) {
  if (buffer.size < FLOAT_VAL_SIZE) {
    return PARSE_MORE;
  }
  memcpy(val, buffer.data, FLOAT_VAL_SIZE);
  // TODO Allow parsing nan?
  if (isnan(*val)) {
    return PARSE_ERR;
  }
  return FLOAT_VAL_SIZE;
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

void req_object_destroy(struct req_object *obj) {
  switch (obj->type) {
    case SER_STR:
      free(obj->str_val.data);
      break;
    default:
      break;
  }
}

ssize_t parse_req_type(enum req_type *type, struct const_slice buffer) {
  if (buffer.size < 1) {
    return PARSE_MORE;
  }

  *type = const_slice_get(buffer, 0);
  const_slice_advance(&buffer, 1);
  return 1;
}

ssize_t parse_req_object(struct req_object *obj, struct const_slice buffer) {
  if (buffer.size < 1) {
    return PARSE_MORE;
  }

  obj->type = const_slice_get(buffer, 0);
  const_slice_advance(&buffer, 1);

  ssize_t val_size;
  switch (obj->type) {
    case SER_INT:
      val_size = parse_int_value(&obj->int_val, buffer);
      break;
    case SER_FLOAT:
      val_size = parse_float_value(&obj->float_val, buffer);
      break;
    case SER_STR: {
      struct const_slice parsed;
      val_size = parse_str_value(&parsed, buffer);
      if (val_size > 0) {
        obj->str_val = slice_dup(parsed);
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

static void write_size(struct buffer *out, proto_size_t size) {
  buffer_ensure_cap(out, PROTO_SIZE_SIZE);
  memcpy(buffer_tail(out), &size, PROTO_SIZE_SIZE);
  out->size += PROTO_SIZE_SIZE;
}

void write_obj_type(struct buffer *out, enum proto_type type) {
  buffer_append_byte(out, type);
}

void write_nil_value(struct buffer *out) { write_obj_type(out, SER_NIL); }

void write_bool_value(struct buffer *out, bool val) {
  write_obj_type(out, val ? SER_TRUE : SER_FALSE);
}

void write_int_value(struct buffer *out, int_val_t n) {
  write_obj_type(out, SER_INT);
  buffer_append(out, &n, INT_VAL_SIZE);
}

void write_float_value(struct buffer *out, double val) {
  // TODO: Allow writing nan?
  assert(!isnan(val));
  write_obj_type(out, SER_FLOAT);
  buffer_append(out, &val, sizeof(val));
}

void write_str_value(struct buffer *out, struct const_slice str) {
  write_obj_type(out, SER_STR);
  write_size(out, str.size);
  buffer_append_slice(out, str);
}

void write_array_header(struct buffer *out, uint32_t arr_size) {
  write_obj_type(out, SER_ARR);
  write_size(out, arr_size);
}

void write_response_header(struct buffer *out, enum res_type res_type) {
  buffer_append_byte(out, res_type);
}

void write_nil_response(struct buffer *out) {
  write_response_header(out, RES_OK);
  write_nil_value(out);
}

void write_bool_response(struct buffer *out, bool val) {
  write_response_header(out, RES_OK);
  write_bool_value(out, val);
}

void write_int_response(struct buffer *out, int_val_t n) {
  write_response_header(out, RES_OK);
  write_int_value(out, n);
}

void write_float_response(struct buffer *out, double val) {
  write_response_header(out, RES_OK);
  write_float_value(out, val);
}

void write_str_response(struct buffer *out, struct const_slice str) {
  write_response_header(out, RES_OK);
  write_str_value(out, str);
}

void write_arr_response_header(struct buffer *out, uint32_t size) {
  write_response_header(out, RES_OK);
  write_array_header(out, size);
}

void write_err_response(struct buffer *out, const char *msg) {
  write_response_header(out, RES_ERR);
  write_str_value(out, make_str_slice(msg));
}

int print_req_object(FILE *stream, const struct req_object *obj) {
  switch (obj->type) {
    case SER_NIL:
      return fprintf(stream, "<NIL>");
    case SER_INT:
      return fprintf(stream, "%ld", obj->int_val);
    case SER_FLOAT:
      return fprintf(stream, "%g", obj->float_val);
    case SER_STR:
      return fprintf(
          stream, "%.*s", (int)obj->str_val.size,
          (const char *)obj->str_val.data);
    default:
      return fprintf(stream, "<INVALID>");
  }
}
