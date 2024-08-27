#include "protocol.h"

#include <assert.h>
#include <ctype.h>
#include <math.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>

#include "buffer.h"
#include "types.h"

enum {
  PARSE_INT_BASE = 10,
};

// Re-implementations of strtol, strtod, etc. which respect input slice size

static bool parse_int_helper(int_val_t *val, struct const_slice *input) {
  if (input->size == 0) {
    return false;
  }

  bool negative = false;
  uint8_t prefix = const_slice_get(*input, 0);
  if (prefix == '+') {
    const_slice_advance(input, 1);
  } else if (prefix == '-') {
    negative = true;
    const_slice_advance(input, 1);
  }

  *val = 0;
  size_t index;
  for (index = 0; index < input->size; index++) {
    uint8_t byte = const_slice_get(*input, index);
    if (!isdigit(byte)) {
      break;
    }
    *val = *val * PARSE_INT_BASE + byte - '0';
  }
  const_slice_advance(input, index);

  if (negative) {
    *val = -*val;
  }
  return true;
}

bool parse_int_arg(int_val_t *val, struct const_slice input) {
  if (!parse_int_helper(val, &input)) {
    return false;
  }

  return input.size == 0;
}

static void parse_float_fractional(double *val, struct const_slice *input) {
#define ONE_TENTH 0.1
  size_t index = 0;
  for (double scalar = ONE_TENTH; index < input->size;
       index++, scalar *= ONE_TENTH) {
#undef ONE_TENTH
    uint8_t byte = const_slice_get(*input, index);
    if (!isdigit(byte)) {
      break;
    }
    *val += (byte - '0') * scalar;
  }

  const_slice_advance(input, index);
}

static bool parse_float_exponent(double *val, struct const_slice *input) {
  int_val_t exponent;
  if (!parse_int_helper(&exponent, input)) {
    return false;
  }

  *val *= pow(PARSE_INT_BASE, (double)exponent);
  return true;
}

bool parse_float_arg(double *val, struct const_slice input) {
  if (input.size == 0) {
    return false;
  }

  // TODO: Check for nan/inf later since they are probably rare?
  if (slice_eq(input, make_const_slice("nan", 3))) {
    *val = NAN;
    return true;
  }
  if (slice_eq(input, make_const_slice("inf", 3))) {
    *val = INFINITY;
    return true;
  }
  if (slice_eq(input, make_const_slice("-inf", 4))) {
    *val = -INFINITY;
    return true;
  }

  bool negative = false;
  uint8_t prefix = const_slice_get(input, 0);
  if (prefix == '+') {
    const_slice_advance(&input, 1);
  } else if (prefix == '-') {
    negative = true;
    const_slice_advance(&input, 1);
  }

  // Integral part
  *val = 0;
  size_t index;
  for (index = 0; index < input.size; index++) {
    uint8_t byte = const_slice_get(input, index);
    if (!isdigit(byte)) {
      break;
    }
    *val = *val * PARSE_INT_BASE + byte - '0';
  }
  const_slice_advance(&input, index);

  if (input.size > 0 && const_slice_get(input, 0) == '.') {
    const_slice_advance(&input, 1);
    parse_float_fractional(val, &input);
  }

  if (input.size > 0 && tolower(const_slice_get(input, 0)) == 'e') {
    const_slice_advance(&input, 1);
    if (!parse_float_exponent(val, &input)) {
      return false;
    }
  }

  if (input.size > 0) {
    return false;
  }

  if (negative) {
    *val = -*val;
  }
  return true;
}

/**
 * Check a type byte and parse the size of the object (including the \r\n).
 */
static ssize_t parse_size(
    enum resp_type type, uint64_t *restrict size, struct const_slice buffer) {
  if (buffer.size < 1) {
    return PARSE_MORE;
  }
  if (const_slice_get(buffer, 0) != type) {
    return PARSE_ERR;
  }

  *size = 0;
  // Start after the type byte
  for (size_t i = 1; i < buffer.size; i++) {
    uint8_t byte = const_slice_get(buffer, i);
    if (isdigit(byte)) {
      *size = *size * PARSE_INT_BASE + byte - '0';
    } else if (byte == '\r') {
      if (i == 0) {
        return PARSE_ERR;
      }

      i++;
      if (i >= buffer.size) {
        return PARSE_MORE;
      }
      if (const_slice_get(buffer, i) != '\n') {
        return PARSE_ERR;
      }
      // + 1 to include the last char parsed ('\n')
      return (ssize_t)i + 1;
    } else {
      return PARSE_ERR;
    }
  }

  return PARSE_MORE;
}

ssize_t parse_array_header(uint32_t *size, struct const_slice buffer) {
  uint64_t arr_size;
  ssize_t res = parse_size(RESP_ARRAY, &arr_size, buffer);
  if (res < 0) {
    return res;
  }

  if (arr_size > UINT32_MAX) {
    return PARSE_ERR;
  }

  *size = arr_size;
  return res;
}

ssize_t parse_blob_str(struct const_slice *str, struct const_slice buffer) {
  uint64_t str_size;
  ssize_t res = parse_size(RESP_BLOB_STR, &str_size, buffer);
  if (res < 0) {
    return res;
  }
  const_slice_advance(&buffer, res);

  if (buffer.size < str_size + 2) {
    return PARSE_MORE;
  }

  if (const_slice_get(buffer, str_size) != '\r') {
    return PARSE_ERR;
  }
  if (const_slice_get(buffer, str_size + 1) != '\n') {
    return PARSE_ERR;
  }

  *str = make_const_slice(buffer.data, str_size);
  return res + (ssize_t)str_size + 2;
}

static void write_end(struct buffer *out) {
  buffer_append_byte(out, '\r');
  buffer_append_byte(out, '\n');
}

void write_null_value(struct buffer *out) {
  buffer_append_byte(out, RESP_NULL);
  write_end(out);
}

void write_bool_value(struct buffer *out, bool val) {
  buffer_append_byte(out, RESP_BOOLEAN);
  buffer_append_byte(out, val ? 't' : 'f');
  write_end(out);
}

static void write_format_str(
    struct buffer *out, enum resp_type type, const char *fmt, ...) {
  buffer_append_byte(out, type);

  // This seems to be a bug in the linter where it sometimes reports this
  // va_list usage as invalid
  // NOLINTBEGIN(clang-analyzer-valist.Uninitialized)
  va_list args;
  va_start(args, fmt);
  // First try in case the buffer already has enough space available
  int required_size =
      vsnprintf(buffer_tail(out), buffer_remaining(out), fmt, args);
  va_end(args);

  assert(required_size >= 0);
  if ((uint32_t)required_size > buffer_remaining(out)) {
    // Write again if truncated
    // Include +2 to make sure no second expansion is needed for \r\n
    buffer_ensure_cap(out, required_size + 2);

    va_start(args, fmt);
    required_size =
        vsnprintf(buffer_tail(out), buffer_remaining(out), fmt, args);
    va_end(args);
    // NOLINTEND(clang-analyzer-valist.Uninitialized)

    assert(required_size >= 0);
    assert((uint32_t)required_size <= buffer_remaining(out));
  }
  buffer_inc_size(out, required_size);
  write_end(out);
}

void write_int_value(struct buffer *out, int_val_t n) {
  write_format_str(out, RESP_NUMBER, "%ld", n);
}

void write_float_value(struct buffer *out, double val) {
  if (isnan(val)) {
    write_format_str(out, RESP_DOUBLE, "nan");
  } else if (isinf(val)) {
    write_format_str(out, RESP_DOUBLE, val < 0 ? "-inf" : "inf");
  } else {
    write_format_str(out, RESP_DOUBLE, "%g", val);
  }
}

void write_simple_str_value(struct buffer *out, const char *str) {
  buffer_append_byte(out, RESP_SIMPLE_STR);
  // TODO: Assert str doesn't contain \r or \n
  buffer_append_slice(out, make_str_slice(str));
  write_end(out);
}

void write_simple_err_value(struct buffer *out, const char *str) {
  buffer_append_byte(out, RESP_SIMPLE_ERR);
  // TODO: Assert str doesn't contain \r or \n
  buffer_append_slice(out, make_str_slice(str));
  write_end(out);
}

void write_str_value(struct buffer *out, struct const_slice str) {
  write_format_str(out, RESP_BLOB_STR, "%lu", str.size);
  buffer_append_slice(out, str);
  write_end(out);
}

void write_array_header(struct buffer *out, uint32_t arr_size) {
  write_format_str(out, RESP_ARRAY, "%u", arr_size);
}
