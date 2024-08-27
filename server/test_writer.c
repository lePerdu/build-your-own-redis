#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "buffer.h"
#include "protocol.h"
#include "test.h"
#include "types.h"

#define make_output_slice(output_str) \
  make_const_slice((output_str), sizeof(output_str) - 1)

#define assert_slice_eq(a, b)                          \
  do {                                                 \
    assert((a).size == (b).size);                      \
    assert(memcmp((a).data, (b).data, (a).size) == 0); \
  } while (false)

// NOLINTBEGIN(readability-magic-numbers)

static void test_write_null_value(void) {
  struct const_slice expected = make_output_slice("_\r\n");

  struct buffer buffer;
  buffer_init(&buffer, expected.size);

  write_null_value(&buffer);

  assert_slice_eq(buffer_const_slice(&buffer), expected);
  buffer_destroy(&buffer);
}

static void test_write_int_value(void) {
  struct const_slice expected = make_output_slice(":1200451\r\n");

  struct buffer buffer;
  buffer_init(&buffer, expected.size);

  write_int_value(&buffer, 1200451);

  assert_slice_eq(buffer_const_slice(&buffer), expected);
  buffer_destroy(&buffer);
}

static void test_write_int_value_negative(void) {
  struct const_slice expected = make_output_slice(":-287634\r\n");

  struct buffer buffer;
  buffer_init(&buffer, expected.size);

  write_int_value(&buffer, -287634);

  assert_slice_eq(buffer_const_slice(&buffer), expected);
  buffer_destroy(&buffer);
}

static void test_write_simple_str_value(void) {
  struct const_slice expected = make_output_slice("+OK\r\n");

  struct buffer buffer;
  buffer_init(&buffer, expected.size);

  write_simple_str_value(&buffer, "OK");

  assert_slice_eq(buffer_const_slice(&buffer), expected);
  buffer_destroy(&buffer);
}

static void test_write_str_value_empty(void) {
  struct const_slice expected = make_output_slice("$0\r\n\r\n");

  struct buffer buffer;
  buffer_init(&buffer, expected.size);

  write_str_value(&buffer, make_const_slice(NULL, 0));

  assert_slice_eq(buffer_const_slice(&buffer), expected);
  buffer_destroy(&buffer);
}

static void test_write_str_value_non_empty(void) {
  struct const_slice expected = make_output_slice("$13\r\nHello, World!\r\n");

  struct buffer buffer;
  buffer_init(&buffer, expected.size);

  write_str_value(&buffer, make_str_slice("Hello, World!"));

  assert_slice_eq(buffer_const_slice(&buffer), expected);
  buffer_destroy(&buffer);
}

static void test_write_simple_err_value(void) {
  struct const_slice expected = make_output_slice("-NOT FOUND\r\n");

  struct buffer buffer;
  buffer_init(&buffer, expected.size);

  write_simple_err_value(&buffer, "NOT FOUND");

  assert_slice_eq(buffer_const_slice(&buffer), expected);
  buffer_destroy(&buffer);
}

static void test_write_arr_value_mixed(void) {
  struct const_slice expected = make_output_slice(
      "*3\r\n"
      ":-123\r\n"
      "$4\r\nAbCd\r\n"
      "_\r\n");

  struct buffer buffer;
  buffer_init(&buffer, expected.size);

  write_array_header(&buffer, 3);
  write_int_value(&buffer, -123);
  write_str_value(&buffer, make_str_slice("AbCd"));
  write_null_value(&buffer);

  assert_slice_eq(buffer_const_slice(&buffer), expected);
  buffer_destroy(&buffer);
}

// NOLINTEND(readability-magic-numbers)

void test_writer(void) {
  RUN_TEST(test_write_null_value);
  RUN_TEST(test_write_int_value);
  RUN_TEST(test_write_int_value_negative);
  RUN_TEST(test_write_simple_str_value);
  RUN_TEST(test_write_str_value_empty);
  RUN_TEST(test_write_str_value_non_empty);
  RUN_TEST(test_write_simple_err_value);
  RUN_TEST(test_write_arr_value_mixed);
}
