#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "protocol.h"
#include "test.h"
#include "types.h"

// NOLINTBEGIN(readability-magic-numbers)

#define make_input_slice(input_str) \
  make_const_slice((input_str), sizeof(input_str) - 1)

#define assert_slice_eq(a, b)                          \
  do {                                                 \
    assert((a).size == (b).size);                      \
    assert(memcmp((a).data, (b).data, (a).size) == 0); \
  } while (false)

static void test_parse_blob_str_empty(void) {
  struct const_slice input = make_input_slice("$0\r\n\r\n");
  struct const_slice str;
  ssize_t n_parsed = parse_blob_str(&str, input);
  assert(n_parsed == (ssize_t)input.size);
  assert(str.size == 0);
}

static void test_parse_blob_str_small(void) {
  struct const_slice input = make_input_slice("$6\r\nHello!\r\n");
  struct const_slice str;
  ssize_t n_parsed = parse_blob_str(&str, input);
  assert(n_parsed == (ssize_t)input.size);
  assert_slice_eq(str, make_str_slice("Hello!"));
}

static void test_parse_blob_str_large(void) {
#define HEAD_SIZE (1 + 7 + 2)
#define STR_SIZE 1200000
#define TAIL_SIZE 2
#define TOTAL_SIZE (HEAD_SIZE + STR_SIZE + TAIL_SIZE)

  char *input = malloc(TOTAL_SIZE);
  memcpy(input, "$1200000\r\n", HEAD_SIZE);
  memset(&input[HEAD_SIZE], 0x55, STR_SIZE);
  // NOLINTNEXTLINE(bugprone-not-null-terminated-result)
  memcpy(&input[HEAD_SIZE + STR_SIZE], "\r\n", TAIL_SIZE);

  struct const_slice str;
  ssize_t n_parsed = parse_blob_str(&str, make_const_slice(input, TOTAL_SIZE));
  assert(n_parsed == TOTAL_SIZE);
  assert(str.size == STR_SIZE);

  free(input);
#undef HEAD_SIZE
#undef STR_SIZE
#undef TAIL_SIZE
#undef TOTAL_SIZE
}

// Not enough data

static void test_parse_blob_str_empty_buf(void) {
  struct const_slice str;
  ssize_t n_parsed = parse_blob_str(&str, make_const_slice(NULL, 0));
  assert(n_parsed == PARSE_MORE);
}

static void test_parse_blob_str_not_full_number(void) {
  struct const_slice input = make_input_slice("$12");
  struct const_slice str;
  ssize_t n_parsed = parse_blob_str(&str, input);
  assert(n_parsed == PARSE_MORE);
}

static void test_parse_blob_str_number_and_cr(void) {
  struct const_slice input = make_input_slice("$12\r");
  struct const_slice str;
  ssize_t n_parsed = parse_blob_str(&str, input);
  assert(n_parsed == PARSE_MORE);
}

static void test_parse_blob_str_number_no_content(void) {
  struct const_slice input = make_input_slice("$12\r\n");
  struct const_slice str;
  ssize_t n_parsed = parse_blob_str(&str, input);
  assert(n_parsed == PARSE_MORE);
}

static void test_parse_blob_str_number_and_not_full_content(void) {
  struct const_slice input = make_input_slice("$12\r\nabcdef");
  struct const_slice str;
  ssize_t n_parsed = parse_blob_str(&str, input);
  assert(n_parsed == PARSE_MORE);
}

static void test_parse_blob_str_number_and_full_content_no_crlf(void) {
  struct const_slice input = make_input_slice("$5\r\nABCdef");
  struct const_slice str;
  ssize_t n_parsed = parse_blob_str(&str, input);
  assert(n_parsed == PARSE_MORE);
}

// Invalid data

static void test_parse_blob_str_invalid_type(void) {
  struct const_slice input = make_input_slice(":5a\r\n");
  struct const_slice str;
  ssize_t n_parsed = parse_blob_str(&str, input);
  assert(n_parsed == PARSE_ERR);
}

static void test_parse_blob_str_invalid_number(void) {
  struct const_slice input = make_input_slice("$5a\r\n");
  struct const_slice str;
  ssize_t n_parsed = parse_blob_str(&str, input);
  assert(n_parsed == PARSE_ERR);
}

static void test_parse_blob_str_invalid_crlf(void) {
  struct const_slice input = make_input_slice("$5\r\r");
  struct const_slice str;
  ssize_t n_parsed = parse_blob_str(&str, input);
  assert(n_parsed == PARSE_ERR);
}

static void test_parse_blob_str_no_crlf_after_content(void) {
  struct const_slice input = make_input_slice("$5\r\nHELLOabc\r\n");
  struct const_slice str;
  ssize_t n_parsed = parse_blob_str(&str, input);
  assert(n_parsed == PARSE_ERR);
}

// NOLINTEND(readability-magic-numbers)

void test_parser(void) {
  RUN_TEST(test_parse_blob_str_empty);
  RUN_TEST(test_parse_blob_str_small);
  RUN_TEST(test_parse_blob_str_large);
  RUN_TEST(test_parse_blob_str_empty_buf);
  RUN_TEST(test_parse_blob_str_not_full_number);
  RUN_TEST(test_parse_blob_str_number_and_cr);
  RUN_TEST(test_parse_blob_str_number_no_content);
  RUN_TEST(test_parse_blob_str_number_and_not_full_content);
  RUN_TEST(test_parse_blob_str_number_and_full_content_no_crlf);
  RUN_TEST(test_parse_blob_str_invalid_type);
  RUN_TEST(test_parse_blob_str_invalid_number);
  RUN_TEST(test_parse_blob_str_invalid_crlf);
  RUN_TEST(test_parse_blob_str_no_crlf_after_content);
}
