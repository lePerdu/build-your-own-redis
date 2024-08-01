#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#include "buffer.h"
#include "protocol.h"
#include "test.h"
#include "types.h"

#define make_array_const_slice(arr) make_const_slice(arr, sizeof(arr))
#define make_array_slice(arr) make_slice(arr, sizeof(arr))

#define assert_slice_eq(a, b) do { \
		assert((a).size == (b).size); \
		assert(memcmp((a).data, (b).data, (a).size) == 0); \
	} while (false)

static void test_parse_req_type(void) {
	uint8_t buffer[] = { REQ_GET };

	enum req_type type;
	ssize_t n = parse_req_type(&type, make_array_const_slice(buffer));
	assert(n == 1);
	assert(type == REQ_GET);
}

static void test_parse_req_type_invalid(void) {
	uint8_t buffer[] = { 245 };

	enum req_type type;
	ssize_t n = parse_req_type(&type, make_array_const_slice(buffer));
	assert(n == PARSE_ERR);
}

static void test_parse_req_type_empty_buffer(void) {
	enum req_type type;
	ssize_t n = parse_req_type(&type, make_const_slice(NULL, 0));
	assert(n == PARSE_MORE);
}

static void test_parse_req_object_int(void) {
	uint8_t buffer[] = {
		SER_INT, 0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12
	};

	struct req_object o;
	ssize_t n = parse_req_object(&o, make_array_const_slice(buffer));
	assert(n == 9);
	assert(o.type = SER_INT);
	assert(o.int_val == 0x123456789abcdef0L);
}

static void test_parse_req_object_int_need_more_data(void) {
	uint8_t buffer[] = { SER_INT, 0xf0, 0xde, 0xbc };

	struct req_object o;
	ssize_t n = parse_req_object(&o, make_array_const_slice(buffer));
	assert(n == PARSE_MORE);
}

static void test_parse_req_object_str(void) {
	uint8_t buffer[] = {
		SER_STR, 5, 0, 0, 0,
		'a', 'b', 'c', 'd', 'e',
	};

	struct req_object o;
	ssize_t n = parse_req_object(&o, make_array_const_slice(buffer));
	assert(n == 10);
	assert(o.type = SER_STR);
	assert_slice_eq(o.str_val, make_str_slice("abcde"));

	req_object_destroy(&o);
}

static void test_parse_req_object_str_need_more_data_for_len(void) {
	uint8_t buffer[] = {
		SER_STR, 5, 0, 0,
	};

	struct req_object o;
	ssize_t n = parse_req_object(&o, make_array_const_slice(buffer));
	assert(n == PARSE_MORE);
}

static void test_parse_req_object_str_need_more_data_after_len(void) {
	uint8_t buffer[] = {
		SER_STR, 5, 0, 0, 0,
		'a', 'b', 'c'
	};

	struct req_object o;
	ssize_t n = parse_req_object(&o, make_array_const_slice(buffer));
	assert(n == PARSE_MORE);
}

static void test_parse_req_object_invalid_type(void) {
	uint8_t buffer[18] = { 245 };

	struct req_object o;
	ssize_t n = parse_req_object(&o, make_array_const_slice(buffer));
	assert(n == PARSE_ERR);
}

static void test_parse_req_object_empty_buffer(void) {
	struct req_object o;
	ssize_t n = parse_req_object(&o, make_const_slice(NULL, 0));
	assert(n == PARSE_MORE);
}

static void test_write_nil_response(void) {
	uint8_t expected[] = { RES_OK, SER_NIL };

	struct buffer buffer;
	buffer_init(&buffer, sizeof(expected));

	write_nil_response(&buffer);

	assert_slice_eq(buffer_const_slice(&buffer), make_array_const_slice(expected));
	buffer_destroy(&buffer);
}

static void test_write_int_response(void) {
	uint8_t expected[] = {
		RES_OK,
		SER_INT, 0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12
	};

	struct buffer buffer;
	buffer_init(&buffer, sizeof(expected));

	write_int_response(&buffer, 0x123456789abcdef0L);

	assert_slice_eq(buffer_const_slice(&buffer), make_array_const_slice(expected));
	buffer_destroy(&buffer);
}

static void test_write_str_response(void) {
	uint8_t expected[] = {
		RES_OK,
		SER_STR, 5, 0, 0, 0,
		'a', 'b', 'c', 'd', 'e'
	};

	struct buffer buffer;
	buffer_init(&buffer, sizeof(expected));

	write_str_response(&buffer, make_str_slice("abcde"));

	assert_slice_eq(buffer_const_slice(&buffer), make_array_const_slice(expected));
	buffer_destroy(&buffer);
}

static void test_write_err_response(void) {
	uint8_t expected[] = {
		RES_ERR,
		SER_STR, 5, 0, 0, 0,
		'a', 'b', 'c', 'd', 'e'
	};

	struct buffer buffer;
	buffer_init(&buffer, sizeof(expected));

	write_err_response(&buffer, "abcde");

	assert_slice_eq(buffer_const_slice(&buffer), make_array_const_slice(expected));
	buffer_destroy(&buffer);
}

static void test_write_arr_response(void) {
	uint8_t expected[] = {
		RES_OK,
		SER_ARR, 2, 0, 0, 0,
		SER_INT, 0xff, 0x51, 0, 0, 0, 0, 0, 0,
		SER_STR, 3, 0, 0, 0, '1', '2', '3',
	};

	struct buffer buffer;
	buffer_init(&buffer, sizeof(expected));

	write_arr_response_header(&buffer, 2);
	write_int_value(&buffer, 0x51ff);
	write_str_value(&buffer, make_str_slice("123"));

	assert_slice_eq(buffer_const_slice(&buffer), make_array_const_slice(expected));
	buffer_destroy(&buffer);
}

void test_parser(void) {
	RUN_TEST(test_parse_req_type);
	RUN_TEST(test_parse_req_type_invalid);
	RUN_TEST(test_parse_req_type_empty_buffer);

	RUN_TEST(test_parse_req_object_int);
	RUN_TEST(test_parse_req_object_int_need_more_data);

	RUN_TEST(test_parse_req_object_str);
	RUN_TEST(test_parse_req_object_str_need_more_data_for_len);
	RUN_TEST(test_parse_req_object_str_need_more_data_after_len);

	RUN_TEST(test_parse_req_object_empty_buffer);
	RUN_TEST(test_parse_req_object_invalid_type);

	RUN_TEST(test_write_nil_response);
	RUN_TEST(test_write_int_response);
	RUN_TEST(test_write_str_response);
	RUN_TEST(test_write_err_response);
	RUN_TEST(test_write_arr_response);
}
