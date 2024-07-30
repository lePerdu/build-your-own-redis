#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#include "protocol.h"
#include "test.h"

#define make_array_const_slice(arr) make_const_slice(arr, sizeof(arr))
#define make_array_slice(arr) make_slice(arr, sizeof(arr))

#define assert_slice_eq(a, b) do { \
		assert((a).size == (b).size); \
		assert(memcmp((a).data, (b).data, (a).size) == 0); \
	} while (false)

static void test_parse_get_req(void) {
	uint8_t buffer[] = {
		9, 0, 0, 0,
		REQ_GET,
		OBJ_STR,
		3, 0, 0, 0,
		'a', 'b', 'c',
	};

	struct request req;
	ssize_t parse_size = parse_request(&req, make_array_const_slice(buffer));
	assert(parse_size > 0);
	assert(parse_size == sizeof(buffer));

	assert(req.type == REQ_GET);
	assert(req.args[0].type == OBJ_STR);
	assert_slice_eq(req.args[0].str_ref, make_str_slice("abc"));
}

static void test_parse_set_req(void) {
	uint8_t buffer[] = {
		18, 0, 0, 0,
		REQ_SET,
		OBJ_STR,
		3, 0, 0, 0,
		'a', 'b', 'c',
		OBJ_INT,
		0xf0, 0xde, 0xbc, 0x9a,
		0x78, 0x56, 0x34, 0x12,
	};

	struct request req;
	ssize_t parse_size = parse_request(&req, make_array_const_slice(buffer));
	assert(parse_size > 0);
	assert(parse_size == sizeof(buffer));

	assert(req.type == REQ_SET);
	assert(req.args[0].type == OBJ_STR);
	assert_slice_eq(req.args[0].str_ref, make_str_slice("abc"));
	assert(req.args[1].type == OBJ_INT);
	assert(req.args[1].int_val == 0x123456789abcdef0UL);
}

static void test_parse_del_req_max_size(void) {
	uint8_t header[] = {
		0, 0, 0, 0,
		REQ_DEL,
		OBJ_STR,
		0, 0, 0, 0,
	};
	proto_size_t total_size = PROTO_MAX_PAYLOAD_SIZE;
	memcpy(header, &total_size, PROTO_HEADER_SIZE);
	proto_size_t str_size = total_size - (sizeof(header) - PROTO_HEADER_SIZE);
	memcpy(&header[6], &str_size, PROTO_HEADER_SIZE);

	uint8_t buffer[PROTO_MAX_MESSAGE_SIZE];
	memset(buffer, 'A', PROTO_MAX_MESSAGE_SIZE);
	memcpy(buffer, header, sizeof(header));

	struct request req;
	ssize_t parse_size = parse_request(&req, make_array_const_slice(buffer));
	assert(parse_size > 0);
	assert(parse_size == sizeof(buffer));

	assert(req.type == REQ_DEL);
	assert(req.args[0].type == OBJ_STR);
	assert(req.args[0].str_ref.size == str_size);
}

static void test_parse_ok_res_nil(void) {}

static void test_parse_ok_res_with_str_val(void) {}

static void test_parse_ok_res_max_size(void) {}

static void test_parse_err_res_with_str_val(void) {}

static void test_parse_partial_req(void) {
	uint8_t buffer[] = {
		8, 0, 0, 0,
		REQ_DEL,
		3, 0, 0, 0,
		'1', '2', '3',
	};

	struct request req_in;
	ssize_t parse_size = parse_request(
		&req_in, make_const_slice(buffer, sizeof(buffer) - 4)
	);
	assert(parse_size == PARSE_MORE);
}

static void test_parse_partial_req_no_header(void) {
	uint8_t buffer[] = {
		8, 0, 0, 0,
		REQ_GET,
		3, 0, 0, 0,
		'1', '2', '3',
	};
	struct request req_in;
	ssize_t parse_size = parse_request(
		&req_in, make_const_slice(buffer, 3)
	);
	assert(parse_size == PARSE_MORE);
}

static void test_parse_partial_res(void) {
	uint8_t buffer[] = {
		8, 0, 0, 0,
		RES_OK,
		3, 0, 0, 0,
		'1', '2', '3',
	};
	struct response res_in;
	ssize_t parse_size = parse_response(
		&res_in, make_const_slice(buffer, sizeof(buffer) - 4)
	);
	assert(parse_size == PARSE_MORE);
}

static void test_parse_partial_res_no_header(void) {
	uint8_t buffer[] = {
		8, 0, 0, 0,
		RES_OK,
		3, 0, 0, 0,
		'1', '2', '3',
	};
	struct response res_in;
	ssize_t parse_size = parse_response(
		&res_in, make_const_slice(buffer, 2)
	);
	assert(parse_size == PARSE_MORE);
}

static void test_parse_invalid_req_invalid_type(void) {
	uint8_t buffer[] = {
		8, 0, 0, 0,
		18,
		3, 0, 0, 0,
		'1', '2', '3',
	};
	struct request req_in;
	ssize_t parse_size = parse_request(
		&req_in, make_const_slice(buffer, sizeof(buffer))
	);
	assert(parse_size == PARSE_ERR);
}

static void test_parse_invalid_req_content_too_short(void) {
	uint8_t buffer[] = {
		11, 0, 0, 0,
		REQ_DEL,
		3, 0, 0, 0,
		'1', '2', '3',
		0, 0, 0 // Padding
	};
	struct request req_in;
	ssize_t parse_size = parse_request(
		&req_in, make_const_slice(buffer, sizeof(buffer))
	);
	assert(parse_size == PARSE_ERR);
}

static void test_parse_invalid_req_message_len_too_small(void) {
	uint8_t buffer[] = {
		// Should be 9
		7, 0, 0, 0,
		REQ_GET,
		OBJ_STR,
		3, 0, 0, 0,
		'a', 'b', 'c',
	};

	struct request req_in;
	ssize_t parse_size = parse_request(
		&req_in, make_const_slice(buffer, sizeof(buffer))
	);
	assert(parse_size == PARSE_ERR);
}

static void test_parse_invalid_req_over_max_size(void) {
	uint8_t buffer[0xFFFF] = {
		0xFB, 0xFF, 0, 0,
		REQ_SET,
		3, 0, 0, 0,
		'1', '2', '3',
	};
	struct request req_in;
	ssize_t parse_size = parse_request(
		&req_in, make_const_slice(buffer, sizeof(buffer))
	);
	assert(parse_size == PARSE_ERR);
}

static void test_parse_invalid_res_invalid_type(void) {
	uint8_t buffer[] = {
		8, 0, 0, 0,
		18,
		3, 0, 0, 0,
		'1', '2', '3',
	};
	struct response res_in;
	ssize_t parse_size = parse_response(
		&res_in, make_const_slice(buffer, sizeof(buffer))
	);
	assert(parse_size == PARSE_ERR);
}

static void test_parse_invalid_res_content_too_short(void) {
	uint8_t buffer[] = {
		11, 0, 0, 0,
		RES_OK,
		3, 0, 0, 0,
		'1', '2', '3',
		0, 0, 0 // Padding
	};
	struct response res_in;
	ssize_t parse_size = parse_response(
		&res_in, make_const_slice(buffer, sizeof(buffer))
	);
	assert(parse_size == PARSE_ERR);
}

static void test_parse_invalid_res_over_max_size(void) {
	uint8_t buffer[0xFFFF] = {
		0xFB, 0xFF, 0, 0,
		RES_OK,
		3, 0, 0, 0,
		'1', '2', '3',
	};
	struct response res_in;
	ssize_t parse_size = parse_response(
		&res_in, make_const_slice(buffer, sizeof(buffer))
	);
	assert(parse_size == PARSE_ERR);
}

void test_parser(void) {
	RUN_TEST(test_parse_get_req);
	RUN_TEST(test_parse_set_req);
	RUN_TEST(test_parse_del_req_max_size);

	RUN_TEST(test_parse_ok_res_nil);
	RUN_TEST(test_parse_ok_res_with_str_val);
	RUN_TEST(test_parse_ok_res_max_size);
	RUN_TEST(test_parse_err_res_with_str_val);

	RUN_TEST(test_parse_partial_req);
	RUN_TEST(test_parse_partial_res);
	RUN_TEST(test_parse_partial_req_no_header);
	RUN_TEST(test_parse_partial_res_no_header);

	RUN_TEST(test_parse_invalid_req_invalid_type);
	RUN_TEST(test_parse_invalid_req_message_len_too_small);
	RUN_TEST(test_parse_invalid_req_content_too_short);
	RUN_TEST(test_parse_invalid_req_over_max_size);
	RUN_TEST(test_parse_invalid_res_invalid_type);
	RUN_TEST(test_parse_invalid_res_content_too_short);
	RUN_TEST(test_parse_invalid_res_over_max_size);
}
