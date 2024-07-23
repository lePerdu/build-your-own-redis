#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#include "protocol.h"
#include "test.h"

#define assert_slice_eq(a, b) do { \
		assert((a).size == (b).size); \
		assert(memcmp((a).data, (b).data, (a).size) == 0); \
	} while (false)

static void test_write_parse_get_req(void) {
	uint8_t buffer[PROTO_MAX_MESSAGE_SIZE];
	struct request req_out = {
		.type = REQ_GET,
		.key = {
			.size = 3,
			.data = "abc",
		},
	};
	ssize_t write_size = write_request(
		make_slice(buffer, PROTO_MAX_MESSAGE_SIZE), &req_out
	);
	assert(write_size > 0);
	assert(write_size == 4 + 1 + 4 + 3);

	struct request req_in;
	ssize_t parse_size =
		parse_request(&req_in, make_const_slice(buffer, write_size));
	assert(parse_size > 0);
	assert(parse_size == write_size);

	assert(req_out.type == req_out.type);
	assert_slice_eq(req_in.key, req_out.key);
}

static void test_write_parse_set_req(void) {
	uint8_t buffer[PROTO_MAX_MESSAGE_SIZE];
	struct request req_out = {
		.type = REQ_SET,
		.key = {
			.size = 3,
			.data = "abc",
		},
		.val = {
			.size = 8,
			.data = "12345678",
		},
	};
	ssize_t write_size = write_request(make_slice(buffer, PROTO_MAX_MESSAGE_SIZE), &req_out);
	assert(write_size > 0);
	assert(write_size == 4 + 1 + 4 + 3 + 4 + 8);

	struct request req_in;
	ssize_t parse_size = parse_request(
		&req_in, make_const_slice(buffer, write_size)
	);
	assert(parse_size > 0);
	assert(parse_size == write_size);

	assert(req_out.type == req_out.type);
	assert_slice_eq(req_in.key, req_out.key);
}

static void test_write_parse_del_req_max_size(void) {
	uint8_t max_size_data[PROTO_MAX_PAYLOAD_SIZE];
	memset(max_size_data, 'A', PROTO_MAX_PAYLOAD_SIZE);

	uint8_t buffer[PROTO_MAX_MESSAGE_SIZE];
	struct request req_out = {
		.type = REQ_DEL,
		.key = {
			.size = PROTO_MAX_PAYLOAD_SIZE - 4 - 1,
			.data = max_size_data,
		},
	};
	ssize_t write_size = write_request(make_slice(buffer, PROTO_MAX_MESSAGE_SIZE), &req_out);
	assert(write_size > 0);
	assert(write_size == PROTO_MAX_MESSAGE_SIZE);

	struct request req_in;
	ssize_t parse_size = parse_request(
		&req_in, make_const_slice(buffer, write_size)
	);
	assert(parse_size > 0);
	assert(parse_size == write_size);

	assert(req_out.type == req_out.type);
	assert_slice_eq(req_in.key, req_out.key);
}

static void test_write_parse_ok_res(void) {
	uint8_t buffer[PROTO_MAX_MESSAGE_SIZE];
	struct response res_out = {
		.type = RES_OK,
		.data = {
			.size = 8,
			.data = "12345678",
		},
	};
	ssize_t write_size = write_response(
		make_slice(buffer, PROTO_MAX_MESSAGE_SIZE), &res_out
	);
	assert(write_size > 0);
	assert(write_size == 4 + 1 + 4 + 8);

	struct response res_in;
	ssize_t parse_size = parse_response(
		&res_in, make_const_slice(buffer, write_size)
	);
	assert(parse_size > 0);
	assert(parse_size == write_size);

	assert(res_out.type == res_out.type);
	assert_slice_eq(res_in.data, res_out.data);
}

static void test_write_parse_err_res_max_size(void) {
	uint8_t max_size_data[PROTO_MAX_PAYLOAD_SIZE];
	memset(max_size_data, 'A', PROTO_MAX_PAYLOAD_SIZE);

	uint8_t buffer[PROTO_MAX_MESSAGE_SIZE];
	struct response res_out = {
		.type = RES_OK,
		.data = {
			.size = PROTO_MAX_PAYLOAD_SIZE - 1 - 4,
			.data = max_size_data,
		},
	};
	ssize_t write_size = write_response(make_slice(buffer, PROTO_MAX_MESSAGE_SIZE), &res_out);
	assert(write_size > 0);
	assert(write_size == PROTO_MAX_MESSAGE_SIZE);

	struct response res_in;
	ssize_t parse_size = parse_response(
		&res_in, make_const_slice(buffer, write_size)
	);
	assert(parse_size > 0);
	assert(parse_size == write_size);

	assert(res_out.type == res_out.type);
	assert_slice_eq(res_in.data, res_out.data);
}

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
	RUN_TEST(test_write_parse_get_req);
	RUN_TEST(test_write_parse_del_req_max_size);
	RUN_TEST(test_write_parse_set_req);
	RUN_TEST(test_write_parse_ok_res);
	RUN_TEST(test_write_parse_err_res_max_size);

	RUN_TEST(test_parse_partial_req);
	RUN_TEST(test_parse_partial_res);
	RUN_TEST(test_parse_partial_req_no_header);
	RUN_TEST(test_parse_partial_res_no_header);

	RUN_TEST(test_parse_invalid_req_invalid_type);
	RUN_TEST(test_parse_invalid_req_content_too_short);
	RUN_TEST(test_parse_invalid_req_over_max_size);
	RUN_TEST(test_parse_invalid_res_invalid_type);
	RUN_TEST(test_parse_invalid_res_content_too_short);
	RUN_TEST(test_parse_invalid_res_over_max_size);
}
