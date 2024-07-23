#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "protocol.h"

[[noreturn]] static void die_errno(const char *msg) {
	perror(msg);
	exit(EXIT_FAILURE);
}

static int setup_socket(void) {
	// result variable used for various syscalls
	int res;

	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd == -1) {
		die_errno("failed to open socket");
	}

	struct sockaddr_in addr = {
		.sin_family = AF_INET,
		.sin_port = ntohs(1234),
		.sin_addr = {ntohl(INADDR_LOOPBACK)},
	};
	res = connect(fd, (const struct sockaddr *) &addr, sizeof(addr));
	if (res == -1) {
		die_errno("failed to connect to socket");
	}

	return fd;
}

static ssize_t read_exact(int fd, void *buf, size_t size) {
	size_t remaining = size;
	while (remaining > 0) {
		ssize_t n_read = recv(fd, buf, remaining, 0);
		if (n_read <= 0) {
			// TODO Report specific error, like with errno?
			return -1;
		}

		remaining -= n_read;
		buf += n_read;
	}
	return size;
}

static ssize_t read_one_response(
	int fd, uint8_t buf[PROTO_MAX_MESSAGE_SIZE], struct response *res
) {
	if (read_exact(fd, buf, PROTO_HEADER_SIZE) == -1) {
		return -1;
	}
	message_len_t message_len;
	memcpy(&message_len, buf, PROTO_HEADER_SIZE);

	if (message_len > PROTO_MAX_PAYLOAD_SIZE) {
		return -1;
	}

	if (read_exact(fd, &buf[PROTO_HEADER_SIZE], message_len) == -1) {
		return -1;
	}

	ssize_t res_size = parse_response(
		res, (struct slice){ .size = PROTO_MAX_MESSAGE_SIZE, .data = buf}
	);
	if (res_size != PROTO_HEADER_SIZE + message_len) {
		return -1;
	}
	return res_size;
}

static ssize_t write_exact(int fd, const void *buf, size_t size) {
	size_t remaining = size;
	while (remaining > 0) {
		ssize_t n_write = send(fd, buf, remaining, MSG_NOSIGNAL);
		if (n_write <= 0) {
			// TODO Report specific error, like with errno?
			return -1;
		}

		remaining -= n_write;
		buf += n_write;
	}
	return size;
}

static ssize_t write_one_request(
	int fd, uint8_t buf[PROTO_MAX_MESSAGE_SIZE], const struct request *req
) {
	ssize_t req_size = write_request(
		(struct slice){.size = PROTO_MAX_MESSAGE_SIZE, .data = buf}, req
	);
	if (req_size < 0) {
		return -1;
	}

	return write_exact(fd, buf, req_size);
}

int main(int argc, char *argv[argc]) {
	int fd = setup_socket();

	fprintf(stderr, "openned connection [%d]\n", fd);

	const char *key;
	if (argc == 2) {
		key = argv[1];
	} else {
		key = "abc";
	}

	struct request req;
	uint8_t write_buf[PROTO_MAX_MESSAGE_SIZE];
	ssize_t n_write;

	req.type = REQ_GET;
	req.key = make_str_slice(key);
	n_write = write_one_request(fd, write_buf, &req);
	if (n_write == -1) {
		die_errno("failed to write message");
	}
	fprintf(stderr, "to server: ");
	print_request(stderr, &req);
	putc('\n', stderr);
	sleep(1);

	struct response res;

	uint8_t read_buf[PROTO_MAX_MESSAGE_SIZE];
	ssize_t n_read;

	n_read = read_one_response(fd, read_buf, &res);
	if (n_read == -1) {
		die_errno("failed to read response");
	}
	fprintf(stderr, "from server: ");
	print_response(stderr, &res);
	putc('\n', stderr);

	int close_res = close(fd);
	if (close_res == -1) {
		die_errno("failed to close connection");
	}
	fprintf(stderr, "closed connection [%d]\n", fd);

	return 0;
}
