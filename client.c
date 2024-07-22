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
		ssize_t n_read = read(fd, buf, remaining);
		if (n_read <= 0) {
			// TODO Report specific error, like with errno?
			return -1;
		}

		remaining -= n_read;
		buf += n_read;
	}
	return size;
}

static ssize_t read_one_message(int fd, void *buf, size_t size) {
	message_len_t message_len;

	if (read_exact(fd, &message_len, PROTO_HEADER_SIZE) == -1) {
		return -1;
	}

	if (message_len > PROTO_MAX_PAYLOAD_SIZE) {
		return -1;
	}

	if (message_len > size) {
		// TODO Read rest of the message to "reset" the protocol?
		return -1;
	}

	return read_exact(fd, buf, message_len);
}

static ssize_t write_exact(int fd, const void *buf, size_t size) {
	size_t remaining = size;
	while (remaining > 0) {
		ssize_t n_write = write(fd, buf, remaining);
		if (n_write <= 0) {
			// TODO Report specific error, like with errno?
			return -1;
		}

		remaining -= n_write;
		buf += n_write;
	}
	return size;
}

static ssize_t write_one_message(int fd, const void *buf, size_t size) {
	if (size > PROTO_MAX_PAYLOAD_SIZE) {
		return -1;
	}

	message_len_t message_len = size;
	if (write_exact(fd, &message_len, PROTO_HEADER_SIZE) == -1) {
		return -1;
	}

	return write_exact(fd, buf, size);
}

int main(int argc, char *argv[argc]) {
	int fd = setup_socket();

	fprintf(stderr, "openned connection [%d]\n", fd);

	const char *msg;
	ssize_t n_write;

	msg = "[[";
	n_write = write_one_message(fd, msg, strlen(msg));
	if (n_write == -1) {
		die_errno("failed to write message");
	}
	printf("to server: %s\n", msg);
	sleep(1);

	if (argc == 2) {
		msg = argv[1];
	} else {
		msg = "hello";
	}

	uint8_t filled[PROTO_MAX_PAYLOAD_SIZE];
	memset(filled, 'A', sizeof(filled));
	msg = filled;
	n_write = write_one_message(fd, msg, sizeof(filled));
	if (n_write == -1) {
		die_errno("failed to write message");
	}
	printf("to server: %s\n", msg);
	sleep(1);

	msg = "]]";
	n_write = write_one_message(fd, msg, strlen(msg));
	if (n_write == -1) {
		die_errno("failed to write message");
	}
	printf("to server: %s\n", msg);
	sleep(1);

	char rbuf[PROTO_MAX_PAYLOAD_SIZE + 1];
	ssize_t n_read;
	n_read = read_one_message(fd, rbuf, sizeof(rbuf) - 1);
	if (n_read == -1) {
		die_errno("failed to read response");
	}
	rbuf[n_read] = 0;

	printf("from server: %s\n", rbuf);

	n_read = read_one_message(fd, rbuf, sizeof(rbuf) - 1);
	if (n_read == -1) {
		die_errno("failed to read response");
	}
	rbuf[n_read] = 0;
	sleep(1);

	printf("from server: %s\n", rbuf);

	n_read = read_one_message(fd, rbuf, sizeof(rbuf) - 1);
	if (n_read == -1) {
		die_errno("failed to read response");
	}
	rbuf[n_read] = 0;
	sleep(1);

	printf("from server: %s\n", rbuf);
	sleep(1);

	int res = close(fd);
	if (res == -1) {
		die_errno("failed to close connection");
	}
	fprintf(stderr, "closed connection [%d]\n", fd);

	return 0;
}
