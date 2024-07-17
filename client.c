#include <netinet/in.h>
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

	if (argc == 2) {
		msg = argv[1];
	} else {
		msg = "hello";
	}

	n_write = write_one_message(fd, msg, strlen(msg));
	if (n_write == -1) {
		die_errno("failed to write message");
	}

	msg = "]]";
	n_write = write_one_message(fd, msg, strlen(msg));
	if (n_write == -1) {
		die_errno("failed to write message");
	}

	char rbuf[PROTO_MAX_MESSAGE_SIZE + 1];
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

	printf("from server: %s\n", rbuf);

	n_read = read_one_message(fd, rbuf, sizeof(rbuf) - 1);
	if (n_read == -1) {
		die_errno("failed to read response");
	}
	rbuf[n_read] = 0;

	printf("from server: %s\n", rbuf);

	int res = close(fd);
	if (res == -1) {
		die_errno("failed to close connection");
	}
	fprintf(stderr, "closed connection [%d]\n", fd);

	return 0;
}
