#include <bits/types/struct_iovec.h>
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <liburing.h>

#include "protocol.h"

#define MAX_CONNS 20
#define QUEUE_DEPTH (MAX_CONNS + 1)

enum conn_state {
	CONN_READ_REQ,
	CONN_WAIT_READ,
	CONN_PROCESS_REQ,
	CONN_WRITE_RES,
	CONN_WAIT_WRITE,
	CONN_CLOSE,
	CONN_WAIT_CLOSE,
	CONN_END,
};

struct conn {
	int fd;
	enum conn_state state;
	struct read_buf read_buf;
	struct write_buf write_buf;
};

struct server_state {
	int socket_fd;
	struct io_uring uring;
	unsigned active_conn_count;
	// TODO: better data structure for faster searching
	struct conn active_connections[MAX_CONNS];
};

static void conn_init(struct conn *c, int fd) {
	c->fd = fd;
	c->state = CONN_READ_REQ;
	read_buf_init(&c->read_buf);
	write_buf_init(&c->write_buf);
}

[[noreturn]] static void die_errno(const char *msg) {
	perror(msg);
	exit(EXIT_FAILURE);
}

// TODO Return error to caller instead of dying?
static int setup_socket(void) {
	// result variable used for various syscalls
	int res;

	int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
	if (fd == -1) {
		die_errno("failed to open socket");
	}

	int val = 1;
	res = setsockopt(
		fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)
	);
	if (res == -1) {
		die_errno("failed to configure socket");
	}

	struct sockaddr_in addr = {
		.sin_family = AF_INET,
		.sin_port = ntohs(1234),
		.sin_addr = {ntohl(INADDR_LOOPBACK)},
	};
	res = bind(fd, (const struct sockaddr *) &addr, sizeof(addr));
	if (res == -1) {
		die_errno("failed to bind socket");
	}

	res = listen(fd, SOMAXCONN);
	if (res == -1) {
		die_errno("failed to listen on socket");
	}

	struct sockaddr_in bound_addr;
	socklen_t bound_addr_size = sizeof(bound_addr);
	res = getsockname(fd, (struct sockaddr *) &bound_addr, &bound_addr_size);
	if (res == 0 && bound_addr.sin_family == AF_INET) {
		char addr_name[INET_ADDRSTRLEN];
		if (inet_ntop(AF_INET, &bound_addr.sin_addr, addr_name, sizeof(addr_name)) == NULL) {
			perror("failed to get bound address name");
		}

		unsigned short addr_port = ntohs(bound_addr.sin_port);
		fprintf(stderr, "listening on %s:%d\n", addr_name, addr_port);
	} else {
		fprintf(stderr, "listening on unknown address\n");
	}

	return fd;
}

static void server_state_init(struct server_state *s) {
	s->socket_fd = setup_socket();
	s->active_conn_count = 0;
	for (int i = 0; i < MAX_CONNS; i++) {
		s->active_connections[i].fd = -1;
		s->active_connections[i].state = CONN_END;
	}
	io_uring_queue_init(QUEUE_DEPTH, &s->uring, 0);
}

static struct conn *find_available_conn(struct server_state *s) {
	for (int i = 0; i < MAX_CONNS; i++) {
		if (s->active_connections[i].fd == -1) {
			return &s->active_connections[i];
		}
	}
	return NULL;
}

static void submit_accept_req(struct server_state *s) {
	struct io_uring_sqe *sqe = io_uring_get_sqe(&s->uring);

	io_uring_prep_accept(sqe, s->socket_fd, NULL, NULL, 0);
	io_uring_sqe_set_data(sqe, NULL);

	// TODO Wait until all processing is done before submitting events
	int res = io_uring_submit(&s->uring);
	if (res < 0) {
		errno = -res;
		die_errno("failed to setup accept");
	}
}

static void handle_read_req(struct server_state *s, struct conn *conn) {
	struct io_uring_sqe *sqe = io_uring_get_sqe(&s->uring);

	struct read_buf *rb = &conn->read_buf;
	// TODO: Better heuristic for when to move data
	read_buf_reset_start(rb);
	size_t cap = read_buf_cap(rb);
	assert(cap > 0);
	io_uring_prep_read(
		sqe, conn->fd, read_buf_read_pos(rb), cap, 0
	);
	io_uring_sqe_set_data(sqe, conn);

	// TODO Wait until all processing is done before submitting events
	int res = io_uring_submit(&s->uring);
	if (res < 0) {
		errno = -res;
		perror("failed to submit read request");
		conn->state = CONN_CLOSE;
		return;
	}

	conn->state = CONN_WAIT_READ;
}

static void handle_check_read(struct conn *conn, int res) {
	if (res < 0) {
		fprintf(
			stderr,
			"failed to read from stream [%d]: %s\n",
			conn->fd,
			strerror(-res)
		);
		conn->state = CONN_CLOSE;
		return;
	} else if (res == 0) {
		if (read_buf_size(&conn->read_buf) == 0) {
			fprintf(stderr, "end of stream [%d]\n", conn->fd);
		} else  {
			fprintf(
				stderr, "end of stream with partial message [%d]\n", conn->fd
			);
		}

		conn->state = CONN_CLOSE;
		return;
	}

	read_buf_inc_size(&conn->read_buf, res);
	conn->state = CONN_PROCESS_REQ;
}

static void handle_process_req(struct conn *conn) {
	uint8_t msg_buf[PROTO_MAX_PAYLOAD_SIZE + 1];
	ssize_t msg_size = read_buf_parse(&conn->read_buf, msg_buf);
	switch (msg_size) {
		case PARSE_ERR:
			fprintf(stderr, "invalid message\n");
			conn->state = CONN_CLOSE;
			return;
		case PARSE_MORE:
			conn->state= CONN_READ_REQ;
			return;
	}

	assert(msg_size >= 0);
	msg_buf[msg_size] = 0;
	printf("from client [%d]: %s\n", conn->fd, msg_buf);

	write_buf_set_message(&conn->write_buf, msg_buf, msg_size);

	conn->state = CONN_WRITE_RES;
}

static void handle_write_res(struct server_state *s, struct conn *conn) {
	struct io_uring_sqe *sqe = io_uring_get_sqe(&s->uring);

	struct write_buf *wb = &conn->write_buf;
	size_t remaining = write_buf_remaining(wb);

	io_uring_prep_write(
		sqe,
		conn->fd,
		write_buf_write_pos(wb),
		remaining,
		0
	);
	io_uring_sqe_set_data(sqe, conn);

	// TODO Wait until all processing is done before submitting events
	int res = io_uring_submit(&s->uring);
	if (res < 0) {
		errno = -res;
		perror("failed to submit write request");
		conn->state = CONN_CLOSE;
		return;
	}

	conn->state = CONN_WAIT_WRITE;
}

static void handle_check_write( struct conn *conn, int res) {
	if (res < 0) {
		fprintf(
			stderr,
			"failed to write to stream [%d]: %s\n",
			conn->fd,
			strerror(-res)
		);
		conn->state = CONN_CLOSE;
		return;
	} else if (res == 0) {
		// TODO: Can this happen?
		fprintf(
			stderr,
			"failed to write to stream [%d]: end of stream\n",
			conn->fd
		);
		conn->state = CONN_CLOSE;
		return;
	}

	struct write_buf *wb = &conn->write_buf;
	write_buf_advance(wb, res);
	size_t remaining = write_buf_remaining(wb);
	if (remaining == 0) {
		write_buf_reset(wb);
		conn->state = CONN_PROCESS_REQ;
	} else {
		conn->state = CONN_WRITE_RES;
	}
}

static void handle_close(struct server_state *s, struct conn *conn) {
	struct io_uring_sqe *sqe = io_uring_get_sqe(&s->uring);

	io_uring_prep_close(sqe, conn->fd);
	io_uring_sqe_set_data(sqe, conn);

	// TODO Wait until all processing is done before submitting events
	int res = io_uring_submit(&s->uring);
	if (res < 0) {
		errno = -res;
		perror("failed to submit close request");
		// Nothing to do but "forget" the connection?
		conn->state = CONN_END;
		return;
	}

	conn->state = CONN_WAIT_CLOSE;
}

static void handle_check_close(struct conn *conn, int res) {
	if (res < 0) {
		errno = -res;
		perror("failed to close stream");
		// Nothing to do but "forget" the connection?
	} else {
		fprintf(stderr, "closed connection [%d]\n", conn->fd);
	}

	conn->state = CONN_END;
}

static void handle_end(struct server_state *s, struct conn *conn) {
	// Mark for re-use
	conn->fd = -1;

	if (s->active_conn_count == MAX_CONNS) {
		submit_accept_req(s);
	}
	s->active_conn_count--;
}

static void run_conn_state_machine(struct server_state *s, struct conn *conn) {
	while (true) {
		switch (conn->state) {
		case CONN_WAIT_READ:
		case CONN_WAIT_WRITE:
		case CONN_WAIT_CLOSE:
			// Pause execution until data is available
			return;
		case CONN_READ_REQ:
			handle_read_req(s, conn);
			break;
		case CONN_PROCESS_REQ:
			handle_process_req(conn);
			break;
		case CONN_WRITE_RES:
			handle_write_res(s, conn);
			break;
		case CONN_CLOSE:
			handle_close(s, conn);
			break;
		case CONN_END:
			handle_end(s, conn);
			return;
		default:
			fprintf(stderr, "Invalid state: %d\n", conn->state);
			handle_close(s, conn);
			return;
		}
	}
}

static void handle_data_available(struct server_state *s, struct conn *conn, int res) {
	// Reset wait states from poll
	switch (conn->state) {
	case CONN_WAIT_READ:
		handle_check_read(conn, res);
		break;
	case CONN_WAIT_WRITE:
		handle_check_write(conn, res);
		break;
	case CONN_WAIT_CLOSE:
		handle_check_close(conn, res);
		break;
	default:
		// Should only be in wait state
		assert(false);
	}

	run_conn_state_machine(s, conn);
}

static void handle_new_connection(struct server_state *s, int res) {
	if (res < 0) {
		fprintf(stderr, "failed to accept connection: %s\n", strerror(-res));
		return;
	}

	struct conn *new_conn = find_available_conn(s);
	if (new_conn == NULL) {
		fprintf(stderr, "too many active connections. Dropping.\n");
		/*close(res);*/
		return;
	}

	s->active_conn_count++;
	if (s->active_conn_count < MAX_CONNS) {
		submit_accept_req(s);
	}

	conn_init(new_conn, res);
	fprintf(stderr, "openned connection [%d]\n", new_conn->fd);

	run_conn_state_machine(s, new_conn);
}

static void handle_completion(struct server_state *s) {
	struct io_uring_cqe *cqe;
	int wait_res = io_uring_wait_cqe(&s->uring, &cqe);
	if (wait_res < 0) {
		die_errno("failed to get completion entry");
	}

	// Extract fields so the entry can be released
	int compl_res = cqe->res;
	void *user_data = io_uring_cqe_get_data(cqe);
	io_uring_cqe_seen(&s->uring, cqe);

	if (user_data == NULL) {
		handle_new_connection(s, compl_res);
	} else {
		handle_data_available(s, user_data, compl_res);
	}
}

int main(void) {
	struct server_state server;
	server_state_init(&server);

	submit_accept_req(&server);

	while (true) {
		handle_completion(&server);
	}

	return 0;
}
