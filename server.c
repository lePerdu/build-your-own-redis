#include <arpa/inet.h>
#include <assert.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "protocol.h"

#define MAX_CONNS 10
#define MAX_EVENTS 10

enum conn_state {
	CONN_WAIT_REQ,
	CONN_REQ,
	CONN_PROCESS_REQ,
	CONN_WAIT_RES,
	CONN_RES,
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
	int epoll_fd;
	// TODO: better data structure for faster searching
	struct conn active_connections[MAX_CONNS];
};

static void conn_init(struct conn *c, int fd) {
	c->fd = fd;
	c->state = CONN_REQ;
	read_buf_init(&c->read_buf);
	write_buf_init(&c->write_buf);
}

static void server_state_init(struct server_state *s, int socket_fd, int epoll_fd) {
	s->socket_fd = socket_fd;
	s->epoll_fd = epoll_fd;
	for (int i = 0; i < MAX_CONNS; i++) {
		s->active_connections[i].fd = -1;
	}
}

static struct conn *find_available_conn(struct server_state *s) {
	for (int i = 0; i < MAX_CONNS; i++) {
		if (s->active_connections[i].fd == -1) {
			return &s->active_connections[i];
		}
	}
	return NULL;
}

[[noreturn]] static void die_errno(const char *msg) {
	perror(msg);
	exit(EXIT_FAILURE);
}

static int set_nonblocking(int fd) {
	int flags = fcntl(fd, F_GETFL);
	if (flags == -1) {
		return -1;
	}

	flags = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
	if (flags == -1) {
		return -1;
	}

	return 0;
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

static void handle_read_req(struct conn *conn) {
	enum read_result res = read_buf_fill(conn->fd, &conn->read_buf);
	switch (res) {
		case READ_OK:
			conn->state = CONN_PROCESS_REQ;
			break;
		case READ_IO_ERR:
			perror("failed to read from socket");
			conn->state = CONN_END;
			break;
		case READ_EOF:
			fprintf(stderr, "socket EOF [%d]\n", conn->fd);
			conn->state= CONN_END;
			break;
		case READ_MORE:
			conn->state = CONN_WAIT_REQ;
			break;
		default:
			assert(false);
	}
}

static void handle_process_req(struct conn *conn) {
	uint8_t msg_buf[PROTO_MAX_MESSAGE_SIZE + 1];
	ssize_t msg_size = read_buf_parse(&conn->read_buf, msg_buf);
	switch (msg_size) {
		case PARSE_ERR:
			fprintf(stderr, "invalid message\n");
			conn->state = CONN_END;
			return;
		case PARSE_MORE:
			conn->state= CONN_REQ;
			return;
	}

	assert(msg_size >= 0);
	msg_buf[msg_size] = 0;
	printf("from client [%d]: %s\n", conn->fd, msg_buf);

	write_buf_set_message(&conn->write_buf, msg_buf, msg_size);

	conn->state = CONN_RES;
}

static void handle_write_res(struct conn *conn) {
	enum write_result res = write_buf_flush(conn->fd, &conn->write_buf);
	switch (res) {
		case WRITE_OK:
			conn->state = CONN_PROCESS_REQ;
			break;
		case WRITE_IO_ERR:
			fprintf(stderr, "failed to write message\n");
			conn->state = CONN_END;
			break;
		case WRITE_MORE:
			conn->state = CONN_WAIT_RES;
			break;
		default:
			assert(false);
	}
}

static void handle_end(struct server_state *s, struct conn *conn) {
	int res = close(conn->fd);
	if (res == -1) {
		fprintf(stderr, "failed to close socket\n");
	}
	fprintf(stderr, "closed connected [%d]\n", conn->fd);

	// Mark for re-use
	conn->fd = -1;

	// TODO: Only do this when required
	struct epoll_event ev = {
		.events = EPOLLIN,
		.data.ptr = NULL,
	};
	res = epoll_ctl(s->epoll_fd, EPOLL_CTL_MOD, s->socket_fd, &ev);
	if (res == -1) {
		perror("failed to re-enable listening socket to epoll group");
	}
}

static void handle_data_available(struct server_state *s, struct conn *conn) {
	// Reset wait states from poll
	if (conn->state == CONN_WAIT_REQ) {
		conn->state = CONN_REQ;
	} else if (conn->state == CONN_WAIT_RES) {
		conn->state = CONN_RES;
	}

	while (true) {
		switch (conn->state) {
		case CONN_WAIT_REQ:
		case CONN_WAIT_RES:
			return;
		case CONN_REQ:
			handle_read_req(conn);
			break;
		case CONN_PROCESS_REQ:
			handle_process_req(conn);
			break;
		case CONN_RES:
			handle_write_res(conn);
			break;
		case CONN_END:
			handle_end(s, conn);
			return;
		default:
			fprintf(stderr, "Invalid state: %d\n", conn->state);
			handle_end(s, conn);
			return;
		}
	}
}

static void handle_new_connection(struct server_state *s) {
	struct conn *new_conn = find_available_conn(s);
	if (new_conn == NULL) {
		fprintf(stderr, "too many active connections\n");
		// Hold off on new connections until a spot is available
		struct epoll_event ev = {
			.events = 0,
			.data.ptr = NULL,
		};
		int res = epoll_ctl(s->epoll_fd, EPOLL_CTL_MOD, s->socket_fd, &ev);
		if (res == -1) {
			perror("could not disable epoll for listening socket");
		}
		return;
	}

	struct sockaddr_in client_addr;
	socklen_t client_addr_len = sizeof(client_addr);
	int conn_fd = accept(
		s->socket_fd, (struct sockaddr *)&client_addr, &client_addr_len
	);
	if (conn_fd == -1) {
		perror("failed to connect to client");
		return;
	}
	if (set_nonblocking(conn_fd) == -1) {
		perror("failed to set non-blocking");
		return;
	}

	struct epoll_event ev = {
		.events = EPOLLIN | EPOLLOUT | EPOLLET,
		.data.ptr = new_conn,
	};
	int res = epoll_ctl(s->epoll_fd, EPOLL_CTL_ADD, conn_fd, &ev);
	if (res == -1) {
		perror("failed to add connection to epoll group");
		return;
	}

	conn_init(new_conn, conn_fd);
	fprintf(stderr, "openned connection [%d]\n", conn_fd);
	// Check immediately in case data is available
	handle_data_available(s, new_conn);
}

int main(void) {
	int socket_fd = setup_socket();

	int epoll_fd = epoll_create1(0);
	if (epoll_fd == -1) {
		die_errno("failed to create epoll group");
	}

	// Setup listening socket
	struct epoll_event ev;
	// Leave this event as level-triggered since some connections can't be accepted right away
	ev.events = EPOLLIN;
	ev.data.ptr = NULL; // Tag to indicate the listening socket
	int res = epoll_ctl(
		epoll_fd, EPOLL_CTL_ADD, socket_fd, &ev
	);
	if (res == -1) {
		die_errno("failed to add socket to epoll group");
	}

	struct server_state server;
	server_state_init(&server, socket_fd, epoll_fd);

	while (true) {
		struct epoll_event events[MAX_EVENTS];
		int n_events = epoll_wait(server.epoll_fd, events, MAX_EVENTS, -1);
		if (n_events == -1) {
			die_errno("failed to get epoll events");
		}

		for (int i = 0; i < n_events; i++) {
			if (events[i].data.ptr == NULL) {
				handle_new_connection(&server);
			} else {
				handle_data_available(&server, events[i].data.ptr);
			}
		}
	}

	return 0;
}
