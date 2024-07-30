#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "protocol.h"
#include "hashmap.h"

#define MAX_EVENTS 256

enum conn_state {
	CONN_WAIT_READ,
	CONN_READ_REQ,
	CONN_PROCESS_REQ,
	CONN_WAIT_WRITE,
	CONN_WRITE_RES,
	CONN_CLOSE,
};

struct conn {
	// Linked list pointers for managing connection pool
	struct conn *prev;
	struct conn *next;

	int fd;
	enum conn_state state;

	struct read_buf read_buf;
	struct write_buf write_buf;
};

struct store_entry {
	struct hash_entry entry;

	// Owned
	struct slice key;
	// Owned
	struct object val;
};

// Same memory layout, but with different const modifiers
struct store_key {
	struct hash_entry entry;
	struct const_slice key;
};

struct server_state {
	int socket_fd;
	int epoll_fd;

	struct hash_map store;

	// Free-list of connection objects
	struct conn *connection_pool;
	// active connection objects
	struct conn *active_connections;
};

static void conn_init(struct conn *c, int fd) {
	c->fd = fd;
	c->state = CONN_READ_REQ;
	read_buf_init(&c->read_buf);
	write_buf_init(&c->write_buf);
}

static void server_state_init(struct server_state *s, int socket_fd, int epoll_fd) {
	s->socket_fd = socket_fd;
	s->epoll_fd = epoll_fd;
	hash_map_init(&s->store, 16);
	s->connection_pool = NULL;
	s->active_connections = NULL;
}

static struct conn *get_available_conn(struct server_state *s) {
	struct conn *available = s->connection_pool;
	if (available != NULL) {
		// prev pointers aren't used in the connection pool
		s->connection_pool = available->next;
	} else {
		available = malloc(sizeof(*available));
	}

	available->prev = NULL;
	available->next = s->active_connections;
	s->active_connections = available;
	if (available->next != NULL) {
		available->next->prev = available;
	}
	return available;
}

static void free_conn(struct server_state *s, struct conn *c) {
	if (c->prev != NULL) {
		c->prev->next = c->next;
	} else {
		s->active_connections = c->next;
	}

	if (c->next != NULL) {
		c->next->prev = c->prev;
	}

	c->next = s->connection_pool;
	s->connection_pool = c;
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

enum read_result {
	READ_OK = 0,
	READ_IO_ERR = -1,
	READ_EOF = -3,
	READ_MORE = -4,
};

/**
 * Fill read buffer with data from `fd`.
 *
 * The full buffer capacity is requested from `fd`, although this may read less
 * than the full capacity.
 */
static enum read_result read_buf_fill(int fd, struct read_buf *r) {
	// Make room for full message.
	// TODO: Better heuristic for when to move data in buffer?
	read_buf_reset_start(r);

	size_t cap = read_buf_cap(r);
	// TODO: Are there valid scenarios where this is desired?
	assert(cap > 0);

	ssize_t n_read;
	// Repeat for EINTR
	do {
		n_read = recv(fd, read_buf_read_pos(r), cap, 0);
	} while (n_read == -1 && errno == EINTR);

	if (n_read == -1) {
		if (errno == EAGAIN) {
			return READ_MORE;
		} else {
			return READ_IO_ERR;
		}
	} else if (n_read == 0) {
		return READ_EOF;
	} else {
		assert(n_read > 0);
		read_buf_inc_size(r, n_read);
		return READ_OK;
	}
}

enum send_result {
	SEND_OK = 0,
	SEND_IO_ERR = -1,
	SEND_MORE = -2,
};

static enum send_result write_buf_flush(int fd, struct write_buf *w) {
	size_t remaining = write_buf_remaining(w);
	assert(remaining > 0);

	ssize_t n_write;
	do {
		n_write = send(fd, write_buf_write_pos(w), remaining, MSG_NOSIGNAL);
	} while (n_write == -1 && errno == EINTR);

	if (n_write == -1) {
		if (errno == EAGAIN) {
			return SEND_MORE;
		} else {
			return SEND_IO_ERR;
		}
	}

	assert(n_write > 0);
	write_buf_advance(w, n_write);
	remaining = write_buf_remaining(w);
	if (remaining == 0) {
		write_buf_reset(w);
		return SEND_OK;
	} else {
		return SEND_MORE;
	}
}

static void handle_read_req(struct conn *conn) {
	enum read_result res = read_buf_fill(conn->fd, &conn->read_buf);
	switch (res) {
		case READ_OK:
			conn->state = CONN_PROCESS_REQ;
			break;
		case READ_IO_ERR:
			perror("failed to read from socket");
			conn->state = CONN_CLOSE;
			break;
		case READ_EOF:
			fprintf(stderr, "socket EOF [%d]\n", conn->fd);
			conn->state= CONN_CLOSE;
			break;
		case READ_MORE:
			conn->state = CONN_WAIT_READ;
			break;
		default:
			assert(false);
	}
}

static hash_t slice_hash(struct const_slice s) {
	const uint8_t *data = s.data;
    hash_t h = 0x811C9DC5;
    for (size_t i = 0; i < s.size; i++) {
        h = (h + data[i]) * 0x01000193;
    }
    return h;
}

static bool store_ent_compare(
	const struct hash_entry *raw_a, const struct hash_entry *raw_b
) {
  const struct store_entry *a = container_of(raw_a, struct store_entry, entry);
  const struct store_entry *b = container_of(raw_b, struct store_entry, entry);

  return (a->key.size == b->key.size &&
          memcmp(a->key.data, b->key.data, a->key.size) == 0);
}

static void do_get(
	struct hash_map *store,
	const struct object args[1],
	struct response *res
) {
	struct const_slice key;
	if (!object_to_slice(&key, args[0])) {
		res->type = RES_ERR;
		res->arg = make_str_object("invalid key");
		return;
	}

	struct store_key store_key = {
		.entry.hash_code = slice_hash(key),
		.key = key,
	};

	struct store_entry *entry = (void *)hash_map_get(
		store, (void*)&store_key, store_ent_compare
	);
	if (entry == NULL) {
		res->type = RES_ERR;
		res->arg = make_str_object("not found");
		return;
	}

	res->type = RES_OK;
	res->arg = object_to_ref(entry->val);
}

static struct slice slice_dup(struct const_slice s) {
	void *copy = malloc(s.size);
	memcpy(copy, s.data, s.size);
	return make_slice(copy, s.size);
}

static struct store_entry *store_entry_alloc(
	struct const_slice key, struct object val
) {
	struct store_entry *new = malloc(sizeof(*new));
	assert(new != NULL);
	new->entry.hash_code = slice_hash(key);
	new->key = slice_dup(key);
	new->val = object_to_owned(val);
	return new;
}

static void store_entry_free(struct store_entry *ent) {
	free(ent->key.data);
	object_destroy(ent->val);
	free(ent);
}

static void do_set(
	struct hash_map *store,
	const struct object args[2],
	struct response *res
) {
	struct const_slice key;
	if (!object_to_slice(&key, args[0])) {
		res->type = RES_ERR;
		res->arg = make_str_object("invalid key");
		return;
	}

	struct object val = args[1];

	// TODO: Re-structure hashmap API to avoid double hashing when inserting?
	struct store_key store_key = {
		.entry.hash_code = slice_hash(key),
		.key = key,
	};

	struct store_entry *existing = (void *)hash_map_get(
		store, (void*)&store_key, store_ent_compare
	);
	if (existing == NULL) {
		struct store_entry *new_ent = store_entry_alloc(key, val);
		hash_map_insert(store, (void *)new_ent);
	} else {
		object_destroy(existing->val);
		existing->val = val;
	}

	res->type = RES_OK;
	res->arg = make_nil_object();
}

static void do_del(
	struct hash_map *store,
	const struct object args[1],
	struct response *res
) {
	struct const_slice key;
	if (!object_to_slice(&key, args[0])) {
		res->type = RES_ERR;
		res->arg = make_str_object("invalid key");
		return;
	}

	struct store_key store_key = {
		.entry.hash_code = slice_hash(key),
		.key = key,
	};

	struct store_entry *entry = (void *)hash_map_delete(
		store, (void*)&store_key, store_ent_compare
	);
	if (entry == NULL) {
		res->type = RES_ERR;
		res->arg = make_str_object("not found");
		return;
	}
	store_entry_free(entry);

	res->type = RES_OK;
	res->arg = make_nil_object();
}

struct appender_context {
	size_t added;
	struct object *arr;
};

static void append_key_to_response(struct hash_entry *raw_entry, void *arg) {
	struct appender_context *ctx = arg;
	struct store_entry *entry =
		container_of(raw_entry, struct store_entry, entry);

	assert(ctx->added < ctx->arr->arr_val.size);

	ctx->arr->arr_val.data[ctx->added] =
		make_slice_object(to_const_slice(entry->key));
	ctx->added++;
}

static void do_keys(
	struct hash_map *store,
	struct response *res
) {
	res->type = RES_OK;
	res->arg = make_arr_object(store->ht.size);
	struct appender_context ctx = {.added = 0, .arr = &res->arg};

	hash_map_iter(store, append_key_to_response, &ctx);
}

static int do_request(
	struct conn *conn, struct hash_map *store, const struct request *req
) {
	fprintf(stderr, "from client [%d]: ", conn->fd);
	print_request(stderr, req);
	putc('\n', stderr);

	struct response res;
	switch (req->type) {
        case REQ_GET:
			do_get(store, req->args, &res);
			break;
        case REQ_SET:
			do_set(store, req->args, &res);
			break;
        case REQ_DEL:
			do_del(store, req->args, &res);
			break;
		case REQ_KEYS:
			do_keys(store, &res);
			break;
		default:
			assert(false);
	}

	fprintf(stderr, "to client [%d]: ", conn->fd);
	print_response(stderr, &res);
	putc('\n', stderr);

	ssize_t res_size = write_response(write_buf_tail_slice(&conn->write_buf), &res);
	if (res_size < 0) {
		fprintf(stderr, "failed to write response to buffer");
		return -1;
	}
	write_buf_inc_size(&conn->write_buf, res_size);
	// Cleanup in case it allocated
	// TODO: Remove this and avoid allocations in response processing?
	object_destroy(res.arg);

	return 0;
}

static void handle_process_req(struct conn *conn, struct hash_map *store) {
	struct request req;
	ssize_t msg_size = parse_request(&req, read_buf_head_slice(&conn->read_buf));
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
	read_buf_advance(&conn->read_buf, msg_size);

	int res = do_request(conn, store, &req);
	if (res < 0) {
		conn->state = CONN_CLOSE;
	}
	conn->state = CONN_WRITE_RES;
}

static void handle_write_res(struct conn *conn) {
	enum send_result res = write_buf_flush(conn->fd, &conn->write_buf);
	switch (res) {
		case SEND_OK:
			conn->state = CONN_PROCESS_REQ;
			break;
		case SEND_IO_ERR:
			fprintf(stderr, "failed to write message\n");
			conn->state = CONN_CLOSE;
			break;
		case SEND_MORE:
			conn->state = CONN_WAIT_WRITE;
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

	conn->fd = -1;
	free_conn(s, conn);
}

static void handle_data_available(struct server_state *s, struct conn *conn) {
	// Reset wait states from poll
	if (conn->state == CONN_WAIT_READ) {
		conn->state = CONN_READ_REQ;
	} else if (conn->state == CONN_WAIT_WRITE) {
		conn->state = CONN_WRITE_RES;
	}

	while (true) {
		switch (conn->state) {
		case CONN_WAIT_READ:
		case CONN_WAIT_WRITE:
			return;
		case CONN_READ_REQ:
			handle_read_req(conn);
			break;
		case CONN_PROCESS_REQ:
			handle_process_req(conn, &s->store);
			break;
		case CONN_WRITE_RES:
			handle_write_res(conn);
			break;
		case CONN_CLOSE:
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

	struct conn *new_conn = get_available_conn(s);
	conn_init(new_conn, conn_fd);

	struct epoll_event ev = {
		.events = EPOLLIN | EPOLLOUT | EPOLLET,
		.data.ptr = new_conn,
	};
	int res = epoll_ctl(s->epoll_fd, EPOLL_CTL_ADD, conn_fd, &ev);
	if (res == -1) {
		perror("failed to add connection to epoll group");
		handle_end(s, new_conn);
		return;
	}

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
	ev.events = EPOLLIN | EPOLLET;
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
