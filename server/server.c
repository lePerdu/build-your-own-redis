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
#include <sys/types.h>
#include <unistd.h>

#include "buffer.h"
#include "commands.h"
#include "protocol.h"
#include "store.h"

enum {
  PORT = 1234,

  MAX_EVENTS = 256,

  READ_BUF_INIT_CAP = 4096,
  // Minimum amount of space in the buffer before expanding
  READ_BUF_MIN_CAP = 4096,

  WRITE_BUF_INIT_CAP = 4096,
};

enum conn_state {
  CONN_WAIT_READ,
  CONN_READ_REQ,
  CONN_PROCESS_REQ,
  CONN_WAIT_WRITE,
  CONN_WRITE_RES,
  CONN_CLOSE,
};

struct req_parser {
  const struct command *cmd;
  struct req_object args[COMMAND_ARGS_MAX];
  int parsed_args;
};

struct conn {
  // Linked list pointers for managing connection pool
  struct conn *prev;
  struct conn *next;

  int fd;
  enum conn_state state;

  struct offset_buf read_buf;
  struct req_parser req_parser;

  struct offset_buf write_buf;
};

struct server_state {
  int socket_fd;
  int epoll_fd;

  struct store store;

  // Free-list of connection objects
  struct conn *connection_pool;
  // active connection objects
  struct conn *active_connections;
};

[[noreturn]] static void die_errno(const char *msg) {
  perror(msg);
  exit(EXIT_FAILURE);
}

static int set_nonblocking(int fildes) {
  int flags = fcntl(fildes, F_GETFL);
  if (flags == -1) {
    return -1;
  }

  flags = fcntl(fildes, F_SETFL, flags | O_NONBLOCK);
  if (flags == -1) {
    return -1;
  }

  return 0;
}

// TODO Return error to caller instead of dying?
static int setup_socket(void) {
  // result variable used for various syscalls
  int res;

  int socket_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (socket_fd == -1) {
    die_errno("failed to open socket");
  }

  int val = 1;
  res = setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
  if (res == -1) {
    die_errno("failed to configure socket");
  }

  struct sockaddr_in addr = {
      .sin_family = AF_INET,
      .sin_port = ntohs(PORT),
      .sin_addr = {ntohl(INADDR_LOOPBACK)},
  };
  res = bind(socket_fd, (const struct sockaddr *)&addr, sizeof(addr));
  if (res == -1) {
    die_errno("failed to bind socket");
  }

  res = listen(socket_fd, SOMAXCONN);
  if (res == -1) {
    die_errno("failed to listen on socket");
  }

  struct sockaddr_in bound_addr;
  socklen_t bound_addr_size = sizeof(bound_addr);
  res =
      getsockname(socket_fd, (struct sockaddr *)&bound_addr, &bound_addr_size);
  if (res == 0 && bound_addr.sin_family == AF_INET) {
    char addr_name[INET_ADDRSTRLEN];
    const char *addr_res =
        inet_ntop(AF_INET, &bound_addr.sin_addr, addr_name, sizeof(addr_name));
    if (addr_res == NULL) {
      perror("failed to get bound address name");
    }

    unsigned short addr_port = ntohs(bound_addr.sin_port);
    fprintf(stderr, "listening on %s:%d\n", addr_name, addr_port);
  } else {
    fprintf(stderr, "listening on unknown address\n");
  }

  return socket_fd;
}

static void req_parser_init(struct req_parser *parser) {
  parser->cmd = NULL;
  parser->parsed_args = 0;
}

static void conn_init(struct conn *conn, int fildes) {
  conn->fd = fildes;
  conn->state = CONN_READ_REQ;

  offset_buf_init(&conn->read_buf, READ_BUF_INIT_CAP);
  req_parser_init(&conn->req_parser);

  offset_buf_init(&conn->write_buf, WRITE_BUF_INIT_CAP);
}

static void server_state_setup(struct server_state *server) {
  server->socket_fd = setup_socket();

  server->epoll_fd = epoll_create1(0);
  if (server->epoll_fd == -1) {
    die_errno("failed to create epoll group");
  }

  // Setup listening socket
  struct epoll_event listen_event;
  listen_event.events = EPOLLIN | EPOLLET;
  listen_event.data.ptr = NULL;  // Tag to indicate the listening socket
  int res = epoll_ctl(
      server->epoll_fd, EPOLL_CTL_ADD, server->socket_fd, &listen_event);
  if (res == -1) {
    die_errno("failed to add socket to epoll group");
  }

  store_init(&server->store);
  server->connection_pool = NULL;
  server->active_connections = NULL;
}

static struct conn *get_available_conn(struct server_state *server) {
  struct conn *available = server->connection_pool;
  if (available != NULL) {
    // prev pointers aren't used in the connection pool
    server->connection_pool = available->next;
  } else {
    available = malloc(sizeof(*available));
  }

  available->prev = NULL;
  available->next = server->active_connections;
  server->active_connections = available;
  if (available->next != NULL) {
    available->next->prev = available;
  }
  return available;
}

static void free_conn(struct server_state *server, struct conn *conn) {
  if (conn->prev != NULL) {
    conn->prev->next = conn->next;
  } else {
    server->active_connections = conn->next;
  }

  if (conn->next != NULL) {
    conn->next->prev = conn->prev;
  }

  conn->next = server->connection_pool;
  server->connection_pool = conn;
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
static enum read_result read_buf_fill(
    int conn_fd, struct offset_buf *read_buf) {
  // Make room for full message.
  // TODO: Better heuristic for when to move data back
  offset_buf_reset_start(read_buf);

  // TODO: Better heuristic for when/how much to grow buffer
  uint32_t cap = offset_buf_cap(read_buf);
  if (cap < READ_BUF_MIN_CAP) {
    offset_buf_grow(read_buf, READ_BUF_MIN_CAP);
  }
  cap = offset_buf_cap(read_buf);
  // TODO: This could happen if the client sends a too-big message
  assert(cap >= READ_BUF_MIN_CAP);

  ssize_t n_read;
  // Repeat for EINTR
  do {
    n_read = recv(conn_fd, offset_buf_tail(read_buf), cap, 0);
  } while (n_read == -1 && errno == EINTR);

  if (n_read == -1) {
    if (errno == EAGAIN) {
      return READ_MORE;
    }
    return READ_IO_ERR;
  }

  if (n_read == 0) {
    return READ_EOF;
  }

  assert(n_read > 0);
  offset_buf_inc_size(read_buf, n_read);
  return READ_OK;
}

enum send_result {
  SEND_OK = 0,
  SEND_IO_ERR = -1,
  SEND_MORE = -2,
};

static enum send_result write_buf_flush(
    int conn_fd, struct offset_buf *write_buf) {
  size_t remaining = offset_buf_remaining(write_buf);
  assert(remaining > 0);

  ssize_t n_write;
  do {
    n_write =
        send(conn_fd, offset_buf_head(write_buf), remaining, MSG_NOSIGNAL);
  } while (n_write == -1 && errno == EINTR);

  if (n_write == -1) {
    if (errno == EAGAIN) {
      return SEND_MORE;
    }
    return SEND_IO_ERR;
  }

  assert(n_write > 0);
  offset_buf_advance(write_buf, n_write);
  remaining = offset_buf_remaining(write_buf);
  if (remaining == 0) {
    offset_buf_reset(write_buf);
    return SEND_OK;
  }
  return SEND_MORE;
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
      conn->state = CONN_CLOSE;
      break;
    case READ_MORE:
      conn->state = CONN_WAIT_READ;
      break;
    default:
      assert(false);
  }
}

static enum parse_result run_req_parser(struct conn *conn) {
  struct req_parser *parser = &conn->req_parser;

  ssize_t res;
  if (parser->cmd == NULL) {
    enum req_type type;
    res = parse_req_type(&type, offset_buf_head_slice(&conn->read_buf));
    if (res < 0) {
      return res;
    }

    parser->cmd = lookup_command(type);
    if (parser->cmd == NULL) {
      return PARSE_ERR;
    }
    parser->parsed_args = 0;

    offset_buf_advance(&conn->read_buf, res);
  }

  while (parser->parsed_args < parser->cmd->arg_count) {
    res = parse_req_object(
        &parser->args[parser->parsed_args],
        offset_buf_head_slice(&conn->read_buf));
    if (res < 0) {
      return res;
    }
    offset_buf_advance(&conn->read_buf, res);
    parser->parsed_args++;
  }

  return PARSE_OK;
}

static void reset_req_parser(struct req_parser *parser) {
  parser->cmd = NULL;
  for (int i = 0; i < parser->parsed_args; i++) {
    req_object_destroy(&parser->args[i]);
  }
  parser->parsed_args = 0;
}

static void handle_process_req(struct conn *conn, struct store *store) {
  enum parse_result parsed_size = run_req_parser(conn);
  switch (parsed_size) {
    case PARSE_ERR:
      fprintf(stderr, "invalid message\n");
      reset_req_parser(&conn->req_parser);
      conn->state = CONN_CLOSE;
      return;
    case PARSE_MORE:
      conn->state = CONN_READ_REQ;
      return;
    case PARSE_OK:
      // Continue on
      break;
    default:
      assert(false);
  }

  offset_buf_advance(&conn->read_buf, parsed_size);

  fprintf(stderr, "from client [%d]: ", conn->fd);
  print_request(stderr, conn->req_parser.cmd, conn->req_parser.args);
  fputc('\n', stderr);

  struct buffer *out_buf = &conn->write_buf.buf;
  conn->req_parser.cmd->handler(store, conn->req_parser.args, out_buf);
  reset_req_parser(&conn->req_parser);
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

static void handle_end(struct server_state *server, struct conn *conn) {
  int res = close(conn->fd);
  if (res == -1) {
    fprintf(stderr, "failed to close socket\n");
  }
  fprintf(stderr, "closed connected [%d]\n", conn->fd);

  conn->fd = -1;
  free_conn(server, conn);
}

static void handle_data_available(
    struct server_state *server, struct conn *conn) {
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
        handle_process_req(conn, &server->store);
        break;
      case CONN_WRITE_RES:
        handle_write_res(conn);
        break;
      case CONN_CLOSE:
        handle_end(server, conn);
        return;
      default:
        fprintf(stderr, "Invalid state: %d\n", conn->state);
        handle_end(server, conn);
        return;
    }
  }
}

static void handle_new_connection(struct server_state *server) {
  struct sockaddr_in client_addr;
  socklen_t client_addr_len = sizeof(client_addr);
  int conn_fd = accept(
      server->socket_fd, (struct sockaddr *)&client_addr, &client_addr_len);
  if (conn_fd == -1) {
    perror("failed to connect to client");
    return;
  }
  if (set_nonblocking(conn_fd) == -1) {
    perror("failed to set non-blocking");
    return;
  }

  struct conn *new_conn = get_available_conn(server);
  conn_init(new_conn, conn_fd);

  struct epoll_event conn_rw_event = {
      .events = EPOLLIN | EPOLLOUT | EPOLLET,
      .data.ptr = new_conn,
  };
  int res = epoll_ctl(server->epoll_fd, EPOLL_CTL_ADD, conn_fd, &conn_rw_event);
  if (res == -1) {
    perror("failed to add connection to epoll group");
    handle_end(server, new_conn);
    return;
  }

  fprintf(stderr, "openned connection [%d]\n", conn_fd);
  // Check immediately in case data is available
  handle_data_available(server, new_conn);
}

int main(void) {
  struct server_state server;
  server_state_setup(&server);

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
