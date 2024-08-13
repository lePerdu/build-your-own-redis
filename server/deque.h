#ifndef DEQUE_H_
#define DEQUE_H_

#include <stdbool.h>

struct deque_node {
  struct deque_node *prev;
  struct deque_node *next;
};

struct deque {
  struct deque_node head;
};

void deque_init(struct deque *queue);
void deque_destroy(struct deque *queue);

bool deque_empty(const struct deque *queue);
void deque_push_back(struct deque *queue, struct deque_node *item);
struct deque_node *deque_pop_front(struct deque *queue);
struct deque_node *deque_peek_front(struct deque *queue);
void deque_detach(struct deque *queue, struct deque_node *item);

#endif
