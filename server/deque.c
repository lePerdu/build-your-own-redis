#include "deque.h"

#include <stdlib.h>

void deque_init(struct deque *queue) {
  queue->head.prev = queue->head.next = &queue->head;
}

void deque_destroy(struct deque *queue) {
  // No-op for now since entries are externally-managed
  (void)queue;
}

bool deque_empty(const struct deque *queue) {
  return queue->head.next == &queue->head;
}

void deque_push_back(struct deque *queue, struct deque_node *item) {
  struct deque_node *prev = queue->head.prev;
  prev->next = item;
  item->prev = prev;

  item->next = &queue->head;
  queue->head.prev = item;
}

struct deque_node *deque_peek_front(struct deque *queue) {
  if (deque_empty(queue)) {
    return NULL;
  }

  return queue->head.next;
}

struct deque_node *deque_pop_front(struct deque *queue) {
  if (deque_empty(queue)) {
    return NULL;
  }

  struct deque_node *popped = queue->head.next;
  deque_detach(queue, popped);
  return popped;
}

void deque_detach(struct deque *queue, struct deque_node *item) {
  (void)queue;
  item->next->prev = item->prev;
  item->prev->next = item->next;
}
