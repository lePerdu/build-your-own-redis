#ifndef QUEUE_H_
#define QUEUE_H_

#include <stdbool.h>
#include <stdint.h>
#include <threads.h>

struct work_task {
  void (*callback)(void *arg);
  void *arg;
};

/** Blocking work queue. */
struct work_queue {
  mtx_t lock;
  cnd_t not_empty;

  uint32_t head;
  uint32_t size;
  uint32_t cap;
  struct work_task *data;
};

void work_queue_init(struct work_queue *queue);

static inline bool work_queue_empty(const struct work_queue *queue) {
  return queue->size == 0;
}

void work_queue_push(struct work_queue *queue, struct work_task task);
/**
 * Wait until there is at least 1 item in the queue, then remove and return the
 * first item
 */
struct work_task work_queue_pop(struct work_queue *queue);

#endif
