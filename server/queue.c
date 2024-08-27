#include "queue.h"

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <threads.h>

enum {
  QUEUE_INIT_CAP = 8,
};

void work_queue_init(struct work_queue *queue) {
  int res = mtx_init(&queue->lock, mtx_plain);
  assert(res == thrd_success);
  res = cnd_init(&queue->not_empty);
  assert(res == thrd_success);

  queue->head = 0;
  queue->size = 0;
  queue->cap = QUEUE_INIT_CAP;
  queue->data = malloc(sizeof(queue->data[0]) * QUEUE_INIT_CAP);
  assert(queue->data != NULL);
}

void work_queue_push(struct work_queue *queue, struct work_task task) {
  mtx_lock(&queue->lock);

  if (queue->head > 0 && queue->head + queue->size == queue->cap) {
    memmove(queue->data, &queue->data[queue->head], queue->size);
    queue->head = 0;
  } else if (queue->size == queue->cap) {
    // Can't use re-alloc since the data will be moved inside the allocation
    uint32_t new_cap = queue->cap * 2;
    struct work_task *new_data = malloc(sizeof(new_data[0]) * new_cap);
    assert(new_data != NULL);

    memcpy(new_data, queue->data, sizeof(queue->data[0]) * queue->cap);
    free(queue->data);
    queue->data = new_data;
    queue->cap = new_cap;
    queue->head = 0;
  }

  queue->data[queue->head + queue->size] = task;
  queue->size++;

  mtx_unlock(&queue->lock);
  cnd_signal(&queue->not_empty);
}

void work_queue_push_front(struct work_queue *queue, struct work_task task) {
  mtx_lock(&queue->lock);

  if (queue->head == 0) {
    if (queue->size < queue->cap) {
      memmove(&queue->data[1], &queue->data[0], queue->size);
    } else {
      // Can't use re-alloc since the data will be moved inside the allocation
      uint32_t new_cap = queue->cap * 2;
      struct work_task *new_data = malloc(sizeof(new_data[0]) * new_cap);
      assert(new_data != NULL);

      memcpy(&new_data[1], queue->data, sizeof(queue->data[0]) * queue->size);
      free(queue->data);
      queue->data = new_data;
      queue->cap = new_cap;
    }
    queue->head = 1;
  }

  queue->head--;
  queue->size++;
  queue->data[queue->head] = task;

  mtx_unlock(&queue->lock);
  cnd_signal(&queue->not_empty);
}

struct work_task work_queue_pop(struct work_queue *queue) {
  mtx_lock(&queue->lock);

  while (work_queue_empty(queue)) {
    cnd_wait(&queue->not_empty, &queue->lock);
  }

  struct work_task popped = queue->data[queue->head];
  queue->size--;
  if (queue->size == 0) {
    queue->head = 0;
  } else {
    queue->head++;
  }
  mtx_unlock(&queue->lock);

  return popped;
}
