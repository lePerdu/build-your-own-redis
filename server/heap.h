#ifndef HEAP_H_
#define HEAP_H_

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>

struct heap_node {
  uint64_t value;
  struct heap_ref *backref;
};

struct heap_ref {
  uint32_t index;
};

struct heap {
  uint32_t size;
  uint32_t cap;
  struct heap_node *data;
};

void heap_init(struct heap *heap);
void heap_destroy(struct heap *heap);

static inline bool heap_empty(const struct heap *heap) { return heap->size == 0; }

void heap_insert(struct heap *heap, uint64_t value, struct heap_ref *backref);
void heap_update(struct heap *heap, uint32_t index, uint64_t new_value);

struct heap_node heap_pop(struct heap *heap, uint32_t index);

static inline struct heap_node heap_peek_min(struct heap *heap) {
  assert(!heap_empty(heap));
  return heap->data[0];
}

static inline struct heap_node heap_pop_min(struct heap *heap) {
  return heap_pop(heap, 0);
}

#endif
