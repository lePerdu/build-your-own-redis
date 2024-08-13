#include "heap.h"

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

enum {
  HEAP_INIT_CAP = 8,
};

void heap_init(struct heap *heap) {
  heap->size = 0;
  heap->cap = HEAP_INIT_CAP;
  heap->data = malloc(sizeof(heap->data[0]) * heap->cap);
}

void heap_destroy(struct heap *heap) {
  free(heap->data);
}

static inline uint32_t heap_parent(uint32_t index) {
  assert(index > 0);
  return (index + 1) / 2 - 1;
}

static inline uint32_t heap_left(uint32_t index) { return index * 2 + 1; }

static inline uint32_t heap_right(uint32_t index) { return index * 2 + 2; }

static void heap_fix_up(struct heap *heap, uint32_t index) {
  while (index > 0) {
    uint32_t parent = heap_parent(index);
    if (heap->data[index].value >= heap->data[parent].value) {
      break;
    }

    // Swap with parent
    struct heap_node tmp = heap->data[parent];
    heap->data[parent] = heap->data[index];
    heap->data[index] = tmp;
    heap->data[parent].backref->index = parent;
    // TODO: Wait until the end to update the backref for the new node
    heap->data[index].backref->index = index;

    index = parent;
  }
}

static void heap_fix_down(struct heap *heap, uint32_t index) {
  while (true) {
    uint32_t left = heap_left(index);
    uint32_t right = heap_right(index);

    // Start with -1 since one of the children might not exist
    int64_t min_child = -1;
    uint64_t min_value = heap->data[index].value;

    if (left < heap->size && heap->data[left].value < min_value) {
      min_child = left;
      min_value = heap->data[left].value;
    }

    if (right < heap->size && heap->data[right].value < min_value) {
      min_child = right;
      min_value = heap->data[right].value;
    }

    if (min_child == -1) {
      break;
    }

    struct heap_node tmp = heap->data[index];
    heap->data[index] = heap->data[min_child];
    heap->data[min_child] = tmp;
    // TODO: Wait until the end to update the backref for the new node
    heap->data[index].backref->index = index;
    heap->data[min_child].backref->index = min_child;

    index = min_child;
  }
}

void heap_insert(struct heap *heap, uint64_t value, struct heap_ref *backref) {
  if (heap->size == heap->cap) {
    heap->cap *= 2;
    struct heap_node *new_data =
        realloc(heap->data, sizeof(heap->data[0]) * heap->cap);
    assert(new_data != NULL);
    heap->data = new_data;
  }

  heap->data[heap->size] = (struct heap_node){
      .value = value,
      .backref = backref,
  };
  backref->index = heap->size;
  heap->size++;

  heap_fix_up(heap, heap->size - 1);
}

void heap_update(struct heap *heap, uint32_t index, uint64_t new_value) {
  assert(index < heap->size);
  heap->data[index].value = new_value;

  if (index > 0 && new_value < heap->data[heap_parent(index)].value) {
    heap_fix_up(heap, index);
  } else {
    heap_fix_down(heap, index);
  }
}

struct heap_node heap_pop(struct heap *heap, uint32_t index) {
  assert(index < heap->size);
  struct heap_node popped = heap->data[index];
  if (index == heap->size - 1) {
    heap->size--;
    return popped;
  }

  heap->data[index] = heap->data[heap->size - 1];
  heap->data[index].backref->index = index;
  heap->size--;
  heap_fix_down(heap, index);
  return popped;
}
