#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "heap.h"
#include "test.h"

// NOLINTBEGIN(readability-magic-numbers)

static void test_heap_empty_at_init(void) {
  struct heap heap;
  heap_init(&heap);
  assert(heap_empty(&heap));
  heap_destroy(&heap);
}

static void test_heap_pop_min_same_as_single_insert(void) {
  struct heap heap;
  heap_init(&heap);
  struct heap_ref ref;
  heap_insert(&heap, 5, &ref);
  assert(!heap_empty(&heap));
  struct heap_node popped = heap_pop_min(&heap);
  assert(popped.value == 5);
  assert(popped.backref == &ref);
  heap_destroy(&heap);
}

static void verify_heap_refs(
    struct heap *heap, size_t size, struct heap_ref refs[size]) {
  for (size_t i = 0; i < size; i++) {
    uint32_t index = refs[i].index;
    assert(index < heap->size);
    assert(heap->data[index].backref == &refs[i]);
  }
}

static void verify_full_heap(
    struct heap *heap, size_t size, struct heap_ref refs[size]) {
  verify_heap_refs(heap, size, refs);

  for (size_t val = 1; val <= size; val++) {
    assert(!heap_empty(heap));
    struct heap_node popped = heap_pop_min(heap);
    assert(popped.value == val);
  }

  assert(heap_empty(heap));
}

static void verify_heap_ordered(
    struct heap *heap, size_t size, struct heap_ref refs[size]) {
  verify_heap_refs(heap, size, refs);

  uint64_t last_val = 0;
  for (size_t i = 0; i < size; i++) {
    assert(!heap_empty(heap));
    struct heap_node popped = heap_pop_min(heap);
    assert(popped.value >= last_val);
    last_val = popped.value;
  }

  assert(heap_empty(heap));
}

/** Fill array with 1..size in random order */
static void generate_seq(uint32_t size, uint64_t arr[size]) {
  for (uint32_t i = 1; i <= size; i++) {
    arr[i - 1] = i;
  }

  for (uint32_t i = 1; i < size; i++) {
    uint32_t remaining_count = size - i;
    uint32_t swap_index = rand() % remaining_count;

    uint64_t tmp = arr[i - 1];
    arr[i - 1] = arr[swap_index];
    arr[swap_index] = tmp;
  }
}

static void build_full_heap(
    struct heap *heap, size_t size, struct heap_ref refs[size]) {
  heap_init(heap);

  uint64_t seq[size];
  generate_seq(size, seq);
  for (uint32_t i = 0; i < size; i++) {
    heap_insert(heap, seq[i], &refs[i]);
  }
}

enum {
  TEST_HEAP_RAND_SEED = 42,
  TEST_HEAP_RAND_MAX_SIZE = 20,
  TEST_HEAP_RAND_REPEAT = 5,
};

static void test_heap_random_order_inserts(void) {
  srand(TEST_HEAP_RAND_SEED);

  for (int i = 1; i <= TEST_HEAP_RAND_MAX_SIZE; i++) {
    for (int j = 0; j < TEST_HEAP_RAND_REPEAT; j++) {
      struct heap heap;
      struct heap_ref refs[i];
      build_full_heap(&heap, i, refs);
      verify_full_heap(&heap, i, refs);
      heap_destroy(&heap);
    }
  }
}

enum {
  TEST_HEAP_RAND_INSERT_COUNT = 100,
  TEST_HEAP_RAND_INSERT_MAX = 1000,
};

static void test_heap_random_inserts(void) {
  srand(TEST_HEAP_RAND_SEED);

  struct heap heap;
  heap_init(&heap);
  struct heap_ref refs[TEST_HEAP_RAND_INSERT_COUNT];
  for (int i = 0; i < TEST_HEAP_RAND_INSERT_COUNT; i++) {
    uint64_t val = rand() % TEST_HEAP_RAND_INSERT_MAX;
    heap_insert(&heap, val, &refs[i]);
  }

  verify_heap_ordered(&heap, TEST_HEAP_RAND_INSERT_COUNT, refs);
  heap_destroy(&heap);
}

// NOLINTEND(readability-magic-numbers)

void test_heap(void) {
  RUN_TEST(test_heap_empty_at_init);
  RUN_TEST(test_heap_pop_min_same_as_single_insert);
  RUN_TEST(test_heap_random_order_inserts);
  RUN_TEST(test_heap_random_inserts);
}
