#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "hashmap.h"
#include "test.h"
#include "types.h"

// NOLINTBEGIN(readability-magic-numbers)

struct test_node {
  struct hash_entry entry;
  int key;
  int val;
};

static bool test_node_cmp(
    const struct hash_entry *raw_a, const struct hash_entry *raw_b) {
  return (
      container_of(raw_a, struct test_node, entry)->key ==
      container_of(raw_b, struct test_node, entry)->key);
}

static void test_key_init(struct test_node *key_node, int key) {
  key_node->entry.hash_code = key;
  key_node->key = key;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static struct test_node *test_node_alloc(int key, int val) {
  struct test_node *node = malloc(sizeof(*node));
  test_key_init(node, key);
  node->val = val;
  return node;
}

static bool destroy_entry(struct hash_entry *raw_ent, void *arg) {
  (void)arg;
  struct test_node *ent = container_of(raw_ent, struct test_node, entry);
  free(ent);
  return true;
}

static void destroy(struct hash_map *map) {
  hash_map_iter(map, destroy_entry, NULL);
  hash_map_destroy(map);
}

static void test_hashmap_get_missing(void) {
  struct hash_map map;
  hash_map_init(&map, 8);
  struct test_node key;
  test_key_init(&key, 5);
  void *found = hash_map_get(&map, (void *)&key, test_node_cmp);
  assert(found == NULL);
  destroy(&map);
}

static void test_hashmap_get_after_insert(void) {
  struct hash_map map;
  hash_map_init(&map, 8);
  struct test_node *node = test_node_alloc(5, 10);
  hash_map_insert(&map, (void *)node);

  struct test_node key;
  test_key_init(&key, 5);
  struct test_node *found =
      (void *)hash_map_get(&map, (void *)&key, test_node_cmp);
  assert(found != NULL);
  assert(found->key == 5);
  assert(found->val == 10);
  destroy(&map);
}

static void test_hashmap_get_other_key_after_insert(void) {
  struct hash_map map;
  hash_map_init(&map, 8);
  struct test_node *node = test_node_alloc(5, 10);
  hash_map_insert(&map, (void *)node);

  struct test_node key;
  test_key_init(&key, 4);
  void *found = hash_map_get(&map, (void *)&key, test_node_cmp);
  assert(found == NULL);
  destroy(&map);
}

static void test_hashmap_get_other_key_same_bucket_after_insert(void) {
  struct hash_map map;
  hash_map_init(&map, 8);
  struct test_node *node = test_node_alloc(5, 10);
  hash_map_insert(&map, (void *)node);

  struct test_node key;
  test_key_init(&key, 5 + 8);
  void *found = hash_map_get(&map, (void *)&key, test_node_cmp);
  assert(found == NULL);
  destroy(&map);
}

static void test_hashmap_get_missing_after_delete(void) {
  struct hash_map map;
  hash_map_init(&map, 8);
  struct test_node *inserted = test_node_alloc(5, 10);
  hash_map_insert(&map, (void *)inserted);

  struct test_node key;
  test_key_init(&key, 5);
  struct test_node *removed =
      (void *)hash_map_delete(&map, (void *)&key, test_node_cmp);
  assert(removed == inserted);
  assert(removed->key == 5);
  assert(removed->val == 10);
  free(removed);
  destroy(&map);
}

static void test_hashmap_delete_missing(void) {
  struct hash_map map;
  hash_map_init(&map, 8);

  struct test_node key;
  test_key_init(&key, 5);
  struct test_node *removed =
      (void *)hash_map_delete(&map, (void *)&key, test_node_cmp);
  assert(removed == NULL);
  destroy(&map);
}

static void test_hashmap_get_after_delete_and_reinsert(void) {
  struct hash_map map;
  hash_map_init(&map, 8);
  struct test_node *inserted = test_node_alloc(5, 10);
  hash_map_insert(&map, (void *)inserted);

  struct test_node key;
  test_key_init(&key, 5);
  struct test_node *removed =
      (void *)hash_map_delete(&map, (void *)&key, test_node_cmp);
  assert(removed != NULL);
  free(removed);

  struct test_node *new = test_node_alloc(5, 6);
  hash_map_insert(&map, (void *)new);

  test_key_init(&key, 5);
  struct test_node *found =
      (void *)hash_map_get(&map, (void *)&key, test_node_cmp);
  assert(found != NULL);
  assert(found->key == 5);
  assert(found->val == 6);
  destroy(&map);
}

enum { STRESS_TEST_COUNT = 1000000 };

static void test_hashmap_insert_and_delete_many_entries(void) {
  struct hash_map map;
  hash_map_init(&map, 8);

  for (int i = 0; i < STRESS_TEST_COUNT; i++) {
    // Store double to easily check later
    struct test_node *inserted = test_node_alloc(i, i * 2);
    hash_map_insert(&map, (void *)inserted);
  }

  // Remove even ones
  for (int i = 0; i < STRESS_TEST_COUNT; i += 2) {
    struct test_node key;
    test_key_init(&key, i);
    struct test_node *removed =
        (void *)hash_map_delete(&map, (void *)&key, test_node_cmp);
    assert(removed != NULL);
    free(removed);
  }

  // Get odd ones
  for (int i = 1; i < STRESS_TEST_COUNT; i += 2) {
    struct test_node key;
    test_key_init(&key, i);
    struct test_node *found =
        (void *)hash_map_get(&map, (void *)&key, test_node_cmp);
    assert(found->key == i);
    assert(found->val == i * 2);
  }

  destroy(&map);
}

// NOLINTEND(readability-magic-numbers)

void test_hashmap(void) {
  RUN_TEST(test_hashmap_get_missing);
  RUN_TEST(test_hashmap_get_after_insert);
  RUN_TEST(test_hashmap_get_other_key_after_insert);
  RUN_TEST(test_hashmap_get_other_key_same_bucket_after_insert);

  RUN_TEST(test_hashmap_delete_missing);
  RUN_TEST(test_hashmap_get_missing_after_delete);
  RUN_TEST(test_hashmap_get_after_delete_and_reinsert);

  RUN_TEST(test_hashmap_insert_and_delete_many_entries);
}
