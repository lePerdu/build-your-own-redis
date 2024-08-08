#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "avl.h"
#include "hashmap.h"
#include "test.h"

struct test_node {
  struct avl_node node;
  int val;
};

struct test_key {
  int val;
};

static struct test_node *test_node_alloc(int val) {
  struct test_node *node = malloc(sizeof(*node));
  avl_init(&node->node);
  node->val = val;
  return node;
}

static inline int test_val(const struct avl_node *node) {
  return container_of(node, struct test_node, node)->val;
}

static int compare(
    const struct avl_node *node_a, const struct avl_node *node_b) {
  return test_val(node_a) - test_val(node_b);
}

static int compare_key(const void *key, const struct avl_node *node) {
  return ((const struct test_key *)key)->val - test_val(node);
}

static void verify_tree(struct avl_node *node) {
  if (node == NULL) {
    return;
  }

  verify_tree(node->left);
  verify_tree(node->right);

  if (node->parent != NULL) {
    assert(node->parent->left == node || node->parent->right == node);
  }

  int self_val = test_val(node);
  if (node->left != NULL) {
    assert(node->left->parent == node);
    assert(test_val(node->left) < self_val);
  }
  if (node->right != NULL) {
    assert(node->right->parent == node);
    assert(test_val(node->right) >= self_val);
  }
}

static void cleanup_tree(struct avl_node *node) {
  if (node == NULL) {
    return;
  }

  cleanup_tree(node->left);
  cleanup_tree(node->right);

  free(container_of(node, struct test_node, node));
}

enum {
  AVL_RAND_TEST_SEED = 42,
  AVL_RAND_TEST_INSERT_COUNT = 100,
  AVL_RAND_TEST_DELETE_COUNT = 200,
  AVL_RAND_TEST_RANGE = 1000,
};

static void test_avl_random_insert_delete(void) {
  srand(AVL_RAND_TEST_SEED);

  struct avl_node *root = NULL;
  for (unsigned i = 0; i < AVL_RAND_TEST_INSERT_COUNT; i++) {
    int val = rand() % AVL_RAND_TEST_RANGE;
    avl_insert(&root, &test_node_alloc(val)->node, compare);
    verify_tree(root);
  }

  unsigned delete_count = 0;
  for (unsigned i = 0; i < AVL_RAND_TEST_DELETE_COUNT; i++) {
    int val = rand() % AVL_RAND_TEST_RANGE;
    struct test_key key = {val};
    struct avl_node *to_remove = avl_search(root, &key, compare_key);
    if (to_remove != NULL) {
      delete_count++;
      avl_delete(&root, to_remove);
      free(container_of(to_remove, struct test_node, node));
      verify_tree(root);
    }
  }

  assert(delete_count > 0);

  cleanup_tree(root);
}

/** Fill array with 1..size in random order */
static void generate_seq(int size, int arr[size]) {
  for (int i = 1; i <= size; i++) {
    arr[i - 1] = i;
  }

  for (int i = 1; i < size; i++) {
    int remaining_count = size - i;
    int swap_index = rand() % remaining_count;

    int tmp = arr[i - 1];
    arr[i - 1] = arr[swap_index];
    arr[swap_index] = tmp;
  }
}

/** Make tree with 1..size inserted in random order */
static struct avl_node *generate_tree(int size) {
  struct avl_node *root = NULL;

  int seq[size];
  generate_seq(size, seq);
  for (int i = 0; i < size; i++) {
    avl_insert(&root, &test_node_alloc(seq[i])->node, compare);
    verify_tree(root);
  }

  return root;
}

static void test_insert_all_values(int size) {
  for (int val = 0; val <= size + 1; val++) {
    struct avl_node *root = generate_tree(size);
    avl_insert(&root, &test_node_alloc(val)->node, compare);
    verify_tree(root);
    cleanup_tree(root);
  }
}

static void test_delete_all_values(int size) {
  for (int val = 1; val <= size; val++) {
    struct avl_node *root = generate_tree(size);

    struct test_key key = {val};
    struct avl_node *to_remove = avl_search(root, &key, compare_key);
    assert(to_remove != NULL);
    avl_delete(&root, to_remove);
    free(container_of(to_remove, struct test_node, node));

    verify_tree(root);
    cleanup_tree(root);
  }
}

enum {
  AVL_TEST_SMALL_TREE_SIZE = 20,
  AVL_TEST_SMALL_TREE_REPEAT = 5,
  AVL_TEST_SMALL_TREE_SEED = 101,
};

static void test_avl_small_trees(void) {
  srand(AVL_TEST_SMALL_TREE_SEED);

  for (int i = 1; i <= AVL_TEST_SMALL_TREE_SIZE; i++) {
    for (int j = 0; j < AVL_TEST_SMALL_TREE_REPEAT; j++) {
      test_insert_all_values(i);
      test_delete_all_values(i);
    }
  }
}

void test_avl(void) {
  RUN_TEST(test_avl_random_insert_delete);
  RUN_TEST(test_avl_small_trees);
}
