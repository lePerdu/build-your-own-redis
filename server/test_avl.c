#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "avl.h"
#include "test.h"
#include "types.h"

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
    assert(test_val(node->left) <= self_val);
  }
  if (node->right != NULL) {
    assert(node->right->parent == node);
    assert(test_val(node->right) >= self_val);
  }

  assert(avl_size(node) == 1 + avl_size(node->left) + avl_size(node->right));

  uint32_t l_depth = avl_depth(node->left);
  uint32_t r_depth = avl_depth(node->right);
  assert(avl_depth(node) == 1 + (l_depth >= r_depth ? l_depth : r_depth));
  assert(l_depth < r_depth + 2);
  assert(r_depth < l_depth + 2);
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
      verify_tree(root);
      free(container_of(to_remove, struct test_node, node));
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

static struct avl_node *generate_odd_tree(int size) {
  struct avl_node *root = NULL;

  int seq[size];
  generate_seq(size, seq);
  for (int i = 0; i < size; i++) {
    avl_insert(&root, &test_node_alloc(seq[i] * 2 - 1)->node, compare);
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
    verify_tree(root);
    free(container_of(to_remove, struct test_node, node));
    cleanup_tree(root);
  }
}

static void test_search_lte_exact_match_all_values(int size) {
  // Generate the tree with odd values to search between nodes
  for (int val = 1; val <= size; val++) {
    struct avl_node *root = generate_tree(size);

    struct test_key key = {val};
    struct avl_node *found = avl_search_lte(root, &key, compare_key);
    assert(found != NULL);
    assert(test_val(found) == val);

    cleanup_tree(root);
  }
}

static void test_search_lte_no_exact_match_all_values(int size) {
  // Generate the tree with odd values to search between nodes
  for (int val = 0; val < size * 2; val += 2) {
    struct avl_node *root = generate_odd_tree(size);

    struct test_key key = {val};
    struct avl_node *found = avl_search_lte(root, &key, compare_key);
    assert(found != NULL);
    assert(test_val(found) == val + 1);

    cleanup_tree(root);
  }

  // Edge case at the end
  struct avl_node *root = generate_odd_tree(size);

  struct test_key key = {size * 2};
  struct avl_node *found = avl_search_lte(root, &key, compare_key);
  assert(found == NULL);

  cleanup_tree(root);
}

static void test_rank_all_values(int size) {
  for (int val = 1; val <= size; val++) {
    struct avl_node *root = generate_tree(size);
    struct test_key key = {val};
    struct avl_node *found = avl_search(root, &key, compare_key);
    assert(found != NULL);
    uint32_t rank = avl_rank(root, found);
    assert(rank == (uint32_t)val - 1);
    cleanup_tree(root);
  }
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
static void test_avl_offset_from_all_nodes_to_all_nodes(int size) {
  for (int val = 1; val <= size; val++) {
    for (int offset = -val; offset <= size - val + 1; offset++) {
      struct avl_node *root = generate_tree(size);
      struct test_key key = {val};
      struct avl_node *found = avl_search(root, &key, compare_key);
      assert(found != NULL);
      struct avl_node *target = avl_offset(found, offset);
      if (val + offset <= 0) {
        assert(target == NULL);
      } else if (val + offset > size) {
        assert(target == NULL);
      } else {
        assert(target != NULL);
        assert(test_val(target) == val + offset);
      }

      cleanup_tree(root);
    }
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
      test_search_lte_exact_match_all_values(i);
      test_search_lte_no_exact_match_all_values(i);
      test_rank_all_values(i);
      test_avl_offset_from_all_nodes_to_all_nodes(i);
    }
  }
}

void test_avl(void) {
  RUN_TEST(test_avl_random_insert_delete);
  RUN_TEST(test_avl_small_trees);
}
