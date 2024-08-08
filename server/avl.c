#include "avl.h"

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

void avl_init(struct avl_node *node) {
  node->parent = NULL;
  node->left = NULL;
  node->right = NULL;
  node->depth = 1;
}

static uint32_t u32_max(uint32_t val1, uint32_t val2) {
  return val1 >= val2 ? val1 : val2;
}

/**
 * Update (and check in debug mode) metadata of the node after changing its
 * position.
 */
static void update_node(struct avl_node *node) {
  node->depth = 1 + u32_max(avl_depth(node->left), avl_depth(node->right));
  if (node->left != NULL) {
    assert(node->left->parent == node);
  }
  if (node->right != NULL) {
    assert(node->right->parent == node);
  }
}

static void fix_tree(struct avl_node **root, struct avl_node *node) {
  (void)root;
  while (node != NULL) {
    update_node(node);
    node = node->parent;
  }
}

void avl_insert(
    struct avl_node **root, struct avl_node *new, avl_compare_fn compare) {
  struct avl_node **from = root;
  struct avl_node *parent = NULL;
  while (*from != NULL) {
    parent = *from;
    int cmp = compare(new, *from);
    if (cmp < 0) {
      from = &(*from)->left;
    } else /* >= 0 */ {
      from = &(*from)->right;
    }
  }

  new->parent = parent;
  *from = new;
  fix_tree(root, new);
}

struct avl_node *avl_search(
    struct avl_node *root, const void *key, avl_compare_key_fn compare) {
  struct avl_node *node = root;
  while (node != NULL) {
    int cmp = compare(key, node);
    if (cmp < 0) {
      node = node->left;
    } else if (cmp > 0) {
      node = node->right;
    } else {
      return node;
    }
  }

  return NULL;
}

static void update_parent_pointers(
    struct avl_node *parent,
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    struct avl_node *child, struct avl_node *new_child) {
  if (parent->left == child) {
    parent->left = new_child;
  } else if (parent->right == child) {
    parent->right = new_child;
  } else {
    assert(false);
  }
}

void avl_delete(struct avl_node **root, struct avl_node *node) {
  struct avl_node *parent = node->parent;
  struct avl_node *replacement;
  if (node->right == NULL) {
    replacement = node->left;
    if (replacement != NULL) {
      replacement->parent = parent;
    }

    if (parent != NULL) {
      update_parent_pointers(parent, node, replacement);
      fix_tree(root, parent);
    } else {
      assert(node == *root);
      *root = replacement;
    }
  } else {
    // Replace deleted node with successor (left-most leaf of the right
    // sub-tree)
    replacement = node->right;
    while (replacement->left != NULL) {
      replacement = replacement->left;
    }
    avl_delete(root, replacement);

    // Re-attach where `node` is
    replacement->left = node->left;
    if (node->left != NULL) {
      node->left->parent = replacement;
    }
    replacement->right = node->right;
    if (replacement->right != NULL) {
      replacement->right->parent = replacement;
    }

    replacement->parent = parent;
    update_node(replacement);

    if (parent != NULL) {
      update_parent_pointers(parent, node, replacement);
      // Don't need to fix parent tree since the depth was already fixed by the
      // recursive call
    } else {
      assert(node == *root);
      *root = replacement;
    }
  }
}
