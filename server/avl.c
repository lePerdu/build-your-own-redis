#include "avl.h"

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>

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

void avl_init(struct avl_node *node) {
  node->parent = NULL;
  node->left = NULL;
  node->right = NULL;
}

void avl_delete(struct avl_node **root, struct avl_node *node) {
  struct avl_node *parent = node->parent;
  struct avl_node *replacement;
  if (node->right == NULL) {
    replacement = node->left;
    if (replacement != NULL) {
      replacement->parent = parent;
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
  }

  if (parent != NULL) {
    if (parent->left == node) {
      parent->left = replacement;
    } else if (parent->right == node) {
      parent->right = replacement;
    } else {
      assert(false);
    }
  } else {
    assert(node == *root);
    *root = replacement;
  }
}
