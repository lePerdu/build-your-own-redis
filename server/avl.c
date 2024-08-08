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
  node->size = 1;
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
  node->size = 1 + avl_size(node->left) + avl_size(node->right);
  if (node->left != NULL) {
    assert(node->left->parent == node);
  }
  if (node->right != NULL) {
    assert(node->right->parent == node);
  }
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

static struct avl_node *avl_rot_left(struct avl_node *node) {
  assert(node->right != NULL);
  struct avl_node *parent = node->parent;
  struct avl_node *new_top = node->right;

  node->right = new_top->left;
  if (node->right) {
    node->right->parent = node;
  }
  node->parent = new_top;
  update_node(node);

  new_top->left = node;
  new_top->parent = parent;
  update_node(new_top);

  return new_top;
}

static struct avl_node *avl_rot_right(struct avl_node *node) {
  assert(node->left != NULL);
  struct avl_node *parent = node->parent;
  struct avl_node *new_top = node->left;

  node->left = new_top->right;
  if (node->left) {
    node->left->parent = node;
  }
  node->parent = new_top;
  update_node(node);

  new_top->right = node;
  new_top->parent = parent;
  update_node(new_top);

  return new_top;
}

static struct avl_node *avl_fix_left_deep(struct avl_node *node) {
  assert(node->left != NULL);
  if (avl_depth(node->left->left) < avl_depth(node->left->right)) {
    node->left = avl_rot_left(node->left);
    assert(avl_depth(node->left->left) >= avl_depth(node->left->right));
  }

  node = avl_rot_right(node);
  assert(avl_depth(node->left) < avl_depth(node->right) + 2);
  return node;
}

static struct avl_node *avl_fix_right_deep(struct avl_node *node) {
  assert(node->right != NULL);
  if (avl_depth(node->right->right) < avl_depth(node->right->left)) {
    node->right = avl_rot_right(node->right);
    assert(avl_depth(node->right->right) >= avl_depth(node->right->left));
  }

  node = avl_rot_left(node);
  assert(avl_depth(node->right) < avl_depth(node->left) + 2);
  return node;
}

static void fix_tree(struct avl_node **root, struct avl_node *node) {
  while (node != NULL) {
    update_node(node);
    uint32_t l_depth = avl_depth(node->left);
    uint32_t r_depth = avl_depth(node->right);

    struct avl_node *parent = node->parent;
    struct avl_node *new_child;
    if (l_depth >= r_depth + 2) {
      new_child = avl_fix_left_deep(node);
    } else if (r_depth >= l_depth + 2) {
      new_child = avl_fix_right_deep(node);
    } else {
      node = parent;
      continue;
    }

    if (parent == NULL) {
      *root = new_child;
      return;
    }

    update_parent_pointers(parent, node, new_child);
    node = parent;
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

void avl_delete(struct avl_node **root, struct avl_node *node) {
  if (node->right == NULL) {
    struct avl_node *parent = node->parent;
    if (node->left != NULL) {
      node->left->parent = parent;
    }

    if (parent != NULL) {
      update_parent_pointers(parent, node, node->left);
      fix_tree(root, parent);
    } else {
      assert(node == *root);
      *root = node->left;
    }
  } else {
    // Replace deleted node with successor (left-most leaf of the right
    // sub-tree)
    struct avl_node *replacement = node->right;
    while (replacement->left != NULL) {
      replacement = replacement->left;
    }
    avl_delete(root, replacement);

    // Get the parent here since the nested call could change the structure
    struct avl_node *parent = node->parent;

    // Re-attach where `node` is
    replacement->left = node->left;
    if (replacement->left != NULL) {
      replacement->left->parent = replacement;
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
