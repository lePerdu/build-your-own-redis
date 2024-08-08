#ifndef AVL_H_
#define AVL_H_

struct avl_node {
  struct avl_node *parent;
  struct avl_node *left;
  struct avl_node *right;
};

typedef int (*avl_compare_fn)(const struct avl_node *, const struct avl_node *);
typedef int (*avl_compare_key_fn)(const void *key, const struct avl_node *node);

void avl_init(struct avl_node *node);

/** Insert, returning the new root node. */
void avl_insert(
    struct avl_node **root, struct avl_node *new, avl_compare_fn compare);
/** Delete the existing node, returning the new root node. */
void avl_delete(struct avl_node **root, struct avl_node *node);
struct avl_node *avl_search(
    struct avl_node *root, const void *key, avl_compare_key_fn compare);

#endif
