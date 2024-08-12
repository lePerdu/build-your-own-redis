#ifndef AVL_H_
#define AVL_H_

#include <stddef.h>
#include <stdint.h>

struct avl_node {
  struct avl_node *parent;
  struct avl_node *left;
  struct avl_node *right;
  /** Depth of the subtree starting from this node */
  uint32_t depth;
  /** Size of the subtree starting from this node */
  uint32_t size;
};

typedef int (*avl_compare_fn)(const struct avl_node *, const struct avl_node *);
typedef int (*avl_compare_key_fn)(const void *key, const struct avl_node *node);

void avl_init(struct avl_node *node);

/** Null-safe depth getter */
static inline uint32_t avl_depth(struct avl_node *node) {
  return node != NULL ? node->depth : 0;
}

/** Null-safe size getter */
static inline uint32_t avl_size(struct avl_node *node) {
  return node != NULL ? node->size : 0;
}

/** Insert, returning the new root node. */
void avl_insert(
    struct avl_node **root, struct avl_node *new, avl_compare_fn compare);
/** Delete the existing node, returning the new root node. */
void avl_delete(struct avl_node **root, struct avl_node *node);
/**
 * Find the right-most element less-than or equal to the key.
 *
 * If there are elements equal to the one returned, the left-most one will be
 * returned.
 */
struct avl_node *avl_search_lte(
    struct avl_node *root, const void *key, avl_compare_key_fn compare);
/** Find the left-most element equal to the key */
struct avl_node *avl_search(
    struct avl_node *root, const void *key, avl_compare_key_fn compare);

/** Find the node `offset` ranks from a target node. */
struct avl_node *avl_offset(struct avl_node *node, int64_t offset);

uint32_t avl_rank(const struct avl_node *root, const struct avl_node *target);

#endif
