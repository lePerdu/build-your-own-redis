#ifndef LIST_H_
#define LIST_H_

#include <stdbool.h>
#include <stddef.h>

struct list_node {
  struct list_node *next;
};

struct list {
  struct list_node *head;
};

static inline void list_init(struct list *list) { list->head = NULL; }

static inline void list_destroy(struct list *list) {
  // No-op since entries are externally-manged
  (void)list;
}

static inline bool list_empty(const struct list *list) {
  return list->head == NULL;
}

static inline void list_push(struct list *list, struct list_node *item) {
  item->next = list->head;
  list->head = item;
}

static inline struct list_node *list_peek(struct list *list) {
  return list->head;
}

static inline struct list_node *list_pop(struct list *list) {
  struct list_node *popped = list->head;
  if (popped == NULL) {
    return NULL;
  }
  list->head = list->head->next;
  return popped;
}

struct dlist_node {
  struct dlist_node *prev;
  struct dlist_node *next;
};

struct dlist {
  struct dlist_node head;
};

void dlist_init(struct dlist *list);
void dlist_destroy(struct dlist *list);

bool dlist_empty(const struct dlist *list);
void dlist_push_front(struct dlist *list, struct dlist_node *item);
void dlist_push_back(struct dlist *list, struct dlist_node *item);
struct dlist_node *dlist_pop_front(struct dlist *list);
struct dlist_node *dlist_peek_front(struct dlist *list);
void dlist_detach(struct dlist *list, struct dlist_node *item);

#endif
