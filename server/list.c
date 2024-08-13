#include "list.h"

#include <stdlib.h>

void dlist_init(struct dlist *list) {
  list->head.prev = list->head.next = &list->head;
}

void dlist_destroy(struct dlist *list) {
  // No-op since entries are externally-managed
  (void)list;
}

bool dlist_empty(const struct dlist *list) {
  return list->head.next == &list->head;
}

void dlist_push_front(struct dlist *list, struct dlist_node *item) {
  struct dlist_node *next = list->head.next;
  next->prev = item;
  item->next = next;

  item->prev = &list->head;
  list->head.next = item;
}

void dlist_push_back(struct dlist *list, struct dlist_node *item) {
  struct dlist_node *prev = list->head.prev;
  prev->next = item;
  item->prev = prev;

  item->next = &list->head;
  list->head.prev = item;
}

struct dlist_node *dlist_peek_front(struct dlist *list) {
  if (dlist_empty(list)) {
    return NULL;
  }

  return list->head.next;
}

struct dlist_node *dlist_pop_front(struct dlist *list) {
  if (dlist_empty(list)) {
    return NULL;
  }

  struct dlist_node *popped = list->head.next;
  dlist_detach(list, popped);
  return popped;
}

void dlist_detach(struct dlist *list, struct dlist_node *item) {
  (void)list;
  item->next->prev = item->prev;
  item->prev->next = item->next;
}
