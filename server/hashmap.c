#include "hashmap.h"

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "types.h"

enum {
  MAX_LOAD_FACTOR = 8,
  HASH_MAP_RESIZE_MAX_WORK = 128,
};

static void ht_init(struct hash_table *table, uint32_t cap) {
  assert(cap > 0);
  assert((cap & (cap - 1)) == 0);
  table->mask = cap - 1;
  table->size = 0;
  table->data = (struct hash_entry **)calloc(sizeof(struct hash_entry *), cap);
  assert(table->data != NULL);
}

static void ht_init_empty(struct hash_table *table) {
  table->mask = 0;
  table->size = 0;
  table->data = NULL;
}

static void ht_destroy(struct hash_table *table) { free((void *)table->data); }

static void ht_insert(struct hash_table *table, struct hash_entry *entry) {
  uint32_t index = entry->hash_code & table->mask;
  struct hash_entry *next = table->data[index];
  entry->next = next;
  table->data[index] = entry;
  table->size++;
}

static struct hash_entry **ht_lookup(
    const struct hash_table *table, const struct hash_entry *key,
    hash_entry_cmp_fn compare) {
  uint32_t index = key->hash_code & table->mask;
  struct hash_entry **from = &table->data[index];
  while (*from != NULL) {
    if ((*from)->hash_code == key->hash_code && compare(key, *from)) {
      return from;
    }
    from = &(*from)->next;
  }

  return NULL;
}

static struct hash_entry *ht_detach(
    struct hash_table *table, struct hash_entry **from) {
  struct hash_entry *node = *from;
  *from = node->next;
  table->size--;
  return node;
}

static struct hash_entry *ht_lookup_detach(
    struct hash_table *table, const struct hash_entry *key,
    hash_entry_cmp_fn compare) {
  struct hash_entry **location = ht_lookup(table, key, compare);
  if (location == NULL) {
    return NULL;
  }

  return ht_detach(table, location);
}

static struct hash_entry *ht_peek(struct hash_table *table) {
  // Don't need to iterate in this case
  if (table->size == 0) {
    return NULL;
  }

  // TODO: Is there a way to optimize this for big, sparse tables?
  for (uint32_t index = 0; index <= table->mask; index++) {
    struct hash_entry *entry = table->data[index];
    if (entry != NULL) {
      return entry;
    }
  }
  assert(false);
}

static struct hash_entry *ht_pop(struct hash_table *table) {
  // TODO: Share code with ht_peek?

  // Don't need to iterate in this case
  if (table->size == 0) {
    return NULL;
  }

  for (uint32_t index = 0; index <= table->mask; index++) {
    struct hash_entry *entry = table->data[index];
    if (entry != NULL) {
      table->data[index] = entry->next;
      table->size--;
      return entry;
    }
  }
  assert(false);
}

static bool ht_iter(
    struct hash_table *table, hash_entry_iter_fn iter, void *arg) {
  if (table->size == 0) {
    return true;
  }

  for (uint32_t index = 0; index <= table->mask; index++) {
    struct hash_entry *entry = table->data[index];
    while (entry != NULL) {
      // Get the next before before calling the callback in case the
      // entry is freed
      struct hash_entry *next = entry->next;
      if (!iter(entry, arg)) {
        return false;
      }

      entry = next;
    }
  }

  return true;
}

void hash_map_init(struct hash_map *map, uint32_t cap) {
  ht_init(&map->table, cap);
  // Marker for missing
  ht_init_empty(&map->old_table);
  map->resizing_pos = 0;
}

static inline bool hash_map_is_resizing(const struct hash_map *map) {
  return map->old_table.data != NULL;
}

static void hash_map_do_resizing(struct hash_map *map) {
  if (!hash_map_is_resizing(map)) {
    return;
  }

  unsigned moved_count = 0;

  for (; map->resizing_pos <= map->old_table.mask; map->resizing_pos++) {
    // Head pointer of the hash, not an insult
    struct hash_entry **bucket_head = &map->old_table.data[map->resizing_pos];
    while (*bucket_head != NULL) {
      struct hash_entry *removed = ht_detach(&map->old_table, bucket_head);
      ht_insert(&map->table, removed);
      moved_count++;
      if (moved_count > HASH_MAP_RESIZE_MAX_WORK) {
        return;
      }
    }
  }

  // No early break due to max work
  assert(map->old_table.size == 0);
  ht_destroy(&map->old_table);
  ht_init_empty(&map->old_table);
}

static void hash_map_resize_if_needed(struct hash_map *map) {
  uint32_t capacity = map->table.mask + 1;
  if (map->table.size >= MAX_LOAD_FACTOR * capacity) {
    assert(!hash_map_is_resizing(map));
    map->old_table = map->table;
    ht_init(&map->table, capacity * 2);
    map->resizing_pos = 0;
  }
}

void hash_map_destroy(struct hash_map *map) {
  ht_destroy(&map->table);
  if (hash_map_is_resizing(map)) {
    ht_destroy(&map->old_table);
  }
}

struct hash_entry *hash_map_get(
    struct hash_map *map, const struct hash_entry *key,
    hash_entry_cmp_fn compare) {
  hash_map_do_resizing(map);
  struct hash_entry **location = ht_lookup(&map->table, key, compare);
  if (location == NULL && hash_map_is_resizing(map)) {
    location = ht_lookup(&map->old_table, key, compare);
  }

  if (location == NULL) {
    return NULL;
  }

  return *location;
}

void hash_map_insert(struct hash_map *map, struct hash_entry *entry) {
  ht_insert(&map->table, entry);
  hash_map_resize_if_needed(map);
  hash_map_do_resizing(map);
}

struct hash_entry *hash_map_delete(
    struct hash_map *map, const struct hash_entry *key,
    hash_entry_cmp_fn compare) {
  struct hash_entry *deleted = ht_lookup_detach(&map->table, key, compare);
  if (deleted == NULL && hash_map_is_resizing(map)) {
    deleted = ht_lookup_detach(&map->old_table, key, compare);
  }

  // Move keys after deleting so the deleted entry isn't moved for no reason
  hash_map_do_resizing(map);
  return deleted;
}

struct hash_entry *hash_map_peek(struct hash_map *map) {
  hash_map_do_resizing(map);
  struct hash_entry *found = ht_peek(&map->table);
  if (found == NULL && hash_map_is_resizing(map)) {
    found = ht_peek(&map->old_table);
  }
  return found;
}

struct hash_entry *hash_map_pop(struct hash_map *map) {
  hash_map_do_resizing(map);
  struct hash_entry *found = ht_pop(&map->table);
  if (found == NULL && hash_map_is_resizing(map)) {
    found = ht_pop(&map->old_table);
  }
  return found;
}

bool hash_map_iter(struct hash_map *map, hash_entry_iter_fn iter, void *arg) {
  hash_map_do_resizing(map);
  if (!ht_iter(&map->table, iter, arg)) {
    return false;
  }
  if (hash_map_is_resizing(map)) {
    if (!ht_iter(&map->old_table, iter, arg)) {
      return false;
    }
  }
  return true;
}

enum {
  HASH_SEED = 0x811C9DC5,
  HASH_MULTIPLIER = 0x01000193,
};

hash_t slice_hash(struct const_slice slice) {
  const uint8_t *data = slice.data;
  hash_t hash = HASH_SEED;
  for (size_t i = 0; i < slice.size; i++) {
    hash = (hash + data[i]) * HASH_MULTIPLIER;
  }
  return hash;
}
