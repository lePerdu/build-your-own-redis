#include "hashmap.h"

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "types.h"

enum { MAX_LOAD_FACTOR = 8 };

static void ht_init(struct hash_table *table, uint32_t cap) {
  assert(cap > 0);
  assert((cap & (cap - 1)) == 0);
  table->mask = cap - 1;
  table->size = 0;
  table->data = (struct hash_entry **)calloc(sizeof(struct hash_entry *), cap);
  assert(table->data != NULL);
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

void hash_map_init(struct hash_map *map, uint32_t cap) {
  ht_init(&map->table, cap);
}

void hash_map_destroy(struct hash_map *map) { ht_destroy(&map->table); }

struct hash_entry *hash_map_get(
    const struct hash_map *map, const struct hash_entry *key,
    hash_entry_cmp_fn compare) {
  struct hash_entry **location = ht_lookup(&map->table, key, compare);
  if (location == NULL) {
    return NULL;
  }

  return *location;
}

// This helper function is very local so it's easy to check the order
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static void ht_move_entries(struct hash_table *dst, struct hash_table *src) {
  for (uint32_t index = 0; index <= src->mask; index++) {
    struct hash_entry *entry = src->data[index];
    while (entry != NULL) {
      struct hash_entry *next = entry->next;
      ht_insert(dst, entry);
      entry = next;
      src->size--;
    }
    src->data[index] = NULL;
  }

  assert(src->size == 0);
}

void hash_map_insert(struct hash_map *map, struct hash_entry *entry) {
  ht_insert(&map->table, entry);

  uint32_t capacity = map->table.mask + 1;
  if (map->table.size >= MAX_LOAD_FACTOR * capacity) {
    struct hash_table new_table;
    ht_init(&new_table, capacity * 2);
    ht_move_entries(&new_table, &map->table);
    ht_destroy(&map->table);
    memcpy(&map->table, &new_table, sizeof(new_table));
  }
}

struct hash_entry *hash_map_delete(
    struct hash_map *map, const struct hash_entry *key,
    hash_entry_cmp_fn compare) {
  struct hash_entry **location = ht_lookup(&map->table, key, compare);
  if (location == NULL) {
    return NULL;
  }

  return ht_detach(&map->table, location);
}

struct hash_entry *hash_map_peek(const struct hash_map *map) {
  // Don't need to iterate in this case
  if (hash_map_size(map) == 0) {
    return NULL;
  }

  // TODO: Is there a way to optimize this for big, sparse tables?
  for (uint32_t index = 0; index <= map->table.mask; index++) {
    struct hash_entry *entry = map->table.data[index];
    if (entry != NULL) {
      return entry;
    }
  }

  assert(false);
}

struct hash_entry *hash_map_pop(struct hash_map *map) {
  // TODO: Share code with hash_map_peek

  // Don't need to iterate in this case
  if (hash_map_size(map) == 0) {
    return NULL;
  }

  for (uint32_t index = 0; index <= map->table.mask; index++) {
    struct hash_entry *entry = map->table.data[index];
    if (entry != NULL) {
      map->table.data[index] = entry->next;
      map->table.size--;
      return entry;
    }
  }

  assert(false);
}

bool hash_map_iter(struct hash_map *map, hash_entry_iter_fn iter, void *arg) {
  for (uint32_t index = 0; index <= map->table.mask; index++) {
    struct hash_entry *entry = map->table.data[index];
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
