#ifndef HASHMAP_H_
#define HASHMAP_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "types.h"

typedef uint32_t hash_t;

struct hash_entry {
  struct hash_entry *next;
  hash_t hash_code;
};

struct hash_table {
  /** Must be 2^n - 1 */
  uint32_t mask;
  uint32_t size;
  struct hash_entry **data;
};

struct hash_map {
  struct hash_table table;
};

typedef bool (*hash_entry_cmp_fn)(
    const struct hash_entry *, const struct hash_entry *);

void hash_map_init(struct hash_map *map, uint32_t cap);
// Entries must be freed beforehand with hash_map_iter
void hash_map_destroy(struct hash_map *map);

static inline uint32_t hash_map_size(const struct hash_map *map) {
  return map->table.size;
}

struct hash_entry *hash_map_get(
    const struct hash_map *map, const struct hash_entry *key,
    hash_entry_cmp_fn compare);
void hash_map_insert(struct hash_map *map, struct hash_entry *entry);
struct hash_entry *hash_map_delete(
    struct hash_map *map, const struct hash_entry *key,
    hash_entry_cmp_fn compare);

struct hash_entry *hash_map_peek(const struct hash_map *map);
struct hash_entry *hash_map_pop(struct hash_map *map);

typedef bool (*hash_entry_iter_fn)(struct hash_entry *entry, void *arg);
bool hash_map_iter(struct hash_map *map, hash_entry_iter_fn iter, void *arg);

hash_t slice_hash(struct const_slice slice);

#endif
