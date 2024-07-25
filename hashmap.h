#ifndef HASHMAP_H_
#define HASHMAP_H_

#include <stdbool.h>
#include <stdint.h>

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
	struct hash_table ht;
};

typedef bool (*hash_entry_cmp_fn)(const void *, const void *);

void hash_map_init(struct hash_map *m, uint32_t cap);

struct hash_entry *hash_map_get(
	const struct hash_map *m,
	const struct hash_entry *key,
	hash_entry_cmp_fn compare
);
void hash_map_insert(struct hash_map *m, struct hash_entry *entry);
struct hash_entry *hash_map_delete(
	struct hash_map *m, const struct hash_entry *key, hash_entry_cmp_fn compare
);

#endif
