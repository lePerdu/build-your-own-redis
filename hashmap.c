#include <assert.h>
#include <stdlib.h>

#include "hashmap.h"

static void ht_init(struct hash_table *ht, size_t cap) {
	assert(cap > 0);
	assert((cap & (cap - 1)) == 0);
	ht->mask = cap - 1;
	ht->size = 0;
	ht->data = calloc(sizeof(struct hash_entry *), cap);
	assert(ht->data != NULL);
}

static void ht_insert(struct hash_table *ht, struct hash_entry *entry) {
	size_t index = entry->hash_code & ht->mask;
	struct hash_entry *next = ht->data[index];
	entry->next = next;
	ht->data[index] = entry;
	ht->size++;
}

static struct hash_entry **ht_lookup(
	const struct hash_table *ht,
	const struct hash_entry *key,
	hash_entry_cmp_fn compare
) {
	size_t index = key->hash_code & ht->mask;
	struct hash_entry **from = &ht->data[index];
	while (*from != NULL) {
		if ((*from)->hash_code == key->hash_code && compare(key, *from)) {
			return from;
		}
		from = &(*from)->next;
	}

	return NULL;
}

static struct hash_entry *ht_detach(
	struct hash_table *ht, struct hash_entry **from
) {
	struct hash_entry *node = *from;
	*from = node->next;
	ht->size--;
	return node;
}

void hash_map_init(struct hash_map *m, size_t cap) {
	ht_init(&m->ht, cap);
}

struct hash_entry *hash_map_get(
	const struct hash_map *m,
	const struct hash_entry *key,
	hash_entry_cmp_fn compare
) {
	struct hash_entry **location = ht_lookup(&m->ht, key, compare);
	if (location == NULL) {
		return NULL;
	}

	return *location;
}

void hash_map_insert(struct hash_map *m, struct hash_entry *entry) {
	ht_insert(&m->ht, entry);
}

struct hash_entry *hash_map_delete(
	struct hash_map *m, const struct hash_entry *key, hash_entry_cmp_fn compare
) {
	struct hash_entry **location = ht_lookup(&m->ht, key, compare);
	if (location == NULL) {
		return NULL;
	}

	return ht_detach(&m->ht, location);
}
