#ifndef STORE_H_
#define STORE_H_

#include "hashmap.h"
#include "object.h"
#include "types.h"
#include <stdint.h>

struct store {
	struct hash_map map;
};

struct store_entry {
	struct hash_entry entry;

	// Owned
	struct slice key;
	// Owned
	struct object val;
};

// Same memory layout, but with different const modifiers
struct store_key {
	struct hash_entry entry;
	struct const_slice key;
};

void store_init(struct store *s);

static inline uint32_t store_size(const struct store *s) {
	return hash_map_size(&s->map);
}

struct object *store_get(struct store *s, struct const_slice key);
void store_set(struct store *s, struct const_slice key, struct object val);
bool store_del(struct store *s, struct const_slice key);

typedef bool (*store_iter_fn)(
	struct const_slice key, struct object *val, void *arg
);
void store_iter(struct store *s, store_iter_fn cb, void *arg);

#endif
