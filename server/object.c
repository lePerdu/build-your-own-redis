#include <assert.h>
#include <stdbool.h>
#include <sys/types.h>

#include "object.h"
#include "hashmap.h"
#include "protocol.h"
#include "types.h"

struct hmap_entry {
	struct hash_entry entry;
	struct slice key;
	struct object val;
};

struct hmap_key {
	struct hash_entry entry;
	struct const_slice key;
};

static struct hmap_entry *hmap_entry_alloc(
	struct const_slice key, struct object val
) {
	struct hmap_entry *ent = malloc(sizeof(*ent));
	assert(ent != NULL);
	ent->entry.hash_code = slice_hash(key);
	ent->key = slice_dup(key);
	ent->val = val;
	return ent;
}

static bool hmap_entry_compare(
	const struct hash_entry *raw_a, const struct hash_entry *raw_b
) {
	const struct hmap_key *a = container_of(raw_a, struct hmap_key, entry);
	const struct hmap_key *b = container_of(raw_b, struct hmap_key, entry);
	return (
		a->key.size == b->key.size
		&& memcmp(a->key.data, b->key.data, a->key.size) == 0
	);
}

static bool hmap_entry_free(struct hash_entry *raw_ent, void *arg) {
	(void)arg;
	struct hmap_entry *ent = container_of(raw_ent, struct hmap_entry, entry);
	free(ent->key.data);
	object_destroy(ent->val);
	free(ent);
	return true;
}

void object_destroy(struct object o) {
	switch (o.type) {
		case OBJ_INT:
			break;
		case OBJ_STR:
			free(o.str_val.data);
			break;
		case OBJ_HMAP:
			hash_map_iter(&o.hmap_val, hmap_entry_free, NULL);
			hash_map_destroy(&o.hmap_val);
			break;
		default:
			assert(false);
	}
}

void write_object(struct buffer *b, struct object *o) {
	switch (o->type) {
		case OBJ_INT:
			return write_int_value(b, o->int_val);
		case OBJ_STR:
			return write_str_value(b, to_const_slice(o->str_val));
		default:
			assert(false);
	}
}

struct object *hmap_get(struct object *o, struct const_slice key) {
	assert(o->type == OBJ_HMAP);
	struct hash_map *m = &o->hmap_val;

	// TODO: Re-structure hashmap API to avoid double hashing when inserting?
	struct hmap_key key_ent = {
		.entry.hash_code = slice_hash(key),
		.key = key,
	};

	struct hash_entry *found = hash_map_get(
		m, &key_ent.entry, hmap_entry_compare
	);
	if (found == NULL) {
		return NULL;
	} else {
		struct hmap_entry *existing =
			container_of(found, struct hmap_entry, entry);
		return &existing->val;
	}
}

void hmap_set(struct object *o, struct const_slice key, struct object val) {
	assert(o->type == OBJ_HMAP);
	struct hash_map *m = &o->hmap_val;

	// TODO: Re-structure hashmap API to avoid double hashing when inserting?
	struct hmap_key key_ent = {
		.entry.hash_code = slice_hash(key),
		.key = key,
	};

	struct hash_entry *existing = hash_map_get(
		m, &key_ent.entry, hmap_entry_compare
	);
	if (existing == NULL) {
		struct hmap_entry *new_ent = hmap_entry_alloc(key, val);
		hash_map_insert(m, &new_ent->entry);
	} else {
		struct hmap_entry *existing_ent =
			container_of(existing, struct hmap_entry, entry);
		object_destroy(existing_ent->val);
		existing_ent->val = val;
	}
}

bool hmap_del(struct object *o, struct const_slice key) {
	assert(o->type == OBJ_HMAP);
	struct hash_map *m = &o->hmap_val;

	// TODO: Re-structure hashmap API to avoid double hashing when inserting?
	struct hmap_key key_ent = {
		.entry.hash_code = slice_hash(key),
		.key = key,
	};

	struct hash_entry *removed = hash_map_delete(
		m, &key_ent.entry, hmap_entry_compare
	);
	if (removed != NULL) {
		hmap_entry_free(removed, NULL);
		return true;
	} else {
		return false;
	}
}

int_val_t hmap_size(struct object *o) {
	assert(o->type == OBJ_HMAP);
	return hash_map_size(&o->hmap_val);
}

struct hmap_iter_ctx {
	hmap_iter_fn callback;
	void *arg;
};

static bool hmap_iter_wrapper(struct hash_entry *raw_ent, void *arg) {
	struct hmap_iter_ctx *ctx = arg;
	struct hmap_entry *ent = container_of(raw_ent, struct hmap_entry, entry);
	return ctx->callback(to_const_slice(ent->key), &ent->val, ctx->arg);
}

void hmap_iter(struct object *o, hmap_iter_fn cb, void *arg) {
	assert(o->type == OBJ_HMAP);
	struct hmap_iter_ctx ctx = { .callback = cb, .arg = arg };
	hash_map_iter(&o->hmap_val, hmap_iter_wrapper, &ctx);
}
