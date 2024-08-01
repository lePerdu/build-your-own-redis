#include <stdio.h>
#include <stdlib.h>

#include "hashmap.h"
#include "test.h"

struct test_node {
	struct hash_entry entry;
	int key;
	int val;
};

static bool test_node_cmp(const struct hash_entry *a, const struct hash_entry *b) {
	return (
		container_of(a, struct test_node, entry)->key ==
		container_of(b, struct test_node, entry)->key
	);
}

static void test_key_init(struct test_node *key_node, int key) {
	key_node->entry.hash_code = key;
	key_node->key = key;
}

static struct test_node *test_node_alloc(int key, int val) {
	struct test_node *n = malloc(sizeof(*n));
	test_key_init(n, key);
	n->val = val;
	return n;
}

static bool destroy_entry(struct hash_entry *raw_ent, void *arg) {
	(void)arg;
	struct test_node *ent = container_of(raw_ent, struct test_node, entry);
	free(ent);
	return true;
}

static void destroy(struct hash_map *m) {
	hash_map_iter(m, destroy_entry, NULL);
	hash_map_destroy(m);
}

static void test_hashmap_get_missing(void) {
	struct hash_map m;
	hash_map_init(&m, 8);
	struct test_node key;
	test_key_init(&key, 5);
	void *found = hash_map_get(&m, (void *) &key, test_node_cmp);
	assert(found == NULL);
	destroy(&m);
}

static void test_hashmap_get_after_insert(void) {
	struct hash_map m;
	hash_map_init(&m, 8);
	struct test_node* node = test_node_alloc(5, 10);
	hash_map_insert(&m, (void *) node);

	struct test_node key;
	test_key_init(&key, 5);
	struct test_node *found = (void *) hash_map_get(&m, (void *) &key, test_node_cmp);
	assert(found != NULL);
	assert(found->key == 5);
	assert(found->val == 10);
	destroy(&m);
}

static void test_hashmap_get_other_key_after_insert(void) {
	struct hash_map m;
	hash_map_init(&m, 8);
	struct test_node* node = test_node_alloc(5, 10);
	hash_map_insert(&m, (void *) node);

	struct test_node key;
	test_key_init(&key, 4);
	void *found = hash_map_get(&m, (void *) &key, test_node_cmp);
	assert(found == NULL);
	destroy(&m);
}

static void test_hashmap_get_other_key_same_bucket_after_insert(void) {
	struct hash_map m;
	hash_map_init(&m, 8);
	struct test_node* node = test_node_alloc(5, 10);
	hash_map_insert(&m, (void *) node);

	struct test_node key;
	test_key_init(&key, 5 + 8);
	void *found = hash_map_get(&m, (void *) &key, test_node_cmp);
	assert(found == NULL);
	destroy(&m);
}

static void test_hashmap_get_missing_after_delete(void) {
	struct hash_map m;
	hash_map_init(&m, 8);
	struct test_node* inserted = test_node_alloc(5, 10);
	hash_map_insert(&m, (void *) inserted);

	struct test_node key;
	test_key_init(&key, 5);
	struct test_node *removed =
		(void *) hash_map_delete(&m, (void *) &key, test_node_cmp);
	assert(removed == inserted);
	assert(removed->key == 5);
	assert(removed->val == 10);
	free(removed);
	destroy(&m);
}

static void test_hashmap_delete_missing(void) {
	struct hash_map m;
	hash_map_init(&m, 8);

	struct test_node key;
	test_key_init(&key, 5);
	struct test_node *removed =
		(void *) hash_map_delete(&m, (void *) &key, test_node_cmp);
	assert(removed == NULL);
	destroy(&m);
}

static void test_hashmap_get_after_delete_and_reinsert(void) {
	struct hash_map m;
	hash_map_init(&m, 8);
	struct test_node* inserted = test_node_alloc(5, 10);
	hash_map_insert(&m, (void *) inserted);

	struct test_node key;
	test_key_init(&key, 5);
	struct test_node *removed =
		(void *) hash_map_delete(&m, (void *) &key, test_node_cmp);
	assert(removed != NULL);
	free(removed);

	struct test_node* new = test_node_alloc(5, 6);
	hash_map_insert(&m, (void *) new);

	test_key_init(&key, 5);
	struct test_node *found =
		(void *) hash_map_get(&m, (void *) &key, test_node_cmp);
	assert(found != NULL);
	assert(found->key == 5);
	assert(found->val == 6);
	destroy(&m);
}

#define STRESS_TEST_COUNT 1000000

static void test_hashmap_insert_and_delete_many_entries(void) {
	struct hash_map m;
	hash_map_init(&m, 8);

	for (int i = 0; i < STRESS_TEST_COUNT; i++) {
		// Store double to easily check later
		struct test_node* inserted = test_node_alloc(i, i * 2);
		hash_map_insert(&m, (void *) inserted);
	}

	// Remove even ones
	for (int i = 0; i < STRESS_TEST_COUNT; i += 2) {
		struct test_node key;
		test_key_init(&key, i);
		struct test_node *removed =
			(void *)hash_map_delete(&m, (void *) &key, test_node_cmp);
		assert(removed != NULL);
		free(removed);
	}

	// Get odd ones
	for (int i = 1; i < STRESS_TEST_COUNT; i += 2) {
		struct test_node key;
		test_key_init(&key, i);
		struct test_node *found =
			(void *)hash_map_get(&m, (void *) &key, test_node_cmp);
		assert(found->key == i);
		assert(found->val == i * 2);
	}

	destroy(&m);
}

void test_hashmap(void) {
	RUN_TEST(test_hashmap_get_missing);
	RUN_TEST(test_hashmap_get_after_insert);
	RUN_TEST(test_hashmap_get_other_key_after_insert);
	RUN_TEST(test_hashmap_get_other_key_same_bucket_after_insert);

	RUN_TEST(test_hashmap_delete_missing);
	RUN_TEST(test_hashmap_get_missing_after_delete);
	RUN_TEST(test_hashmap_get_after_delete_and_reinsert);

	RUN_TEST(test_hashmap_insert_and_delete_many_entries);
}
