#ifndef OBJECT_H_
#define OBJECT_H_

#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>

#include "types.h"

enum obj_type {
	OBJ_INT,
	OBJ_STR,
};

struct object {
	enum obj_type type;
	union {
		int_val_t int_val;

		struct slice str_val; 
	};
};

static inline struct object make_slice_object(struct slice s) {
	return (struct object) { .type = OBJ_STR, .str_val = s };
}

static inline struct object make_int_object(int_val_t n) {
	return (struct object) { .type = OBJ_INT, .int_val = n };
}

/**
 * Destroys the object and all sub-objects.
 */
void object_destroy(struct object o);

ssize_t write_object(struct slice buffer, struct object);

#endif
