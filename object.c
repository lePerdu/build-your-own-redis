#include <assert.h>
#include <sys/types.h>

#include "object.h"
#include "protocol.h"
#include "types.h"

void object_destroy(struct object o) {
	switch (o.type) {
		case OBJ_INT:
			break;
		case OBJ_STR:
			free(o.str_val.data);
			break;
		default:
			assert(false);
	}
}

ssize_t write_object(struct slice buffer, struct object o) {
	switch (o.type) {
		case OBJ_INT:
			return write_int_value(buffer, o.int_val);
		case OBJ_STR:
			return write_str_value(buffer, to_const_slice(o.str_val));
		default:
			assert(false);
	}
}
