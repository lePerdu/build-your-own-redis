#include "commands.h"
#include "protocol.h"

static void write_object_response(struct buffer *b, struct object o) {
	write_response_header(b, RES_OK);
	write_object(b, o);
}

static void do_get(
	struct store *store,
	struct req_object args[1],
	struct buffer *out_buf
) {
	if (args[0].type != SER_STR) {
		write_err_response(out_buf, "invalid key");
		return;
	}

	struct const_slice key = to_const_slice(args[0].str_val);
	struct object *found = store_get(store, key);
	if (found == NULL) {
		write_err_response(out_buf, "not found");
		return;
	}

	write_object_response(out_buf, *found);
}

static struct object make_object_from_req(struct req_object *req_o) {
	switch (req_o->type) {
        case SER_INT:
			return make_int_object(req_o->int_val);
        case SER_STR: {
			struct object new = make_slice_object(req_o->str_val);
			// Mark as NIL since the value has been moved out
			req_o->type = SER_NIL;
			return new;
		}
		default:
			assert(false);
	}
}

static void do_set(
	struct store *store,
	struct req_object *args,
	struct buffer *out_buf
) {
	if (args[0].type != SER_STR) {
		write_err_response(out_buf, "invalid key");
		return;
	}

	struct const_slice key = to_const_slice(args[0].str_val);
	struct object val = make_object_from_req(&args[1]);

	store_set(store, key, val);
	write_nil_response(out_buf);
}

static void do_del(
	struct store *store,
	struct req_object *args,
	struct buffer *out_buf
) {
	if (args[0].type != SER_STR) {
		write_err_response(out_buf, "invalid key");
		return;
	}

	struct const_slice key = to_const_slice(args[0].str_val);
	bool found = store_del(store, key);
	if (!found) {
		write_err_response(out_buf, "not found");
		return;
	}

	write_nil_response(out_buf);
}

static bool append_key_to_response(struct const_slice key, struct object *val, void *arg) {
	(void)val;
	struct buffer *out_buf = arg;
	write_str_value(out_buf, key);
	return true;
}

static void do_keys(
	struct store *store,
	struct req_object *args,
	struct buffer *out_buf
) {
	(void)args;
	write_arr_response_header(out_buf, store_size(store));

	store_iter(store, append_key_to_response, out_buf);
}

static const struct command all_commands[REQ_MAX_ID] = {
	[REQ_GET] = { "GET", 1, do_get },
	[REQ_SET] = { "SET", 2, do_set },
	[REQ_DEL] = { "DEL", 1, do_del },
	[REQ_KEYS] = { "KEYS", 0, do_keys },
};

const struct command *lookup_command(enum req_type t) {
	if (t >= REQ_MAX_ID) {
		return NULL;
	}

	const struct command *cmd = &all_commands[t];
	if (cmd->handler == NULL) {
		return NULL;
	}
	return cmd;
}

void print_request(
	FILE *stream, const struct command *cmd, const struct req_object *args
) {
	fprintf(stream, "%s", cmd->name);
	if (cmd->arg_count > 0) {
		for (int i = 0; i < cmd->arg_count; i++) {
			fputc(' ', stream);
			print_req_object(stream, &args[i]);
		}
	}
}
