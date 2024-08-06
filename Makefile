CFLAGS_BASE = -std=c17 -Wall -Wextra
CFLAGS_OPT = -Og -g -fno-omit-frame-pointer -fsanitize=address

CFLAGS ?= $(CFLAGS_BASE) $(CFLAGS_OPT)

CLANG_FORMAT ?= clang-format
CLANG_TIDY ?= clang-tidy

BUILD = build
BIN = bin

SERVER_SRC = server

COMMON_SRCS = buffer.c commands.c hashmap.c object.c protocol.c store.c
COMMON_OBJS = $(COMMON_SRCS:%.c=$(BUILD)/%.o)

SERVER_SRCS = server.c
SERVER_OBJS = $(SERVER_SRCS:%.c=$(BUILD)/%.o)
SERVER_EXEC = $(BIN)/server

TEST_SRCS = test.c
TEST_OBJS = $(TEST_SRCS:%.c=$(BUILD)/%.o)
TEST_EXEC = $(BIN)/unit_test

all: $(SERVER_EXEC)

$(BUILD)/%.o: $(SERVER_SRC)/%.c | $(BUILD)
	$(CC) $(CFLAGS) -MMD -c -o $@ $<

$(SERVER_EXEC): $(SERVER_OBJS)
$(TEST_EXEC): $(TEST_OBJS)

$(SERVER_EXEC) $(TEST_EXEC): $(COMMON_OBJS) | $(BIN)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LDLIBS)

$(BUILD) $(BIN):
	mkdir -p $@

run-server: $(SERVER_EXEC)
	$<

debug-server: $(SERVER_EXEC)
	gdb $<

run-client:
	env PYTHONSTARTUP=test/client.py python3

.PHONY: run-server debug-server run-client

unit-test: $(TEST_EXEC)
	$<

debug-unit-test: $(TEST_EXEC)
	gdb $<

e2e-test: $(SERVER_EXEC)
	python3 test/test.py

test: unit-test e2e-test

.PHONY: unit-test debug-unit-test e2e-test test

compile-commands:
	bear -- $(MAKE) CFLAGS="$(CFLAGS_BASE)" clean all

.PHONY: compile-commands

check:
	$(CLANG_TIDY) --warnings-as-errors='*' $(SERVER_SRC)/*

format:
	$(CLANG_FORMAT) -i $(SERVER_SRC)/*

format-check:
	$(CLANG_FORMAT) --Werror --dry-run $(SERVER_SRC)/*

.PHONY: check format format-check

clean:
	$(RM) -r $(BUILD) $(BIN)

.PHONY: clean

-include $(BUILD)/*.d
