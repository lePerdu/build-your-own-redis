CFLAGS = -std=c17 -Wall -Wextra
CFLAGS += -Og -g
CFLAGS += -fno-omit-frame-pointer -fsanitize=address

BUILD = build
BIN = bin

SERVER_SRC = server

COMMON_SRCS = buffer.c hashmap.c object.c protocol.c
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

run_server: $(SERVER_EXEC)
	$<

debug_server: $(SERVER_EXEC)
	gdb $<

run_client: $(CLIENT_EXEC)
	env PYTHONSTARTUP=client/__init__.py python3

.PHONY: run_server debug_server run_client

unit_test: $(TEST_EXEC)
	$<

debug_unit_test: $(TEST_EXEC)
	gdb $<

test: unit_test

.PHONY: unit_test debug_unit_test test

clean:
	$(RM) -r $(BUILD) $(BIN)

.PHONY: clean

-include $(BUILD)/*.d
