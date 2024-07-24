CFLAGS = -std=c17 -Wall -Wextra -Og -g

BUILD = build
BIN = bin

COMMON_SRCS = hashmap.c protocol.c
COMMON_OBJS = $(COMMON_SRCS:%.c=$(BUILD)/%.o)

SERVER_SRCS = server.c
SERVER_OBJS = $(SERVER_SRCS:%.c=$(BUILD)/%.o)
SERVER_EXEC = $(BIN)/server

CLIENT_SRCS = client.c
CLIENT_OBJS = $(CLIENT_SRCS:%.c=$(BUILD)/%.o)
CLIENT_EXEC = $(BIN)/client

TEST_SRCS = test.c
TEST_OBJS = $(TEST_SRCS:%.c=$(BUILD)/%.o)
TEST_EXEC = $(BIN)/test

all: $(SERVER_EXEC) $(CLIENT_EXEC)

$(BUILD)/%.o: %.c | $(BUILD)
	$(CC) $(CFLAGS) -MMD -c -o $@ $<

$(SERVER_EXEC): $(SERVER_OBJS)
$(CLIENT_EXEC): $(CLIENT_OBJS)
$(TEST_EXEC): $(TEST_OBJS)

$(SERVER_EXEC) $(CLIENT_EXEC) $(TEST_EXEC): $(COMMON_OBJS) | $(BIN)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LDLIBS)

$(BUILD) $(BIN):
	mkdir -p $@

run_server: $(SERVER_EXEC)
	$<

debug_server: $(SERVER_EXEC)
	gdb $<

run_client: $(CLIENT_EXEC)
	$<

test: $(TEST_EXEC)
	$<

debug_tests: $(TEST_EXEC)
	gdb $<

clean:
	$(RM) -r $(BUILD) $(BIN)

.PHONY: run_server debug_server run_client run_tests debug_tests clean

-include $(BUILD)/*.d
