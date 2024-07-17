CFLAGS = -std=c11 -Wall -Wextra -Og -g

BUILD = build
BIN = bin

COMMON = $(BUILD)/protocol.o
SERVER = $(BIN)/server
CLIENT = $(BIN)/client

all: $(SERVER) $(CLIENT)

$(BUILD)/%.o: %.c | $(BUILD)
	$(CC) $(CFLAGS) -MMD -c -o $@ $<

$(BIN)/%: $(BUILD)/%.o $(COMMON) | $(BIN)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

$(BUILD):
	mkdir -p $@

$(BIN):
	mkdir -p $@

run_server: $(SERVER)
	$<

debug_server: $(SERVER)
	gdb $<

run_client: $(CLIENT)
	$<

clean:
	$(RM) -r $(BUILD) $(BIN)

.PHONY: run_server run_client clean

-include $(BUILD)/*.d
