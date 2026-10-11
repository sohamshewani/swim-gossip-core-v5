CC ?= gcc
CFLAGS ?= -std=c99 -O3 -Wall -Wextra -pedantic
INCLUDES ?= -Iinclude
LDFLAGS ?= -lpthread

SRC_DIR ?= src
OBJ_DIR ?= obj
BIN_DIR ?= bin
TEST_DIR ?= tests
BENCH_DIR ?= benchmarks

TARGET_EXEC ?= swim-gossip-core
TARGET ?= $(BIN_DIR)/$(TARGET_EXEC)

SRCS := $(wildcard $(SRC_DIR)/*.c)
OBJS := $(SRCS:$(SRC_DIR)/%.c=$(OBJ_DIR)/%.o)

TEST_SRCS := $(wildcard $(TEST_DIR)/*.c)
TEST_BINS := $(TEST_SRCS:$(TEST_DIR)/%.c=$(BIN_DIR)/test_%)

BENCH_SRCS := $(wildcard $(BENCH_DIR)/*.c)
BENCH_BINS := $(BENCH_SRCS:$(BENCH_DIR)/%.c=$(BIN_DIR)/bench_%)

.PHONY: all build test benchmark clean directories

all: build

directories:
	@mkdir -p $(OBJ_DIR) $(BIN_DIR)

build: directories $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(OBJS) $(LDFLAGS) -o $@

$(OBJ_DIR)/%.o: $(SRC_DIR)/%[a-zA-Z0-9_].c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

test: build
	@if [ -d "$(TEST_DIR)" ] && [ -n "$(TEST_SRCS)" ]; then \
		mkdir -p $(BIN_DIR); \
		for src in $(TEST_SRCS); do \
			test_name=$$(basename $$src .c); \
			bin="$(BIN_DIR)/test_$$test_name"; \
			echo "Building and running test: $$test_name"; \
			$(CC) $(CFLAGS) $(INCLUDES) $$src $(filter-out $(OBJ_DIR)/main.o, $(OBJS)) $(LDFLAGS) -o $$bin && $$bin; \
		done; \
	else \
		echo "No tests found in '$(TEST_DIR)'. Skipping."; \
	fi

benchmark: build
	@if [ -d "$(BENCH_DIR)" ] && [ -n "$(BENCH_SRCS)" ]; then \
		mkdir -p $(BIN_DIR); \
		for src in $(BENCH_SRCS); do \
			bench_name=$$(basename $$src .c); \
			bin="$(BIN_DIR)/bench_$$bench_name"; \
			echo "Building and running benchmark: $$bench_name"; \
			$(CC) $(CFLAGS) $(INCLUDES) $$src $(filter-out $(OBJ_DIR)/main.o, $(OBJS)) $(LDFLAGS) -o $$bin && $$bin; \
		done; \
	else \
		echo "No benchmarks found in '$(BENCH_DIR)'. Skipping."; \
	fi

clean:
	@rm -rf $(OBJ_DIR) $(BIN_DIR)
	@echo "Cleaned build artifacts."