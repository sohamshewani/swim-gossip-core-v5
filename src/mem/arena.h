#ifndef SWIM_ARENA_H
#define SWIM_ARENA_H

#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#define SWIM_CACHE_LINE_SIZE 64
#define SWIM_ARENA_DEFAULT_BLOCK_SIZE (1024 * 1024) /* 1MB */

typedef struct SwimArenaBlock {
    uint8_t *buffer;
    size_t capacity;
    size_t offset;
    struct SwimArenaBlock *next;
} SwimArenaBlock;

typedef struct alignas(SWIM_CACHE_LINE_SIZE) SwimArena {
    SwimArenaBlock *head;
    SwimArenaBlock *current;
    size_t block_size;
    size_t total_allocated;
    atomic_size_t active_allocations;
} SwimArena;

static inline size_t swim_arena_align(size_t size) {
    return (size + (SWIM_CACHE_LINE_SIZE - 1)) & ~(SWIM_CACHE_LINE_SIZE - 1);
}

static inline SwimArenaBlock *swim_arena_block_create(size_t capacity) {
    SwimArenaBlock *block = (SwimArenaBlock *)malloc(sizeof(SwimArenaBlock));
    if (!block) {
        return NULL;
    }
    block->buffer = (uint8_t *)malloc(capacity);
    if (!block->buffer) {
        free(block);
        return NULL;
    }
    block->capacity = capacity;
    block->offset = 0;
    block->next = NULL;
    return block;
}

static inline void swim_arena_block_destroy(SwimArenaBlock *block) {
    if (!block) {
        return;
    }
    free(block->buffer);
    free(block);
}

static inline bool swim_arena_init(SwimArena *arena, size_t block_size) {
    if (!arena) {
        return false;
    }
    size_t actual_block_size = block_size > 0 ? block_size : SWIM_ARENA_DEFAULT_BLOCK_SIZE;
    actual_block_size = swim_arena_align(actual_block_size);

    SwimArenaBlock *initial_block = swim_arena_block_create(actual_block_size);
    if (!initial_block) {
        return false;
    }

    arena->head = initial_block;
    arena->current = initial_block;
    arena->block_size = actual_block_size;
    arena->total_allocated = actual_block_size;
    atomic_init(&arena->active_allocations, 0);
    return true;
}

static inline void *swim_arena_alloc(SwimArena *arena, size_t size) {
    if (!arena || size == 0) {
        return NULL;
    }

    size_t aligned_size = swim_arena_align(size);
    if (aligned_size > arena->block_size) {
        /* Allocation exceeds standard block size; allocate a dedicated block */
        SwimArenaBlock *large_block = swim_arena_block_create(aligned_size);
        if (!large_block) {
            return NULL;
        }
        large_block->offset = aligned_size;
        large_block->next = arena->head->next;
        arena->head->next = large_block;
        arena->total_allocated += aligned_size;
        atomic_fetch_add(&arena->active_allocations, 1);
        return large_block->buffer;
    }

    /* Check current block */
    if (arena->current->offset + aligned_size <= arena->current->capacity) {
        void *ptr = arena->current->buffer + arena->current->offset;
        arena->current->offset += aligned_size;
        atomic_fetch_add(&arena->active_allocations, 1);
        return ptr;
    }

    /* Traverse or allocate new block */
    if (arena->current->next != NULL) {
        arena->current = arena->current->next;
        arena->current->offset = 0;
        void *ptr = arena->current->buffer + arena->current->offset;
        arena->current->offset += aligned_size;
        atomic_fetch_add(&arena->active_allocations, 1);
        return ptr;
    }

    SwimArenaBlock *new_block = swim_arena_block_create(arena->block_size);
    if (!new_block) {
        return NULL;
    }

    arena->current->next = new_block;
    arena->current = new_block;
    arena->total_allocated += arena->block_size;

    void *ptr = arena->current->buffer + arena->current->offset;
    arena->current->offset += aligned_size;
    atomic_fetch_add(&arena->active_allocations, 1);
    return ptr;
}

static inline void swim_arena_reset(SwimArena *arena) {
    if (!arena) {
        return;
    }
    SwimArenaBlock *block = arena->head;
    while (block != NULL) {
        block->offset = 0;
        block = block->next;
    }
    arena->current = arena->head;
    atomic_store(&arena->active_allocations, 0);
}

static inline void swim_arena_destroy(SwimArena *arena) {
    if (!arena) {
        return;
    }
    SwimArenaBlock *block = arena->head;
    while (block != NULL) {
        SwimArenaBlock *next = block->next;
        swim_arena_block_destroy(block);
        block = next;
    }
    arena->head = NULL;
    arena->current = NULL;
    arena->total_allocated = 0;
    atomic_store(&arena->active_allocations, 0);
}

#endif /* SWIM_ARENA_H */