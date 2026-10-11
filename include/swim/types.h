#ifndef SWIM_TYPES_H
#define SWIM_TYPES_H

#define _GNU_SOURCE
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdatomic.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define SWIM_CACHE_LINE 64
#define SWIM_MAX_IP_LEN 46
#define SWIM_MAX_PIGGYBACK_PAYLOAD 256
#define SWIM_RING_BUFFER_SIZE 1024
#define SWIM_HASH_MAP_INITIAL_CAPACITY 256
#define SWIM_MAX_INFECTION_COUNT 5

typedef enum {
    SWIM_STATE_ALIVE = 0,
    SWIM_STATE_SUSPECT = 1,
    SWIM_STATE_DEAD = 2,
    SWIM_STATE_LEFT = 3
} SwimNodeState;

typedef enum {
    SWIM_MSG_ALIVE = 1,
    SWIM_MSG_SUSPECT = 2,
    SWIM_MSG_DEAD = 3,
    SWIM_MSG_JOIN = 4
} SwimMsgType;

typedef struct {
    uint64_t node_id;
    uint64_t incarnation;
} SwimVectorClockEntry;

typedef struct {
    uint64_t node_id;
    char ip[SWIM_MAX_IP_LEN];
    uint16_t port;
    _Atomic SwimNodeState state;
    _Atomic uint64_t incarnation;
    _Atomic uint64_t last_updated_ns;
    uint32_t suspicion_counter;
    uint32_t padding_align;
} __attribute__((aligned(SWIM_CACHE_LINE))) SwimNode;

typedef struct {
    uint64_t slot_id;
    SwimMsgType type;
    uint64_t target_id;
    uint64_t incarnation;
    uint32_t infection_count;
    uint16_t payload_len;
    uint8_t payload[SWIM_MAX_PIGGYBACK_PAYLOAD];
} SwimGossipMessage;

typedef struct {
    _Atomic size_t head;
    _Atomic size_t tail;
    size_t capacity;
    SwimGossipMessage *buffer;
} SwimPiggybackQueue;

typedef struct {
    uint64_t node_id;
    SwimNode node;
    uint32_t probe_seq;
    bool occupied;
} SwimHashMapEntry;

typedef struct {
    _Atomic size_t capacity;
    _Atomic size_t size;
    SwimHashMapEntry *entries;
    uint64_t rcu_epoch;
} SwimNodeTable;

typedef struct {
    _Atomic int_fast32_t head;
    _Atomic int_fast32_t tail;
    size_t capacity;
    void **buffer;
} SwimMPSCQueue;

static inline uint64_t swim_current_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static inline uint64_t swim_hash_mix(uint64_t key) {
    key ^= key >> 30;
    key *= 0xbf58476d1ce4e5b9ULL;
    key ^= key >> 27;
    key *= 0x94d049bb133111ebULL;
    key ^= key >> 31;
    return key;
}

static inline SwimPiggybackQueue* swim_piggyback_queue_init(size_t capacity) {
    SwimPiggybackQueue *q = (SwimPiggybackQueue*)malloc(sizeof(SwimPiggybackQueue));
    if (!q) return NULL;
    atomic_init(&q->head, 0);
    atomic_init(&q->tail, 0);
    q->capacity = capacity;
    q->buffer = (SwimGossipMessage*)calloc(capacity, sizeof(SwimGossipMessage));
    if (!q->buffer) {
        free(q);
        return NULL;
    }
    return q;
}

static inline void swim_piggyback_queue_free(SwimPiggybackQueue *q) {
    if (!q) return;
    free(q->buffer);
    free(q);
}

static inline bool swim_piggyback_queue_push(SwimPiggybackQueue *q, const SwimGossipMessage *msg) {
    size_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
    size_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);
    
    if ((tail + 1) % q->capacity == head) {
        return false;
    }
    
    memcpy(&q->buffer[tail], msg, sizeof(SwimGossipMessage));
    atomic_store_explicit(&q->tail, (tail + 1) % q->capacity, memory_order_release);
    return true;
}

static inline bool swim_piggyback_queue_pop(SwimPiggybackQueue *q, SwimGossipMessage *msg) {
    size_t head = atomic_load_explicit(&q->head, memory_order_acquire);
    size_t tail = atomic_load_explicit(&q->tail, memory_order_relaxed);
    
    if (head == tail) {
        return false;
    }
    
    memcpy(msg, &q->buffer[head], sizeof(SwimGossipMessage));
    atomic_store_explicit(&q->head, (head + 1) % q->capacity, memory_order_release);
    return true;
}

static inline SwimNodeTable* swim_node_table_init(size_t capacity) {
    SwimNodeTable *table = (SwimNodeTable*)malloc(sizeof(SwimNodeTable));
    if (!table) return NULL;
    
    atomic_init(&table->capacity, capacity);
    atomic_init(&table->size, 0);
    table->entries = (SwimHashMapEntry*)calloc(capacity, sizeof(SwimHashMapEntry));
    if (!table->entries) {
        free(table);
        return NULL;
    }
    table->rcu_epoch = 0;
    return table;
}

static inline void swim_node_table_free(SwimNodeTable *table) {
    if (!table) return;
    free(table->entries);
    free(table);
}

static inline bool swim_node_table_insert(SwimNodeTable *table, const SwimNode *node) {
    size_t cap = atomic_load_explicit(&table->capacity, memory_order_relaxed);
    size_t sz = atomic_load_explicit(&table->size, memory_order_relaxed);
    
    if (sz >= cap * 3 / 4) {
        return false;
    }
    
    uint64_t hash = swim_hash_mix(node->node_id);
    size_t idx = hash % cap;
    
    SwimHashMapEntry new_entry;
    memset(&new_entry, 0, sizeof(new_entry));
    new_entry.node_id = node->node_id;
    memcpy(&new_entry.node, node, sizeof(SwimNode));
    new_entry.probe_seq = 0;
    new_entry.occupied = true;
    
    size_t current_idx = idx;
    uint32_t current_probe = 0;
    
    while (true) {
        if (!table->entries[current_idx].occupied) {
            memcpy(&table->entries[current_idx], &new_entry, sizeof(SwimHashMapEntry));
            atomic_fetch_add_explicit(&table->size, 1, memory_order_relaxed);
            return true;
        }
        
        if (table->entries[current_idx].node_id == node->node_id) {
            memcpy(&table->entries[current_idx].node, node, sizeof(SwimNode));
            return true;
        }
        
        if (table->entries[current_idx].probe_seq < current_probe) {
            SwimHashMapEntry tmp;
            memcpy(&tmp, &table->entries[current_idx], sizeof(SwimHashMapEntry));
            memcpy(&table->entries[current_idx], &new_entry, sizeof(SwimHashMapEntry));
            memcpy(&new_entry, &tmp, sizeof(SwimHashMapEntry));
            current_probe = new_entry.probe_seq;
        }
        
        current_idx = (current_idx + 1) % cap;
        current_probe++;
        new_entry.probe_seq = current_probe;
    }
}

static inline bool swim_node_table_get(SwimNodeTable *table, uint64_t node_id, SwimNode *out_node) {
    size_t cap = atomic_load_explicit(&table->capacity, memory_order_relaxed);
    uint64_t hash = swim_hash_mix(node_id);
    size_t idx = hash % cap;
    uint32_t probe = 0;
    
    while (table->entries[idx].occupied) {
        if (table->entries[idx].probe_seq < probe) {
            return false;
        }
        if (table->entries[idx].node_id == node_id) {
            memcpy(out_node, &table->entries[idx].node, sizeof(SwimNode));
            return true;
        }
        idx = (idx + 1) % cap;
        probe++;
        if (probe >= cap) break;
    }
    return false;
}

static inline bool swim_node_table_remove(SwimNodeTable *table, uint64_t node_id) {
    size_t cap = atomic_load_explicit(&table->capacity, memory_order_relaxed);
    uint64_t hash = swim_hash_mix(node_id);
    size_t idx = hash % cap;
    uint32_t probe = 0;
    size_t found_idx = (size_t)-1;
    
    while (table->entries[idx].occupied) {
        if (table->entries[idx].probe_seq < probe) {
            return false;
        }
        if (table->entries[idx].node_id == node_id) {
            found_idx = idx;
            break;
        }
        idx = (idx + 1) % cap;
        probe++;
        if (probe >= cap) break;
    }
    
    if (found_idx == (size_t)-1) {
        return false;
    }
    
    size_t current = found_idx;
    size_t next = (current + 1) % cap;
    
    while (table->entries[next].occupied && table->entries[next].probe_seq > 0) {
        memcpy(&table->entries[current], &table->entries[next], sizeof(SwimHashMapEntry));
        table->entries[current].probe_seq--;
        current = next;
        next = (next + 1) % cap;
    }
    
    memset(&table->entries[current], 0, sizeof(SwimHashMapEntry));
    atomic_fetch_sub_explicit(&table->size, 1, memory_order_relaxed);
    return true;
}

static inline SwimMPSCQueue* swim_mpsc_queue_init(size_t capacity) {
    SwimMPSCQueue *q = (SwimMPSCQueue*)malloc(sizeof(SwimMPSCQueue));
    if (!q) return NULL;
    atomic_init(&q->head, 0);
    atomic_init(&q->tail, 0);
    q->capacity = capacity;
    q->buffer = (void**)calloc(capacity, sizeof(void*));
    if (!q->buffer) {
        free(q);
        return NULL;
    }
    return q;
}

static inline void swim_mpsc_queue_free(SwimMPSCQueue *q) {
    if (!q) return;
    free(q->buffer);
    free(q);
}

static inline bool swim_mpsc_queue_push(SwimMPSCQueue *q, void *data) {
    int_fast32_t tail = atomic_load_explicit(&q->tail, memory_order_relaxed);
    int_fast32_t next_tail;
    do {
        next_tail = (tail + 1) % (int_fast32_t)q->capacity;
        if (next_tail == atomic_load_explicit(&q->head, memory_order_acquire)) {
            return false;
        }
    } while (!atomic_compare_exchange_weak_explicit(&q->tail, &tail, next_tail,
                                                   memory_order_release,
                                                   memory_order_relaxed));
    q->buffer[tail] = data;
    return true;
}

static inline void* swim_mpsc_queue_pop(SwimMPSCQueue *q) {
    int_fast32_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
    if (head == atomic_load_explicit(&q->tail, memory_order_acquire)) {
        return NULL;
    }
    void *data = q->buffer[head];
    atomic_store_explicit(&q->head, (head + 1) % (int_fast32_t)q->capacity, memory_order_release);
    return data;
}

#endif /* SWIM_TYPES_H */