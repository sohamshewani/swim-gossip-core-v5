#ifndef INCLUDE_SWIM_PROTOCOL_H
#define INCLUDE_SWIM_PROTOCOL_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdatomic.h>
#include <time.h>

#define SWIM_CACHE_LINE_SIZE 64
#define SWIM_MAX_PIGGYBACK_MSGS 256
#define SWIM_MAX_PAYLOAD_SIZE 512
#define SWIM_HASH_MAP_INIT_CAPACITY 1024
#define SWIM_MAX_INFECTION_COUNT 5

typedef enum {
    SWIM_MSG_PING = 1,
    SWIM_MSG_ACK = 2,
    SWIM_MSG_PING_REQ = 3,
    SWIM_MSG_GOSSIP = 4
} SwimMsgType;

typedef enum {
    SWIM_STATE_ALIVE = 0,
    SWIM_STATE_SUSPECT = 1,
    SWIM_STATE_DEAD = 2,
    SWIM_STATE_LEFT = 3
} SwimNodeState;

typedef struct {
    uint64_t node_id;
    uint64_t counter;
} SwimVectorClockEntry;

typedef struct {
    uint64_t node_id;
    SwimNodeState state;
    uint64_t epoch;
    uint32_t incarnation;
    uint8_t infection_count;
    uint64_t last_state_change_ns;
    uint32_t _padding[2];
} __attribute__((aligned(SWIM_CACHE_LINE_SIZE))) SwimNode;

typedef struct {
    uint64_t key;
    int32_t probe_length;
    uint32_t active;
    SwimNode node;
} SwimHashMapEntry;

typedef struct {
    SwimHashMapEntry *entries;
    size_t capacity;
    size_t mask;
    _Atomic size_t size;
} SwimHashMap;

typedef struct {
    uint8_t data[SWIM_MAX_PAYLOAD_SIZE];
    uint16_t length;
    uint8_t infection_count;
    uint8_t msg_type;
} SwimPiggybackMsg;

typedef struct {
    SwimPiggybackMsg buffer[SWIM_MAX_PIGGYBACK_MSGS];
    _Atomic uint32_t head;
    _Atomic uint32_t tail;
} SwimPiggybackQueue;

typedef struct {
    _Atomic uintptr_t node_ptr;
    uint64_t epoch;
} SwimRCUNode;

typedef struct {
    uint64_t target_id;
    uint32_t seq_num;
    uint8_t msg_type;
    uint8_t flags;
    uint16_t piggyback_len;
    uint8_t piggyback_payload[SWIM_MAX_PAYLOAD_SIZE];
} __attribute__((packed)) SwimPacket;

static inline uint64_t swim_hash_mix(uint64_t key) {
    key ^= key >> 30;
    key *= 0xbf58476d1ce4e5b9ULL;
    key ^= key >> 27;
    key *= 0x94d049bb133111ebULL;
    key ^= key >> 31;
    return key;
}

static inline bool swim_hash_map_init(SwimHashMap *map, size_t capacity) {
    if (capacity < 16) capacity = 16;
    size_t cap = 1;
    while (cap < capacity) cap <<= 1;
    
    map->entries = (SwimHashMapEntry *)calloc(cap, sizeof(SwimHashMapEntry));
    if (!map->entries) return false;
    
    map->capacity = cap;
    map->mask = cap - 1;
    atomic_init(&map->size, 0);
    return true;
}

static inline void swim_hash_map_free(SwimHashMap *map) {
    if (map && map->entries) {
        free(map->entries);
        map->entries = NULL;
        map->capacity = 0;
        map->mask = 0;
        atomic_store(&map->size, 0);
    }
}

static inline bool swim_hash_map_insert(SwimHashMap *map, uint64_t key, const SwimNode *node) {
    if (key == 0) return false;
    
    size_t current_capacity = map->capacity;
    if (atomic_load(&map->size) >= current_capacity * 3 / 4) {
        size_t new_cap = current_capacity * 2;
        SwimHashMapEntry *new_entries = (SwimHashMapEntry *)calloc(new_cap, sizeof(SwimHashMapEntry));
        if (!new_entries) return false;
        
        SwimHashMap old_map = *map;
        map->entries = new_entries;
        map->capacity = new_cap;
        map->mask = new_cap - 1;
        atomic_store(&map->size, 0);
        
        for (size_t i = 0; i < old_map.capacity; i++) {
            if (old_map.entries[i].active) {
                swim_hash_map_insert(map, old_map.entries[i].key, &old_map.entries[i].node);
            }
        }
        free(old_map.entries);
    }

    SwimHashMapEntry new_entry;
    new_entry.key = key;
    new_entry.probe_length = 0;
    new_entry.active = 1;
    new_entry.node = *node;

    size_t idx = swim_hash_mix(key) & map->mask;
    int32_t probe = 0;

    while (true) {
        if (!map->entries[idx].active) {
            map->entries[idx] = new_entry;
            map->entries[idx].probe_length = probe;
            atomic_fetch_add(&map->size, 1);
            return true;
        }

        if (map->entries[idx].key == key) {
            map->entries[idx].node = *node;
            return true;
        }

        if (map->entries[idx].probe_length < probe) {
            SwimHashMapEntry temp = map->entries[idx];
            map->entries[idx] = new_entry;
            map->entries[idx].probe_length = probe;
            new_entry = temp;
            probe = new_entry.probe_length;
        }

        idx = (idx + 1) & map->mask;
        probe++;
    }
}

static inline bool swim_hash_map_get(SwimHashMap *map, uint64_t key, SwimNode *out_node) {
    if (key == 0 || map->capacity == 0) return false;
    size_t idx = swim_hash_mix(key) & map->mask;
    int32_t probe = 0;

    while (map->entries[idx].active) {
        if (map->entries[idx].probe_length < probe) {
            return false;
        }
        if (map->entries[idx].key == key) {
            *out_node = map->entries[idx].node;
            return true;
        }
        idx = (idx + 1) & map->mask;
        probe++;
        if ((size_t)probe >= map->capacity) break;
    }
    return false;
}

static inline bool swim_hash_map_remove(SwimHashMap *map, uint64_t key) {
    if (key == 0 || map->capacity == 0) return false;
    size_t idx = swim_hash_mix(key) & map->mask;
    int32_t probe = 0;

    while (map->entries[idx].active) {
        if (map->entries[idx].probe_length < probe) {
            return false;
        }
        if (map->entries[idx].key == key) {
            map->entries[idx].active = 0;
            size_t current = idx;
            size_t next = (idx + 1) & map->mask;

            while (map->entries[next].active && map->entries[next].probe_length > 0) {
                map->entries[current] = map->entries[next];
                map->entries[current].probe_length--;
                map->entries[next].active = 0;
                current = next;
                next = (next + 1) & map->mask;
            }
            atomic_fetch_sub(&map->size, 1);
            return true;
        }
        idx = (idx + 1) & map->mask;
        probe++;
        if ((size_t)probe >= map->capacity) break;
    }
    return false;
}

static inline void swim_piggyback_queue_init(SwimPiggybackQueue *q) {
    atomic_init(&q->head, 0);
    atomic_init(&q->tail, 0);
}

static inline bool swim_piggyback_queue_push(SwimPiggybackQueue *q, const SwimPiggybackMsg *msg) {
    uint32_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
    uint32_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);

    if ((tail - head) >= SWIM_MAX_PIGGYBACK_MSGS) {
        return false;
    }

    uint32_t index = tail % SWIM_MAX_PIGGYBACK_MSGS;
    q->buffer[index] = *msg;
    q->buffer[index].infection_count = 0;

    atomic_store_explicit(&q->tail, tail + 1, memory_order_release);
    return true;
}

static inline bool swim_piggyback_queue_pop(SwimPiggybackQueue *q, SwimPiggybackMsg *out_msg) {
    uint32_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
    uint32_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);

    if (head == tail) {
        return false;
    }

    uint32_t index = head % SWIM_MAX_PIGGYBACK_MSGS;
    *out_msg = q->buffer[index];

    if (out_msg->infection_count < SWIM_MAX_INFECTION_COUNT) {
        out_msg->infection_count++;
        q->buffer[index].infection_count = out_msg->infection_count;
        if (out_msg->infection_count < SWIM_MAX_INFECTION_COUNT) {
            atomic_store_explicit(&q->head, head + 1, memory_order_release);
            uint32_t current_tail = atomic_load_explicit(&q->tail, memory_order_relaxed);
            if ((current_tail - (head + 1)) < SWIM_MAX_PIGGYBACK_MSGS) {
                uint32_t new_tail_idx = current_tail % SWIM_MAX_PIGGYBACK_MSGS;
                q->buffer[new_tail_idx] = *out_msg;
                atomic_store_explicit(&q->tail, current_tail + 1, memory_order_release);
            }
            return true;
        }
    }

    atomic_store_explicit(&q->head, head + 1, memory_order_release);
    return true;
}

static inline bool swim_node_state_transition(SwimNode *node, SwimNodeState new_state, uint32_t new_incarnation, uint64_t current_epoch) {
    if (!node) return false;

    if (node->state == SWIM_STATE_DEAD || node->state == SWIM_STATE_LEFT) {
        return false;
    }

    if (new_state == SWIM_STATE_ALIVE) {
        if (new_incarnation > node->incarnation || (new_incarnation == node->incarnation && node->state == SWIM_STATE_SUSPECT)) {
            node->state = SWIM_STATE_ALIVE;
            node->incarnation = new_incarnation;
            node->epoch = current_epoch;
            node->last_state_change_ns = (uint64_t)time(NULL) * 1000000000ULL;
            node->infection_count = 0;
            return true;
        }
    } else if (new_state == SWIM_STATE_SUSPECT) {
        if (node->state == SWIM_STATE_ALIVE) {
            node->state = SWIM_STATE_SUSPECT;
            node->epoch = current_epoch;
            node->last_state_change_ns = (uint64_t)time(NULL) * 1000000000ULL;
            node->infection_count = 0;
            return true;
        }
    } else if (new_state == SWIM_STATE_DEAD || new_state == SWIM_STATE_LEFT) {
        node->state = new_state;
        node->epoch = current_epoch;
        node->last_state_change_ns = (uint64_t)time(NULL) * 1000000000ULL;
        return true;
    }

    return false;
}

static inline bool swim_vector_clock_merge(SwimVectorClockEntry *local_vc, size_t local_len, 
                                          const SwimVectorClockEntry *remote_vc, size_t remote_len, 
                                          uint64_t *current_frontier_epoch) {
    bool updated = false;
    for (size_t i = 0; i < remote_len; i++) {
        bool found = false;
        for (size_t j = 0; j < local_len; j++) {
            if (local_vc[j].node_id == remote_vc[i].node_id) {
                found = true;
                if (remote_vc[i].counter > local_vc[j].counter) {
                    local_vc[j].counter = remote_vc[i].counter;
                    updated = true;
                    if (local_vc[j].counter > *current_frontier_epoch) {
                        *current_frontier_epoch = local_vc[j].counter;
                    }
                }
                break;
            }
        }
        if (!found) {
            return false;
        }
    }
    return updated;
}

#endif /* INCLUDE_SWIM_PROTOCOL_H */