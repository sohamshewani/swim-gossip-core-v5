#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdatomic.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>

#define SWIM_CACHE_LINE 64
#define SWIM_HASH_CAPACITY 1024
#define SWIM_PIGGYBACK_CAPACITY 256
#define SWIM_MAX_MSG_SIZE 512
#define SWIM_MAX_INFECTION_COUNT 5

typedef enum {
    NODE_ALIVE = 0,
    NODE_SUSPECT = 1,
    NODE_DEAD = 2,
    NODE_LEFT = 3
} SwimNodeState;

typedef struct {
    uint64_t node_id;
    uint64_t epoch;
    SwimNodeState state;
    uint8_t infection_count;
    uint32_t probe_sequence;
    char address[48];
    uint16_t port;
    uint8_t padding[6];
} __attribute__((aligned(SWIM_CACHE_LINE))) SwimNode;

typedef struct {
    uint64_t key;
    int32_t dib;
    bool occupied;
    uint8_t padding[7];
    SwimNode node;
} SwimHashMapEntry;

typedef struct {
    SwimHashMapEntry entries[SWIM_HASH_CAPACITY];
    atomic_size_t size;
} SwimNodeTable;

typedef struct {
    uint8_t data[SWIM_MAX_MSG_SIZE];
    size_t length;
    uint8_t infection_count;
    uint64_t target_id;
} SwimGossipMessage;

typedef struct {
    SwimGossipMessage messages[SWIM_PIGGYBACK_CAPACITY];
    atomic_size_t head;
    atomic_size_t tail;
} SwimPiggybackQueue;

typedef struct SwimMPSCNode {
    struct SwimMPSCNode* atomic_next;
    SwimGossipMessage payload;
} SwimMPSCNode;

typedef struct {
    _Atomic(SwimMPSCNode*) head;
    SwimMPSCNode* tail;
    SwimMPSCNode stub;
} SwimMPSCQueue;

typedef struct {
    uint64_t version;
    atomic_uint_fast64_t current_epoch;
} SwimVectorClock;

struct SwimEngine {
    uint64_t local_id;
    SwimNodeTable node_table;
    SwimPiggybackQueue piggyback_queue;
    SwimMPSCQueue mpsc_queue;
    SwimVectorClock vclock;
    atomic_bool running;
    pthread_t worker_thread;
    pthread_rwlock_t rcu_lock;
};

static uint64_t hash_node_id(uint64_t id) {
    id ^= id >> 30;
    id *= 0xbf58476d1ce4e5b9ULL;
    id ^= id >> 27;
    id *= 0x94d049bb133111ebULL;
    id ^= id >> 31;
    return id & (SWIM_HASH_CAPACITY - 1);
}

void swim_node_table_init(SwimNodeTable* table) {
    memset(table, 0, sizeof(SwimNodeTable));
    atomic_init(&table->size, 0);
}

bool swim_node_table_insert(SwimNodeTable* table, const SwimNode* node) {
    uint64_t idx = hash_node_id(node->node_id);
    int32_t dib = 0;
    SwimHashMapEntry incoming;
    memset(&incoming, 0, sizeof(SwimHashMapEntry));
    incoming.key = node->node_id;
    incoming.dib = 0;
    incoming.occupied = true;
    memcpy(&incoming.node, node, sizeof(SwimNode));

    while (true) {
        if (!table->entries[idx].occupied) {
            table->entries[idx] = incoming;
            atomic_fetch_add(&table->size, 1);
            return true;
        }

        if (table->entries[idx].key == node->node_id) {
            table->entries[idx].node = *node;
            return true;
        }

        if (table->entries[idx].dib < dib) {
            SwimHashMap.Entry temp = table->entries[idx];
            table->entries[idx] = incoming;
            incoming = temp;
            dib = incoming.dib;
        }

        idx = (idx + 1) & (SWIM_HASH_CAPACITY - 1);
        dib++;
        if (dib > SWIM_HASH_CAPACITY) {
            return false;
        }
    }
}

bool swim_node_table_find(SwimNodeTable* table, uint64_t node_id, SwimNode* out_node) {
    uint64_t idx = hash_node_id(node_id);
    int32_t dib = 0;

    while (table->entries[idx].occupied) {
        if (table->entries[idx].dib < dib) {
            return false;
        }
        if (table->entries[idx].key == node_id) {
            if (out_node) {
                *out_node = table->entries[idx].node;
            }
            return true;
        }
        idx = (idx + 1) & (SWIM_HASH_CAPACITY - 1);
        dib++;
        if (dib > SWIM_HASH_CAPACITY) {
            break;
        }
    }
    return false;
}

bool swim_node_table_remove(SwimNodeTable* table, uint64_t node_id) {
    uint64_t idx = hash_node_id(node_id);
    int32_t dib = 0;
    uint64_t target_idx = SWIM_HASH_CAPACITY;

    while (table->entries[idx].occupied) {
        if (table->entries[idx].dib < dib) {
            return false;
        }
        if (table->entries[idx].key == node_id) {
            target_idx = idx;
            break;
        }
        idx = (idx + 1) & (SWIM_HASH_CAPACITY - 1);
        dib++;
        if (dib > SWIM_HASH_CAPACITY) {
            return false;
        }
    }

    if (target_idx == SWIM_HASH_CAPACITY) {
        return false;
    }

    idx = target_idx;
    uint64_t next_idx = (idx + 1) & (SWIM_HASH_CAPACITY - 1);

    while (table->entries[next_idx].occupied && table->entries[next_idx].dib > 0) {
        table->entries[idx] = table->entries[next_idx];
        table->entries[idx].dib--;
        idx = next_idx;
        next_idx = (next_idx + 1) & (SWIM_HASH_CAPACITY - 1);
    }

    table->entries[idx].occupied = false;
    table->entries[idx].key = 0;
    table->entries[idx].dib = 0;
    memset(&table->entries[idx].node, 0, sizeof(SwimNode));
    atomic_fetch_sub(&table->size, 1);
    return true;
}

void swim_piggyback_init(SwimPiggybackQueue* q) {
    atomic_init(&q->head, 0);
    atomic_init(&q->tail, 0);
    memset(q->messages, 0, sizeof(q->messages));
}

bool swim_piggyback_push(SwimPiggybackQueue* q, const SwimGossipMessage* msg) {
    size_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
    size_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);

    if ((tail + 1) % SWIM_PIGGYBACK_CAPACITY == head) {
        return false;
    }

    q->messages[tail] = *msg;
    atomic_store_explicit(&q->tail, (tail + 1) % SWIM_PIGGYBACK_CAPACITY, memory_order_release);
    return true;
}

bool swim_piggyback_pop(SwimPiggybackQueue* q, SwimGossipMessage* out_msg) {
    size_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
    size_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);

    if (head == tail) {
        return false;
    }

    *out_msg = q->messages[head];
    if (out_msg->infection_count >= SWIM_MAX_INFECTION_COUNT) {
        atomic_store_explicit(&q->head, (head + 1) % SWIM_PIGGYBACK_CAPACITY, memory_order_release);
        return false;
    }

    out_msg->infection_count++;
    q->messages[head].infection_count = out_msg->infection_count;
    atomic_store_explicit(&q->head, (head + 1) % SWIM_PIGGYBACK_CAPACITY, memory_order_release);
    return true;
}

void swim_mpsc_init(SwimMPSCQueue* q) {
    q->stub.atomic_next = NULL;
    memset(&q->stub.payload, 0, sizeof(SwimGossipMessage));
    atomic_init(&q->head, (uintptr_t)&q->stub);
    q->tail = &q->stub;
}

void swim_mpsc_push(SwimMPSCQueue* q, SwimMPSCNode* node) {
    node->atomic_next = NULL;
    SwimMPSCNode* prev = (SwimMPSCNode*)atomic_exchange_explicit(&q->head, (uintptr_t)node, memory_order_acq_rel);
    prev->atomic_next = node;
}

bool swim_mpsc_pop(SwimMPSCQueue* q, SwimGossipMessage* out_msg) {
    SwimMPSCNode* tail = q->tail;
    SwimMPSCNode* next = (SwimMPSCNode*)atomic_load_explicit(&tail->atomic_next, memory_order_acquire);

    if (tail == &q->stub) {
        if (next == NULL) {
            return false;
        }
        q->tail = next;
        tail = next;
        next = (SwimMPSCNode*)atomic_load_explicit(&next->atomic_next, memory_order_acquire);
    }

    if (next != NULL) {
        *out_msg = tail->payload;
        q->tail = next;
        return true;
    }

    SwimMPSCNode* head = (SwimMPSCNode*)atomic_load_explicit(&q->head, memory_order_acquire);
    if (tail != head) {
        return false;
    }

    SwimMPSCNode* dummy = &q->stub;
    dummy->atomic_next = NULL;
    swim_mpsc_push(q, dummy);

    next = (SwimMPSCNode*)atomic_load_explicit(&tail->atomic_next, memory_order_acquire);
    if (next != NULL) {
        *out_msg = tail->payload;
        q->tail = next;
        return true;
    }

    return false;
}

static void* swim_engine_worker(void* arg) {
    SwimEngine* engine = (SwimEngine*)arg;
    SwimGossipMessage msg;

    while (atomic_load(&engine->running)) {
        bool processed = false;

        while (swim_mpsc_pop(&engine->mpsc_queue, &msg)) {
            processed = true;
            pthread_rwlock_wrlock(&engine->rcu_lock);
            SwimNode node;
            if (swim_node_table_find(&engine->node_table, msg.target_id, &node)) {
                if (msg.infection_count < SWIM_MAX_INFECTION_COUNT) {
                    swim_piggyback_push(&engine->piggyback_queue, &msg);
                }
            }
            pthread_rwlock_unlock(&engine->rcu_lock);
        }

        while (swim_piggyback_pop(&engine->piggyback_queue, &msg)) {
            processed = true;
            pthread_rwlock_wrlock(&engine->rcu_lock);
            uint64_t current_epoch = atomic_load(&engine->vclock.current_epoch);
            if (msg.infection_count <= SWIM_MAX_INFECTION_COUNT) {
                swim_piggyback_push(&engine->piggyback_queue, &msg);
            }
            pthread_rwlock_unlock(&engine->rcu_lock);
        }

        if (!processed) {
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 10000000 };
            nanosleep(&ts, NULL);
        }
    }
    return NULL;
}

SwimEngine* swim_engine_create(uint64_t local_id) {
    SwimEngine* engine = (SwimEngine*)malloc(sizeof(SwimEngine));
    if (!engine) {
        return NULL;
    }

    engine->local_id = local_id;
    swim_node_table_init(&engine->node_table);
    swim_piggyback_init(&engine->piggyback_queue);
    swim_mpsc_init(&engine->mpsc_queue);

    engine->vclock.version = 1;
    atomic_init(&engine->vclock.current_epoch, 0);

    atomic_init(&engine->running, false);
    pthread_rwlock_init(&engine->rcu_lock, NULL);

    return engine;
}

bool swim_engine_start(SwimEngine* engine) {
    if (atomic_load(&engine->running)) {
        return false;
    }
    atomic_store(&engine->running, true);
    if (pthread_create(&engine->worker_thread, NULL, swim_engine_worker, engine) != 0) {
        atomic_store(&engine->running, false);
        return false;
    }
    return true;
}

void swim_engine_stop(SwimEngine* engine) {
    if (!atomic_load(&engine->running)) {
        return;
    }
    atomic_store(&engine->running, false);
    pthread_join(engine->worker_thread, NULL);
}

void swim_engine_destroy(SwimEngine* engine) {
    if (!engine) {
        return;
    }
    swim_engine_stop(engine);
    pthread_rwlock_destroy(&engine->rcu_lock);

    SwimMPSCNode* current = (SwimMPSCNode*)atomic_load(&engine->mpsc_queue.head);
    while (current != NULL && current != &engine->mpsc_queue.stub) {
        SwimMPSCNode* next = (SwimMPSCNode*)atomic_load(&current->atomic_next);
        free(current);
        current = next;
    }

    free(engine);
}

bool swim_engine_update_node_state(SwimEngine* engine, uint64_t node_id, SwimNodeState state, uint64_t epoch) {
    uint64_t current_epoch = atomic_load(&engine->vclock.current_epoch);
    if (epoch < current_epoch) {
        return false;
    }

    if (epoch > current_epoch) {
        atomic_store(&engine->vclock.current_epoch, epoch);
    }

    pthread_rwlock_wrlock(&engine->rcu_lock);
    SwimNode node;
    bool exists = swim_node_table_find(&engine->node_table, node_id, &node);

    if (!exists) {
        memset(&node, 0, sizeof(SwimNode));
        node.node_id = node_id;
        node.epoch = epoch;
        node.state = state;
        node.infection_count = 0;
    } else {
        if (epoch < node.epoch) {
            pthread_rwlock_unlock(&engine->rcu_lock);
            return false;
        }
        node.state = state;
        node.epoch = epoch;
    }

    bool success = swim_node_table_insert(&engine->node_table, &node);
    pthread_rwlock_unlock(&engine->rcu_lock);