#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdatomic.h>
#include <pthread.h>
#include <time.h>

#define MAX_EVENTS 64
#define CACHE_LINE_SIZE 64
#define HASH_MAP_CAPACITY 1024
#define PIGGYBACK_QUEUE_CAPACITY 256
#define MAX_MESSAGE_SIZE 1400

typedef enum {
    NODE_STATE_ALIVE = 0,
    NODE_STATE_SUSPECT = 1,
    NODE_STATE_DEAD = 2,
    NODE_STATE_LEFT = 3
} SwimNodeState;

typedef struct {
    uint64_t node_id;
    uint32_t ip;
    uint16_t port;
    _Atomic uint32_t state;
    _Atomic uint64_t epoch;
    uint64_t incarnation;
    uint8_t pb_count;
    uint8_t padding[CACHE_LINE_SIZE - sizeof(uint64_t) - sizeof(uint32_t) - sizeof(uint16_t) - sizeof(_Atomic uint32_t) - sizeof(_Atomic uint64_t) - sizeof(uint64_t) - sizeof(uint8_t)];
} __attribute__((aligned(CACHE_LINE_SIZE))) SwimNode;

typedef struct {
    uint64_t source_id;
    uint64_t target_id;
    uint32_t state;
    uint64_t incarnation;
    uint8_t infection_count;
} GossipMessage;

typedef struct {
    GossipMessage buffer[PIGGYBACK_QUEUE_CAPACITY];
    _Atomic uint32_t head;
    _Atomic uint32_t tail;
} MPSC_PiggybackQueue;

typedef struct {
    int epoll_fd;
    int listen_fd;
    _Atomic bool running;
    pthread_t worker_thread;
    MPSC_PiggybackQueue mutation_queue;
    SwimNode node_table[HASH_MAP_CAPACITY];
    uint8_t table_occupied[HASH_MAP_CAPACITY];
    pthread_rwlock_t table_lock;
} SwimPoller;

static inline uint64_t hash_node_id(uint64_t id) {
    id ^= id >> 33;
    id *= 0xff51afd7ed558ccdULL;
    id ^= id >> 33;
    id *= 0xc4ceb9fe1a85ec53ULL;
    id ^= id >> 33;
    return id % HASH_MAP_CAPACITY;
}

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void swim_poller_init_queue(MPSC_PiggybackQueue *q) {
    atomic_init(&q->head, 0);
    atomic_init(&q->tail, 0);
}

bool swim_poller_enqueue(MPSC_PiggybackQueue *q, const GossipMessage *msg) {
    uint32_t tail = atomic_load_explicit(&q->tail, memory_order_relaxed);
    uint32_t head = atomic_load_explicit(&q->head, memory_order_acquire);
    
    if ((tail - head) >= PIGGYBACK_QUEUE_CAPACITY) {
        return false;
    }
    
    q->buffer[tail % PIGGYBACK_QUEUE_CAPACITY] = *msg;
    atomic_store_explicit(&q->tail, tail + 1, memory_order_release);
    return true;
}

bool swim_poller_dequeue(MPSC_PiggybackQueue *q, GossipMessage *msg) {
    uint32_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
    uint32_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);
    
    if (head == tail) {
        return false;
    }
    
    *msg = q->buffer[head % PIGGYBACK_QUEUE_CAPACITY];
    atomic_store_explicit(&q->head, head + 1, memory_order_release);
    return true;
}

SwimPoller* swim_poller_create(uint16_t port) {
    SwimPoller *poller = (SwimPoller*)malloc(sizeof(SwimPoller));
    if (!poller) return NULL;

    poller->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (poller->epoll_fd == -1) {
        free(poller);
        return NULL;
    }

    poller->listen_fd = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (poller->listen_fd == -1) {
        close(poller->epoll_fd);
        free(poller);
        return NULL;
    }

    int opt = 1;
    setsockopt(poller->listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (bind(poller->listen_fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        close(poller->listen_fd);
        close(poller->epoll_fd);
        free(poller);
        return NULL;
    }

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = poller->listen_fd;

    if (epoll_ctl(poller->epoll_fd, EPOLL_CTL_ADD, poller->listen_fd, &ev) == -1) {
        close(poller->listen_fd);
        close(poller->epoll_fd);
        free(poller);
        return NULL;
    }

    swim_poller_init_queue(&poller->mutation_queue);
    memset(poller->node_table, 0, sizeof(poller->node_table));
    memset(poller->table_occupied, 0, sizeof(poller->table_occupied));
    pthread_rwlock_init(&poller->table_lock, NULL);
    atomic_store(&poller->running, false);

    return poller;
}

void swim_poller_destroy(SwimPoller *poller) {
    if (!poller) return;
    atomic_store(&poller->running, false);
    close(poller->listen_fd);
    close(poller->epoll_fd);
    pthread_rwlock_destroy(&poller->table_lock);
    free(poller);
}

void swim_poller_handle_io(SwimPoller *poller) {
    char buf[MAX_MESSAGE_SIZE];
    struct sockaddr_in cliaddr;
    socklen_t len = sizeof(cliaddr);

    while (true) {
        ssize_t n = recvfrom(poller->listen_fd, buf, sizeof(buf), 0, (struct sockaddr*)&cliaddr, &len);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            break;
        }
        
        GossipMessage msg;
        if (n >= (ssize_t)sizeof(GossipMessage)) {
            memcpy(&msg, buf, sizeof(GossipMessage));
            
            pthread_rwlock_wrlock(&poller->table_lock);
            uint64_t idx = hash_node_id(msg.target_id);
            uint64_t start_idx = idx;
            bool found = false;

            while (poller->table_occupied[idx]) {
                if (poller->node_table[idx].node_id == msg.target_id) {
                    found = true;
                    break;
                }
                idx = (idx + 1) % HASH_MAP_CAPACITY;
                if (idx == start_idx) break;
            }

            if (found) {
                uint64_t current_incarnation = poller->node_table[idx].incarnation;
                if (msg.incarnation > current_incarnation) {
                    poller->node_table[idx].incarnation = msg.incarnation;
                    atomic_store(&poller->node_table[idx].state, msg.state);
                    atomic_store(&poller->node_table[idx].epoch, atomic_load(&poller->node_table[idx].epoch) + 1);
                } else if (msg.incarnation == current_incarnation) {
                    uint32_t current_state = atomic_load(&poller->node_table[idx].state);
                    if (msg.state > current_state) {
                        atomic_store(&poller->node_table[idx].state, msg.state);
                    }
                }
            }
            pthread_rwlock_unlock(&poller->table_lock);
        }
    }
}

void* swim_poller_worker_loop(void *arg) {
    SwimPoller *poller = (SwimPoller*)arg;
    struct epoll_event events[MAX_EVENTS];
    
    atomic_store(&poller->running, true);

    while (atomic_load(&poller->running)) {
        int nfds = epoll_wait(poller->epoll_fd, events, MAX_EVENTS, 100);
        if (nfds < 0) {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == poller->listen_fd) {
                swim_poller_handle_io(poller);
            }
        }

        GossipMessage mutation;
        while (swim_poller_dequeue(&poller->mutation_queue, &mutation)) {
            if (mutation.infection_count < 3) {
                mutation.infection_count++;
                swim_poller_enqueue(&poller->mutation_queue, &mutation);
            }
        }
    }

    return NULL;
}

bool swim_poller_start(SwimPoller *poller) {
    if (pthread_create(&poller->worker_thread, NULL, swim_poller_worker_loop, poller) != 0) {
        return false;
    }
    return true;
}

void swim_poller_stop(SwimPoller *poller) {
    atomic_store(&poller->running, false);
    pthread_join(poller->worker_thread, NULL);
}

bool swim_poller_upsert_node(SwimPoller *poller, const SwimNode *node) {
    pthread_rwlock_wrlock(&poller->table_lock);
    
    uint64_t idx = hash_node_id(node->node_id);
    uint64_t start_idx = idx;
    uint64_t dist = 0;

    while (poller->table_occupied[idx]) {
        if (poller->node_table[idx].node_id == node->node_id) {
            poller->node_table[idx].ip = node->ip;
            poller->node_table[idx].port = node->port;
            atomic_store(&poller->node_table[idx].state, atomic_load(&node->state));
            poller->node_table[idx].incarnation = node->incarnation;
            pthread_rwlock_unlock(&poller->table_lock);
            return true;
        }

        uint64_t existing_hash = hash_node_id(poller->node_table[idx].node_id);
        uint64_t existing_dist = (idx + HASH_MAP_CAPACITY - existing_hash) % HASH_MAP_CAPACITY;

        if (existing_dist < dist) {
            SwimNode temp_node = poller->node_table[idx];
            poller->node_table[idx] = *node;
            *(SwimNode*)node = temp_node;
            dist = existing_dist;
        }

        idx = (idx + 1) % HASH_MAP_CAPACITY;
        dist++;
        if (idx == start_idx) {
            pthread_rwlock_unlock(&poller->table_lock);
            return false;
        }
    }

    poller->node_table[idx] = *node;
    poller->table_occupied[idx] = 1;

    pthread_rwlock_unlock(&poller->table_lock);
    return true;
}

bool swim_poller_get_node(SwimPoller *poller, uint64_t node_id, SwimNode *out_node) {
    pthread_rwlock_rdlock(&poller->table_lock);
    
    uint64_t idx = hash_node_id(node_id);
    uint64_t start_idx = idx;

    while (poller->table_occupied[idx]) {
        if (poller->node_table[idx].node_id == node_id) {
            *out_node = poller->node_table[idx];
            pthread_rwlock_unlock(&poller->table_lock);
            return true;
        }
        idx = (idx + 1) % HASH_MAP_CAPACITY;
        if (idx == start_idx) break;
    }

    pthread_rwlock_unlock(&poller->table_lock);
    return false;
}