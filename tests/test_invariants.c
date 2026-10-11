#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define NODE_ID_LEN 16
#define MAX_NODES 1024
#define PROTOCOL_PERIOD_MS 10

typedef enum {
    NODE_ALIVE = 0,
    NODE_SUSPECT,
    NODE_DEAD,
    NODE_LEFT
} swim_state_t;

typedef struct {
    char id[NODE_ID_LEN];
    uint64_t incarnation;
    swim_state_t state;
    uint64_t last_state_change;
} swim_node_t;

typedef struct {
    char id[NODE_ID_LEN];
    uint64_t incarnation;
    swim_state_t state;
} swim_member_t;

typedef struct {
    char sender_id[NODE_ID_LEN];
    swim_member_t members[MAX_NODES];
    size_t member_count;
} swim_message_t;

typedef struct {
    char id[NODE_ID_LEN];
    swim_node_t nodes[MAX_NODES];
    size_t node_count;
    uint64_t incarnation;
    pthread_rwlock_t lock;
    uint32_t seed;
} swim_cluster_t;

static uint64_t current_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
}

void swim_cluster_init(swim_cluster_t *cluster, const char *id) {
    memset(cluster, 0, sizeof(swim_cluster_t));
    snprintf(cluster->id, NODE_ID_LEN, "%s", id);
    cluster->incarnation = 0;
    pthread_rwlock_init(&cluster->lock, NULL);
    cluster->seed = (uint32_t)time(NULL) ^ (uint32_t)(uintptr_t)cluster;

    snprintf(cluster->nodes[0].id, NODE_ID_LEN, "%s", id);
    cluster->nodes[0].incarnation = 0;
    cluster->nodes[0].state = NODE_ALIVE;
    cluster->nodes[0].last_state_change = current_time_ms();
    cluster->node_count = 1;
}

void swim_cluster_destroy(swim_cluster_t *cluster) {
    pthread_rwlock_destroy(&cluster->lock);
}

int swim_node_upsert(swim_cluster_t *cluster, const char *id, uint64_t incarnation, swim_state_t state) {
    pthread_rwlock_wrlock(&cluster->lock);
    
    if (strncmp(cluster->id, id, NODE_ID_LEN) == 0) {
        if (incarnation > cluster->incarnation) {
            cluster->incarnation = incarnation;
        }
        pthread_rwlock_unlock(&cluster->lock);
        return 0;
    }

    size_t i;
    for (i = 0; i < cluster->node_count; i++) {
        if (strncmp(cluster->nodes[i].id, id, NODE_ID_LEN) == 0) {
            swim_node_t *node = &cluster->nodes[i];
            
            if (incarnation > node->incarnation) {
                node->incarnation = incarnation;
                node->state = state;
                node->last_state_change = current_time_ms();
            } else if (incarnation == node->incarnation) {
                if (node->state == NODE_ALIVE && state == NODE_SUSPECT) {
                    node->state = NODE_SUSPECT;
                    node->last_state_change = current_time_ms();
                } else if (node->state == NODE_SUSPECT && state == NODE_ALIVE) {
                    node->state = NODE_ALIVE;
                    node->last_state_change = current_time_ms();
                } else if (state == NODE_DEAD) {
                    node->state = NODE_DEAD;
                    node->last_state_change = current_time_ms();
                } else if (state == NODE_LEFT) {
                    node->state = NODE_LEFT;
                    node->last_state_change = current_time_ms();
                }
            }
            pthread_rwlock_unlock(&cluster->lock);
            return 0;
        }
    }

    if (cluster->node_count >= MAX_NODES) {
        pthread_rwlock_unlock(&cluster->lock);
        return -1;
    }

    snprintf(cluster->nodes[cluster->node_count].id, NODE_ID_LEN, "%s", id);
    cluster->nodes[cluster->node_count].incarnation = incarnation;
    cluster->nodes[cluster->node_count].state = state;
    cluster->nodes[cluster->node_count].last_state_change = current_time_ms();
    cluster->node_count++;

    pthread_rwlock_unlock(&cluster->lock);
    return 0;
}

void swim_pack_message(swim_cluster_t *cluster, swim_message_t *msg) {
    pthread_rwlock_rdlock(&cluster->lock);
    snprintf(msg->sender_id, NODE_ID_LEN, "%s", cluster->id);
    
    msg->member_count = cluster->node_count;
    if (msg->member_count > MAX_NODES) {
        msg->member_count = MAX_NODES;
    }

    size_t i;
    for (i = 0; i < msg->member_count; i++) {
        snprintf(msg->members[i].id, NODE_ID_LEN, "%s", cluster->nodes[i].id);
        msg->members[i].incarnation = cluster->nodes[i].incarnation;
        msg->members[i].state = cluster->nodes[i].state;
    }
    pthread_rwlock_unlock(&cluster->lock);
}

void swim_unpack_message(swim_cluster_t *cluster, const swim_message_t *msg) {
    size_t i;
    for (i = 0; i < msg->member_count; i++) {
        swim_node_upsert(cluster, msg->members[i].id, msg->members[i].incarnation, msg->members[i].state);
    }
    swim_node_upsert(cluster, msg->sender_id, 0, NODE_ALIVE);
}

void swim_check_timeouts(swim_cluster_t *cluster, uint64_t suspect_timeout_ms, uint64_t dead_timeout_ms) {
    uint64_t now = current_time_ms();
    pthread_rwlock_wrlock(&cluster->lock);
    size_t i;
    for (i = 0; i < cluster->node_count; i++) {
        swim_node_t *node = &cluster->nodes[i];
        if (strncmp(cluster->id, node->id, NODE_ID_LEN) == 0) continue;

        if (node->state == NODE_SUSPECT && (now - node->last_state_change > suspect_timeout_ms)) {
            node->state = NODE_DEAD;
            node->last_state_change = now;
        } else if (node->state == NODE_DEAD && (now - node->last_state_change > dead_timeout_ms)) {
            node->state = NODE_LEFT;
            node->last_state_change = now;
        }
    }
    pthread_rwlock_unlock(&cluster->lock);
}

int swim_get_random_peer(swim_cluster_t *cluster, char *peer_id_out) {
    pthread_rwlock_rdlock(&cluster->lock);
    if (cluster->node_count <= 1) {
        pthread_rwlock_unlock(&cluster->lock);
        return -1;
    }

    uint32_t idx = 1 + (rand_r(&cluster->seed) % (cluster->node_count - 1));
    snprintf(peer_id_out, NODE_ID_LEN, "%s", cluster->nodes[idx].id);
    pthread_rwlock_unlock(&cluster->lock);
    return 0;
}

void test_unit_basic_lifecycle(void) {
    printf("[RUN] test_unit_basic_lifecycle\n");
    swim_cluster_t cluster;
    swim_cluster_init(&cluster, "node-1");

    assert(cluster.node_count == 1);
    assert(cluster.nodes[0].state == NODE_ALIVE);

    assert(swim_node_upsert(&cluster, "node-2", 1, NODE_ALIVE) == 0);
    assert(cluster.node_count == 2);

    assert(swim_node_upsert(&cluster, "node-2", 1, NODE_SUSPECT) == 0);
    
    pthread_rwlock_rdlock(&cluster.lock);
    assert(cluster.nodes[1].state == NODE_SUSPECT);
    pthread_rwlock_unlock(&cluster.lock);

    assert(swim_node_upsert(&cluster, "node-2", 2, NODE_ALIVE) == 0);
    pthread_rwlock_rdlock(&cluster.lock);
    assert(cluster.nodes[1].state == NODE_ALIVE);
    assert(cluster.nodes[1].incarnation == 2);
    pthread_rwlock_unlock(&cluster.lock);

    swim_cluster_destroy(&cluster);
    printf("[OK]  test_unit_basic_lifecycle\n");
}

void test_unit_incarnation_superseding(void) {
    printf("[RUN] test_unit_incarnation_superseding\n");
    swim_cluster_t cluster;
    swim_cluster_init(&cluster, "node-1");

    assert(swim_node_upsert(&cluster, "node-2", 5, NODE_DEAD) == 0);
    
    pthread_rwlock_rdlock(&cluster.lock);
    assert(cluster.nodes[1].state == NODE_DEAD);
    assert(cluster.nodes[1].incarnation == 5);
    pthread_rwlock_unlock(&cluster.lock);

    assert(swim_node_upsert(&cluster, "node-2", 3, NODE_ALIVE) == 0);
    
    pthread_rwlock_rdlock(&cluster.lock);
    assert(cluster.nodes[1].state == NODE_DEAD);
    assert(cluster.nodes[1].incarnation == 5);
    pthread_rwlock_unlock(&cluster.lock);

    assert(swim_node_upsert(&cluster, "node-2", 5, NODE_ALIVE) == 0);
    
    pthread_rwlock_rdlock(&cluster.lock);
    assert(cluster.nodes[1].state == NODE_ALIVE);
    assert(cluster.nodes[1].incarnation == 5);
    pthread_rwlock_unlock(&cluster.lock);

    swim_cluster_destroy(&cluster);
    printf("[OK]  test_unit_incarnation_superseding\n");
}

void test_boundary_max_nodes(void) {
    printf("[RUN] test_boundary_max_nodes\n");
    swim_cluster_t cluster;
    swim_cluster_init(&cluster, "node-0");

    size_t i;
    for (i = 1; i < MAX_NODES; i++) {
        char id[NODE_ID_LEN];
        snprintf(id, NODE_ID_LEN, "node-%zu", i);
        assert(swim_node_upsert(&cluster, id, 1, NODE_ALIVE) == 0);
    }

    assert(cluster.node_count == MAX_NODES);
    assert(swim_node_upsert(&cluster, "node-overflow", 1, NODE_ALIVE) == -1);

    swim_cluster_destroy(&cluster);
    printf("[OK]  test_boundary_max_nodes\n");
}

void test_failure_recovery_timeouts(void) {
    printf("[RUN] test_failure_recovery_timeouts\n");
    swim_cluster_t cluster;
    swim_cluster_