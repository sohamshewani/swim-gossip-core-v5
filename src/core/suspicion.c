#include "core/suspicion.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <time.h>
#include <stdatomic.h>

#define PHI_WINDOW_SIZE 100
#define MAX_TRACKED_NODES 1024

typedef struct {
    uint64_t node_id;
    uint64_t *heartbeat_timestamps;
    size_t head;
    size_t tail;
    size_t count;
    uint64_t last_heartbeat;
    bool active;
} SuspicionTracker;

struct SuspicionManager {
    SuspicionTracker trackers[MAX_TRACKED_NODES];
    size_t capacity;
    double suspicion_threshold;
    uint64_t min_suspicion_duration_ms;
};

static uint64_t get_current_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
}

static int compare_uint64(const void *a, const void *b) {
    uint64_t fa = *(const uint64_t *)a;
    uint64_t fb = *(const uint64_t *)b;
    return (fa > fb) - (fa < fb);
}

SuspicionManager* suspicion_manager_create(double threshold, uint64_t min_duration_ms) {
    SuspicionManager *manager = (SuspicionManager *)malloc(sizeof(SuspicionManager));
    if (!manager) {
        return NULL;
    }
    memset(manager, 0, sizeof(SuspicionManager));
    manager->capacity = MAX_TRACKED_NODES;
    manager->suspicion_threshold = threshold;
    manager->min_suspicion_duration_ms = min_duration_ms;

    for (size_t i = 0; i < manager->capacity; i++) {
        manager->trackers[i].heartbeat_timestamps = (uint64_t *)malloc(sizeof(uint64_t) * PHI_WINDOW_SIZE);
        if (!manager->trackers[i].heartbeat_timestamps) {
            for (size_t j = 0; j < i; j++) {
                free(manager->trackers[j].heartbeat_timestamps);
            }
            free(manager);
            return NULL;
        }
        memset(manager->trackers[i].heartbeat_timestamps, 0, sizeof(uint64_t) * PHI_WINDOW_SIZE);
    }

    return manager;
}

void suspicion_manager_destroy(SuspicionManager *manager) {
    if (!manager) {
        return;
    }
    for (size_t i = 0; i < manager->capacity; i++) {
        free(manager->trackers[i].heartbeat_timestamps);
    }
    free(manager);
}

static SuspicionTracker* find_or_create_tracker(SuspicionManager *manager, uint64_t node_id) {
    SuspicionTracker *free_slot = NULL;
    for (size_t i = 0; i < manager->capacity; i++) {
        if (manager->trackers[i].active && manager->trackers[i].node_id == node_id) {
            return &manager->trackers[i];
        }
        if (!manager->trackers[i].active && !free_slot) {
            free_slot = &manager->trackers[i];
        }
    }

    if (free_slot) {
        free_slot->node_id = node_id;
        free_slot->head = 0;
        free_slot->tail = 0;
        free_slot->count = 0;
        free_slot->last_heartbeat = get_current_time_ms();
        free_slot->active = true;
        memset(free_slot->heartbeat_timestamps, 0, sizeof(uint64_t) * PHI_WINDOW_SIZE);
        return free_slot;
    }

    return NULL;
}

void suspicion_manager_update_heartbeat(SuspicionManager *manager, uint64_t node_id) {
    if (!manager) {
        return;
    }

    uint64_t now = get_current_time_ms();
    SuspicionTracker *tracker = find_or_create_tracker(manager, node_id);
    if (!tracker) {
        return;
    }

    if (tracker->last_heartbeat > 0) {
        uint64_t interval = now - tracker->last_heartbeat;
        
        if (tracker->count < PHI_WINDOW_SIZE) {
            tracker->heartbeat_timestamps[tracker->tail] = interval;
            tracker->tail = (tracker->tail + 1) % PHI_WINDOW_SIZE;
            tracker->count++;
        } else {
            tracker->heartbeat_timestamps[tracker->tail] = interval;
            tracker->tail = (tracker->tail + 1) % PHI_WINDOW_SIZE;
            tracker->head = (tracker->head + 1) % PHI_WINDOW_SIZE;
        }
    }

    tracker->last_heartbeat = now;
}

double suspicion_manager_calculate_phi(SuspicionManager *manager, uint64_t node_id) {
    if (!manager) {
        return 0.0;
    }

    SuspicionTracker *tracker = NULL;
    for (size_t i = 0; i < manager->capacity; i++) {
        if (manager->trackers[i].active && manager->trackers[i].node_id == node_id) {
            tracker = &manager->trackers[i];
            break;
        }
    }

    if (!tracker || tracker->count == 0) {
        return 0.0;
    }

    uint64_t now = get_current_time_ms();
    uint64_t time_since_last_heartbeat = now - tracker->last_heartbeat;

    uint64_t intervals[PHI_WINDOW_SIZE];
    size_t count = tracker->count;
    for (size_t i = 0; i < count; i++) {
        intervals[i] = tracker->heartbeat_timestamps[(tracker->head + i) % PHI_WINDOW_SIZE];
    }

    uint64_t sum = 0;
    for (size_t i = 0; i < count; i++) {
        sum += intervals[i];
    }
    double mean = (double)sum / (double)count;

    double variance = 0.0;
    for (size_t i = 0; i < count; i++) {
        double diff = (double)intervals[i] - mean;
        variance += diff * diff;
    }
    variance /= (double)count;
    double stddev = sqrt(variance);

    if (stddev < 1.0) {
        stddev = 1.0;
    }

    double diff = (double)time_since_last_heartbeat;
    double p = 0.5 * (1.0 + erf((diff - mean) / (stddev * 1.41421356237309504880))));
    
    if (p >= 1.0) {
        return 308.0;
    }
    if (p <= 0.0) {
        return 0.0;
    }

    return -log10(1.0 - p);
}

bool suspicion_manager_is_suspect(SuspicionManager *manager, uint64_t node_id) {
    if (!manager) {
        return false;
    }
    double phi = suspicion_manager_calculate_phi(manager, node_id);
    return phi >= manager->suspicion_threshold;
}

void suspicion_manager_remove_node(SuspicionManager *manager, uint64_t node_id) {
    if (!manager) {
        return;
    }
    for (size_t i = 0; i < manager->capacity; i++) {
        if (manager->trackers[i].active && manager->trackers[i].node_id == node_id) {
            manager->trackers[i].active = false;
            manager->trackers[i].count = 0;
            manager->trackers[i].head = 0;
            manager->trackers[i].tail = 0;
            manager->trackers[i].last_heartbeat = 0;
            break;
        }
    }
}