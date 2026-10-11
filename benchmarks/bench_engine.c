#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <math.h>

#include "swim.h"

#define WARMUP_ITERATIONS 10000
#define MEASURE_ITERATIONS 1000000
#define NUM_NODES 100

static uint64_t get_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int compare_uint64(const void *a, const void *b) {
    uint64_t fa = *(const uint64_t *)a;
    uint64_t fb = *(const uint64_t *)b;
    return (fa > fb) - (fa < fb);
}

int main(void) {
    swim_node_t *local_node = swim_node_create("127.0.0.1", 8000);
    if (!local_node) {
        fprintf(stderr, "Failed to create local SWIM node\n");
        return 1;
    }

    swim_node_t *peers[NUM_NODES];
    for (int i = 0; i < NUM_NODES; i++) {
        char ip[32];
        snprintf(ip, sizeof(ip), "192.168.1.%d", i + 1);
        peers[i] = swim_node_create(ip, 8001 + i);
        if (peers[i]) {
            swim_cluster_add_member(local_node, peers[i]);
        }
    }

    uint64_t *latencies = malloc(MEASURE_ITERATIONS * sizeof(uint64_t));
    if (!latencies) {
        fprintf(stderr, "Memory allocation failed\n");
        return 1;
    }

    // Warmup phase
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        swim_node_t *target = peers[i % NUM_NODES];
        swim_message_t *msg = swim_message_create_ping(local_node, target);
        if (msg) {
            swim_message_destroy(msg);
        }
    }

    // Measurement phase
    uint64_t start_time = get_time_ns();

    for (int i = 0; i < MEASURE_ITERATIONS; i++) {
        swim_node_t *target = peers[i % NUM_NODES];
        
        uint64_t op_start = get_time_ns();
        swim_message_t *msg = swim_message_create_ping(local_node, target);
        if (msg) {
            swim_message_destroy(msg);
        }
        uint64_t op_end = get_time_ns();

        latencies[i] = op_end - op_start;
    }

    uint64_t end_time = get_time_ns();
    uint64_t total_time_ns = end_time - start_time;

    qsort(latencies, MEASURE_ITERATIONS, sizeof(uint64_t), compare_uint64);

    uint64_t p50 = latencies[(int)(MEASURE_ITERATIONS * 0.50)];
    uint64_t p99 = latencies[(int)(MEASURE_ITERATIONS * 0.99)];
    uint64_t p999 = latencies[(int)(MEASURE_ITERATIONS * 0.999)];

    double total_seconds = (double)total_time_ns / 1e9;
    double ops_sec = (double)MEASURE_ITERATIONS / total_seconds;

    printf("\n");
    printf("+------------------------+----------------------+\n");
    printf("| Metric                 | Value                |\n");
    printf("+------------------------+----------------------+\n");
    printf("| Total Iterations       | %-20d |\n", MEASURE_ITERATIONS);
    printf("| Total Time (s)         | %-20.6f |\n", total_seconds);
    printf("| Throughput (ops/sec)   | %-20.2f |\n", ops_sec);
    printf("| Latency p50 (ns)       | %-20lu |\n", p50);
    printf("| Latency p99 (ns)       | %-20lu |\n", p99);
    printf("| Latency p99.9 (ns)     | %-20lu |\n", p999);
    printf("+------------------------+----------------------+\n");
    printf("\n");

    free(latencies);
    for (int i = 0; i < NUM_NODES; i++) {
        if (peers[i]) {
            swim_node_destroy(peers[i]);
        }
    }
    swim_node_destroy(local_node);

    return 0;
}