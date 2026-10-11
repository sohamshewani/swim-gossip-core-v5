# swim-gossip-core
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/swim-gossip-core/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/swim-gossip-core/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/swim-gossip-core) [![Stars](https://img.shields.io/github/stars/sohamshewani/swim-gossip-core?style=flat)](https://github.com/sohamshewani/swim-gossip-core)

## Executive Summary & Problem Space

`swim-gossip-core` is an enterprise-grade, zero-allocation C implementation of the SWIM (Structured Weakly-Consistent Infection-Style Process Group Membership) gossip protocol. Designed for ultra-low latency distributed systems, data grids, and consensus layers, it provides decentralized cluster membership, failure detection, and metadata dissemination without centralized coordination.

In large-scale distributed deployments, traditional heartbeat mechanisms saturate network interfaces ($O(N^2)$ messaging overhead) and suffer from false positives caused by transient network congestion. `swim-gossip-core` resolves these bottlenecks through:
- **Randomized Peer Sampling**: Decentralized peer probing reduces network bandwidth to $O(1)$ per node.
- **Phi-Accrual Suspicion Mechanisms**: Probabilistic failure detection distinguishes between network partitions and node crashes, drastically mitigating catastrophic cascading failures.
- **Arena-Based Scratch Memory**: Eliminates runtime heap fragmentation (`malloc`/`free` loops in the critical path) by utilizing deterministic bump-pointer memory arenas.
- **Lockless Multiplexed Polling**: Epoll/kqueue-backed event loops guarantee predictable IO throughput under high concurrency.

---

## Structural ASCII Architecture / Dataflow Diagram

The system separates core protocol state machines from network transport layers, utilizing non-blocking polling mechanics and deterministic memory management.

```
 +-----------------------------------------------------------------------+
 |                     Application / Consensus Layer                     |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                       include/swim/protocol.h                         |
 |              (Packet Encoding, Ping-Req Orchestration)                |
 +-----------------------------------------------------------------------+
            |                                         ^
            v                                         |
 +---------------------+                   +---------------------+
 | include/swim/types.h|                   | src/core/suspicion.c|
 | (Node States, Inc.  |                   | (Phi-Accrual Timer, |
 |  Vector Clocks)     |                   |  Suspicion Matrix)  |
 +---------------------+                   +---------------------+
            |                                         ^
            +-------------------+---------------------+
                                |
                                v
 +-----------------------------------------------------------------------+
 |                         src/core/engine.c                             |
 |           (Tick Loop, State Transitions, Membership Registry)         |
 +-----------------------------------------------------------------------+
         |                                                 ^
         v                                                 |
 +-------------------------------+               +-----------------------+
 |        src/mem/arena.h        |               | src/concurrency/poller.c|
 | (Bump-Pointer Allocator, Zero |               | (Non-blocking Epoll/  |
 |  Heap Allocation on Hot Path) |               |  Kqueue IO Multiplex) |
 +-------------------------------+               +-----------------------+
         |                                                 ^
         +------------------------+------------------------+
                                  |
                                  v
 +-----------------------------------------------------------------------+
 |                         Operating System Kernel                       |
 |                       (Network Socket Buffers)                        |
 +-----------------------------------------------------------------------+
```

---

## Concurrency Model & Memory Invariants

### Thread Safety & Lock Contention Prevention
`swim-gossip-core` employs a thread-per-core or single-threaded event-loop model per engine instance, backed by non-blocking asynchronous multiplexing (`src/concurrency/poller.c`). By avoiding shared mutable state across threads within the hot path, lock contention is entirely eliminated. Cross-thread administrative updates (e.g., node joins) utilize single-producer single-consumer (SPSC) lock-free ring buffers and atomic state transitions (`stdatomic.h`).

### Memory Ownership & Arena Allocation (`src/mem/arena.h`)
To eliminate allocator lock contention and micro-fragmentation during high-frequency gossip rounds:
- **Zero Heap Churn**: All ephemeral protocol messages, buffer serializers, and peer tracking structures are allocated out of pre-initialized thread-local memory arenas.
- **Deterministic Lifetime**: Memory blocks allocated within a tick's arena are invalidated in $O(1)$ time by resetting the arena offset pointer, preventing memory leaks and minimizing cache misses.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation / Path | Algorithmic Complexity | Latency Estimation (p99) | Bottleneck Vector |
|---|---|---|---|
| **Node State Read (Lookup)** | $O(1)$ hash / array index | $< 45\text{ ns}$ | L1/L2 Cache hit rate |
| **Peer Membership Merge** | $O(K)$ ($K = \text{piggyback count}$) | $< 800\text{ ns}$ | Vector clock comparison |
| **Failure Detection Check** | $O(N)$ ($N = \text{cluster size}$) | $< 1.2\,\mu\text{s}$ | Suspicion timer scan |
| **Network Packet Serialization** | $O(M)$ ($M = \text{payload size}$) | $< 250\text{ ns}$ | Arena memory writes |
| **Event Loop Polling** | $O(E)$ ($E = \text{active events}$) | $< 5.0\,\mu\text{s}$ | Kernel context switches |

---

## Security Architecture & Threat Model

`swim-gossip-core` is engineered to operate in untrusted, multi-tenant network environments. 

- **Input Boundary Sanitization (`include/swim/protocol.h`)**: All incoming UDP datagrams undergo strict bounds checking. Payload lengths, header magic bytes, and variable-length string fields are validated against absolute limits before deserialization into arena buffers.
- **Memory Corruption Mitigations**: 
  - Zero use of unbounded string manipulation functions (`strcpy`, `sprintf`).
  - Strict integer overflow checks on packet size calculations.
  - Integration with compiler hardening flags (`-D_FORTIFY_SOURCE=2`, `-fstack-protector-strong`, `-Werror=format-security`).
- **Threat Mitigations**:
  - **Node Impersonation / Ghosting**: Mitigated via incrementing generation numbers and cryptographic node identifiers.
  - **Denial of Service (Malformed Packets)**: Malformed gossip payloads trigger immediate packet drop protocols without modifying cluster state or destabilizing the event loop.

---

## Build, Verification & Benchmarking Recipes

### Prerequisites
- GCC 9.0+ or Clang 10.0+
- GNU Make
- POSIX-compliant system (Linux/macOS)

### 1. Build Production Binaries
Compile the core library, tests, and benchmarks with optimizations enabled (`-O3`):
```bash
make clean
make release
```

### 2. Run Invariant & Unit Tests
Execute the comprehensive test suite (`tests/test_invariants.c`) to verify protocol correctness, suspicion timeouts, and memory safety invariants:
```bash
make test
```

### 3. Execute Performance Benchmarks
Run the high-throughput engine benchmark (`benchmarks/bench_engine.c`) to profile serialization, state merging, and tick latency under simulated cluster loads:
```bash
make benchmark