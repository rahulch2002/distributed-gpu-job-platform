# Distributed GPU Job Scheduling Platform

[![Node.js](https://img.shields.io/badge/Node.js-20-green)]()
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)]()
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)]()
[![Redis](https://img.shields.io/badge/Redis-7-red)]()
[![Kafka](https://img.shields.io/badge/Redpanda-Kafka-orange)]()
[![License](https://img.shields.io/badge/License-MIT-lightgrey)]()

A distributed GPU job scheduling platform that demonstrates the design and implementation of a scalable compute scheduler using distributed systems principles. The platform supports priority-aware scheduling, user fairness, shard-based scheduling, consistent hashing, GPU capability matching, lease-based fault recovery, rate limiting, live job monitoring, and object storage integration while simulating execution across multiple GPU workers.


## Project Overview

This project simulates a production-style distributed GPU compute platform similar to those used by cloud providers for executing AI inference and training workloads.

The system accepts compute jobs through a REST API, stores job metadata in PostgreSQL, distributes work using Redis-backed scheduling queues, assigns jobs to GPU workers through Kafka topics, stores artifacts and results in MinIO, and continuously monitors worker health through lease and heartbeat mechanisms.

To improve scalability and scheduling efficiency, the scheduler combines multiple distributed systems techniques including:

- Priority-aware scheduling
- User-level fair scheduling
- Consistent hashing
- Shard-based queue partitioning
- GPU capability-aware worker selection
- Lease-based fault tolerance
- Distributed rate limiting
- Live job progress streaming
- Runtime metrics collection

Each subsystem is designed independently while communicating through well-defined interfaces, making the architecture modular and horizontally scalable.


## Motivation

Modern AI infrastructure must efficiently distribute thousands of heterogeneous workloads across clusters of GPU resources while maintaining fairness, scalability, and fault tolerance.

Traditional centralized schedulers often become bottlenecks as system size increases. Large-scale platforms therefore partition scheduling responsibilities, distribute queues across shards, cache frequently accessed metadata, and continuously monitor worker health to maximize throughput and resource utilization.

This project was developed to explore these distributed systems concepts through an end-to-end implementation that combines message queues, distributed caching, object storage, relational databases, and scalable scheduling algorithms into a unified platform.

Rather than focusing solely on job execution, the project emphasizes scheduler design, resource allocation strategies, failure recovery, workload isolation, and observability.


## Key Features

- Distributed multi-worker job execution
- Priority-aware scheduling across multiple service tiers
- User-level fair scheduling within each priority level
- Consistent hashing for deterministic shard assignment
- Shard-local scheduling queues
- GPU capability-aware provider selection based on VRAM and GPU class
- Distributed provider metadata cache using Redis
- Lease-based job ownership with automatic recovery of abandoned jobs
- Heartbeat-based worker health monitoring
- Token Bucket rate limiting per user
- Live job status streaming using Server-Sent Events (SSE)
- Automatic ETA estimation and execution progress tracking
- Artifact and result storage using MinIO (S3-compatible object storage)
- Kafka-based asynchronous communication between scheduler and workers
- Metrics aggregation for shard utilization and queue depth
- Horizontally scalable worker architecture



## Tech Stack

| Category | Technologies |
|----------|--------------|
| Language | JavaScript (Node.js) |
| API Framework | Express.js |
| Database | PostgreSQL |
| Distributed Cache | Redis |
| Message Broker | Apache Kafka (Redpanda) |
| Object Storage | MinIO |
| Containerization | Docker, Docker Compose |
| Communication | REST API, Server-Sent Events (SSE) |
| Scheduling | Fair Scheduling, Priority Scheduling, Consistent Hashing |
| Fault Tolerance | Lease Recovery, Heartbeats |


## Distributed Systems Concepts

The scheduler combines several distributed systems techniques that are commonly employed in large-scale compute platforms.

### Priority Scheduling

Jobs are assigned priorities based on user tiers. Higher-priority queues are processed before lower-priority queues while preserving fairness among users within the same tier.

---

### Fair Scheduling

Instead of scheduling individual jobs directly, the scheduler maintains user queues. Each user receives scheduling opportunities in a round-robin fashion, preventing a single user from monopolizing compute resources.

---

### Consistent Hashing

User identifiers are deterministically mapped to scheduler shards using consistent hashing. This minimizes redistribution when the number of shards changes and ensures that jobs belonging to the same user are consistently routed to the same scheduling partition.

---

### Sharding

Scheduling queues are partitioned across multiple shards. Each shard independently manages its own users, workers, and scheduling decisions, allowing throughput to increase horizontally as additional shards are introduced.

---

### GPU Capability Matching

Each worker advertises its available GPU characteristics including VRAM capacity and GPU class. The scheduler selects only those providers capable of executing a job's requested hardware requirements.

---

### Distributed Provider Cache

Provider metadata is cached in Redis to avoid repeated database lookups during scheduling decisions, reducing scheduling latency.

---

### Lease-Based Fault Recovery

When a worker begins executing a job, it acquires a lease that is periodically renewed. If the worker crashes or stops renewing its lease, the scheduler automatically detects the expired lease and safely requeues the job.

---

### Token Bucket Rate Limiting

Each user is assigned a configurable token bucket that controls job submission rate while still permitting short bursts of requests.

---

### Live Job Monitoring

Job execution progress, ETA, queue position, and runtime metrics are streamed to clients using Server-Sent Events, enabling near real-time monitoring.



## High-Level Architecture

```text
                        ┌────────────────────┐
                        │       Client       │
                        └─────────┬──────────┘
                                  │
                             REST Requests
                                  │
                        ┌─────────▼──────────┐
                        │    API Service     │
                        └──────┬─────┬───────┘
                               │     │
                               │     │
                        PostgreSQL   Redis
                               │     │
                               └──┬──┘
                                  │
                          Scheduler Service
                                  │
                     Priority + Fair Scheduling
                                  │
                     Consistent Hashing + Shards
                                  │
                     Kafka Provider Topics
                                  │
            ┌─────────────────────┼──────────────────────┐
            │                     │                      │
      GPU Worker A         GPU Worker B          GPU Worker C
            │                     │                      │
            └─────────────────────┼──────────────────────┘
                                  │
                               MinIO
                         Result & Artifact Store
```



## Folder Structure

```text
Distributed-GPU-Job-Scheduler/
│
├── api/
│   ├── index.js
│   └── Dockerfile
│
├── scheduler/
│   ├── scheduler.js
│   └── Dockerfile
│
├── worker/
│   ├── worker.js
│   └── Dockerfile
│
├── autoscaler/
│   ├── autoscaler.js
│   └── Dockerfile
│
├── utils/
│   ├── hashRing.js
│   ├── providerCache.js
│   ├── providerMatcher.js
│   ├── metrics.js
│   ├── queue.js
│   ├── priority.js
│   └── shard.js
│
├── postgres-init/
│   └── init.sql
│
├── docker-compose.yml
├── package.json
└── README.md
```


# End-to-End Request Lifecycle

The following sequence diagram illustrates the complete lifecycle of a job submitted to the distributed GPU scheduling platform. It captures interactions between the client, API service, distributed scheduler, Redis cache, PostgreSQL, Kafka, GPU workers, and object storage.

The workflow includes:

- Job submission
- Token Bucket rate limiting
- Consistent hashing for shard selection
- Job persistence
- Fair scheduling
- Priority scheduling
- GPU capability matching
- Provider selection
- Lease acquisition and renewal
- Runtime progress updates
- Result storage
- Live status streaming
- Automatic recovery of failed or abandoned jobs

This sequence represents the complete execution path of a job from submission until completion, including all distributed coordination mechanisms.

<img width="1450" height="3275" alt="dLRDRXit4BxlKmnoge4O2TBsq20suifAeZHsb5tPlHW8AEv8OiqbDoHNgL7qtJlyYIWgQo7enUPgPZxEppSpV6qTCsxePEM4amXJrPuwVKh_uEty1jxh9JHyW-qWXLf3Wry6L1ohYWrEgw5Reg4pTzh0H0fKhgfwmXDSz4mvU78ndv6HsGgEJu1PeE-gPOujcKLuKcChRFWzdFOaSstT" src="https://github.com/user-attachments/assets/2f1fd96c-e0fc-4ad1-8cf0-9095d0722554" />


## Class Diagram

The following class diagram illustrates the primary logical components of the distributed GPU scheduling platform and their interactions. Each component encapsulates a distinct responsibility within the system, ranging from request handling and scheduling to worker execution, caching, storage, and infrastructure services.

Unlike a traditional object-oriented class diagram, this diagram models the architectural components of the platform, their public responsibilities, and the relationships between them.

<img width="1243" height="955" alt="RLFRSXCn37ttL-nZcKnd-0AcwGK8f4EQjF2is4hYYNUzI7BAmF3lx7ft7SVqpPBEERf8lc41ab0x9LOSLlUMXUms29hgq-gbskW3ck1iiwzM3cJdgQwkgpi7p1qMzWIrKdoqJGlTiz9zO2wZn4BxPcD_O_LUgHYfvgl51kbaDLP_r8p3jhRonM-ltgdP7llNHa02zpOB8TGNBkthDWW-" src="https://github.com/user-attachments/assets/037532db-e10a-4b03-b728-948f7f2448f0" />

## Class Responsibilities

### APIService

The **APIService** serves as the primary entry point into the platform and is responsible for handling all client interactions.

**Responsibilities**

- Accepts GPU job submission requests through REST endpoints.
- Applies Token Bucket rate limiting to control request frequency.
- Computes the destination shard using consistent hashing.
- Uploads input artifacts to MinIO object storage.
- Persists job metadata in PostgreSQL.
- Enqueues users into shard-local fair scheduling queues maintained in Redis.
- Exposes job status, provider status, fairness, and monitoring endpoints.
- Streams live execution updates using Server-Sent Events (SSE).

---

### Scheduler

The **Scheduler** acts as the central orchestration component responsible for assigning jobs to GPU workers.

**Responsibilities**

- Continuously polls distributed scheduling queues.
- Implements priority-aware scheduling across service tiers.
- Maintains fairness by scheduling users in a round-robin manner.
- Selects eligible providers based on GPU capabilities and current utilization.
- Assigns execution leases to workers.
- Dispatches jobs asynchronously using Kafka.
- Detects expired leases and automatically recovers abandoned jobs.
- Publishes shard utilization and scheduling metrics.

---

### Worker

The **Worker** represents an individual GPU execution node responsible for processing assigned workloads.

**Responsibilities**

- Registers itself as an available GPU provider.
- Advertises GPU capabilities and execution capacity.
- Periodically sends heartbeat updates.
- Consumes assigned jobs from Kafka.
- Acquires and periodically renews execution leases.
- Executes GPU workloads.
- Updates runtime progress in Redis.
- Uploads generated results to MinIO.
- Updates job status in PostgreSQL.
- Requeues users with additional pending jobs.

---

### ProviderMatcher

The **ProviderMatcher** encapsulates the provider selection algorithm used by the scheduler.

**Responsibilities**

- Retrieves candidate providers for the selected shard.
- Filters providers based on GPU class compatibility.
- Validates VRAM requirements.
- Excludes providers operating at full capacity.
- Selects the least-loaded eligible provider.

---

### ProviderCache

The **ProviderCache** provides fast access to provider metadata stored in Redis.

**Responsibilities**

- Retrieves provider metadata from Redis.
- Caches GPU capabilities.
- Stores current provider utilization.
- Minimizes database access during scheduling decisions.

---

### HashRing

The **HashRing** implements consistent hashing for deterministic shard assignment.

**Responsibilities**

- Maps users to scheduling shards.
- Maintains the virtual hash ring.
- Supports virtual nodes for improved load distribution.
- Minimizes remapping when shards are added or removed.

---

### FairQueue

The **FairQueue** manages user-level scheduling fairness.

**Responsibilities**

- Maintains independent queues for each priority and shard.
- Prevents duplicate user entries.
- Supports round-robin scheduling across users.
- Requeues users with remaining pending jobs.

---

### TokenBucketLimiter

The **TokenBucketLimiter** enforces per-user request rate limiting.

**Responsibilities**

- Maintains token buckets for each user.
- Refills tokens at a configurable rate.
- Allows burst traffic within bucket capacity.
- Rejects requests when available tokens are exhausted.

---

### RedisCache

The **RedisCache** functions as the distributed in-memory cache for frequently accessed scheduling metadata.

**Responsibilities**

- Stores provider metadata.
- Maintains runtime execution progress.
- Stores distributed scheduling queues.
- Tracks active users.
- Maintains shard metrics.
- Reduces scheduling latency by avoiding repeated database queries.

---

### PostgreSQL

**PostgreSQL** acts as the persistent metadata repository for the platform.

**Responsibilities**

- Persists job metadata.
- Stores provider information.
- Maintains lease ownership.
- Records execution history.
- Stores rate limiting state.
- Maintains scheduling metadata required for recovery.

---

### KafkaBroker

The **KafkaBroker** provides asynchronous communication between the scheduler and GPU workers.

**Responsibilities**

- Delivers jobs to assigned workers.
- Decouples scheduling from execution.
- Enables scalable asynchronous processing.
- Supports independent scaling of scheduler and worker services.

---

### MinIO

**MinIO** provides S3-compatible object storage for job artifacts and execution results.

**Responsibilities**

- Stores uploaded input artifacts.
- Stores generated execution outputs.
- Simulates cloud object storage for distributed workloads.

---

### Autoscaler

The **Autoscaler** continuously monitors cluster utilization to support future horizontal scaling.

**Responsibilities**

- Collects shard-level scheduling metrics.
- Monitors queue depth.
- Tracks provider utilization.
- Detects overloaded shards.
- Provides scaling decisions for worker allocation.


### Architectural Design Principles

The class interactions collectively implement several software engineering and distributed systems design principles:

- **Single Responsibility Principle (SRP):** Each component performs a well-defined task.
- **Loose Coupling:** Services communicate through Redis, Kafka, and PostgreSQL rather than direct dependencies.
- **Asynchronous Messaging:** Kafka decouples scheduling from execution.
- **Distributed Caching:** Redis minimizes repeated database access.
- **Strategy Pattern:** ProviderMatcher encapsulates provider selection logic.
- **Consistent Hashing:** HashRing distributes users across shards while minimizing redistribution.
- **Fault Tolerance:** Lease-based ownership enables automatic recovery of interrupted jobs.
- **Horizontal Scalability:** Workers and shards can be added independently without modifying application logic.
