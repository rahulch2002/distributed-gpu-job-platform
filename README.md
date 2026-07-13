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
