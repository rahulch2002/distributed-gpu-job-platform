# GPU Marketplace Scheduler — Project Context

## Overview

This project is a distributed GPU job scheduling and orchestration platform inspired by systems such as:

- Kubernetes Jobs
- AWS Batch
- Slurm
- Modal
- RunPod
- Ray
- Distributed ML inference platforms

The architecture evolved from a simple queue into a distributed, fault-tolerant, fair, priority-aware scheduling system.

---

# High-Level Architecture

```text
Client/API
   ↓
Priority Queue
   ↓
Shard Selection
   ↓
Least-loaded provider inside shard
   ↓
Kafka Topic Dispatch
   ↓
Worker Execution
   ↓
MinIO Result Storage
```

---

# Tech Stack

## Backend
- Node.js
- Express.js

## Databases / Storage
- PostgreSQL
- Redis
- MinIO (S3-compatible object storage)

## Messaging / Streaming
- Redpanda
- KafkaJS

## Infrastructure
- Docker
- Docker Compose

---

# Core Services

## 1. API Service (`index.js`)

Responsible for:

- job submission
- rate limiting
- ETA computation
- SSE job streaming
- fairness queue insertion
- queue position tracking
- provider debugging
- job inspection

### Important Features

#### Token Bucket Rate Limiting

Per-user token bucket implementation:

- max tokens: 10
- refill rate: 10/minute

Tables used:

```sql
rate_limits
```

---

#### Job Submission

Jobs include:

- user ID
- tier
- job priority
- estimated runtime
- shard assignment

Job artifacts are uploaded to MinIO.

---

#### Priority Tiers

```js
const PRIORITIES = {
  free: 1,
  premium: 5,
  urgent: 10
};
```

---

#### Fair Scheduling Queues

Redis queues:

```text
fair_queue:<priority>:<shard>
```

Examples:

```text
fair_queue:10:0
fair_queue:5:1
fair_queue:1:2
```

---

#### SSE Streaming

Endpoint:

```text
/job/:id/stream
```

Streams:

- status
- ETA
- queue position
- attempts
- failures
- lease owner
- timestamps

---

#### ETA Computation

ETA model includes:

1. runtime of current job
2. pending workload ahead
3. remaining runtime of running jobs
4. total worker capacity

Formula:

```text
ETA =
  own_runtime
  + pending_workload_ahead / total_capacity
  + remaining_running_workload / total_capacity
```

---

# 2. Scheduler Service (`scheduler.js`)

Responsible for:

- fairness
- weighted priority scheduling
- shard-aware routing
- provider selection
- lease assignment
- recovery
- Kafka dispatching

---

# Scheduling Architecture

## Current Architecture

```text
Priority Queue
   ↓
Shard Selection
   ↓
Least-loaded provider inside shard
```

---

# Fair Scheduling

Goal:

Prevent one user from monopolizing the cluster.

Example:

Instead of:

```text
alice alice alice alice
```

Scheduler produces:

```text
alice bob alice bob
```

Implementation:

- Redis set for deduplication
- Redis queue for round-robin scheduling

Redis structures:

```text
active_users
fair_queue:<priority>:<shard>
```

---

# Weighted Priority Scheduling

Implemented weighted polling:

```js
const PRIORITY_ORDER = [
  10,
  10,
  5,
  5,
  1
];
```

Meaning:

- urgent jobs get more polling opportunities
- premium gets medium weight
- free still eventually executes

This avoids starvation.

---

# User Quotas

Per-user concurrent execution cap:

```js
const USER_QUOTA = 3;
```

A user can queue unlimited jobs.

But only 3 can simultaneously be:

- assigned
- running

Remaining jobs wait fairly.

---

# Shard-Aware Scheduling

Users are deterministically routed to shards.

Hash function:

```js
function getShard(userId)
```

Conceptually:

```text
hash(userId) % NUM_SHARDS
```

Benefits:

- horizontal scalability
- future DB sharding support
- cache locality
- reduced scheduler pressure

---

# Provider Selection

Inside each shard:

```text
least-loaded provider wins
```

Provider metadata stored in Redis:

```text
providers:shard:<id>
provider:<id>:capacity
provider:<id>:load
```

---

# Lease-Based Execution

Distributed execution safety mechanism.

Job columns:

```text
leased_by
leased_until
```

Scheduler assigns lease.

Workers heartbeat lease periodically.

If worker dies:

- lease expires
- recovery loop detects expiration
- job becomes pending again
- scheduler reschedules

This prevents:

- duplicate execution
- zombie workers
- permanently stuck jobs

---

# Recovery Loop

Scheduler periodically checks:

```sql
status IN ('assigned','running')
AND leased_until < NOW()
```

Expired jobs:

- reset to pending
- lease cleared
- user requeued

---

# Kafka Dispatching

Each provider has its own topic:

```text
provider-<providerId>
```

Scheduler publishes jobs.

Workers consume only their provider topic.

Architecture:

```text
Scheduler
  ↓
Kafka Topic
  ↓
Specific Worker
```

---

# 3. Worker Service (`worker.js`)

Responsible for:

- provider registration
- Kafka consumption
- lease heartbeats
- execution
- MinIO uploads
- retries
- failure handling
- user requeueing

---

# Worker Registration

Workers:

- register in Postgres
- register in Redis
- join a shard
- advertise capacity

Redis registration:

```text
providers:shard:<id>
provider:<id>:capacity
provider:<id>:load
```

---

# Worker Capacity Model

Current simplified model:

```js
capacity = 2
```

Scheduler ensures:

```text
load < capacity
```

before assigning jobs.

---

# Lease Heartbeats

Workers periodically extend:

```text
leased_until
```

If heartbeats stop:

- scheduler assumes worker failure
- recovery begins

---

# Retry System

On failure:

```text
attempts += 1
```

Jobs become:

- pending again
OR
- permanently failed

based on:

```text
max_attempts
```

---

# MinIO Usage

MinIO stores:

## Input Artifacts

```text
artifacts/<jobId>.txt
```

## Results

```text
results/<jobId>.txt
```

Workers remain stateless.

---

# PostgreSQL Schema Concepts

## Jobs Table

Important columns:

```text
id
user_id
status
attempts
job_priority
estimated_runtime
leased_by
leased_until
started_at
completed_at
failure_reason
result_path
shard_id
```

---

## Providers Table

Important columns:

```text
id
name
status
max_capacity
last_heartbeat
shard_id
```

---

# Redis Structures

## Active Users

```text
active_users
```

Used to avoid duplicate user queue entries.

---

## Fair Queues

```text
fair_queue:<priority>:<shard>
```

Examples:

```text
fair_queue:10:0
fair_queue:5:1
fair_queue:1:2
```

---

## Provider State

```text
providers:shard:<id>
provider:<id>:load
provider:<id>:capacity
```

---

# Distributed Systems Concepts Implemented

## Scheduling
- weighted fair scheduling
- round-robin fairness
- quotas
- priority queues
- shard-aware routing

## Fault Tolerance
- leases
- heartbeats
- recovery loops
- retries
- rescheduling

## Distributed Coordination
- Redis coordination
- provider discovery
- distributed queues

## Event Streaming
- Kafka topics
- async dispatch
- decoupled workers

## Storage
- durable DB persistence
- object storage
- stateless workers

## Scalability Concepts
- sharding
- queue partitioning
- load balancing
- locality-aware routing

---

# Current Scheduling Flow

```text
1. User submits job
2. API computes shard
3. Job stored in Postgres
4. User added to Redis fair queue
5. Scheduler polls weighted priority queues
6. Scheduler selects shard
7. Scheduler selects least-loaded provider inside shard
8. Lease assigned
9. Job published to Kafka
10. Worker consumes job
11. Worker heartbeats lease
12. Worker uploads result to MinIO
13. Job marked completed
14. User requeued if pending jobs remain
```

---

# Current Project Status

Implemented:

- distributed workers
- fair scheduling
- weighted priorities
- shard-aware scheduling
- Kafka dispatch
- Redis coordination
- PostgreSQL persistence
- MinIO object storage
- lease-based execution
- retry logic
- recovery loops
- SSE streaming
- ETA estimation
- user quotas
- token bucket rate limiting
- provider load balancing
- shard-aware queues

---

# Future Improvements

Potential future phases:

## Infrastructure
- Kubernetes deployment
- autoscaling
- Prometheus metrics
- Grafana dashboards
- OpenTelemetry tracing

## Scheduling
- true consistent hashing
- GPU-aware scheduling
- VRAM-aware scheduling
- capability-aware workers
- model affinity

## Data
- database sharding
- distributed metadata ownership
- partition-aware storage

## Execution
- DAG scheduling
- workflow orchestration
- dependency graphs
- batch pipelines

## Reliability
- multi-scheduler HA
- leader election
- distributed consensus
- Raft/etcd integration

---

# Important Architectural Principles

## Scheduler Should Remain Stateless

Persistent state belongs in:

- PostgreSQL
- Redis

not in scheduler memory.

---

## Workers Are Ephemeral

Workers may:

- crash
- restart
- scale dynamically

System should self-heal.

---

## Fairness First

No single tenant should monopolize cluster resources.

---

## Priority Without Starvation

Urgent users get preference.

Free users still eventually execute.

---

## Distributed Recovery

Every in-progress job must eventually:

- complete
OR
- recover
OR
- fail permanently

Never remain stuck forever.

---

# Recommended Repo Structure

```text
/api
/scheduler
/worker
/docs
  architecture.md
  scheduling.md
  recovery.md
  queues.md
  roadmap.md
/docker
```

---

# Recommended Next Major Milestones

1. GPU capability-aware scheduling
2. true consistent hashing ring
3. autoscaling workers
4. Prometheus + Grafana metrics
5. Kubernetes deployment
6. database sharding
7. multi-scheduler leader election
8. DAG workflow execution
9. distributed tracing
10. real GPU execution instead of simulated workloads

