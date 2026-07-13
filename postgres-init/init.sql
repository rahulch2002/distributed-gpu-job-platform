CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY,
    user_id TEXT,
    status TEXT,
    artifact_path TEXT,
    result_path TEXT,
    attempts INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    leased_by UUID,
    leased_until TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    failure_reason TEXT,
    max_attempts INT DEFAULT 3,
    estimated_runtime INTEGER DEFAULT 20,
    job_priority INTEGER DEFAULT 1,
    shard_id INTEGER DEFAULT 0,
    gpu_class TEXT DEFAULT 'basic',
    required_vram INTEGER DEFAULT 8
);

CREATE TABLE IF NOT EXISTS providers (
    id UUID PRIMARY KEY,
    name TEXT,
    status TEXT DEFAULT 'online',
    last_heartbeat TIMESTAMP DEFAULT NOW(),
    max_capacity INT,
    current_load INT DEFAULT 0,
    shard_id INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    gpu_class TEXT DEFAULT 'basic',
    vram INTEGER DEFAULT 8
);

CREATE TABLE IF NOT EXISTS rate_limits (
    user_id TEXT PRIMARY KEY,
    tokens INT,
    last_refill TIMESTAMP
);