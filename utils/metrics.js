async function collectShardMetrics(
  redis,
  pgClient,
  shardId,
  priorities
){

  let queuedJobs = 0;

  for(const p of priorities){

    const size =
      await redis.llen(
        `fair_queue:${p}:${shardId}`
      );

    queuedJobs += size;
  }

  const providerIds =
    await redis.smembers(
      `providers:shard:${shardId}`
    );

  let totalCapacity = 0;
  let totalLoad = 0;

  for(const id of providerIds){

    const cap = parseInt(
      await redis.get(
        `provider:${id}:capacity`
      ) || "0"
    );

    const load = parseInt(
      await redis.get(
        `provider:${id}:load`
      ) || "0"
    );

    totalCapacity += cap;
    totalLoad += load;
  }

  const running = await pgClient.query(`
    SELECT COUNT(*)
    FROM jobs
    WHERE shard_id=$1
    AND status='running'
  `,[shardId]);

  const pending = await pgClient.query(`
    SELECT COUNT(*)
    FROM jobs
    WHERE shard_id=$1
    AND status='pending'
  `,[shardId]);

  return {

    shardId,

    queuedJobs,

    runningJobs:
      parseInt(
        running.rows[0].count
      ),

    pendingJobs:
      parseInt(
        pending.rows[0].count
      ),

    totalCapacity,

    totalLoad,

    utilization:
      totalCapacity === 0
      ? 0
      : totalLoad / totalCapacity
  };
}

module.exports = {
  collectShardMetrics
};