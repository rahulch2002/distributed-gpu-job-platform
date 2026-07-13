process.on("unhandledRejection", console.error);
process.on("uncaughtException", console.error);

const { Kafka } = require("kafkajs");
const { Client } = require("pg");
const {
  S3Client,
  PutObjectCommand
} = require("@aws-sdk/client-s3");

const { randomUUID } = require("crypto");

const Redis = require("ioredis");

const redis = new Redis(
  "redis://redis:6379"
);

const {
  getQueueName,
  getProviderSetName
} = require("./utils/queue");

const {
  NUM_SHARDS
} = require("./utils/shard");

const GPU_CLASSES = [
  "basic",
  "premium",
  "ultra"
];

const ACTIVE_USERS_SET =
  "active_users";

// ---------------- GLOBAL PROVIDER ----------------
const providerId = randomUUID();

const shardId =
  parseInt(
    process.env.SHARD_ID || "0"
  );

// ---------------- S3 ----------------
const s3 = new S3Client({
  region: "us-east-1",
  endpoint: "http://minio:9000",
  credentials: {
    accessKeyId: "admin",
    secretAccessKey: "password",
  },
  forcePathStyle: true,
});

async function waitForRedis() {

  while (true) {

    try {

      await redis.ping();

      console.log(
        "Connected to Redis"
      );

      break;

    } catch {

      console.log(
        "Redis not ready..."
      );

      await new Promise(
        r => setTimeout(r, 2000)
      );
    }
  }
}

async function startWorker() {

  console.log("Worker starting...");

  await waitForRedis();

  // ---------------- POSTGRES ----------------
  const pgClient = new Client({
    user: "postgres",
    host: "postgres",
    database: "postgres",
    password: "password",
    port: 5432,
  });

  while (true) {

    try {

      await pgClient.connect();

      console.log(
        "Connected to Postgres"
      );

      break;

    } catch {

      console.log(
        "Postgres not ready..."
      );

      await new Promise(
        r => setTimeout(r, 2000)
      );
    }
  }

  // ---------------- PROVIDER CONFIG ----------------
  const capacity = 2;

  const gpuClass =
    GPU_CLASSES[
      Math.floor(
        Math.random() *
        GPU_CLASSES.length
      )
    ];

  const vram =
    gpuClass === "ultra"
    ? 80
    : gpuClass === "premium"
    ? 40
    : 16;

  // ---------------- REGISTER PROVIDER ----------------
  await pgClient.query(`
    INSERT INTO providers
    (
      id,
      name,
      max_capacity,
      current_load,
      status,
      last_heartbeat,
      shard_id,
      gpu_class,
      vram
    )
    VALUES (
      $1,$2,$3,0,
      'online',
      NOW(),
      $4,$5,$6
    )
  `,[
    providerId,
    `worker-${providerId.slice(0,5)}`,
    capacity,
    shardId,
    gpuClass,
    vram
  ]);

  console.log(
    `Registered provider ${providerId} in shard ${shardId}`
  );

  // ---------------- REDIS PROVIDER CACHE ----------------
  await redis.sadd(
    "providers",
    providerId
  );

  await redis.sadd(
    getProviderSetName(shardId),
    providerId
  );

  await redis.hset(
    `provider:${providerId}`,
    {
      capacity,
      load: 0,
      shardId,
      gpuClass,
      vram
    }
  );

  console.log(
    `Provider cached in Redis`
  );

  // ---------------- CLEANUP ----------------
  async function cleanup(){

    console.log(
      `Cleaning provider ${providerId}`
    );

    try{

      await redis.srem(
        "providers",
        providerId
      );

      await redis.srem(
        getProviderSetName(shardId),
        providerId
      );

      await redis.del(
        `provider:${providerId}`
      );

      await pgClient.query(`
        UPDATE providers
        SET status='offline'
        WHERE id=$1
      `,[providerId]);

    }catch(err){

      console.error(err);
    }

    process.exit(0);
  }

  process.on("SIGINT", cleanup);
  process.on("SIGTERM", cleanup);

  // ---------------- HEARTBEAT ----------------
  setInterval(async () => {

    try {

      await pgClient.query(`
        UPDATE providers
        SET last_heartbeat=NOW(),
            status='online'
        WHERE id=$1
      `,[providerId]);

    } catch (err) {

      console.error(
        "Heartbeat error:",
        err
      );
    }

  }, 5000);

  // ---------------- KAFKA ----------------
  const kafka = new Kafka({
    brokers: ["redpanda:9092"]
  });

  const admin = kafka.admin();

  const topicName =
    `provider-${providerId}`;

  await admin.connect();

  const topics =
    await admin.listTopics();

  if (!topics.includes(topicName)) {

    await admin.createTopics({
      topics: [{
        topic: topicName,
        numPartitions: 1,
        replicationFactor: 1
      }]
    });

    console.log(
      "Created topic",
      topicName
    );
  }

  const consumer = kafka.consumer({
    groupId: `worker-${providerId}`
  });

  await consumer.connect();

  await consumer.subscribe({
    topic: topicName,
    fromBeginning: false
  });

  console.log(
    "Subscribed to",
    topicName
  );

  console.log("Worker ready...");

  // ---------------- JOB LOOP ----------------
  await consumer.run({

    eachMessage: async ({ message }) => {

      const {
        jobId,
        userId
      } = JSON.parse(
        message.value.toString()
      );

      console.log(
        "Received job:",
        jobId
      );

      let leaseInterval;
      let progressInterval;

      try {

        // ---------------- CLAIM ----------------
        const claim = await pgClient.query(`
          UPDATE jobs
          SET status='running',
              updated_at=NOW(),
              started_at=NOW()
          WHERE id=$1
          AND status='assigned'
          AND leased_by=$2
          RETURNING *
        `,[jobId, providerId]);

        if (claim.rows.length === 0) {

          console.log(
            "Lease lost for job",
            jobId
          );

          await redis.hincrby(
            `provider:${providerId}`,
            "load",
            -1
          );

          const provider =
            await redis.hgetall(
              `provider:${providerId}`
            );

          if(
            parseInt(
              provider.load || "0"
            ) < 0
          ){

            await redis.hset(
              `provider:${providerId}`,
              "load",
              0
            );
          }

          return;
        }

        // ---------------- LEASE HEARTBEAT ----------------
        leaseInterval = setInterval(async () => {

          try {

            await pgClient.query(`
              UPDATE jobs
              SET leased_until =
                    NOW() + interval '45 seconds',
                  updated_at=NOW()
              WHERE id=$1
              AND leased_by=$2
            `,[jobId, providerId]);

          } catch(err) {

            console.error(
              "Lease heartbeat failed",
              err
            );
          }

        },5000);

        console.log(
          "Processing job:",
          jobId
        );

        const startedAt = Date.now();

        // ---------------- LIVE RUNTIME CACHE ----------------
        progressInterval =
          setInterval(async()=>{

            try{

              const elapsed =
                (
                  Date.now() -
                  startedAt
                ) / 1000;

              const progress =
                Math.min(
                  (elapsed / 20) * 100,
                  100
                );

              const remaining =
                Math.max(
                  20 - elapsed,
                  0
                );

              await redis.hset(
                `job_runtime:${jobId}`,
                {
                  progress,
                  remaining
                }
              );

            }catch(err){

              console.error(err);
            }

          },1000);

        // ---------------- SIMULATED GPU WORK ----------------
        await new Promise(
          r => setTimeout(r, 20000)
        );

        // ---------------- STORE RESULT ----------------
        const resultKey =
          `results/${jobId}.txt`;

        await s3.send(
          new PutObjectCommand({
            Bucket: "jobs",
            Key: resultKey,
            Body: "fake result"
          })
        );

        // ---------------- COMPLETE ----------------
        await pgClient.query(`
          UPDATE jobs
          SET status='completed',
              result_path=$2,
              updated_at=NOW(),
              completed_at=NOW(),
              leased_by=NULL,
              leased_until=NULL
          WHERE id=$1
        `,[jobId,resultKey]);

        // ---------------- LOAD DECREMENT ----------------
        await redis.hincrby(
          `provider:${providerId}`,
          "load",
          -1
        );

        const provider =
          await redis.hgetall(
            `provider:${providerId}`
          );

        if(
          parseInt(
            provider.load || "0"
          ) < 0
        ){

          await redis.hset(
            `provider:${providerId}`,
            "load",
            0
          );
        }

        clearInterval(
          leaseInterval
        );

        clearInterval(
          progressInterval
        );

        console.log(
          "Completed job:",
          jobId
        );

        // ---------------- CLEAN RUNTIME CACHE ----------------
        await redis.del(
          `job_runtime:${jobId}`
        );

        // ---------------- REQUEUE USER ----------------
        const pending =
          await pgClient.query(`
            SELECT COUNT(*)
            FROM jobs
            WHERE user_id=$1
            AND status='pending'
          `,[userId]);

        const priorityResult =
          await pgClient.query(`
            SELECT
              job_priority,
              shard_id
            FROM jobs
            WHERE user_id=$1
            AND status='pending'
            ORDER BY job_priority DESC
            LIMIT 1
          `,[userId]);

        const pendingCount =
          parseInt(
            pending.rows[0].count
          );

        if (
          pendingCount > 0 &&
          priorityResult.rows.length > 0
        ) {

          const priority =
            priorityResult.rows[0]
              .job_priority;

          const nextShardId =
            priorityResult.rows[0]
              .shard_id;

          const added =
            await redis.sadd(
              ACTIVE_USERS_SET,
              userId
            );

          if (added === 1) {

            await redis.rpush(
              getQueueName(
                priority,
                nextShardId
              ),
              userId
            );

            console.log(
              `Requeued user ${userId} in shard ${nextShardId}`
            );
          }
        }

      } catch (err) {

        console.error(
          "Job failed",
          jobId,
          err
        );

        await redis.del(
          `job_runtime:${jobId}`
        );

        if (leaseInterval) {

          clearInterval(
            leaseInterval
          );
        }

        if(progressInterval){

          clearInterval(
            progressInterval
          );
        }

        // ---------------- FAIL/RETRY ----------------
        await pgClient.query(`
          UPDATE jobs
          SET
            status = CASE
              WHEN attempts + 1 >= max_attempts
              THEN 'failed'
              ELSE 'pending'
            END,
            attempts = attempts + 1,
            updated_at = NOW(),
            failure_reason = $2,
            leased_by = NULL,
            leased_until = NULL
          WHERE id=$1
        `,[jobId, err.message]);

        // ---------------- LOAD FIX ----------------
        await redis.hincrby(
          `provider:${providerId}`,
          "load",
          -1
        );

        const provider =
          await redis.hgetall(
            `provider:${providerId}`
          );

        if(
          parseInt(
            provider.load || "0"
          ) < 0
        ){

          await redis.hset(
            `provider:${providerId}`,
            "load",
            0
          );
        }

        // ---------------- REQUEUE FAILED ----------------
        const retryJob =
          await pgClient.query(`
            SELECT
              job_priority,
              shard_id
            FROM jobs
            WHERE id=$1
          `,[jobId]);

        if (
          retryJob.rows.length > 0
        ) {

          const priority =
            retryJob.rows[0]
              .job_priority;

          const retryShardId =
            retryJob.rows[0]
              .shard_id;

          const added =
            await redis.sadd(
              ACTIVE_USERS_SET,
              userId
            );

          if (added === 1) {

            await redis.rpush(
              getQueueName(
                priority,
                retryShardId
              ),
              userId
            );
          }
        }
      }
    }
  });
}

startWorker();