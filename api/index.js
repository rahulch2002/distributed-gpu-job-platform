process.on("unhandledRejection", console.error);
process.on("uncaughtException", console.error);

const express = require("express");
const { randomUUID } = require("crypto");
const { Client } = require("pg");
const {
  S3Client,
  PutObjectCommand
} = require("@aws-sdk/client-s3");

const Redis = require("ioredis");

const redis = new Redis(
  "redis://redis:6379"
);

redis.on("error", err => {
  console.log("Redis connection error:", err.message);
});

const {
  getShard
} = require("./utils/hashRing.js");

const {
  NUM_SHARDS
} = require("./utils/shard.js");

const {
  getQueueName,
  getProviderSetName
} = require("./utils/queue.js");

const {
  PRIORITIES
} = require("./utils/priority.js");

const MAX_TOKENS = 10;
const REFILL_RATE = 10;

const ACTIVE_USERS_SET =
  "active_users";

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

async function startApi() {

  const app = express();

  app.use(express.json());

  await waitForRedis();

  // ---------------- POSTGRES ----------------
  const pgClient = new Client({
    user: "postgres",
    host: "postgres",
    database: "postgres",
    password: "password",
    port: 5432
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

  // ---------------- OFFLINE DETECTION ----------------
  setInterval(async () => {

    try {

      await pgClient.query(`
        UPDATE providers
        SET status='offline'
        WHERE status='online'
        AND NOW() - last_heartbeat
            > interval '10 seconds'
      `);

    } catch (err) {

      console.error(
        "Offline detection error:",
        err
      );
    }

  }, 5000);

  // ---------------- MINIO ----------------
  const s3 = new S3Client({
    region: "us-east-1",
    endpoint: "http://minio:9000",
    credentials: {
      accessKeyId: "admin",
      secretAccessKey: "password"
    },
    forcePathStyle: true
  });

  // ---------------- ETA ----------------
  async function computeETA(
    pgClient,
    job,
    redis
  ) {

    if(job.status === "completed" ||
      job.status === "failed") {
      return 0;
    }

    // ---------------- RUNNING ETA ----------------
    if(job.status === "running"){

      if(!job.started_at){
        return job.estimated_runtime;
      }

      const elapsed =
        (
          Date.now() -
          new Date(job.started_at).getTime()
        ) / 1000;

      const remaining =
        Math.max(
          job.estimated_runtime - elapsed,
          0
        );

      return Math.ceil(remaining);
    }

    // shard-local capacity
    const providers =
      await redis.smembers(
        getProviderSetName(
          job.shard_id
        )
      );

    let totalCapacity = 0;

    for(const p of providers){

      const providerData =
        await redis.hgetall(
          `provider:${p}`
        );

      const cap = parseInt(
        providerData.capacity || "0"
      );

      totalCapacity += cap;
    }

    totalCapacity =
      Math.max(totalCapacity, 1);

    // pending ahead in SAME shard
    const pendingAhead =
      await pgClient.query(`
        SELECT COALESCE(
          SUM(estimated_runtime),
          0
        ) total
        FROM jobs
        WHERE status='pending'
        AND shard_id=$1
        AND (
          job_priority > $2
          OR (
            job_priority = $2
            AND created_at < $3
          )
        )
      `,[
        job.shard_id,
        job.job_priority,
        job.created_at
      ]);

    const pendingSeconds =
      parseFloat(
        pendingAhead.rows[0].total || 0
      );

    // running in SAME shard
    const runningJobs =
      await pgClient.query(`
        SELECT
          estimated_runtime,
          started_at
        FROM jobs
        WHERE status='running'
        AND shard_id=$1
        AND started_at IS NOT NULL
      `,[job.shard_id]);

    let remainingRunningSeconds = 0;

    for(const r of runningJobs.rows){

      const elapsed =
        (
          Date.now() -
          new Date(r.started_at).getTime()
        ) / 1000;

      const remaining =
        Math.max(
          r.estimated_runtime - elapsed,
          0
        );

      remainingRunningSeconds +=
        remaining;
    }

    const eta =
      job.estimated_runtime +
      (
        pendingSeconds /
        totalCapacity
      ) +
      (
        remainingRunningSeconds /
        totalCapacity
      );

    return Math.ceil(eta);
  }

  // METRICS PUBLISHER ENDPOINT
  app.get("/metrics", async(req,res)=>{

    try{

      const shards = {};

      for(
        let shard=0;
        shard<NUM_SHARDS;
        shard++
      ){

        shards[shard] =
          await redis.hgetall(
            `metrics:shard:${shard}`
          );
      }

      res.json({ shards });

    }catch(err){

      console.error(err);

      res.status(500).json({
        error:"internal error"
      });
    }
  });
  // ---------------- SUBMIT JOB ----------------
  app.post(
    "/submit-job",
    async (req, res) => {

      const {
        userId,
        tier = "free",
        gpuClass = "basic",
        requiredVram = 8
      } = req.body;

      if (!userId) {

        return res.status(400)
          .json({
            error:
              "userId required"
          });
      }

      const priority =
        PRIORITIES[tier] || 1;

      const shardId =
        getShard(userId)

      try {

        await pgClient.query(
          "BEGIN"
        );

        // ---------------- RATE LIMIT ----------------
        const rate =
          await pgClient.query(`
            SELECT
              tokens,
              last_refill
            FROM rate_limits
            WHERE user_id=$1
            FOR UPDATE
          `,[userId]);

        let tokens;
        let lastRefill;

        if(rate.rows.length === 0){

          tokens = MAX_TOKENS;
          lastRefill =
            new Date();

          await pgClient.query(`
            INSERT INTO rate_limits
            (
              user_id,
              tokens,
              last_refill
            )
            VALUES ($1,$2,NOW())
          `,[
            userId,
            MAX_TOKENS
          ]);

        } else {

          tokens =
            rate.rows[0].tokens;

          lastRefill =
            new Date(
              rate.rows[0]
                .last_refill
            );
        }

        const now = new Date();

        const secondsPassed =
          (
            now.getTime() -
            lastRefill.getTime()
          ) / 1000;

        const tokensToAdd =
          Math.floor(
            secondsPassed *
            (
              REFILL_RATE / 60
            )
          );

        tokens = Math.min(
          MAX_TOKENS,
          tokens + tokensToAdd
        );

        if(tokens <= 0){

          await pgClient.query(
            "ROLLBACK"
          );

          return res.status(429)
            .json({
              error:
                "Rate limit exceeded"
            });
        }

        tokens -= 1;

        await pgClient.query(`
          UPDATE rate_limits
          SET tokens=$2,
              last_refill=NOW()
          WHERE user_id=$1
        `,[userId, tokens]);

        // ---------------- CREATE JOB ----------------
        const jobId =
          randomUUID();

        const artifactKey =
          `artifacts/${jobId}.txt`;

        await s3.send(
          new PutObjectCommand({
            Bucket: "jobs",
            Key: artifactKey,
            Body:
              "fake job input data"
          })
        );

        const estimatedRuntime = 20;

        await pgClient.query(`
          INSERT INTO jobs
          (
            id,
            user_id,
            status,
            artifact_path,
            updated_at,
            estimated_runtime,
            job_priority,
            shard_id,
            required_vram,
            gpu_class
          )
          VALUES (
            $1,$2,'pending',
            $3,NOW(),
            $4,$5,$6,$7,$8  
          )
        `,[
          jobId,
          userId,
          artifactKey,
          estimatedRuntime,
          priority,
          shardId,
          requiredVram,
          gpuClass
        ]);

        // ---------------- ENQUEUE ----------------
        const added =
          await redis.sadd(
            ACTIVE_USERS_SET,
            userId
          );

        if(added === 1){

          await redis.rpush(
            getQueueName(
              priority,
              shardId
            ),
            userId
          );
        }

        await pgClient.query(
          "COMMIT"
        );

        res.json({
          jobId,
          shardId,
          priority
        });

      } catch (err) {

        console.error(err);

        try {

          await pgClient.query(
            "ROLLBACK"
          );

        } catch {}

        res.status(500).json({
          error:
            "internal error"
        });
      }
    }
  );

  // ---------------- PROVIDERS ----------------
  app.get(
    "/providers",
    async (req, res) => {

      try {

        const result =
          await pgClient.query(`
            SELECT
              id,
              name,
              status,
              max_capacity,
              shard_id,
              gpu_class,
              vram
            FROM providers;
          `);

        const providers =
          result.rows;

        for(const p of providers){

          const providerData =
            await redis.hgetall(
              `provider:${p.id}`
            );

          p.redis_load =
            parseInt(
              providerData.load || "0"
            );

          p.redis_capacity =
            parseInt(
              providerData.capacity || "0"
            );

          p.redis_gpu_class =
            providerData.gpuClass;

          p.redis_vram =
            parseInt(
              providerData.vram || "0"
            );

          p.redis_shard =
            parseInt(
              providerData.shardId || "-1"
            );
        }

        res.json(providers);

      } catch (err) {

        console.error(err);

        res.status(500).json({
          error:
            "internal error"
        });
      }
    }
  );

  // ---------------- FAIRNESS ----------------
  app.get(
    "/fairness",
    async (req, res) => {

      try {

        const queues = {};

        for(const p of [10,5,1]){

          for(
            let s=0;
            s<NUM_SHARDS;
            s++
          ){

            const name =
              getQueueName(p,s);

            queues[name] =
              await redis.lrange(
                name,
                0,
                -1
              );
          }
        }

        const activeUsers =
          await redis.smembers(
            ACTIVE_USERS_SET
          );

        res.json({
          queues,
          activeUsers
        });

      } catch (err) {

        console.error(err);

        res.status(500).json({
          error:
            "internal error"
        });
      }
    }
  );

  // ---------------- JOB STATUS ----------------
  app.get(
    "/job/:id",
    async (req,res)=>{

      try {

        const jobId =
          req.params.id;

        const result =
          await pgClient.query(`
            SELECT *
            FROM jobs
            WHERE id=$1
          `,[jobId]);

        if(
          result.rows.length===0
        ){

          return res.status(404)
            .json({
              error:
                "job not found"
            });
        }

        const job =
          result.rows[0];

        let queuePosition = 0;

        if(
          job.status === "pending"
        ){

          const pos =
            await pgClient.query(`
              SELECT COUNT(*)
              FROM jobs
              WHERE status='pending'
              AND shard_id=$1
              AND (
                job_priority > $2
                OR (
                  job_priority = $2
                  AND created_at < $3
                )
              )
            `,[
              job.shard_id,
              job.job_priority,
              job.created_at
            ]);

          queuePosition =
            parseInt(
              pos.rows[0].count
            ) + 1;
        }

        const etaSeconds =
          await computeETA(
            pgClient,
            job,
            redis
          );

        res.json({
          id: job.id,
          userId: job.user_id,
          shardId: job.shard_id,
          priority:
            job.job_priority,
          status: job.status,
          attempts:
            job.attempts,
          queuePosition,
          etaSeconds,
          leasedBy:
            job.leased_by,
          createdAt:
            job.created_at,
          startedAt:
            job.started_at,
          completedAt:
            job.completed_at,
          failureReason:
            job.failure_reason,
          resultPath:
            job.result_path
        });

      } catch(err){

        console.error(err);

        res.status(500).json({
          error:
            "internal error"
        });
      }
    }
  );

  // ---------------- LIVE JOB STREAM ----------------
  app.get(
    "/job/:id/stream",
    async (req,res)=>{

      const jobId =
        req.params.id;

      res.setHeader(
        "Content-Type",
        "text/event-stream"
      );

      res.setHeader(
        "Cache-Control",
        "no-cache"
      );

      res.setHeader(
        "Connection",
        "keep-alive"
      );

      const interval =
        setInterval(async()=>{

          try {

            const result =
              await pgClient.query(`
                SELECT *
                FROM jobs
                WHERE id=$1
              `,[jobId]);

            if(
              result.rows.length === 0
            ){
              return;
            }

            const job =
              result.rows[0];

            // ---------------- QUEUE POSITION ----------------
            let queuePosition = 0;

            if(
              job.status === "pending"
            ){

              const pos =
                await pgClient.query(`
                  SELECT COUNT(*)
                  FROM jobs
                  WHERE status='pending'
                  AND shard_id=$1
                  AND (
                    job_priority > $2
                    OR (
                      job_priority = $2
                      AND created_at < $3
                    )
                  )
                `,[
                  job.shard_id,
                  job.job_priority,
                  job.created_at
                ]);

              queuePosition =
                parseInt(
                  pos.rows[0].count
                ) + 1;
            }

            // ---------------- ETA ----------------
            const etaSeconds =
              await computeETA(
                pgClient,
                job,
                redis
              );

            const runtime =
              await redis.hgetall(
                `job_runtime:${jobId}`
              );

            // ---------------- PAYLOAD ----------------
            const payload = {
              id: job.id,
              userId: job.user_id,
              shardId: job.shard_id,
              priority:
                job.job_priority,
              status: job.status,
              attempts:
                job.attempts,
              queuePosition,
              etaSeconds,
              leasedBy:
                job.leased_by,
              createdAt:
                job.created_at,
              startedAt:
                job.started_at,
              completedAt:
                job.completed_at,
              failureReason:
                job.failure_reason,
              resultPath:
                job.result_path,
              progressPercent:
                parseFloat(
                  runtime.progress || 0
                ),
              remainingRuntime:
                parseFloat(
                  runtime.remaining || 0
                )
            };

            res.write(
              `data: ${JSON.stringify(payload)}\n\n`
            );

            // ---------------- CLOSE FINISHED ----------------
            if(
              job.status === "completed" ||
              job.status === "failed"
            ){

              clearInterval(interval);
              res.end();
            }

          } catch(err){

            console.error(err);

            clearInterval(interval);
            res.end();
          }

        },1000);

      req.on("close",()=>{

        clearInterval(interval);
      });

    }
  );

  app.listen(3000, () => {

    console.log(
      "API running on port 3000"
    );
  });
}

startApi();
