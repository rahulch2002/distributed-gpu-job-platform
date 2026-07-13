  process.on("unhandledRejection", console.error);
  process.on("uncaughtException", console.error);

  const { Kafka } = require("kafkajs");
  const { Client } = require("pg");
  const Redis = require("ioredis");

  const redis = new Redis("redis://redis:6379");

  const {
    NUM_SHARDS
  } = require("./utils/shard.js");

  const {
    getQueueName,
    getProviderSetName
  } = require("./utils/queue.js");

  const {
    PRIORITY_ORDER
  } = require("./utils/priority");

  const {
    findBestProvider
  } = require(
    "./utils/providerMatcher"
  );

  const USER_QUOTA = 3;

  const ACTIVE_USERS_SET = "active_users";

  async function waitForRedis() {

    while(true){

      try{

        await redis.ping();

        console.log("Connected to Redis");
        break;

      }catch{

        console.log("Redis not ready...");

        await new Promise(
          r => setTimeout(r,2000)
        );
      }
    }
  }

  async function startScheduler() {

    console.log("Scheduler starting...");

    await waitForRedis();

    const pgClient = new Client({
      user:"postgres",
      host:"postgres",
      database:"postgres",
      password:"password",
      port:5432
    });

    while(true){

      try{

        await pgClient.connect();

        console.log("Connected to Postgres");
        break;

      }catch{

        console.log("Postgres not ready...");

        await new Promise(
          r => setTimeout(r,2000)
        );
      }
    }

    const kafka = new Kafka({
      brokers:["redpanda:9092"]
    });

    const producer = kafka.producer();
    const admin = kafka.admin();

    await producer.connect();
    await admin.connect();

    console.log("Scheduler connected to Kafka");

    const createdTopics = new Set();

    // ---------------- RECOVERY LOOP ----------------
    setInterval(async()=>{

      try{

        const stuckJobs = await pgClient.query(`
          SELECT
            id,
            user_id,
            job_priority,
            shard_id
          FROM jobs
          WHERE status IN ('assigned','running')
          AND leased_until < NOW()
        `);

        for(const job of stuckJobs.rows){

          console.log(
            "Recovering stuck job",
            job.id
          );

          await pgClient.query(`
            UPDATE jobs
            SET status='pending',
                leased_by=NULL,
                leased_until=NULL,
                updated_at=NOW()
            WHERE id=$1
          `,[job.id]);

          const added = await redis.sadd(
            ACTIVE_USERS_SET,
            job.user_id
          );

          if(added === 1){

            await redis.rpush(
              getQueueName(
                job.job_priority,
                job.shard_id
              ),
              job.user_id
            );
          }
        }

      }catch(err){

        console.error(
          "Recovery loop error:",
          err
        );
      }

    },30000);

    // METRICS PUBLISHER LOOP
    setInterval(async()=>{

      try{

        for(
          let shard=0;
          shard<NUM_SHARDS;
          shard++
        ){

          let queued = 0;

          for(const p of [10,5,1]){

            queued += await redis.llen(
              getQueueName(p, shard)
            );
          }

          const providers =
            await redis.smembers(
              getProviderSetName(shard)
            );

          let totalLoad = 0;
          let totalCapacity = 0;

          const pipeline = redis.pipeline();

          for(const id of providers){
            pipeline.hgetall(`provider:${id}`);
          }

          const results = await pipeline.exec();

          await redis.hset(
            `metrics:shard:${shard}`,
            {
              queued,
              totalLoad,
              totalCapacity,
              utilization:
                totalCapacity === 0
                ? 0
                : totalLoad / totalCapacity
            }
          );
        }

      }catch(err){

        console.error(err);
      }

    },3000);

    // ---------------- MAIN LOOP ----------------
    setInterval(async()=>{

      try{

        let userId = null;
        let selectedPriority = null;
        let selectedShard = null;

        // ---------------- PRIORITY + SHARD POLLING ----------------
        for(const priority of PRIORITY_ORDER){

          for(let shard=0; shard<NUM_SHARDS; shard++){

            const candidate =
              await redis.lpop(
                getQueueName(priority, shard)
              );

            if(candidate){

              userId = candidate;
              selectedPriority = priority;

              // CONSISTENT HASH VALIDATION
              selectedShard = shard;

              break;
            }
          }

          if(userId){
            break;
          }
        }

        if(!userId){
          return;
        }

        await redis.srem(
          ACTIVE_USERS_SET,
          userId
        );

        // ---------------- FETCH JOB ----------------
        const jobResult = await pgClient.query(`
          SELECT *
          FROM jobs
          WHERE user_id=$1
          AND shard_id=$2
          AND status='pending'
          ORDER BY job_priority DESC,
                  created_at ASC
          LIMIT 1
        `,[userId, selectedShard]);

        if(jobResult.rows.length === 0){
          return;
        }

        const job = jobResult.rows[0];

        // ---------------- USER QUOTA ----------------
        const quotaCheck = await pgClient.query(`
          SELECT COUNT(*)
          FROM jobs
          WHERE user_id=$1
          AND status IN ('assigned','running')
        `,[job.user_id]);

        if(
          parseInt(quotaCheck.rows[0].count)
          >= USER_QUOTA
        ){

          // console.log(
          //   "User quota exceeded",
          //   userId,
          //   job.id
          // );

          const requeue = await redis.sadd(
            ACTIVE_USERS_SET,
            userId
          );

          if(requeue === 1){

            await redis.rpush(
              getQueueName(
                job.job_priority,
                job.shard_id
              ),
              userId
            );
          }

          return;
        }

        // ---------------- SHARD-LOCAL PROVIDERS ----------------
        const selectedProvider =
          await findBestProvider(
            redis,
            pgClient,
            selectedShard,
            job
          );

        // ---------------- NO PROVIDER ----------------
        if(!selectedProvider){

          const requeue = await redis.sadd(
            ACTIVE_USERS_SET,
            userId
          );

          if(requeue === 1){

            await redis.rpush(
              getQueueName(
                job.job_priority,
                job.shard_id
              ),
              userId
            );
          }

          return;
        }

        // ---------------- ASSIGN ----------------
        await pgClient.query(`
          UPDATE jobs
          SET status='assigned',
              updated_at=NOW(),
              leased_by=$2,
              leased_until=
                NOW() + interval '45 seconds'
          WHERE id=$1
        `,[job.id, selectedProvider]);

        await redis.hset(
          `job_runtime:${job.id}`,
          {
            assignedAt: Date.now()
          }
        );

        await redis.hincrby(
          `provider:${selectedProvider}`,
          "load",
          1
        );

        const topicName =
          `provider-${selectedProvider}`;

        if(!createdTopics.has(topicName)){

          const topics =
            await admin.listTopics();

          if(!topics.includes(topicName)){

            await admin.createTopics({
              topics:[{
                topic:topicName,
                numPartitions:1,
                replicationFactor:1
              }]
            });

            console.log(
              `Created topic ${topicName}`
            );
          }

          createdTopics.add(topicName);
        }

        // ---------------- SEND ----------------
        await producer.send({
          topic:topicName,
          messages:[{
            value:JSON.stringify({
              jobId:job.id,
              userId:job.user_id
            })
          }]
        });

        console.log(
          `Assigned job ${job.id} from ${job.user_id} -> provider ${selectedProvider} (shard ${selectedShard}, priority ${selectedPriority})`
        );

        // ---------------- REQUEUE ----------------
        const pendingCount =
          await pgClient.query(`
            SELECT COUNT(*)
            FROM jobs
            WHERE user_id=$1
            AND status='pending'
          `,[userId]);

        if(
          parseInt(pendingCount.rows[0].count)
          > 0
        ){

          const requeue = await redis.sadd(
            ACTIVE_USERS_SET,
            userId
          );

          if(requeue === 1){

            await redis.rpush(
              getQueueName(
                job.job_priority,
                job.shard_id
              ),
              userId
            );
          }
        }

      }catch(err){

        console.error(
          "Scheduler error",
          err
        );
      }

    },200);
  }

  startScheduler();