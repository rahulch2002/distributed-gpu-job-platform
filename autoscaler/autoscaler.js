const Redis = require("ioredis");

const redis =
  new Redis(
    "redis://redis:6379"
  );

const {
  NUM_SHARDS
} = require("./utils/shard");

async function start(){

  console.log(
    "Autoscaler started"
  );

  setInterval(async()=>{

    try{

      for(
        let s=0;
        s<NUM_SHARDS;
        s++
      ){

        let queued = 0;

        for(const p of [10,5,1]){

          queued += await redis.llen(
            `fair_queue:${p}:${s}`
          );
        }

        const providers =
          await redis.smembers(
            `providers:shard:${s}`
          );

        console.log(`
          Shard ${s}
          queue=${queued}
          providers=${providers.length}
        `);

        // placeholder scaling logic
        if(
          queued > 20
        ){

          console.log(
            `Shard ${s} overloaded`
          );
        }
      }

    }catch(err){

      console.error(err);
    }

  },5000);
}

start();