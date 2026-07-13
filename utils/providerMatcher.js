const {
  getProviderData
} = require("./providerCache");

async function findBestProvider(
  redis,
  pgClient,
  shardId,
  job
){

  const providers =
    await redis.smembers(
      `providers:shard:${shardId}`
    );

  let selected = null;
  let minLoad = Infinity;

  for(const id of providers){

    const p =
      await getProviderData(
        redis,
        id
      );

    if(!p){
      continue;
    }

    // capability match
    if(
      p.gpuClass !== job.gpu_class
    ){
      continue;
    }

    if(
      parseInt(p.vram) <
      parseInt(job.required_vram)
    ){
      continue;
    }

    const load =
      parseInt(p.load || "0");

    const cap =
      parseInt(p.capacity || "0");

    if(
      load < cap &&
      load < minLoad
    ){

      minLoad = load;
      selected = id;
    }
  }
  
  if (selected != null) {
    console.log(
      "Selected provider:",
      selected
    );
  }

  return selected;
}

module.exports = {
  findBestProvider
};