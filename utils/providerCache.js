async function getProviderData(
  redis,
  providerId
){

  const data =
    await redis.hgetall(
      `provider:${providerId}`
    );

  if(
    !data ||
    Object.keys(data).length === 0
  ){
    return null;
  }

  return {
    capacity:
      parseInt(data.capacity || "0"),

    load:
      parseInt(data.load || "0"),

    shardId:
      parseInt(data.shardId || "0"),

    gpuClass:
      data.gpuClass,

    vram:
      parseInt(data.vram || "0")
  };
}

module.exports = {
  getProviderData
};