function getQueueName(priority, shardId){

  return `fair_queue:${priority}:${shardId}`;
}

function getProviderSetName(shardId){

  return `providers:shard:${shardId}`;
}

module.exports = {
  getQueueName,
  getProviderSetName
};