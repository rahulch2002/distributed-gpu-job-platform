const NUM_SHARDS = 4;

function getShard(userId){

  let hash = 0;

  for(const c of userId){
    hash += c.charCodeAt(0);
  }

  return hash % NUM_SHARDS;
}

module.exports = {
  NUM_SHARDS,
  getShard
};