const crypto = require("crypto");

const NUM_SHARDS = 3;
const VIRTUAL_NODES = 100;

const ring = [];

// ---------------- HASH FUNCTION ----------------
function hash(value){

  const hex = crypto
    .createHash("sha256")
    .update(value)
    .digest("hex");

  return parseInt(
    hex.substring(0, 8),
    16
  );
}

// ---------------- BUILD RING ----------------
for(
  let shard = 0;
  shard < NUM_SHARDS;
  shard++
){

  for(
    let vnode = 0;
    vnode < VIRTUAL_NODES;
    vnode++
  ){

    const key =
      `shard-${shard}-vnode-${vnode}`;

    ring.push({
      hash: hash(key),
      shardId: shard
    });
  }
}

// sort clockwise
ring.sort(
  (a,b) => a.hash - b.hash
);

// ---------------- LOOKUP ----------------
function getShard(userId){

  const userHash =
    hash(userId);

  // first clockwise vnode
  for(const node of ring){

    if(userHash <= node.hash){

      return node.shardId;
    }
  }

  // wrap around
  return ring[0].shardId;
}

module.exports = {
  getShard,
  NUM_SHARDS,
  VIRTUAL_NODES
};