const Redis = require('ioredis');
const ch04 = require('./main');

async function run() {
  const redis = new Redis({
    db: 15,
  });
  redis.on('error', error => {
    debug('Redis connection error', error);
  });
  await redis.flushdb();

  const seller = 'userX';
  const item = 'itemX';
  await redis.sadd('inventory:' + seller, item);
  await redis.smembers('inventory:' + seller);
  await ch04.listItem(redis, item, seller, 10);

  const buyer = 'userY';
  await redis.hset(`users:${buyer}`, 'funds', 125);

  await redis.hgetall(`users:${buyer}`);
  await ch04.purchaseItem(redis, 'userY', 'itemX', 'userX', 10);

  await ch04.benchmarkUpdateToken(redis, 2);
  await redis.quit();
}

run();
