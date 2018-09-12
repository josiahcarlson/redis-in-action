const Redis = require('ioredis');
const ch05 = require('./main');
const Utils = require('../utils');
const { Config } = require('../config');

async function run() {
  const redis = new Redis({
    db: 15,
  });
  redis.on('error', error => {
    debug('Redis connection error', error);
  });
  await redis.flushdb();

  let num = 200;
  while (num--) {
    await ch05.logRecent(redis, 'test', `this is message ${num}`);
  }

  let count = 1;
  while (count < 6) {
    let i = 0;
    while (i < count) {
      await ch05.logCommon(redis, 'test', `message-${count}`);
      i++;
    }
    count++;
  }
  const common = await redis.zrevrange('common:test:info', 0, -1, 'withscores');
  console.log(common);

  await redis.flushdb();
  const now = Utils.currentTimestamp();
  count = 0;
  while (count < 10) {
    await ch05.updateCounter(
      redis,
      'test',
      Math.ceil(Math.random() * 5),
      now + count,
    );
    count++;
  }

  console.log(await ch05.getCounter(redis, 'test', 1));

  const sourceCurrentTimestamp = Utils.currentTimestamp;
  function newCurrentTimestamp() {
    return sourceCurrentTimestamp() + 2 * 86400;
  }
  Utils.currentTimestamp = newCurrentTimestamp;
  Config.SAMPLE_COUNT = 0;
  console.log(await ch05.cleanCounters(redis));
  Utils.currentTimestamp = sourceCurrentTimestamp;
  console.log(await ch05.getCounter(redis, 'test', 86400));
  Config.SAMPLE_COUNT = 100;

  await redis.quit();
}

run();
