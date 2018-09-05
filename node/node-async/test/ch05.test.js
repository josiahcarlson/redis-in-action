describe('Chapter 5', function() {
  require('should');
  const Redis = require('ioredis');
  const ch05 = require('../src/ch05/main');
  const Utils = require('../src/utils');
  const { Config } = require('../src/config');

  let redis;
  before(async () => {
    redis = new Redis({
      db: 15,
    });
    await redis.flushdb();
    redis.on('error', error => {
      debug('Redis connection error', error);
    });
  });

  after(function() {
    redis.quit();
  });

  describe('test_log_recent', async () => {
    it(`Let's write a few logs to the recent log: five logs`, async () => {
      for (let msg in [0, 1, 2, 3, 4]) {
        await ch05.logRecent(redis, 'test', `this is message ${msg}`);
      }
      const recent = await redis.lrange(
        `recent:test:${ch05.LOG_SEVERITY.INFO}`,
        0,
        -1,
      );

      recent.length.should.be.aboveOrEqual(5);
    });
  });

  describe('test_log_common', async () => {
    it(`Let's write some items to the common log`, async () => {
      let count = 1;
      while (count < 6) {
        let i = 0;
        while (i < count) {
          await ch05.logCommon(redis, 'test', `message-${count}`);
          i++;
        }
        count++;
      }
      const res = await redis.zrevrange(
        `common:test:${ch05.LOG_SEVERITY.INFO}`,
        0,
        -1,
        'withscores',
      );

      res.length.should.equal(10);
      res[0].should.equal('message-5');
      parseInt(res[1], 10).should.equal(5);
    });
  });

  describe('test_counters', async () => {
    it(`Let's update some counters for now and a little in the future`, async () => {
      const now = Utils.currentTimestamp();
      let count = 0;
      while (count < 10) {
        await ch05.updateCounter(
          redis,
          'test',
          Math.ceil(Math.random() * 5),
          now + count,
        );
        count++;
      }
    });

    it(`We have some per-second counters:`, async () => {
      const res = await ch05.getCounter(redis, 'test', 1);
      res.length.should.be.aboveOrEqual(10);
    });

    it(`We have some per-5-second counters:`, async () => {
      const res = await ch05.getCounter(redis, 'test', 5);
      res.length.should.be.aboveOrEqual(2);
    });

    it(`Let's clean out some counters by setting our sample count to 0`, async () => {
      const sourceCurrentTimestamp = Utils.currentTimestamp;
      function newCurrentTimestamp() {
        return sourceCurrentTimestamp() + 2 * 86400;
      }
      Utils.currentTimestamp = newCurrentTimestamp;
      Config.SAMPLE_COUNT = 0;
      await ch05.cleanCounters(redis);
      Utils.currentTimestamp = sourceCurrentTimestamp;
      Config.SAMPLE_COUNT = 100;
    });

    it(`Did we clean out all of the counters?`, async () => {
      const res = await ch05.getCounter(redis, 'test', 86400);
      res.length.should.equal(0);
    });
  });
});
