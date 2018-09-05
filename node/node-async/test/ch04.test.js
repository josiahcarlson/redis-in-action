describe('Chapter 4', function() {
  require('should');
  const Redis = require('ioredis');
  const ch04 = require('../src/ch04/main');

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
  const seller = 'userX';
  const item = 'itemX';
  const buyer = 'userY';

  describe('test_list_item', async () => {
    it(`We need to set up just enough state so that a user can list an item, The user's inventory has: ['itemX']`, async () => {
      await redis.sadd('inventory:' + seller, item);
      const members = await redis.smembers('inventory:' + seller);

      members.length.should.be.above(0);
      members[0].should.equal('itemX');
    });

    it(`Listing the item, Listing the item succeeded: true`, async () => {
      const res = await ch04.listItem(redis, item, seller, 10);
      res.should.equal(true);
    });

    it(`Listing the item: market -> itemX.userX: 10`, async () => {
      const res = await redis.zrange('market:', 0, -1, 'withscores');
      parseInt(res[1], 10).should.equal(10);
      res[0].should.equal('itemX.userX');
    });
  });

  describe('test_purchase_item', async () => {
    it(`We need to set up just enough state so a user can buy an item, The user has some money: funds -> 125`, async () => {
      await redis.hset(`users:${buyer}`, 'funds', 125);

      const res = await redis.hgetall(`users:${buyer}`);
      parseInt(res.funds, 10).should.equal(125);
    });

    it(`Let's purchase an item, Purchasing an item succeeded? -> no enough money`, async () => {
      const res = await ch04.purchaseItem(
        redis,
        'userY',
        'itemX',
        'userX',
        1000,
      );

      res.should.equal(false);
    });

    it(`Let's purchase an item, Purchasing an item succeeded? -> true`, async () => {
      const res = await ch04.purchaseItem(redis, 'userY', 'itemX', 'userX', 10);

      res.should.equal(true);
    });

    it(`Seller money is now: 10`, async () => {
      const res = await redis.hgetall(`users:${seller}`);

      parseInt(res.funds, 10).should.equal(10);
    });

    it(`Buyer money is now: 115`, async () => {
      const res = await redis.hgetall(`users:${buyer}`);

      parseInt(res.funds, 10).should.equal(115);
    });

    it(`Buyer inventory is now: include itemX`, async () => {
      const res = await redis.smembers(`inventory:${buyer}`);

      'itemX'.should.be.oneOf(res);
    });
  });

  describe('test_benchmark_update_token', async () => {
    it(`Now please see console.log`, async () => {
      await ch04.benchmarkUpdateToken(redis, 2);
    });
  });
});
