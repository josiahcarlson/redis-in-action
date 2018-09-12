describe('Chapter 2', function() {
  require('should');
  const Redis = require('ioredis');
  const ch02 = require('../src/ch02/main');
  const { Config, LIMIT } = require('../src/config');
  const uuid4 = require('uuid/v4');
  const { sleep } = require('../src/utils');

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

  let token;
  describe('test_login_cookies', async () => {
    it(`What username do we get when we look-up that token => username`, async () => {
      token = uuid4();
      await ch02.updateToken(redis, token, 'username', 'itemX');

      const res = await ch02.checkToken(redis, token);
      res.should.equal('username');
    });

    it(`Let's drop the maximum number of cookies to 0 to clean them out: len = 0`, async () => {
      Config.LIMIT = 0;

      await ch02.cleanSessions(redis);

      const len = await redis.hlen('login:');
      len.should.equal(0);

      Config.LIMIT = LIMIT;
    });
  });

  describe('test_shoppping_cart_cookies', async () => {
    it(`And add an item to the shopping cart: {itemy: 3}`, async () => {
      token = uuid4();
      await ch02.updateToken(redis, token, 'username', 'itemX');
      await ch02.addToCart(redis, token, 'itemY', 3);
      const res = await redis.hgetall(`cart:${token}`);

      parseInt(res.itemY, 10).should.equal(3);
    });

    it(`Let's clean out our sessions and carts: {}`, async () => {
      Config.LIMIT = 0;

      await ch02.cleanFullSessions(redis);
      const res = await redis.hgetall(`cart:${token}`);
      JSON.stringify(res).should.equal('{}');

      Config.LIMIT = LIMIT;
    });
  });

  describe('test_cache_request', async () => {
    function callback(request) {
      return `content for ${request}`;
    }
    const url = 'http://test.com/?item=itemX';

    it(`We are going to cache a simple request against, We got initial content: content for http://test.com/?item=itemX`, async () => {
      token = uuid4();

      await ch02.updateToken(redis, token, 'username', 'itemX');
      const res = await ch02.cacheRequest(redis, url, callback);
      res.should.equal('content for http://test.com/?item=itemX');
    });

    it(`To test that we've cached the request, we'll pass a bad callback.
            We ended up getting the same response: content for http://test.com/?item=itemX`, async () => {
      const res = await ch02.cacheRequest(redis, url, undefined);
      res.should.equal('content for http://test.com/?item=itemX');
    });

    it(`These url can't cache: http://test.com/`, async () => {
      const res = await ch02.canCache(redis, 'http://test.com/');
      res.should.equal(false);
    });

    it(`These url can't cache: http://test.com/?item=itemX&_=1234536`, async () => {
      const res = await ch02.canCache(
        redis,
        'http://test.com/?item=itemX&_=1234536',
      );
      res.should.equal(false);
    });
  });

  describe('test_cache_rows', async () => {
    it(`First, let's schedule caching of itemX every 5 seconds, Our schedule looks like: [ 'itemX', [timestamp]]`, async () => {
      await ch02.scheduleRowCache(redis, 'itemX', 5);
      const res = await redis.zrange('schedule:', 0, -1, 'WITHSCORES');

      res.length.should.equal(2);
      res[0].should.equal('itemX');
    });

    describe(`We'll run cacheRows() will cache the data...`, async () => {
      let sourceData;
      let changedData;
      it(`Our cached data looks like: {"id":"itemX","data":"data to cache...","cached":[timestamp]}`, async () => {
        await ch02.cacheRows(redis);
        const res = await redis.get('inv:itemX');

        sourceData = JSON.parse(res);
        sourceData.id.should.equal('itemX');
        sourceData.data.should.equal('data to cache...');
      });

      it(`We'll check again in 5 seconds...`, async () => {
        await sleep(5000);
        await ch02.cacheRows(redis);
        const res = await redis.get('inv:itemX');

        changedData = JSON.parse(res);
        changedData.id.should.equal('itemX');
        changedData.data.should.equal('data to cache...');

        sourceData.id.should.equal(changedData.id);
        sourceData.data.should.equal(changedData.data);
        sourceData.cached.should.below(changedData.cached);
      });

      it(`Let's force un-caching`, async () => {
        await ch02.scheduleRowCache(redis, 'itemX', -1);
        await ch02.cacheRows(redis);

        const res = await redis.get('inv:itemX');

        should.not.exist(res);
      });
    });
  });
});
