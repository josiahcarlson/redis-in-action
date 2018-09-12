const Redis = require('ioredis');
const ch02 = require('./main');
const uuid4 = require('uuid/v4');
const Utils = require('../utils');

async function run() {
  const redis = new Redis({
    db: 15,
  });
  redis.on('error', error => {
    debug('Redis connection error', error);
  });
  await redis.flushdb();

  // test_login_cookies
  let token = uuid4();
  await ch02.updateToken(redis, token, 'username', 'itemX');
  console.log(`We just logged-in/updated token:${token}`);
  console.log(`For user:username`);

  console.log(`What username do we get when we look-up that token?`);
  let res = await ch02.checkToken(redis, token);
  console.log(res);

  console.log(
    `Let's drop the maximum number of cookies to 0 to clean them out`,
  );
  console.log(`We will use invoke func to clean`);
  await ch02.cleanSessions(redis);
  const len = await redis.hlen('login:');
  console.log(`The current number of sessions still available is:${len}`);

  // test_shoppping_cart_cookies
  token = uuid4();
  console.log(`We'll refresh our session...`);
  await ch02.updateToken(redis, token, 'username', 'itemX');
  console.log(`And add an item to the shopping cart`);
  await ch02.addToCart(redis, token, 'itemY', 3);
  res = await redis.hgetall(`cart:${token}`);
  console.log(`Our shopping cart currently has:`);
  console.log(res);

  console.log(`Let's clean out our sessions and carts`);
  await ch02.cleanFullSessions(redis);
  res = await redis.hgetall(`cart:${token}`);
  console.log(`Our shopping cart currently has:`);
  console.log(res);

  // test_cache_request
  token = uuid4();
  function callback(request) {
    return `content for ${request}`;
  }
  await ch02.updateToken(redis, token, 'username', 'itemX');
  const url = 'http://test.com/?item=itemX';
  console.log(`We are going to cache a simple request against ${url}`);
  const result1 = await ch02.cacheRequest(redis, url, callback);
  console.log(`We got initial content:${result1}`);

  console.log(
    `To test that we've cached the request, we'll pass a bad callback`,
  );
  const result2 = await ch02.cacheRequest(redis, url, callback);
  console.log(`We ended up getting the same response: ${result2}`);

  const result3 = await ch02.cacheRequest(
    redis,
    'http://test.com/?item=itemX&_=1234536',
    callback,
  );
  console.log(`We ended up getting the different response: ${result3}`);

  // no test_cache_rows
  console.log(`First, let's schedule caching of itemX every 5 seconds`);
  await ch02.scheduleRowCache(redis, 'itemX', 5);
  console.log(`Our schedule looks like:`);
  res = await redis.zrange('schedule:', 0, -1, 'WITHSCORES');
  console.log(res);

  console.log(`We'll run cacheRows() will cache the data...`);
  await ch02.cacheRows(redis);
  const invResult1 = await redis.get('inv:itemX');
  console.log(`Our cached data looks like: ${invResult1}`);
  console.log(`We'll check again in 5 seconds...`);
  await Utils.sleep(5000);
  await ch02.cacheRows(redis);
  const invResult2 = await redis.get('inv:itemX');
  console.log(`Notice that the data has changed...:${invResult2}`);

  console.log(`Let's force un-caching`);
  await ch02.scheduleRowCache(redis, 'itemX', -1);
  await ch02.cacheRows(redis);
  const invResult3 = await redis.get('inv:itemX');
  console.log(`The cache was cleared? :${invResult3}`);

  await redis.quit();
}

run();
