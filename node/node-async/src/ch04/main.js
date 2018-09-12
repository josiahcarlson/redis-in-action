const Utils = require('../utils');

// 4-5
async function listItem(client, itemId, sellerId, price) {
  try {
    const inventory = `inventory:${sellerId}`;
    const item = `${itemId}.${sellerId}`;
    const end = Utils.currentTimestamp() + 5;

    while (Utils.currentTimestamp() < end) {
      client.watch(inventory);

      if (false === (await client.sismember(inventory, itemId))) {
        await client.unwatch();
        return false;
      }

      // const res = await client.multi().zadd(`market:`, price, item).srem(inventory, itemId).exec();
      client.multi({ pipeline: false });
      client.zadd(`market:`, price, item);
      client.srem(inventory, itemId);
      const res = await client.exec();

      if (res) {
        return true;
      } else {
        continue;
      }
    }

    return false;
  } catch (err) {
    console.error(err);
  }
}

// 4-6
async function purchaseItem(client, buyerId, itemId, sellerId, lprice) {
  try {
    const buyer = `users:${buyerId}`;
    const seller = `users:${sellerId}`;

    const inventory = `inventory:${buyerId}`;
    const item = `${itemId}.${sellerId}`;
    const end = Utils.currentTimestamp() + 10;

    while (Utils.currentTimestamp() < end) {
      client.watch(`market:`, buyer);

      const price = parseInt(await client.zscore(`market:`, item), 10);
      const funds = parseInt(await client.hget(buyer, 'funds'), 10);
      if (price != lprice || price > funds) {
        client.unwatch();
        return false;
      }
      client.multi({ pipeline: false });
      client.hincrby(buyer, 'funds', 0 - price);
      client.hincrby(seller, 'funds', price);
      client.sadd(inventory, itemId);
      client.zrem('market:', item);
      const res = await client.exec();

      if (res) {
        return true;
      } else {
        continue;
      }
    }

    return false;
  } catch (err) {
    console.error(err);
  }
}

// for test
async function updateToken(client, token, user, item = '') {
  try {
    const timestamp = Utils.currentTimestamp();
    await client.hset('login:', token, user);
    await client.zadd('recent:', timestamp, token);

    if (item) {
      await client.zadd(`viewed:${token}`, timestamp, item);
      await client.zremrangebyrank(`viewed:${token}`, 0, -26);
      await client.zincrby('viewed:', -1, item);
    }
  } catch (err) {
    console.error(err);
  }
}

// 4-8
async function updateTokenPipeline(client, token, user, item = '') {
  try {
    const timestamp = Utils.currentTimestamp();

    const pipe = client.pipeline();
    pipe.hset('login:', token, user);
    pipe.zadd('recent:', timestamp, token);

    if (item) {
      pipe.zadd(`viewed:${token}`, timestamp, item);
      pipe.zremrangebyrank(`viewed:${token}`, 0, -26);
      pipe.zincrby(`viewed:`, -1, item);
    }

    await pipe.exec();
  } catch (err) {
    console.error(err);
  }
}

// 4-9
async function benchmarkUpdateToken(client, duration) {
  try {
    for (let func of [updateToken, updateTokenPipeline]) {
      let count = 0;

      const start = Utils.currentTimestamp(true);
      const end = start + duration * 1000;
      while (Utils.currentTimestamp(true) < end) {
        count += 1;
        await func(client, 'token', 'user', 'item');
      }

      const delta = Utils.currentTimestamp(true) - start;
      console.log(
        `func.name:${
          func.name
        }; count:${count}, delta:${delta}, count / delta: ${count / delta}`,
      );
    }
  } catch (err) {
    console.error(err);
  }
}

module.exports = {
  listItem,
  purchaseItem,
  benchmarkUpdateToken,
};
