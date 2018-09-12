const Utils = require('../utils');
const URL = require('url');
const { Config } = require('../config');

// 2-1
async function checkToken(client, token) {
  try {
    return await client.hget('login:', token);
  } catch (err) {
    console.error(err);
    return '';
  }
}

// 2-2
// the new code in 2-9
// async function updateToken(client, token, user, item = '') {
//     try {
//         const timestamp = Utils.currentTimestamp();
//         await client.hset('login:', token, user);
//         await client.zadd('recent:', timestamp, token);

//         if (item) {
//             await client.zadd(`viewed:${token}`, timestamp, item);
//             await client.zremrangebyrank(`viewed:${token}`, 0, -26);
//         }
//     } catch (err) {
//         console.error(err);
//     }
// }

// 2-3
// Note: we should't use sleep in NodeJS, maybe you can use schedule or setTimeout(cron job is ok) to invoke function
// Don't block code
async function cleanSessions(client) {
  try {
    const size = await client.zcard('recent:');
    if (size <= Config.LIMIT) {
      return;
    }

    const endIndex = size - Config.LIMIT > 100 ? 100 : size - Config.LIMIT;
    const tokens = await client.zrange('recent:', 0, endIndex - 1);

    const sessionKeys = tokens.map(token => {
      return `viewed:${token}`;
    });

    await client.del(...sessionKeys);
    await client.hdel('login:', ...tokens);
    await client.zrem('recent:', ...tokens);
  } catch (err) {
    console.error(err);
  }
}

// 2-4
async function addToCart(client, session, item, count) {
  try {
    if (count <= 0) {
      await client.hrem(`cart:${session}`, item);
    } else {
      await client.hset(`cart:${session}`, item, count);
    }
  } catch (err) {
    console.error(err);
  }
}

// 2-5
// Note: we should't use sleep in NodeJS, maybe you can use schedule or setTimeout(cron job is ok) to invoke function
async function cleanFullSessions(client) {
  try {
    const size = await client.zcard('recent:');
    if (size <= Config.LIMIT) {
      return;
    }

    const endIndex = size - Config.LIMIT > 100 ? 100 : size - Config.LIMIT;
    const sessions = await client.zrange('recent:', 0, endIndex - 1);

    const sessionKeys = [];
    for (let session of sessions) {
      sessionKeys.push(`viewed:${session}`);
      sessionKeys.push(`cart:${session}`);
    }

    await client.del(...sessionKeys);
    await client.hdel('login:', ...sessions);
    await client.zrem('recent:', ...sessions);
  } catch (err) {
    console.error(err);
  }
}

// 2-6
async function cacheRequest(client, request, callback) {
  try {
    if (false === (await canCache(client, request))) {
      return callback(request);
    }

    const pageKey = 'cache:' + Utils.getHash(request);
    let content = await client.get(pageKey);

    if (!content) {
      content = callback(request);
      await client.setex(pageKey, 300, content);
    }

    return content;
  } catch (err) {
    console.error(err);
  }
}

// 2-7
async function scheduleRowCache(client, rowId, delay) {
  try {
    await client.zadd('delay:', delay, rowId);
    await client.zadd('schedule:', Utils.currentTimestamp(), rowId);
  } catch (err) {
    console.error(err);
  }
}

class Inventory {
  constructor(rowId) {
    this.rowId = rowId;
  }

  toDict() {
    return {
      id: this.rowId,
      data: 'data to cache...',
      cached: Utils.currentTimestamp(),
    };
  }
}

// 2-8
// Note: we should't use sleep in NodeJS, maybe you can use schedule or setTimeout(cron job is ok) to invoke function
async function cacheRows(client) {
  try {
    const next = await client.zrange('schedule:', 0, 0, 'WITHSCORES');
    const now = Utils.currentTimestamp();

    if (next.length === 0 || next[1] > now) {
      return;
    }

    const rowId = next[0];
    const delay = await client.zscore('delay:', rowId);

    if (delay <= 0) {
      await client.zrem('delay:', rowId);
      await client.zrem('schedule:', rowId);
      await client.del(`inv:${rowId}`);
      return;
    }

    const row = new Inventory(rowId);
    await client.zadd('schedule:', now + parseInt(delay, 10), rowId);
    await client.set(`inv:${rowId}`, JSON.stringify(row.toDict()));
  } catch (err) {
    console.error(err);
  }
}

// 2-9
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

// 2-10
// Note: we should't use sleep in NodeJS, maybe you can use schedule or setTimeout(cron job is ok) to invoke function
async function rescaleViewed(client) {
  try {
    await client.zremrangebyrank('viewed:', 0, -20001);
    // xxxxxx test!!!!
    await client.zinterstore('viewed:');
  } catch (err) {
    console.error(err);
  }
}

function extractItemId(request) {
  const params = URL.parse(request, true).query;

  return params.item || 0;
}

function isDynamic(request) {
  const params = URL.parse(request, true).query;
  return params['_'];
}

// 2-11
async function canCache(client, request) {
  try {
    const itemId = extractItemId(request);

    if (itemId === 0 || isDynamic(request)) {
      return false;
    }

    const rank = await client.zrank('viewed:', itemId);

    return rank && rank < 10000;
  } catch (err) {
    console.error(err);
  }
}


module.exports = {
  checkToken,
  updateToken,
  cleanSessions,
  addToCart,
  cleanFullSessions,
  cacheRequest,
  scheduleRowCache,
  cacheRows,
  rescaleViewed,
  canCache,
};
