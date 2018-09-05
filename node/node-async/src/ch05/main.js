const Utils = require('../utils');
const { Config } = require('../config');

const LOG_SEVERITY = {
  DEBUG: 'debug',
  INFO: 'info',
  WARNING: 'warning',
  ERROR: 'error',
  CRITICAL: 'critical',
};

const PRECISION = [1, 5, 60, 300, 3600, 18000, 86400];

// 5-1
// if you are using transaction, pipe is a redis client
async function logRecent(
  client,
  name,
  message,
  serverity = LOG_SEVERITY.INFO,
  pipe = false,
) {
  try {
    const destination = `recent:${name}:${serverity}`;
    const msg = `${Date()} ${message}`;

    pipe = pipe || client.pipeline();

    pipe.lpush(destination, msg);
    pipe.ltrim(destination, 0, 99);

    return await pipe.exec();
  } catch (err) {
    console.error(err);
    return false;
  }
}

// 5-2
async function logCommon(
  client,
  name,
  message,
  serverity = LOG_SEVERITY.INFO,
  timeout = 5,
) {
  try {
    const destination = `common:${name}:${serverity}`;
    const startKey = `${destination}:start`;

    const end = Utils.currentTimestamp() + timeout;
    while (Utils.currentTimestamp() < end) {
      client.watch(startKey);

      const hourStart = Utils.currentHour();

      const existing = await client.get(startKey);
      client.multi({
        pipeline: false,
      });

      if (existing && existing != hourStart) {
        client.rename(destination, `${destination}:last`);
        client.rename(startKey, `${destination}:pstart`);
        client.set(startKey, hourStart);
      } else {
        client.set(startKey, hourStart);
      }

      client.zincrby(destination, 1, message);
      const res = await logRecent(client, name, message, serverity, client);
      if (res) {
        return;
      } else {
        continue;
      }
    }
  } catch (err) {
    console.error(err);
  }
}

// 5-3
async function updateCounter(client, name, count = 1, now = 0) {
  try {
    now = now || Utils.currentTimestamp();
    const pipe = client.pipeline();

    for (let prec of PRECISION) {
      const pnow = Math.floor(now / prec) * prec;
      const hash = `${prec}:${name}`;

      pipe.zadd('known:', 0, hash);
      pipe.hincrby(`count:${hash}`, pnow, count);
    }

    return await pipe.exec();
  } catch (err) {
    console.error(err);
  }
}

// 5-4
async function getCounter(client, name, precision) {
  try {
    const hash = `${precision}:${name}`;
    const data = await client.hgetall(`count:${hash}`);

    const res = [];
    for (let key in data) {
      res.push([key, data[key]]);
    }

    return res.sort();
  } catch (err) {
    console.error(err);
  }
}

function binarySearch(arr, value) {
  let pos = 0;

  let high = arr.length - 1;
  while (pos < high) {
    let mid = Math.floor((high + pos) / 2);
    if (value < arr[mid]) {
      high = mid;
    } else {
      pos = mid + 1;
    }
  }

  return pos;
}

// 5-5
async function cleanCounters(client) {
  try {
        // Keep a record of the number of passes so that we can balance cleaning out per-second vs. per-day counters
        // let passes = 0;
        // Get the start time of the pass to calculate the total duration
        // const start = Utils.currentTimestamp();
        let index = 0;
        while (index < (await client.zcard('known:'))) {
          let hash = await client.zrange('known:', index, index);
          index += 1;
          if (!hash) {
            break;
          }
          hash = hash[0];
          const prec = hash.split(':')[0];
          const bprec = Math.floor(prec / 60) || 1;

          // if (passes % bprec) {
          //   continue;
          // }

          const hkey = `count:${hash}`;
          const cutoff = Utils.currentTimestamp() - Config.SAMPLE_COUNT * prec;
          let samples = await client.hkeys(hkey);
          samples = samples.sort();
          const pos = binarySearch(samples, cutoff);

          if (pos !== -1) {
            const delKeys = samples.slice(0, pos + 1);
            await client.hdel(hkey, ...delKeys);
            if (pos === samples.length - 1) {
              client.watch(hkey);
              if ((await client.hlen(hkey)) === 0) {
                client.multi({ pipeline: false });
                client.zrem('known:', hash);
                await client.exec();
                index -= 1;
              } else {
                await client.unwatch();
              }
            }
          }
        }

        // Update our passes and duration variables for the next pass, as an attempt to clean out counters as often as they are seeing updates
        // passes += 1;
        // const duration = Math.min(Utils.currentTimestamp() - start + 1, 60);
        // Sleep the remainder of the 60 seconds, or at least 1 second, just to offer a bit of a rest
        // Utils.sleep(Math.max(60 - duration, 1));
      } catch (err) {
    console.error(err);
  }
}

module.exports = {
  logRecent,
  logCommon,
  updateCounter,
  getCounter,
  cleanCounters,
  LOG_SEVERITY,
};
