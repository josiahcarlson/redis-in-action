// Config just for test, you can set Config.limit
class Config { }
const LIMIT = 1000000;
const SAMPLE_COUNT = 100;

Config.LIMIT = LIMIT;
Config.SAMPLE_COUNT = SAMPLE_COUNT;

module.exports = {
  Config,
  LIMIT,
  SAMPLE_COUNT,
};