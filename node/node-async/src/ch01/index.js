const Redis = require('ioredis');
const ch01 = require('./main');

async function run() {
  const redis = new Redis({
    db: 15,
  });
  redis.on('error', error => {
    debug('Redis connection error', error);
  });
  await redis.flushdb();

  const articleId = await ch01.postArticle(
    redis,
    'username',
    'A title',
    'http://www.google.com',
  );
  if (articleId > 0) {
    console.log(`We posted a new article with id:${articleId}`);
  } else {
    return;
  }

  console.log('Its HASH looks like:');
  console.log(await ch01.getArticles(redis, 1));

  await ch01.articleVote(redis, 'other_user', `article:${articleId}`);
  console.log('We voted for the article, it now has votes:');
  console.log(await redis.hget(`article:${articleId}`, 'votes'));

  console.log('The currently highest-scoring articles are:');
  console.log(await ch01.getArticles(redis, 1));

  await ch01.addRemoveGroups(redis, articleId, ['new-group']);
  console.log('We added the article to a new group, other articles include:');
  console.log(await ch01.getGroupArticles(redis, 'new-group', 1));

  await redis.quit();
}

run();
