const Utils = require('../utils');

const ONE_WEEK_IN_SECONDS = 7 * 24 * 60 * 60;
const VOTE_SCORE = 432;
const ARTICLES_PER_PAGE = 25;

// 1-6
async function articleVote(client, user, article) {
  try {
    const cutoff = Utils.currentTimestamp() - ONE_WEEK_IN_SECONDS;

    if ((await client.zscore('time:', article)) < cutoff) {
      return;
    }

    const articleId = article.substring(article.indexOf(':') + 1);
    if (await client.sadd(`voted:${articleId}`, user)) {
      await client.zincrby('score:', VOTE_SCORE, article);
      await client.hincrby(article, 'votes', 1);
    }
  } catch (err) {
    console.log(err);
  }
}

// 1-7
async function postArticle(client, user, title, link) {
  try {
    const articleId = await client.incr('article:');

    const voted = `voted:${articleId}`;
    await client.sadd(voted, user);
    await client.expire(voted, ONE_WEEK_IN_SECONDS);

    const article = `article:${articleId}`;
    await client.hmset(article, {
      title,
      link,
      poster: user,
      time: Utils.currentTimestamp(),
      votes: 1,
    });

    await client.zadd('score:', Utils.currentTimestamp() + VOTE_SCORE, article);
    await client.zadd('time:', Utils.currentTimestamp(), article);

    return articleId;
  } catch (err) {
    console.log(err);
    return 0;
  }
}

// 1-8
async function getArticles(client, page, order = 'score:') {
  try {
    const start = (page - 1) * ARTICLES_PER_PAGE;
    const end = start + ARTICLES_PER_PAGE - 1;

    const ids = await client.zrevrange(order, start, end);

    const articles = [];
    for (let id of ids) {
      const article_data = await client.hgetall(id);
      article_data[id] = id;
      articles.push(article_data);
    }

    return articles;
  } catch (err) {
    console.log(err);
    return [];
  }
}

// 1-9
async function addRemoveGroups(client, articleId, toAdd = [], toRemove = []) {
  try {
    const article = `article:${articleId}`;

    for (let group of toAdd) {
      await client.sadd(`group:${group}`, article);
    }
    for (let group of toRemove) {
      await client.srem(`group:${group}`, article);
    }
  } catch (err) {
    console.log(err);
    return [];
  }
}

// 1-10
async function getGroupArticles(client, group, page, order = 'score:') {
  try {
    const key = `${order}${group}`;

    if ((await client.exists(key)) === 0) {
      await client.zinterstore(
        key,
        2,
        `group:${group}`,
        order,
        'AGGREGATE',
        'max',
      );
      await client.expire(key, 60);
    }

    return await getArticles(client, page, key);
  } catch (err) {
    console.log(err);
    return [];
  }
}

module.exports = {
  articleVote,
  postArticle,
  getArticles,
  addRemoveGroups,
  getGroupArticles,
};
