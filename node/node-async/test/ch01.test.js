describe('Chapter 1', function() {
  require('should');
  const Redis = require('ioredis');
  const ch01 = require('../src/ch01/main');

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

  let articleId;
  describe('postArticle', async () => {
    it('We posted a new article with id: 1', async () => {
      articleId = await ch01.postArticle(
        redis,
        'username',
        'A title',
        'http://www.google.com',
      );
      articleId.should.equal(1);
    });
  });

  describe('new post article info', async () => {
    it('Its HASH looks like:', async () => {
      const article = await redis.hgetall(`article:${articleId}`);

      article.title.should.equal('A title');
      article.link.should.equal('http://www.google.com');
      article.poster.should.equal('username');
      parseInt(article.votes, 10).should.equal(1);
    });
  });

  describe('article', async () => {
    it('We voted for the article, it now has votes: 2', async () => {
      await ch01.articleVote(redis, 'other_user', `article:${articleId}`);

      const voted = await redis.hget(`article:${articleId}`, 'votes');
      parseInt(voted, 10).should.equal(2);
    });
  });

  describe('getArticle', async () => {
    it('The currently highest-scoring articles are: length >= 1', async () => {
      const articles = await ch01.getArticles(redis, 1);

      articles.length.should.be.aboveOrEqual(1);
    });
  });

  describe('addRemoveGroups and getGroupArticles', async () => {
    it('We added the article to a new group, other articles include:', async () => {
      await ch01.addRemoveGroups(redis, articleId, ['new-group']);
      const newGroupArticles = await ch01.getGroupArticles(
        redis,
        'new-group',
        1,
      );
      newGroupArticles.length.should.be.aboveOrEqual(1);

      const inexistenceGroupArticles = await ch01.getGroupArticles(
        redis,
        'inexistence-group',
        1,
      );
      inexistenceGroupArticles.length.should.equal(0);
    });
  });
});
