describe('Redis in Action - Chapter 1', function() {
	var ch01 = require('./main'),
		redis = require('redis'),
		should = require('should');

	var client;

	before(function() {
		client = redis.createClient();
		client.flushdb();
	});

	after(function() {
		client.quit();
	});

	describe('Post', function() {
		it('should be possible to post an article and read it back given the id returned', function(done) {
			var before = new Date().getTime() / 1000;
			ch01.postArticle(client, 'username', 'A title', 'http://www.google.com', function(err, id) {
				client.hgetall('article:' + id, function(err, result) {
					result.title.should.equal('A title');
					result.link.should.equal('http://www.google.com');
					result.user.should.equal('username');
					parseInt(result.votes, 10).should.equal(1);
					parseFloat(result.now).should.be.above(before);
					done();
				});
			});
		});
	});

	describe('Empty Get', function() {
		beforeEach(function() {
			client.flushdb();
		});
		it('getArticles should return empty list when db is empty', function(done) {
			ch01.getArticles(client, 1, null, function(err, articles) {
				should.not.exist(err);
				articles.length.should.equal(0);
				done();
			});
		});
	});

	describe('Vote and Get', function() {
		var ids;
		var voteForArticles = function(done) {
			ch01.articleVote(client, 'user2', 'article:' + ids[1], function() {
				ch01.articleVote(client, 'user2', 'article:' + ids[2], function() {
					ch01.articleVote(client, 'user3', 'article:' + ids[2], function(err) {
						done(err);
					});
				});
			});
		};
		beforeEach(function(done) {
			ids = [];
			var cb = function(err, id) {
				ids.push(id);
				if (ids.length === 3) {
					voteForArticles(done);
				}
			};

			client.flushdb(); // Empty db so we know what's there

			ch01.postArticle(client, 'username', 'a0', 'link0', cb);
			ch01.postArticle(client, 'username', 'a1', 'link1', cb);
			ch01.postArticle(client, 'username', 'a2', 'link1', cb);
		});

		it('should return articles sorted according to number of votes', function(done) {
			ch01.getArticles(client, 1, null, function(err, articles) {
				should.not.exist(err);

				articles.length.should.equal(3);

				articles[0].id.should.equal('article:' + ids[2]);
				parseInt(articles[0].votes, 10).should.equal(3);

				articles[1].id.should.equal('article:' + ids[1]);
				parseInt(articles[1].votes, 10).should.equal(2);

				articles[2].id.should.equal('article:' + ids[0]);
				parseInt(articles[2].votes, 10).should.equal(1);

				done();
			});
		});

		it('should not be possible to vote for the same article twice', function(done) {
			ch01.articleVote(client, 'user2', 'article:' + ids[1], function(err) {
				should.exist(err);
				err.should.be.an.instanceOf(Error);
				err.message.should.equal('user2 already voted for article:' + ids[1]);
				done();
			});
		});

	});

	describe('Vote after cutoff', function() {
		var articleId;
		beforeEach(function(done) {
			ch01.postArticle(client, 'username', 'a0', 'link0', function(err, id) {
				articleId = id;
				// Set today to be one week and one millisecond later
				ch01.setToday(function() {
					return new Date(new Date().getTime() + ch01.ONE_WEEK_IN_SECONDS * 1000 + 1);
				});
				done(err);
			});
		});
		afterEach(function() {
			ch01.setToday();
		});

		it('should not be possible to vote for an article after the cutoff', function(done) {
			ch01.articleVote(client, 'user2', 'article:' + articleId, function(err) {
				should.exist(err);
				err.should.be.an.instanceOf(Error);
				err.message.should.equal('cutoff');
				done();
			});
		});

	});

	describe('Groups - create and remove', function() {

		beforeEach(function(done) {
			ch01.addRemoveGroups(client, '1', ['x'], null, function(err) {
				done(err);
			});
		});

		it('should be possible to create group0', function(done) {
			ch01.addRemoveGroups(client, '1', ['group0'], [], function(err) {
				should.not.exist(err);
				client.smembers('group:group0', function(err, result) {
					should.not.exist(err);
					result.length.should.equal(1);
					result[0].should.equal('article:1');
					done();
				});

			});
		});
		it('should be possible to remove group x', function(done) {
			ch01.addRemoveGroups(client, '1', undefined, ['x'], function(err) {
				should.not.exist(err);
				client.smembers('group:x', function(err, result) {
					should.not.exist(err);
					result.length.should.equal(0);
					done();
				});
			});
		});
		it('should be possible to add group1 and remove group x at the same time', function(done) {
			ch01.addRemoveGroups(client, '1', ['group1'], ['x'], function(err) {
				should.not.exist(err);
				client.smembers('group:group1', function(err, result) {
					should.not.exist(err);
					result.length.should.equal(1);
					result[0].should.equal('article:1');

					client.smembers('group:x', function(err, result) {
						should.not.exist(err);
						result.length.should.equal(0);
						done();
					});
				});
			});
		});
	});

	describe('Groups', function() {

		var ids;
		var voteForArticles = function(done) {
			ch01.articleVote(client, 'user2', 'article:' + ids[1], function() {
				ch01.articleVote(client, 'user2', 'article:' + ids[2], function() {
					ch01.articleVote(client, 'user3', 'article:' + ids[2], function() {
						addGroups(done);
					});
				});
			});
		};
		var addGroups = function(done) {
			ch01.addRemoveGroups(client, ids[0], ['g0', 'g1'], null, function() {
				ch01.addRemoveGroups(client, ids[1], ['g1'], null, function() {
					ch01.addRemoveGroups(client, ids[2], ['g0', 'g1', 'g2'], null, function(err) {
						done(err);
					});
				});
			});
		};
		beforeEach(function(done) {
			ids = [];
			var cb = function(err, id) {
				ids.push(id);
				if (ids.length === 3) {
					voteForArticles(done);
				}
			};

			client.flushdb(); // Empty db so we know what's there

			ch01.postArticle(client, 'username', 'a0', 'link0', cb);
			ch01.postArticle(client, 'username', 'a1', 'link1', cb);
			ch01.postArticle(client, 'username', 'a2', 'link1', cb);
		});

		it('group g0 should contain article 2 and 0', function(done) {
			ch01.getGroupArticles(client, 'g0', 1, null, function(err, articles) {
				should.not.exist(err);
				articles.length.should.equal(2);
				articles[0].id.should.equal('article:' + ids[2]);
				articles[1].id.should.equal('article:' + ids[0]);
				done();
			});
		});

		it('group g1 should contain all three articles', function(done) {
			ch01.getGroupArticles(client, 'g1', 1, null, function(err, articles) {
				should.not.exist(err);
				articles.length.should.equal(3);
				articles[0].id.should.equal('article:' + ids[2]);
				articles[1].id.should.equal('article:' + ids[1]);
				articles[2].id.should.equal('article:' + ids[0]);
				done();
			});
		});

		it('group g2 should only contain article 1', function(done) {
			ch01.getGroupArticles(client, 'g2', 1, null, function(err, articles) {
				should.not.exist(err);
				articles.length.should.equal(1);
				articles[0].id.should.equal('article:' + ids[2]);
				done();
			});
		});

	});

});