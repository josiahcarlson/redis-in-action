var ONE_WEEK_IN_SECONDS = 7 * 24 * 60 * 60,
	VOTE_SCORE = 432;

var today = todayOrig = function() {
	return new Date();
};

var articleVote = function(client, user, article, cb) {
	var cutoff = (today() / 1000) - ONE_WEEK_IN_SECONDS;
	client.zscore('time:', article, function(err, result) {
		if (err || result < cutoff) return cb(err || new Error('cutoff'));
		var articleId = article.substring(article.indexOf(':') + 1);
		client.sadd('voted:' + articleId, user, function(err, result) {
			if (err) return cb(err);
			if (result === 1) {
				client.zincrby('score:', VOTE_SCORE, article, function(err) {
					if (err) return cb(err);
					client.hincrby(article, 'votes', 1, function(err, result) {
						return cb(err, result);
					});
				});
			} else {
				return cb(new Error(user + ' already voted for ' + article));
			}
		});
	});
};

var getNextArticleId = function(client,  cb) {
	client.incr("article:", function(err, id) {
		if (err) return cb(err);
		cb(null, id);
	});
};

var postArticle = function(client, user, title, link, cb) {
	getNextArticleId(client, function(err, id) {
		if (err) return cb(err);
		var voted = "voted:" + id;
		client.sadd(voted, user, function(err) {
			if (err) return cb(err);
			client.expire(voted, ONE_WEEK_IN_SECONDS, function(err) {
				if (err) return cb(err);
				var now = today() / 1000;
				var article = 'article:' + id;
				var articleData = {
					title: title,
					link: link,
					user: user,
					now: '' + now,
					votes: '1'
				};
				client.hmset(article, articleData, function(err) {
					if (err) return cb(err);
					client.zadd('score:', now + VOTE_SCORE, article, function(err) {
						if (err) return cb(err);
						client.zadd('time:', now, article, function(err) {
							cb(err, id);
						});
					});
				});
			});
		});
	});
};

var ARTICLES_PER_PAGE = 25;
var getArticles = function(client, page, order, cb) {
	order = order || 'score:';

	var start = (page-1) * ARTICLES_PER_PAGE;
	var end = start + ARTICLES_PER_PAGE - 1;

	client.zrevrange(order, start, end, function(err, ids) {
		if (err) return cb(err);
		var articles = [];
		var count = ids.length;
		ids.forEach(function(id) {
			client.hgetall(id, function(err, articleData) {
				if (err) return cb(err);
				articleData['id'] = id;
				articles.push(articleData);
				count -= 1;
				if (count === 0) {
					return cb(null, articles);
				}
			});
		});
		if (ids.length === 0) {
			cb(null, []);
		}
	});
};

var addRemoveGroups = function(client, articleId, toAdd, toRemove, cb) {
	toAdd = toAdd || [];
	toRemove = toRemove || [];
	var article = 'article:' + articleId;

	var toAddCount = 0, toRemoveCount = 0;
	toAdd.forEach(function(group) {
		client.sadd('group:' + group, article, function(err) {
			if (err) return cb(err);
			toAddCount += 1;
			if (toAddCount === toAdd.length && toRemoveCount === toRemove.length) return cb(null);
		});
	});

	toRemove.forEach(function(group) {
		client.srem('group:' + group, article, function(err) {
			if (err) return cb(err);
			toRemoveCount += 1;
			if (toAddCount === toAdd.length && toRemoveCount === toRemove.length) return cb(null);
		});
	});

};

var getGroupArticles = function(client, group, page, order, cb) {
	order = order || 'score:';
	var key = order + group;
	client.exists(key, function(err, exists) {
		if (err) return cb(err);
		if (!exists) {
			var args = [key, '2', 'group:' + group, order, 'aggregate', 'max'];
			client.zinterstore(args, function(err) {
				if (err) return cb(err);
				client.expire(key, 60, function(err) {
					if (err) return cb(err);
					getArticles(client, page, key, cb);
				});
			});
		} else {
			getArticles(client, page, key, cb);
		}
	});
};


module.exports = {
	articleVote: articleVote,
	postArticle: postArticle,
	getArticles: getArticles,
	addRemoveGroups: addRemoveGroups,
	getGroupArticles: getGroupArticles,

	// Exposed for testing purposes only
	ONE_WEEK_IN_SECONDS: ONE_WEEK_IN_SECONDS,
	setToday: function(f) {
		today = f || todayOrig;
	}
};
