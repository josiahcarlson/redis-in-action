var ch01 = require('./main'),
	client = require('redis').createClient();

client.flushdb();

ch01.postArticle(client, 'username', 'A title', 'http://www.google.com', function(err, articleId) {

	console.log('We posted a new article with id', articleId);

	client.hgetall('article:' + articleId, function(err, hash) {
		console.log("It's HASH looks like", hash);
	});

	ch01.articleVote(client, 'other_user', 'article:' + articleId, function(err, votes) {
		console.log('We voted for the article, it now has votes', votes);

		client.hgetall('article:' + articleId, function(err, hash) {
			console.log("After voting, it's HASH looks like", hash);
		});

		ch01.getArticles(client, 1, null, function(err, articles) {
			console.log('The current highest-scoring articles are');
			console.log(articles);

			ch01.addRemoveGroups(client, articleId, ['new-group'], null, function() {
				ch01.getGroupArticles(client, 'new-group', 1, null, function(err, articles) {
					console.log('We added the article to a new group, other articles include:', articles);
					client.quit();
				});
			});
		});
	});
});


