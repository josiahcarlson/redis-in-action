// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using StackExchange.Redis;

namespace Chapter1;

public class Chapter1 {
	private const int OneWeekInSeconds = 7 * 86400;
	private const int VoteScore = 432;
	private const int ArticlesPerPage = 25;

	public static void Main() {
		new Chapter1().run();
	}

	private void run() {
		var con = ConnectionMultiplexer.Connect("localhost");
		var db = con.GetDatabase();

		var articleId = postArticle(db, "username", "A title", "https://www.google.com");
		Console.WriteLine("We posted a new article with id: " + articleId);
		Console.WriteLine("Its HASH looks like:");
		var articleData = db.HashGetAll("article:" + articleId);

		foreach (var entry in articleData) {
			Console.WriteLine(" " + entry.Name + ": " + entry.Value);
		}

		Console.WriteLine();

		articleVote(db, "other_user", "article:" + articleId);
		var votes = (int?)db.HashGet("article:" + articleId, "votes") ?? 0;
		Console.WriteLine("We voted for the article, it now has votes: " + votes);
		Debug.Assert(votes > 1, "Vote count is less than 1");

		Console.WriteLine("The currently highest-scoring articles are:");
		var articles = getArticles(db, 1);
		printArticles(articles);
		Debug.Assert(articles.Count >= 1, "Article count is less than 1");

		addGroups(db, articleId, new[]{"new-group"});
		Console.WriteLine("We added the article to a new group, other articles include:");
		var groupArticles = getGroupArticles(db, "new-group", 1);
		printArticles(groupArticles);
		Debug.Assert(groupArticles.Count >= 1, "Article group count is less than 1");
	}

	private string postArticle(IDatabase db, string user, string title, string link) {
		var articleId = db.StringIncrement("article:").ToString();

		var voted = "voted:" + articleId;
		db.SetAdd(voted, user);
		db.KeyExpire(voted, TimeSpan.FromSeconds(OneWeekInSeconds));

		var now = DateTimeOffset.Now.ToUnixTimeSeconds();
		var article = "article:" + articleId;
		var articleData = new List<HashEntry> {
			new("title", title),
			new("link", link),
			new("user", user),
			new("now", now.ToString()),
			new("votes", "1")
		};
		db.HashSet(article, articleData.ToArray());

		db.SortedSetAdd("score:", article, now + VoteScore);
		db.SortedSetAdd("time:", article, now);

		return articleId;
	}

	private void articleVote(IDatabase db, string user, string article) {
		var cutoff = DateTimeOffset.Now.ToUnixTimeSeconds() - OneWeekInSeconds;
		var articleScore = db.SortedSetScore("time:", article) ?? 0;

		if (articleScore < cutoff) {
			return;
		}

		var articleId = article.Substring(article.IndexOf(':') + 1);

		if (db.SetAdd("voted:" + articleId, user)) {
			db.SortedSetIncrement("score:", article, VoteScore);
			db.HashIncrement(article, "votes");
		}
	}

	private List<Dictionary<RedisValue, RedisValue>>
		getArticles(IDatabase db, int page, string order = "score:") {
		var start = (page - 1) * ArticlesPerPage;
		var end = start + ArticlesPerPage - 1;

		var ids = db.SortedSetRangeByRank(order, start, end, order: Order.Descending);
		var articles = new List<Dictionary<RedisValue, RedisValue>>();

		foreach (var id in ids) {
			var articleData = db.HashGetAll(id.ToString())
				.ToDictionary(c => c.Name, c => c.Value);
			articleData["id"] = id;
			articles.Add(articleData);
		}

		return articles;
	}

	private void printArticles(List<Dictionary<RedisValue, RedisValue>> articles) {
		foreach (var article in articles) {
			Console.WriteLine(" id: " + article["id"]);
			foreach (var articleData in article.Where(c => !c.Key.Equals("id"))) {
				Console.WriteLine("    " + articleData.Key + ": " + articleData.Value);
			}
		}
	}

	private void addGroups(IDatabase db, string articleId, string[] toAdd) {
		var article = "article:" + articleId;
		foreach (var group in toAdd) {
			db.SetAdd("group:" + group, article);
		}
	}

	private List<Dictionary<RedisValue, RedisValue>> getGroupArticles(IDatabase db, string group, int page, string order = "score:") {
		var key = order + group;
		if (!db.KeyExists(key)) {
			db.SortedSetCombineAndStore(SetOperation.Intersect, key, "group:" + group, order, aggregate: Aggregate.Max);
			db.KeyExpire(key, TimeSpan.FromSeconds(60));
		}

		return getArticles(db, page, key);
	}
}
