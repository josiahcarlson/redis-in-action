using System.Diagnostics;
using StackExchange.Redis;

namespace Chapter2;

public class Chapter2 {
	public static void Main() {
		new Chapter2().run();
	}

	private void run() {
		var connection = ConnectionMultiplexer.Connect("localhost");
		var db = connection.GetDatabase(15);

		testLoginCookies(db);
		testShoppingCartCookies(db);
		testCacheRows(db);
		testCacheRequest(db);
	}

	private void testLoginCookies(IDatabase conn) {
		Console.WriteLine("\n----- testLoginCookies -----");
		var token = Guid.NewGuid().ToString();

		var username = "someUser";
		updateToken(conn, token, username, "itemX");
		Console.WriteLine("We just logged-in/updated token: " + token);
		Console.WriteLine($"For user: '{username}'");
		Console.WriteLine();

		Console.WriteLine("What username do we get when we look-up that token?");
		var r = checkToken(conn, token);
		Console.WriteLine(r);
		Console.WriteLine();
		Debug.Assert(username.Equals(r), "username retrieved from token does not match initial username.");
		Debug.Assert(r is not null, "Token is null");

		Console.WriteLine("Let's drop the maximum number of cookies to 0 to clean them out");
		Console.WriteLine("We will start a thread to do the cleaning, while we stop it later");

		var thread = new CleanSessionsThread(conn, 0);
		thread.Start();
		Thread.Sleep(1000);
		thread.Quit();
		Thread.Sleep(2000);
		if (thread.IsAlive()) {
			throw new Exception("The clean sessions thread is still alive?!?");
		}

		var s = conn.HashLength("login:");
		Console.WriteLine("The current number of sessions still available is: " + s);
		Debug.Assert(s == 0, "sessions are not zero");
	}

	private void testShoppingCartCookies(IDatabase conn) {
		Console.WriteLine("\n----- testShoppingCartCookies -----");
		var token = Guid.NewGuid().ToString();

		Console.WriteLine("We'll refresh our session...");
		updateToken(conn, token, "username", "itemX");
		Console.WriteLine("And add an item to the shopping cart");
		addToCart(conn, token, "itemY", 3);

		var r = conn.HashGetAll("cart:" + token);

		Console.WriteLine("Our shopping cart currently has:");
		foreach (var entry in r) {
			Console.WriteLine("  " + entry.Name + ": " + entry.Value);
		}

		Console.WriteLine();

		Debug.Assert(r.Length >= 1, "Shopping cart is empty");

		Console.WriteLine("Let's clean out our sessions and carts");
		var thread = new CleanFullSessionsThread(conn, 0);
		thread.Start();
		Thread.Sleep(1000);
		thread.Quit();
		Thread.Sleep(2000);
		if (thread.IsAlive()) {
			throw new Exception("The clean sessions thread is still alive?!?");
		}

		r = conn.HashGetAll("cart:" + token);
		Console.WriteLine("Our shopping cart now contains:");
		foreach (var entry in r) {
			Console.WriteLine("  " + entry.Name + ": " + entry.Value);
		}

		Debug.Assert(r.Length == 0, "cart is not empty");
	}

	private void testCacheRows(IDatabase conn) {
		Console.WriteLine("\n----- testCacheRows -----");
		Console.WriteLine("First, let's schedule caching of itemX every 5 seconds");
		scheduleRowCache(conn, "itemX", 5);
		Console.WriteLine("Our schedule looks like:");

		var s = conn.SortedSetRangeByRankWithScores("schedule:", 0, -1);
		foreach (var entry in s) {
			Console.WriteLine("  " + entry.Element + ", " + entry.Score);
		}

		Debug.Assert(s.Length != 0, "schedule set is empty");

		Console.WriteLine("We'll start a caching thread that will cache the data...");

		var thread = new CacheRowsThread(conn);
		thread.Start();
		Thread.Sleep(1000);
		Console.WriteLine("Our cached data looks like:");
		string? r = conn.StringGet("inv:itemX");
		Console.WriteLine(r);
		Debug.Assert(r is not null, "cached data is null");
		Console.WriteLine();

		Console.WriteLine("We'll check again in 5 seconds...");
		Thread.Sleep(5000);
		Console.WriteLine("Notice that the data has changed...");
		string? r2 = conn.StringGet("inv:itemX");
		Console.WriteLine(r2);
		Console.WriteLine();
		Debug.Assert(r2 is not null, "changed cached data is null");
		Debug.Assert(!r.Equals(r2), "cached data did not change");

		Console.WriteLine("Let's force un-caching");
		scheduleRowCache(conn, "itemX", -1);
		Thread.Sleep(1000);
		r = conn.StringGet("inv:itemX");
		Console.WriteLine("The cache was cleared? " + (r == null));
		Debug.Assert(r is null, "cached data was not un-cached");

		thread.Quit();
		Thread.Sleep(2000);
		if (thread.IsAlive()) {
			throw new Exception("The database caching thread is still alive?!?");
		}
	}

	private void testCacheRequest(IDatabase conn) {
		Console.WriteLine("\n----- testCacheRequest -----");
		var token = Guid.NewGuid().ToString();

		updateToken(conn, token, "username", "itemX");
		var url = "http://test.com/?item=itemX";
		Console.WriteLine("We are going to cache a simple request against " + url);
		var result = cacheRequest(conn, url, s=> "content for " + s);
		Console.WriteLine("We got initial content:\n" + result);
		Console.WriteLine();

		Debug.Assert(result is not null,"Request was not cached");

		Console.WriteLine("To test that we've cached the request, we'll pass a bad callback");
		var result2 = cacheRequest(conn, url, null);
		Console.WriteLine("We ended up getting the same response!\n" + result2);

		Debug.Assert(result.Equals(result2),"Cached request was not altered");

		Debug.Assert(!canCache(conn, "http://test.com/"));
		Debug.Assert(!canCache(conn, "http://test.com/?item=itemX&_=1234536"));
	}

	private string? checkToken(IDatabase conn, string token) {
		return conn.HashGet("login:", token);
	}

	private void updateToken(IDatabase conn, string token, string user, string? item) {
		var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
		conn.HashSet("login:", token, user);
		conn.SortedSetAdd("recent:", token, timestamp);
		if (item != null) {
			conn.SortedSetAdd("viewed:" + token, item, timestamp);
			conn.SortedSetRemoveRangeByRank("viewed:" + token, 0, -26);
			conn.SortedSetIncrement("viewed:", item, -1);
		}
	}

	private void addToCart(IDatabase conn, string session, string item, int count) {
		if (count <= 0) {
			conn.HashDelete("cart:" + session, item);
		} else {
			conn.HashSet("cart:" + session, item, count);
		}
	}

	private static void scheduleRowCache(IDatabase conn, string rowId, int delay) {
		conn.SortedSetAdd("delay:", rowId, delay);
		conn.SortedSetAdd("schedule:", rowId, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
	}

	private string? cacheRequest(IDatabase conn, string request, Func<string, string>? callback) {
		if (!canCache(conn, request)) {
			return callback?.Invoke(request);
		}

		var pageKey = "cache:" + hashRequest(request);
		var content = conn.StringGet(pageKey);

		if (!content.HasValue && callback != null) {
			content = callback(request);
			conn.StringSet(pageKey, content);
			conn.KeyExpire(pageKey, TimeSpan.FromSeconds(300));
		}

		return content;
	}

	private bool canCache(IDatabase conn, String request) {
		try {
			var url = new Uri(request);
			var parameters = new Dictionary<string, string?>();
			if (!string.IsNullOrEmpty(url.Query)) {
				foreach (var par in url.Query[1..].Split("&")) {
					var pair = par.Split("=", 2);
					parameters.Add(pair[0], pair.Length == 2 ? pair[1] : null);
				}
			}

			var itemId = extractItemId(parameters);
			if (itemId == null || isDynamic(parameters)) {
				return false;
			}

			var rank = conn.SortedSetRank("viewed:", itemId);
			return rank is < 10000;
		} catch (FormatException) {
			return false;
		}
	}

	private bool isDynamic(Dictionary<String, String?> parameters) {
		return parameters.ContainsKey("_");
	}

	private string? extractItemId(Dictionary<String, String?> parameters) {
		parameters.TryGetValue("item",out var result);
		return result;
	}

	private string hashRequest(String request) {
		return request.GetHashCode().ToString();
	}
}
