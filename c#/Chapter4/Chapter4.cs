using System.Diagnostics;
using System.Reflection;
using StackExchange.Redis;

namespace Chapter4;

public class Chapter4 {
	private const string MarketKey = "market:";

	public static void Main() {
		new Chapter4().run();
	}

	private void run() {
		var connection = ConnectionMultiplexer.Connect("localhost");
		var db = connection.GetDatabase(15);

		testListItem(db, false);
		testPurchaseItem(db);
		testBenchmarkUpdateToken(db);
	}

	private static void testListItem(IDatabase conn, bool nested) {
		if (!nested) {
			Console.WriteLine("\n----- testListItem -----");
		}

		Console.WriteLine("We need to set up just enough state so that a user can list an item");
		var sellerId = "userX";
		var item = "itemX";
		conn.SetAdd("inventory:" + sellerId, item);
		var i = conn.SetMembers("inventory:" + sellerId);

		Console.WriteLine("The user's inventory has:");
		foreach (var member in i) {
			Console.WriteLine("  " + member);
		}

		Debug.Assert(i.Length > 0, "Inventory is empty");
		Console.WriteLine();

		Console.WriteLine("Listing the item...");
		var listResult = listItem(conn, item, sellerId, 10);
		Console.WriteLine("Listing the item succeeded? " + listResult);
		Debug.Assert(listResult, "Changes were not committed");
		var marketItems = conn.SortedSetRangeByRankWithScores(MarketKey, 0, -1);
		Console.WriteLine("The market contains:");
		foreach (var marketItem in marketItems) {
			Console.WriteLine("  " + marketItem.Element + ", " + marketItem.Score);
		}

		Debug.Assert(marketItems.Length > 0, "Market items is empty");
	}

	private void testPurchaseItem(IDatabase conn) {
		Console.WriteLine("\n----- testPurchaseItem -----");
		testListItem(conn, true);

		Console.WriteLine("We need to set up just enough state so a user can buy an item");
		conn.HashSet("users:userY", "funds", "125");
		var r = conn.HashGetAll("users:userY");
		Console.WriteLine("The user has some money:");
		foreach (var entry in r) {
			Console.WriteLine("  " + entry.Name + ": " + entry.Value);
		}

		Debug.Assert(r.Length > 0, "User hashset not found!");
		var funds = r.Any(a => a.Name == "funds");
		Debug.Assert(funds, "Didn't find a hash entry for funds");
		Console.WriteLine();

		Console.WriteLine("Let's purchase an item");
		var purchaseResult = purchaseItem(conn, "userY", "itemX", "userX", 10);
		Console.WriteLine("Purchasing an item succeeded? " + purchaseResult);
		Debug.Assert(purchaseResult, "Changes were not committed");

		r = conn.HashGetAll("users:userY");
		Console.WriteLine("Their money is now:");
		foreach (var entry in r) {
			Console.WriteLine("  " + entry.Name + ": " + entry.Value);
		}

		Debug.Assert(r.Length > 0, "Used data is empty");

		var buyer = "userY";
		var i = conn.SetMembers("inventory:" + buyer);
		Console.WriteLine("Their inventory is now:");
		foreach (var member in i) {
			Console.WriteLine("  " + member);
		}

		Debug.Assert(i.Length > 0, "Buyer inventory is empty");
		Debug.Assert(i.Any(item => item.Equals("itemX")), "itemX was not moved to buyers inventory");
		Debug.Assert(conn.SortedSetScore(MarketKey, "itemX.userX") == null, "Market still contains itemX.userX");
	}

	private static bool listItem(IDatabase conn, string itemId, string sellerId, double price) {
		var inventory = "inventory:" + sellerId;
		var item = itemId + '.' + sellerId;
		var end = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 5000;

		while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < end) {
			// The client has a multiplexer approach to connections.
			// As a result we can't use multi/exec/watch directly.
			// We can however add transaction conditions which are functioning similarly behind the scenes.
			// So we will add them by hand in order to simulate a watch request.
			// We will load the set, verify that cardinality remained the same as well as that items were unchanged
			var inventorySet = conn.SetMembers(inventory);
			var trans = conn.CreateTransaction();

			trans.AddCondition(Condition.SetContains(inventory, itemId));
			trans.AddCondition(Condition.SetLengthEqual(inventory, inventorySet.Length));
			foreach (var invItem in inventorySet) {
				trans.AddCondition(Condition.SetContains(inventory, invItem));
			}

			trans.SortedSetAddAsync(MarketKey, item, price);
			trans.SetRemoveAsync(inventory, itemId);
			var committed = trans.Execute();

			if (!committed) {
				continue;
			}

			return true;
		}

		return false;
	}

	private static bool purchaseItem(
		IDatabase conn, string buyerId, string itemId, string sellerId, double listedPrice) {
		var buyer = "users:" + buyerId;
		var seller = "users:" + sellerId;
		var item = itemId + '.' + sellerId;
		var inventory = "inventory:" + buyerId;
		var end = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 10000;

		while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < end) {
			var trans = conn.CreateTransaction();

			// The client has a multiplexer approach to connections.
			// As a result we can't use multi/exec/watch directly.
			// We can however add transaction conditions which are functioning similarly behind the scenes.
			// So we will add them by hand in order to simulate a watch request.
			// We will load the hashset, verify that cardinality remained the same as well as that items were unchanged
			// Similarly, we will do a check to the market set to verify that everything remained unchanged
			var userSet = conn.HashGetAll(buyer);
			trans.AddCondition(Condition.HashLengthEqual(buyer, userSet.Length));
			foreach (var entry in userSet) {
				trans.AddCondition(Condition.HashEqual(buyer, entry.Name, entry.Value));
			}

			var marketSortedSet = conn.SortedSetRangeByRankWithScores(MarketKey, 0, -1);
			trans.AddCondition(Condition.SortedSetLengthEqual(MarketKey, marketSortedSet.Length));
			foreach (var entry in marketSortedSet) {
				trans.AddCondition(Condition.SortedSetEqual(MarketKey, entry.Element, entry.Score));
			}

			var price = conn.SortedSetScore(MarketKey, item);
			var funds = double.Parse(conn.HashGet(buyer, "funds").ToString());
			if (price != listedPrice || price > funds) {
				return false;
			}

			trans.HashIncrementAsync(seller, "funds", (int)price);
			trans.HashIncrementAsync(buyer, "funds", (int)-price);
			trans.SetAddAsync(inventory, itemId);
			trans.SortedSetRemoveAsync(MarketKey, item);
			var result = trans.Execute();
			// null response indicates that the transaction was aborted due to
			// the watched key changing.
			if (!result) {
				continue;
			}

			return true;
		}

		return false;
	}

	private void testBenchmarkUpdateToken(IDatabase conn) {
		Console.WriteLine("\n----- testBenchmarkUpdate -----");
		benchmarkUpdateToken(conn, 5);
	}

	private void benchmarkUpdateToken(IDatabase conn, int duration) {
		var methods = new List<Action<IDatabase, string, string, string?>>() {
			updateToken,
			updateTokenPipeline
		};

		Console.WriteLine("{0,-20} {1,-10} {2,-15} {3,-30}","Update method","#Runs","Delta(seconds)",$"#Runs to delta(seconds) ratio");
		foreach (var method in methods) {
			var count = 0;
			var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
			var end = start + (duration * 1000);
			while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < end) {
				count++;
				method(conn, "token", "user", "item");
			}

			var delta = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()- start;
			Console.WriteLine("{0,-20} {1,-10} {2,-15} {3,-30}",
				method.GetMethodInfo().Name,
				count,
				(delta / 1000),
				(count / (delta / 1000)));
		}
	}

	private static void updateToken(IDatabase conn, string token, string user, string? item) {
		var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000;

		conn.HashSet("login:", token, user);
		conn.SortedSetAdd("recent:", token, timestamp);
		if (item != null) {
			conn.SortedSetAdd("viewed:" + token, item, timestamp);
			conn.SortedSetRemoveRangeByRank("viewed:" + token, 0, -26);
			conn.SortedSetIncrement("viewed:", item, -1);
		}
	}

	private static void updateTokenPipeline(IDatabase conn, string token, string user, string? item) {
		var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000;
		var x = new List<Task> {
			Capacity = 0
		};
		x.Add(conn.HashSetAsync("login:", token, user));
		x.Add(conn.SortedSetAddAsync("recent:", token, timestamp));
		if (item != null) {
			x.Add(conn.SortedSetAddAsync("viewed:" + token, item, timestamp));
			x.Add(conn.SortedSetRemoveRangeByRankAsync("viewed:" + token, 0, -26));
			x.Add(conn.SortedSetIncrementAsync("viewed:", item, -1));
		}

		conn.WaitAll(x.ToArray());
	}
}
