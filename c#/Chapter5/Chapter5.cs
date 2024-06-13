using System.Diagnostics;
using Chapter5.Configuration;
using Chapter5.Counters;
using Chapter5.IpLookup;
using StackExchange.Redis;

namespace Chapter5;

public class Chapter5 {
	public static void Main() {
		new Chapter5().run();
	}

	private void run() {
		var connection = ConnectionMultiplexer.Connect("localhost");
		var db = connection.GetDatabase(15);

		testLogRecent(db);
		testLogCommon(db);
		testCounters(db);
		testStats(db);
		testAccessTime(db);
		testIpLookup(db);
		testIsUnderMaintenance(db);
		testConfig(db);
	}

	private void testLogRecent(IDatabase conn) {
		Console.WriteLine("\n----- testLogRecent -----");
		Console.WriteLine("Let's write a few logs to the recent log");
		for (var i = 0; i < 5; i++) {
			LoggerOperations.LogRecent(conn, "test", "this is message " + i);
		}

		var recent = conn.ListRange("recent:test:info", 0, -1);
		Console.WriteLine(
			"The current recent message log has this many messages: " +
			recent.Length);
		Console.WriteLine("Those messages include:");
		foreach (var message in recent) {
			Console.WriteLine(message);
		}

		Debug.Assert(recent.Length >= 5, "Expected at least 5 recently logged messages");
	}

	private void testLogCommon(IDatabase conn) {
		Console.WriteLine("\n----- testLogCommon -----");
		Console.WriteLine("Let's write some items to the common log");
		for (var count = 1; count < 6; count++) {
			for (var i = 0; i < count; i++) {
				LoggerOperations.LogCommon(conn, "test", "message-" + count);
			}
		}

		conn.SortedSetRangeByRankWithScores("common:test:info", 0, -1, Order.Descending);
		var common = conn.SortedSetRangeByRankWithScores("common:test:info", 0, -1, Order.Descending);
		Console.WriteLine("The current number of common messages is: " + common.Length);
		Console.WriteLine("Those common messages are:");
		foreach (var tuple in common) {
			Console.WriteLine("  " + tuple.Element + ", " + tuple.Score);
		}

		Debug.Assert(common.Length >= 5, "The common logs contain less than 5 entries");
	}

	private void testCounters(IDatabase conn) {
		Console.WriteLine("\n----- testCounters -----");
		Console.WriteLine("Let's update some counters for now and a little in the future");
		var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
		var random = new Random();
		for (var i = 0; i < 10; i++) {
			var count = (int)random.NextInt64(1, 6);
			CounterOperations.UpdateCounter(conn, "test", count, now + i);
		}

		var counter = CounterOperations.GetCounter(conn, "test", 1);
		Console.WriteLine("We have some per-second counters: " + counter.Count);
		Console.WriteLine("These counters include:");
		foreach (var count in counter) {
			Console.WriteLine("  " + count);
		}

		Debug.Assert(counter.Count >= 10, "Counters are less than 10");

		counter = CounterOperations.GetCounter(conn, "test", 5);
		Console.WriteLine("We have some per-5-second counters: " + counter.Count);
		Console.WriteLine("These counters include:");
		foreach (var count in counter) {
			Console.WriteLine("  " + count);
		}

		Debug.Assert(counter.Count >= 2, "Counters are less than 2");
		Console.WriteLine();

		Console.WriteLine("Let's clean out some counters by setting our sample count to 0");
		var thread = new CleanCountersThread(0, 2 * 86400000);
		thread.Start();
		Thread.Sleep(1000);
		thread.Quit();
		counter = CounterOperations.GetCounter(conn, "test", 86400);
		Console.WriteLine("Did we clean out all of the counters? " + (counter.Count == 0));
		Debug.Assert(counter.Count == 0, "Counters were not cleaned up");
	}

	private void testStats(IDatabase conn) {
		Console.WriteLine("\n----- testStats -----");
		Console.WriteLine("Let's add some data for our statistics!");
		var r = new Dictionary<string, double>();
		var random = new Random();
		for (var i = 0; i < 5; i++) {
			var value = (random.NextDouble() * 11) + 5;
			r = StatsOperations.UpdateStats(conn, "temp", "example", value);
		}

		Debug.Assert(r != null, "Aggregate statistics are null");

		Console.WriteLine("We have some aggregate statistics: " + string.Join(", ", r));
		var stats = StatsOperations.GetStats(conn, "temp", "example");
		Console.WriteLine("Which we can also fetch manually:");
		foreach (var pair in stats) {
			Console.WriteLine($"{pair.Key}:{pair.Value}");
		}

		Debug.Assert(stats["count"] >= 5, "Count is less than 5 in our stats");
	}

	private void testAccessTime(IDatabase conn) {
		Console.WriteLine("\n----- testAccessTime -----");
		Console.WriteLine("Let's calculate some access times...");
		var timer = new AccessTimer(conn, new Stopwatch());
		var rand = new Random();
		for (var i = 0; i < 10; i++) {
			timer.Start();
			Thread.Sleep(rand.Next(0, 1001));
			timer.Stop("req-" + i);
		}

		Console.WriteLine("The slowest access times are:");
		conn.SortedSetRangeByRankWithScores("slowest:AccessTime", 0, -1);
		var accessTimes = conn.SortedSetRangeByRankWithScores("slowest:AccessTime", 0, -1);
		foreach (var pair in accessTimes) {
			Console.WriteLine("  " + pair.Element + ", " + pair.Score);
		}

		Debug.Assert(accessTimes.Length >= 10, "Our access times dataset has less than 10 elements");
		Console.WriteLine();
	}

	private void testIpLookup(IDatabase conn) {
		Console.WriteLine("\n----- testIpLookup -----");
		var cwd = Environment.GetEnvironmentVariable("user.dir");

		if (string.IsNullOrEmpty(cwd)) {
			throw new ArgumentNullException(nameof(cwd), "Environment variable user.dir should be set");
		}

		var blocksPath = (cwd + "/GeoLiteCity-Blocks.csv");
		var locationsPath = cwd + "/GeoLiteCity-Location.csv";
		if (!File.Exists(blocksPath)) {
			Console.WriteLine("********");
			Console.WriteLine("GeoLiteCity-Blocks.csv not found at: " + blocksPath);
			Console.WriteLine("********");
			return;
		}

		if (!File.Exists(locationsPath)) {
			Console.WriteLine("********");
			Console.WriteLine("GeoLiteCity-Location.csv not found at: " + locationsPath);
			Console.WriteLine("********");
			return;
		}

		Console.WriteLine("Importing IP addresses to Redis... (this may take a while)");
		IpLookupOperations.ImportIpsToRedis(conn, blocksPath);
		var ranges = conn.SortedSetLength("ip2cityid:");
		Console.WriteLine("Loaded ranges into Redis: " + ranges);
		Debug.Assert(ranges > 1000, "Ip2CityId dataset has a cardinality less than 1000");
		Console.WriteLine();

		Console.WriteLine("Importing Location lookups to Redis... (this may take a while)");
		IpLookupOperations.ImportCitiesToRedis(conn, locationsPath);
		var cities = conn.HashLength("cityid2city:");
		Console.WriteLine("Loaded city lookups into Redis:" + cities);
		Debug.Assert(cities > 1000, "Cities are less than 1000");
		Console.WriteLine();

		Console.WriteLine("Let's lookup some locations!");

		for (var i = 0; i < 5; i++) {
			var ip =
				$"{IpLookupOperations.RandomOctet(255)}.{IpLookupOperations.RandomOctet(256)}.{IpLookupOperations.RandomOctet(256)}.{IpLookupOperations.RandomOctet(256)}";
			var result = IpLookupOperations.FindCityByIp(conn, ip) ?? Array.Empty<string>();
			var toDisplay = $"[{string.Join(",", result)}]";
			Console.WriteLine(toDisplay);
		}
	}

	private static void testIsUnderMaintenance(IDatabase conn) {
		Console.WriteLine("\n----- testIsUnderMaintenance -----");
		Console.WriteLine("Are we under maintenance (we shouldn't be)? " + Maintenance.IsUnderMaintenance(conn));
		conn.StringSet("is-under-maintenance", "yes");
		Console.WriteLine("We cached this, so it should be the same: " + Maintenance.IsUnderMaintenance(conn));
		Thread.Sleep(1000);
		Console.WriteLine("But after a sleep, it should change: " + Maintenance.IsUnderMaintenance(conn));
		Console.WriteLine("Cleaning up...");
		conn.KeyDelete("is-under-maintenance");
		Thread.Sleep(1000);
		Console.WriteLine("Should be False again: " + Maintenance.IsUnderMaintenance(conn));
	}

	private static void testConfig(IDatabase conn) {
		Console.WriteLine("\n----- testConfig -----");
		Console.WriteLine("Let's set a config and then get a connection from that config...");
		var config = new Dictionary<string,object>();
		config.Add("db", 15);
		ConfigOperations.SetConfig(conn, "redis", "test", config);

		var conn2 = ConfigOperations.RedisConnection("test");
		Console.WriteLine(
			"We can run commands from the configured connection: " + (conn2 != null));
	}
}
