using StackExchange.Redis;

namespace Chapter5;

public static class StatsOperations {
	public static Dictionary<string, double>? UpdateStats(IDatabase conn, string context, string type, double value) {
		var timeout = 5000;
		var destination = $"stats:{context}:{type}";
		var startKey = $"{destination}:start";
		var end = DateTimeOffset.UtcNow.AddMilliseconds(timeout).ToUnixTimeMilliseconds();
		while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < end) {
			var startPrev = conn.StringGet(startKey);
			var trans = conn.CreateTransaction();

			trans.AddCondition(startPrev != RedisValue.Null ? Condition.StringEqual(startKey, startPrev) : Condition.KeyNotExists(startKey));

			var hourStart = DateTime.UtcNow.ToIsoFormat();

			var existing = conn.StringGet(startKey);
			if (existing != RedisValue.Null && DateTime.Compare(DateTime.Parse(existing.ToString()), DateTime.Parse(hourStart)) < 0) {
				trans.KeyRenameAsync(destination, destination + ":last");
				trans.KeyRenameAsync(startKey, destination + ":pstart");
				trans.StringSetAsync(startKey, hourStart);
			}

			var tkey1 = Guid.NewGuid().ToString();
			var tkey2 = Guid.NewGuid().ToString();
			trans.SortedSetAddAsync(tkey1, "min", value);
			trans.SortedSetAddAsync(tkey2, "max", value);

			trans.SortedSetCombineAndStoreAsync(SetOperation.Union, destination, destination, tkey1, Aggregate.Min);
			trans.SortedSetCombineAndStoreAsync(SetOperation.Union, destination, destination, tkey2, Aggregate.Max);

			trans.KeyDeleteAsync(tkey1);
			trans.KeyDeleteAsync(tkey2);
			trans.SortedSetIncrementAsync(destination, "count", 1);
			trans.SortedSetIncrementAsync(destination, "sum", value);
			trans.SortedSetIncrementAsync(destination, "sumsq", value * value);

			var result = trans.Execute();
			if (!result) {
				continue;
			}

			var statsNames = new List<string>() { "count", "sum", "sumsq" };

			var sortedSet = conn.SortedSetRangeByScoreWithScores(destination)
				.Where(c => statsNames.Contains(c.Element.ToString()))
				.OrderBy(c => c.Element.ToString())
				.ToDictionary(c => c.Element.ToString(), c => c.Score);

			return sortedSet;
		}

		return null;
	}

	public static Dictionary<string, double> GetStats(IDatabase conn, string context, string type) {
		var key = $"stats:{context}:{type}";
		var stats = new Dictionary<string, double>();
		var data = conn.SortedSetRangeByRankWithScores(key, 0, -1);
		foreach (var pair in data) {
			stats.Add(pair.Element.ToString(), pair.Score);
		}

		stats.Add("average", stats["sum"] / stats["count"]);
		var numerator = stats["sumsq"] - Math.Pow(stats["sum"], 2) / stats["count"];
		var count = stats["count"];
		stats.Add("stddev", Math.Pow(numerator / (count > 1 ? count - 1 : 1), .5));
		return stats;
	}
}
