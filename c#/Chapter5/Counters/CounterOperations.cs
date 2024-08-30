using StackExchange.Redis;

namespace Chapter5.Counters;

public static class CounterOperations {
	private static readonly int[] _precisions = { 1, 5, 60, 300, 3600, 18000, 86400 };
	public static List<(int, int)> GetCounter(
		IDatabase conn, string name, int precision) {
		var hash = $"{precision}:{name}";
		var data = conn.HashGetAll($"count:{hash}");
		var results = new List<(int, int)>();
		foreach (var entry in data) {
			results.Add((Convert.ToInt32(entry.Name), Convert.ToInt32(entry.Value)));
		}

		results.Sort();
		return results;
	}
	public static void UpdateCounter(IDatabase conn, string name, int count, long now) {
		var trans = conn.CreateTransaction();
		foreach (var precision in _precisions) {
			var precisionNow = (now / precision) * precision;
			var hash = $"{precision}:{name}";
			trans.SortedSetAddAsync("known:", hash, 0);
			trans.HashIncrementAsync($"count:{hash}", precisionNow.ToString(), count);
		}
		trans.Execute();
	}
}
