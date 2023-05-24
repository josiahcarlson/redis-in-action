using System.Diagnostics;
using StackExchange.Redis;

namespace Chapter5;

public class AccessTimer {
	private readonly IDatabase _conn;
	private readonly Stopwatch _watch;

	public AccessTimer(IDatabase conn,Stopwatch watch){
		_conn = conn;
		_watch = watch;
	}

	public void Start() {
		_watch.Restart();
	}

	public void Stop(string context){
		_watch.Stop();
		var delta = _watch.Elapsed.TotalSeconds;
		var stats = StatsOperations.UpdateStats(_conn, context, "AccessTime", delta);
		if (stats is null) {
			throw new NullReferenceException(nameof(stats));
		}
		var average = stats["sum"] / stats["count"];

		var trans = _conn.CreateTransaction();
		trans.SortedSetAddAsync("slowest:AccessTime", context, average);
		trans.SortedSetRemoveRangeByRankAsync("slowest:AccessTime", 0, -101);
		trans.Execute();
	}
}
