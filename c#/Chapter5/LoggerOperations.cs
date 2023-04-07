using StackExchange.Redis;

namespace Chapter5;

public static class LoggerOperations {
	public enum MessageSeverity {
		Debug,
		Info ,
		Warning,
		Error,
		Critical,
	}

	public static void LogRecent(IDatabase conn, string name, string message, MessageSeverity severity = MessageSeverity.Info) {
		var destination = $"recent:{name}:{severity.ToString().ToLower()}";
		var tasks = new List<Task> {
			conn.ListLeftPushAsync(destination, $"{DateTime.UtcNow.ToTimestampFormat()} {message}"),
			conn.ListTrimAsync(destination, 0, 99)
		};
		conn.WaitAll(tasks.ToArray());
	}

	public static void LogCommon(IDatabase conn, string name, string message, MessageSeverity severity = MessageSeverity.Info, int timeout = 5000) {
		var severityString = severity.ToString().ToLower();
		var commonDest = $"common:{name}:{severityString}";
		var startKey = $"{commonDest}:start";
		var end =  DateTimeOffset.UtcNow.AddMilliseconds(timeout).ToUnixTimeMilliseconds();

		while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < end) {
			var existing = conn.StringGet(startKey);
			var transaction = conn.CreateTransaction();
			if (!string.IsNullOrEmpty(existing)) {
				transaction.AddCondition(Condition.StringEqual(startKey, existing));
			}

			var hourStart = DateTime.UtcNow.ToIsoFormat();

			if (existing!=RedisValue.Null && DateTime.Compare(DateTime.Parse(existing.ToString()), DateTime.Parse(hourStart)) < 0) {
				transaction.KeyRenameAsync(commonDest, $"{commonDest}:last");
				transaction.KeyRenameAsync(startKey, $"{commonDest}:pstart");
				transaction.StringSetAsync(startKey, hourStart);
			}

			transaction.SortedSetIncrementAsync(commonDest,message, 1 );

			var recentDest = $"recent:{name}:{severityString}";
			transaction.ListLeftPushAsync(recentDest, $"{DateTime.UtcNow.ToTimestampFormat()} {message}");
			transaction.ListTrimAsync(recentDest, 0, 99);
			var result = transaction.Execute();

			if (result) {
				return;
			}
		}
	}
}
