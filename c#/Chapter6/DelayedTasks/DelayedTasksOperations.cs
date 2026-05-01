using System.Text.Json;
using StackExchange.Redis;

namespace Chapter6.DelayedTasks;

public static class DelayedTasksOperations {
	public static string ExecuteLater(IDatabase db, string queue, string functionName, List<string> args, long delay) {
		var identifier = Guid.NewGuid().ToString();
		var itemArgs = JsonSerializer.Serialize(args);
		var item = JsonSerializer.Serialize(new[] { identifier, queue, functionName, itemArgs });
		if (delay > 0) {
			db.SortedSetAdd("delayed:", item, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + delay);
		} else {
			db.ListRightPush($"queue:{queue}", item);
		}
		return identifier;
	}
}
