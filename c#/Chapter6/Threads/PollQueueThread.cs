using System.Text.Json;
using StackExchange.Redis;

namespace Chapter6.Threads;

public class PollQueueThread : ThreadWrapper {
	public PollQueueThread(IDatabase db) : base(db) {
	}

	protected override void ThreadOperation() {
			var items = _db.SortedSetRangeByRankWithScores("delayed:", 0, 0);

			if (items.Length == 0 || items[0].Score > DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) {
				Thread.Sleep(10);
				return;
			}

			var itemJson = items[0].Element.ToString();
			var values = JsonSerializer.Deserialize<string[]>(itemJson);

			if (values is null) {
				throw new NullReferenceException(nameof(values));
			}

			var identifier = values[0];
			var queue = values[1];

			var locked = DistributedLocks.DistributedLockOperations.AcquireLock(_db, identifier);

			if (locked is null){
				return;
			}

			if (_db.SortedSetRemove("delayed:", itemJson)) {
				_db.ListRightPush($"queue:{queue}", itemJson);
			}
	}
}
