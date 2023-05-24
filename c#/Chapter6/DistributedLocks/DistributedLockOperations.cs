using StackExchange.Redis;

namespace Chapter6.DistributedLocks;

public static class DistributedLockOperations {
	public static string? AcquireLockWithTimeout(IDatabase db, string lockName, long acquireTimeout, long lockTimeout) {
		var identifier = Guid.NewGuid().ToString();
		var lockKey = $"lock:{lockName}";
		var lockExpire = (int)(lockTimeout / 1000);

		var end = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + acquireTimeout;
		while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < end) {
			if (db.StringSet(lockKey, identifier, when: When.NotExists)) {
				db.KeyExpire(lockKey, TimeSpan.FromSeconds(lockExpire));
				return identifier;
			}

			if (!db.KeyTimeToLive(lockKey).HasValue) {
				db.KeyExpire(lockKey, TimeSpan.FromSeconds(lockExpire));
			}

			Thread.Sleep(1);
		}

		// null indicates that the lock was not acquired
		return null;
	}

	public static string? AcquireLock(IDatabase db, string lockName, long acquireTimeout=10000) {
		var identifier = Guid.NewGuid().ToString();
		var lockKey = $"lock:{lockName}";

		var end = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + acquireTimeout;
		while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < end) {
			if (db.StringSet(lockKey, identifier, when: When.NotExists)) {
				return identifier;
			}

			Thread.Sleep(1);
		}

		// null indicates that the lock was not acquired
		return null;
	}

	public static bool ReleaseLock(IDatabase db, string lockName, string identifier) {
		var lockKey = $"lock:{lockName}";

		var tryUnlock = true;

		while (tryUnlock) {
			var trans = db.CreateTransaction();
			var lockValue = db.StringGet(lockKey);

			if (lockKey is not null) {
				trans.AddCondition(Condition.KeyExists(lockKey));
				trans.AddCondition(Condition.StringEqual(lockKey, lockValue));
			} else {
				trans.AddCondition(Condition.KeyNotExists(lockKey));
			}

			if (identifier.Equals(lockValue)) {
				trans.KeyDeleteAsync(lockKey);
				var result = trans.Execute();
				if (result) {
					return true;
				}
			} else {
				tryUnlock = false;
			}
		}

		return false;
	}

	public static string? AcquireFairSemaphore(IDatabase db, string semaphoreName, int limit, long timeout) {
		var identifier = Guid.NewGuid().ToString();
		var ownerSet = $"{semaphoreName}:owner";
		var counterKey = $"{semaphoreName}:counter";

		var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
		var trans = db.CreateTransaction();

		trans.SortedSetRemoveRangeByScoreAsync(semaphoreName, double.MinValue, now - timeout);
		trans.SortedSetCombineAndStoreAsync(SetOperation.Intersect, ownerSet, new RedisKey[] { ownerSet, semaphoreName }, new double[] { 1, 0 });
		trans.StringIncrementAsync(counterKey);
		var counterTask=trans.StringGetAsync(counterKey);

		trans.Execute();

		var counter = (long)counterTask.Result;

		trans = db.CreateTransaction();
		trans.SortedSetAddAsync(semaphoreName, identifier, now);
		trans.SortedSetAddAsync(ownerSet, identifier, counter);
		var rankTask = trans.SortedSetRankAsync(ownerSet, identifier);

		trans.Execute();

		var result =  rankTask.Result;
		if (result < limit) {
			return identifier;
		}

		trans = db.CreateTransaction();
		trans.SortedSetRemoveAsync(semaphoreName, identifier);
		trans.SortedSetRemoveAsync(ownerSet, identifier);
		trans.Execute();
		return null;
	}

	public static bool ReleaseFairSemaphore(IDatabase db, string semaphoreName, string identifier) {
		var trans = db.CreateTransaction();
		trans.SortedSetRemoveAsync(semaphoreName, identifier);
		trans.SortedSetRemoveAsync($"{semaphoreName}:owner", identifier);
		return trans.Execute();
	}
}
