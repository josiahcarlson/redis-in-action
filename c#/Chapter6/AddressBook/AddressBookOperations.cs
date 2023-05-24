using StackExchange.Redis;

namespace Chapter6.AddressBook;

public static class AddressBookOperations {
	private const string ValidCharacters = "`abcdefghijklmnopqrstuvwxyz{";

	public static string[] FindPrefixRange(string prefix) {
		var posn = ValidCharacters.IndexOf(prefix[^1]);
		var suffix = ValidCharacters[posn > 0 ? posn - 1 : 0];
		var start = $"{prefix.Substring(0, prefix.Length - 1)}{suffix}" + '{';
		var end = prefix + '{';
		return new[] { start, end };
	}

	public static HashSet<string> AutocompleteOnPrefix(IDatabase db, string guild, string prefix) {
		var range = FindPrefixRange(prefix);
		var start = range[0];
		var end = range[1];
		var identifier = Guid.NewGuid().ToString();
		start += identifier;
		end += identifier;
		var guildSetKey = "members:" + guild;
		var destinationSet = $"{guildSetKey}:{identifier}";

		db.SortedSetAdd(guildSetKey, start, 0);
		db.SortedSetAdd(guildSetKey, end, 0);

		HashSet<string>? items = null;
		var transactionExecuted = false;
		while (!transactionExecuted) {
			var transaction = db.CreateTransaction();

			var setBeforeTransaction = db.SortedSetRangeByRank(guildSetKey);
			if (setBeforeTransaction.Length > 0) {
				transaction.AddCondition(Condition.SortedSetLengthEqual(guildSetKey, setBeforeTransaction.Length));
				foreach (var item in setBeforeTransaction) {
					transaction.AddCondition(Condition.SortedSetContains(guildSetKey, item));
				}
			} else {
				transaction.AddCondition(Condition.SortedSetLengthEqual(guildSetKey, 0));
			}

			var startIndex = db.SortedSetRank(guildSetKey, start) ?? long.MaxValue;
			var endIndex = db.SortedSetRank(guildSetKey, end) ?? long.MaxValue;
			var erange = Math.Min(startIndex + 9, endIndex - 2);

			if (erange == long.MaxValue) {
				transactionExecuted = true;
			} else {
				transaction.SortedSetRemoveAsync(guildSetKey, start);
				transaction.SortedSetRemoveAsync(guildSetKey, end);
				// sort this to a destination set since we cannot get transaction results with this client
				transaction.SortedSetRangeAndStoreAsync(guildSetKey, destinationSet, startIndex, erange);

				transactionExecuted = transaction.Execute();

				if (!transactionExecuted) {
					continue;
				}

				items = db.SortedSetRangeByRank(destinationSet).Select(x => x.ToString()).ToHashSet();
				db.KeyDelete(destinationSet);
			}
		}

		if (items is not null) {
			items = items.Where(x => !x.Contains('{')).ToHashSet();
		} else {
			throw new NullReferenceException(nameof(items));
		}

		return items;
	}

	public static void JoinGuild(IDatabase db, string guild, string user) {
		db.SortedSetAdd("members:" + guild, user, 0);
	}

	public static void LeaveGuild(IDatabase db, string guild, string user) {
		db.SortedSetRemove("members:" + guild, user);
	}
}
