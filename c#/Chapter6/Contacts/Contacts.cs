using StackExchange.Redis;

namespace Chapter6.Contacts;

public static class Contacts {
	public static void AddUpdateContact(IDatabase db, string user, string contact) {
		var acList = $"recent:{user}";
		var trans = db.CreateTransaction();
		trans.ListRemoveAsync(acList, contact, 0);
		trans.ListLeftPushAsync(acList, contact);
		trans.ListTrimAsync(acList, 0, 99);
		if (!trans.Execute()) {
			throw new Exception("Transaction was not executed");
		}
	}

	public static void RemoveContact(IDatabase db, string user, string contact) {
		db.ListRemove($"recent:{user}", contact, 0);
	}

	public static RedisValue[] FetchAutocompleteList(IDatabase db, string user, string prefix) {
		var candidates = db.ListRange("recent:" + user, 0, -1);
		var matches = new List<RedisValue>();

		foreach (var candidate in candidates) {
			if (candidate.ToString().ToLower().StartsWith(prefix)) {
				matches.Add(candidate);
			}
		}

		return matches.ToArray();
	}

}
