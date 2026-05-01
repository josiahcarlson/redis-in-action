using System.Text.Json;
using StackExchange.Redis;

namespace Chapter6.Chat;

public static class ChatOperations {
	public static string CreateChat(IDatabase db, string sender, HashSet<string> recipients, string message) {
		var chatId = db.StringIncrement("ids:chat:").ToString();
		return CreateChat(db, sender, recipients, message, chatId);
	}

	public static string CreateChat(IDatabase db, string sender, HashSet<string> recipients, string message, string chatId) {
		recipients.Add(sender);

		var trans = db.CreateTransaction();

		foreach (var recipient in recipients) {
			trans.SortedSetAddAsync($"chat:{chatId}", recipient, 0);
			trans.SortedSetAddAsync($"seen:{recipient}", chatId, 0);
		}

		trans.Execute();

		return SendMessage(db, chatId, sender, message);
	}

	public static string SendMessage(IDatabase db, String chatId, String sender, String message) {
		var identifier = DistributedLocks.DistributedLockOperations.AcquireLock(db, $"chat:{chatId}");

		if (identifier == null) {
			throw new Exception("Couldn't get the lock");
		}

		try {
			var messageId = db.StringIncrement($"ids:{chatId}");
			var chatMessage = new ChatMessage(
				messageId,
				DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
				sender,
				message
			);
			var packed = JsonSerializer.Serialize(chatMessage);
			db.SortedSetAdd($"msgs:{chatId}", packed, messageId);
		} finally {
			DistributedLocks.DistributedLockOperations.ReleaseLock(db, $"chat:{chatId}", identifier);
		}

		return chatId;
	}

	public static List<ChatMessages> FetchPendingMessages(IDatabase db, string recipient) {
		var seenList = db.SortedSetRangeByRankWithScores($"seen:{recipient}", 0, -1).ToList();

		var tasks = new List<Task<RedisValue[]>>();
		foreach (var item in seenList) {
			var chatId = item.Element;
			var seenId = (int)item.Score;
			tasks.Add(db.SortedSetRangeByScoreAsync($"msgs:{chatId}", (seenId + 1),double.PositiveInfinity));
		}

		var unreadMessages = tasks.Select(db.Wait).ToList();

		using var unreadIterator = unreadMessages.AsEnumerable().GetEnumerator();

		var pendingChatMessages = new List<ChatMessages>();
		var seenUpdates = new List<Object[]>();
		var msgRemoves = new List<Object[]>();

		foreach (var c in seenList) {
			var chatId = c.Element.ToString();
			if (!unreadIterator.MoveNext()) {
				throw new Exception("Error during enumeration");
			}

			var messageStrings = unreadIterator.Current;
			if (messageStrings.Length == 0) {
				continue;
			}

			var seenId = 0L;
			var messages = new List<ChatMessage>();
			foreach (var messageJson in messageStrings) {
				var message = JsonSerializer.Deserialize<ChatMessage>(messageJson.ToString());

				if (message is null) {
					throw new NullReferenceException(nameof(message));
				}

				var messageId = message.Id;
				if (message.Id > seenId) {
					seenId = messageId;
				}

				messages.Add(message);
			}

			db.SortedSetAdd($"chat:{chatId}", recipient, seenId);

			seenUpdates.Add(new Object[] { $"seen:{recipient}", seenId, chatId });

			var minIdSet = db.SortedSetRangeByRankWithScores($"chat:{chatId}", 0, 0);
			if (minIdSet.Length > 0) {
				msgRemoves.Add(new Object[] {
					$"msgs:{chatId}",
					Convert.ToInt32(minIdSet[0].Score)
				});
			}

			pendingChatMessages.Add(new ChatMessages(chatId, messages));
		}

		var updateTasks = new List<Task>();

		foreach (var seenUpdate in seenUpdates) {
			updateTasks.Add(
				db.SortedSetAddAsync(
					seenUpdate[0].ToString(),
					seenUpdate[2].ToString(),
					(long)seenUpdate[1]
			));
		}

		foreach(var msgRemove in msgRemoves){
			updateTasks.Add(
				db.SortedSetRemoveRangeByScoreAsync(
					msgRemove[0].ToString(),
					0,
					(int)msgRemove[1]
				)
			);
		}

		Task.WaitAll(updateTasks.ToArray());

		return pendingChatMessages;
	}
}
