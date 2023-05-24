using System.IO.Compression;
using Chapter6.Chat;
using StackExchange.Redis;

namespace Chapter6.FileDistribution;

public static class ProcessLogsFromRedis {
	public static void Execute(IDatabase conn, string id, ICallback callback) {
		while (true) {
			var fdata = ChatOperations.FetchPendingMessages(conn, id);

			foreach (var messages in fdata) {
				foreach (var message in messages.Messages) {
					var logFile = message.Message;

					if (":done".Equals(logFile)) {
						return;
					}

					if (string.IsNullOrEmpty(logFile)) {
						continue;
					}

					Stream s;
					s = new RedisStream(conn, messages.ChatId + logFile);
					if (logFile.EndsWith(".gz")) {
						s = new GZipStream(s, CompressionMode.Decompress);
					}


					using (var reader = new StreamReader(s)) {
						while (reader.ReadLine() is { } line) {
							callback.Callback(line);
						}
					}

					callback.Callback(null);

					conn.StringIncrement($"{messages.ChatId}{logFile}:done");
				}
			}

			if (fdata.Count == 0) {
				Thread.Sleep(100);
			}
		}
	}
}
