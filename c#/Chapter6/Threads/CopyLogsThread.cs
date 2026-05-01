using System.Text;
using Chapter6.Chat;
using Chapter6.FileDistribution;
using StackExchange.Redis;

namespace Chapter6.Threads;

public class CopyLogsThread : ThreadWrapper {
	private readonly string _path;
	private readonly string _channel;
	private readonly int _count;
	private readonly long _limit;

	public CopyLogsThread(IDatabase db, string path, string channel, int count, long limit) : base(db, true) {
		_path = path;
		_channel = channel;
		_count = count;
		_limit = limit;
	}

	protected override void ThreadOperation() {
		operation();
	}

	private void operation() {
		var waiting = new Deque<FileStream>();
		long bytesInRedis = 0;

		var recipients = new HashSet<String>();
		for (var i = 0; i < _count; i++) {
			recipients.Add($"user_#{i}");
		}

		ChatOperations.CreateChat(_db, "source", recipients, "", _channel);

		var logFiles = new DirectoryInfo(_path)
			.EnumerateFiles("temp_redis*")
			.Select(c => File.OpenRead(c.FullName))
			.OrderBy(c => c.Name)
			.ToList();

		foreach (var logFile in logFiles) {
			var fsize = logFile.Length;
			while ((bytesInRedis + fsize) > _limit) {
				var cleaned = clean(waiting, _count);
				if (cleaned != 0) {
					bytesInRedis -= cleaned;
				}
				else {
					Thread.Sleep(250);
				}
			}

			var read = 0;
			var buffer = new byte[8192];
			var appendTo = Encoding.UTF8.GetBytes($"{_channel}{logFile.Name}");
			while ((read = logFile.Read(buffer, 0, buffer.Length)) > 0) {
				if (buffer.Length != read) {
					var bytes = new byte[read];
					Array.Copy(buffer, 0, bytes, 0, read);
					_db.StringAppend(appendTo, bytes);
				}
				else {
					_db.StringAppend(appendTo, buffer);
				}
			}

			ChatOperations.SendMessage(_db, _channel, "source", logFile.Name);

			bytesInRedis += fsize;
			waiting.AddLast(logFile);
		}

		ChatOperations.SendMessage(_db, _channel, "source", ":done");

		while (waiting.Count > 0) {
			var cleaned = clean(waiting, _count);
			if (cleaned != 0) {
				bytesInRedis -= cleaned;
			}
			else {
				Thread.Sleep(250);
			}
		}
	}

	private long clean(Deque<FileStream> waiting, int c) {
		if (waiting.Count == 0) {
			return 0;
		}

		var w0 = waiting.PeekFirst();
		var fileName = w0.Name;

		var fileKey = $"{_channel}{fileName}";
		var doneKey = $"{fileKey}:done";

		var counter = _db.StringGet(doneKey);

		if (c.ToString().Equals(counter.ToString())) {
			_db.KeyDelete(new RedisKey[] {
				fileKey,
				doneKey
			});
			return waiting.PopFirst().Length;
		}

		return 0;
	}
}
