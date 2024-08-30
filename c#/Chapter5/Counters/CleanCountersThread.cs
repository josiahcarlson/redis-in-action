using StackExchange.Redis;

namespace Chapter5.Counters;

public class CleanCountersThread {
	private readonly IDatabase _db;
	private bool _quit;
	private readonly Thread _thread;
	private readonly int _sampleCount;
	private readonly long _timeOffset;

	public CleanCountersThread(int sampleCount, long timeOffset) {
		var connection = ConnectionMultiplexer.Connect("localhost");
		_db = connection.GetDatabase(15);
		_thread = new Thread(run) {
			IsBackground = true
		};
		_quit = false;
		_sampleCount = sampleCount;
		_timeOffset = timeOffset;
	}

	public void Start() {
		_thread.Start();
	}

	public void Quit() {
		_quit = true;
	}

	private void run() {
		try {
			var passes = 0;
			while (!_quit) {
				var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + _timeOffset;
				var index = 0;
				while (index < _db.SortedSetLength("known:") && !_quit) {
					var counterToCheck = _db.SortedSetRangeByRank("known:", index, index);
					index++;
					if (counterToCheck.Length == 0) {
						break;
					}

					var hash = counterToCheck[0].ToString();

					var precision = int.Parse(hash.Substring(0, hash.IndexOf(':')));
					var bprec = precision / 60;
					if (bprec == 0) {
						bprec = 1;
					}

					if ((passes % bprec) != 0) {
						continue;
					}

					var counterHashKey = "count:" + hash;
					var cutoff = (((DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + _timeOffset) / 1000) - _sampleCount * precision).ToString();
					var samples = new List<string>(_db.HashKeys(counterHashKey).Select(c => c.ToString()));
					samples.Sort();
					var remove = bisectRight(samples, cutoff);

					if (remove != 0) {
						var samplesToRemove = samples.GetRange(0, remove).Select(c => new RedisValue(c)).ToArray();
						_db.HashDelete(counterHashKey, samplesToRemove);
						var hashPrev = _db.HashGetAll(counterHashKey);
						if (remove == samples.Count) {
							var trans = _db.CreateTransaction();
							if (hashPrev.Length > 0) {
								foreach (var entry in hashPrev) {
									trans.AddCondition(Condition.HashEqual(counterHashKey, entry.Name, entry.Value));
								}

								trans.AddCondition(Condition.HashLengthEqual(counterHashKey, hashPrev.Length));
							} else {
								trans.AddCondition(Condition.HashLengthEqual(counterHashKey, 0));
							}

							if (hashPrev.Length == 0) {
								trans.SortedSetRemoveAsync("known:", hash);
								trans.Execute();
								index--;
							}
						}
					}
				}
				passes++;
				var duration = Math.Min((DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + _timeOffset) - start + 1000, 60000);
				var timeToSleep = TimeSpan.FromMilliseconds(Math.Max(60000 - duration, 1000));
				Thread.Sleep(timeToSleep);
			}
		} catch (Exception ex) when (ex is ThreadAbortException || ex is ThreadInterruptedException) {
			Thread.CurrentThread.Interrupt();
		}
	}

	// mimic python's bisect.bisect_right
	private int bisectRight(List<String> values, String key) {
		var index = values.BinarySearch(key);
		return index < 0 ? Math.Abs(index) - 1 : index + 1;
	}
}
