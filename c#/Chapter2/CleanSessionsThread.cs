using StackExchange.Redis;

namespace Chapter2;

public class CleanSessionsThread {
	private readonly IDatabase _db;
	private readonly int _limit;
	private bool _quit;
	private readonly Thread _thread;

	public CleanSessionsThread(IDatabase db, int limit) {
		_db = db;
		this._limit = limit;
		_thread = new Thread(run);
		_quit = false;
	}

	public void Start() {
		_thread.Start();
	}

	public void Quit() {
		_quit = true;
	}

	public bool IsAlive() {
		return _thread.IsAlive;
	}

	private void run() {
		while (!_quit) {
			var size = _db.SortedSetLength("recent:");

			if (size <= _limit) {
				try {
					Thread.Sleep(1000);
				} catch (Exception ex) {
					Console.WriteLine("error at thread:" + ex);
				}

				continue;
			}

			var endIndex = Math.Min(size - _limit, 100);

			var tokens = _db.SortedSetRangeByRank("recent:", 0, endIndex - 1);

			var sessionKeys = new List<RedisKey>();

			foreach (var token in tokens) {
				sessionKeys.Add("viewed:" + token);
			}

			_db.KeyDelete(sessionKeys.ToArray());

			_db.HashDelete("login:", tokens);
			_db.SortedSetRemove("recent:", tokens);
		}
	}
}
