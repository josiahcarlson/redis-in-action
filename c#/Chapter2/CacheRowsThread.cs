using StackExchange.Redis;
using System.Text.Json;

namespace Chapter2;

public class CacheRowsThread {
	private readonly IDatabase _db;
	private bool _quit;
	private readonly Thread _thread;

	public CacheRowsThread(IDatabase db) {
		_db = db;
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
			var range = _db.SortedSetRangeByRankWithScores("schedule:", 0, 0);
			var enumerator = range.GetEnumerator();
			var next = (SortedSetEntry?)(enumerator.MoveNext() ? enumerator.Current : null);
			var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
			if (next == null || next.Value.Score > now) {
				try {
					Thread.Sleep(50);
				} catch (Exception ex) {
					Console.WriteLine("error at thread:" + ex);
				}

				continue;
			}

			var rowId = next.Value.Element.ToString();
			var delay = _db.SortedSetScore("delay:", rowId) ?? 0;
			if (delay <= 0) {
				_db.SortedSetRemove("delay:", rowId);
				_db.SortedSetRemove("schedule:", rowId);
				_db.KeyDelete("inv:" + rowId);
				continue;
			}

			var row = new Inventory(rowId);
			if (row == null) {
				throw new ArgumentNullException(nameof(row));
			}

			_db.SortedSetAdd("schedule:", rowId, now + delay);
			_db.StringSet("inv:" + rowId, JsonSerializer.Serialize(row));
		}
	}
}
