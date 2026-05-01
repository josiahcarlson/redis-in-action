using StackExchange.Redis;

namespace Chapter6.Threads;

public class ThreadWrapper {
	protected readonly IDatabase _db;
	private bool _quit;
	private readonly Thread _thread;

	protected ThreadWrapper(IDatabase db, bool runOnce=false) {
		_db = db;
		_thread = new Thread(run);
		_quit = runOnce;
	}

	protected internal void Start() {
		_thread.Start();
	}

	protected internal void Quit() {
		_quit = true;
		_thread.Join();
	}

	protected internal bool IsAlive() {
		return _thread.IsAlive;
	}

	private void run() {
		try {
			do {
				ThreadOperation();
			} while ((!_quit));
		} catch (Exception ex) when (ex is ThreadAbortException || ex is ThreadInterruptedException) {
			Thread.CurrentThread.Interrupt();
		}
	}

	protected virtual void ThreadOperation() {
	}
}
