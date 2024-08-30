using StackExchange.Redis;

namespace Chapter5.Configuration;

public static class Maintenance {
	private static long _lastChecked;
	private static bool _underMaintenance;

	public static bool IsUnderMaintenance(IDatabase conn) {
		if (_lastChecked < DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1000){
			_lastChecked = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
			var flag = conn.StringGet("is-under-maintenance");
			_underMaintenance = "yes".Equals(flag);
		}

		return _underMaintenance;
	}
}
