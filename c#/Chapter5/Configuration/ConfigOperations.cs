using System.Text.Json;
using StackExchange.Redis;

namespace Chapter5.Configuration;

public abstract class ConfigOperations {
	private static readonly Dictionary<string, IDatabase?> REDIS_CONNECTIONS = new Dictionary<string, IDatabase?>();
	private static Dictionary<string, Dictionary<string, object>> CONFIGS = new Dictionary<string, Dictionary<string, object>>();
	private static Dictionary<string, long> CHECKED = new Dictionary<string, long>();

	public static void SetConfig(IDatabase conn, string type, string component, Dictionary<string, object> config) {
		conn.StringSet($"config:{type}:{component}", JsonSerializer.Serialize(config));
	}

	public static Dictionary<String, Object> GetConfig(IDatabase conn, string type, string component) {
		var wait = 1000;
		var key = $"config:{type}:{component}";

		CHECKED.TryGetValue(key, out var lastChecked);
		if (lastChecked == default || lastChecked < DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - wait) {
			CHECKED[key] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

			var value = conn.StringGet(key);
			var config = new Dictionary<string, object>();
			if (value != RedisValue.Null) {
				config = JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
			}

			CONFIGS[key] = config ?? new Dictionary<string, object>();
		}

		return CONFIGS[key];
	}

	public static IDatabase? RedisConnection(string component) {
		REDIS_CONNECTIONS.TryGetValue("config", out var configConn);
		if (configConn == null) {
			var connection = ConnectionMultiplexer.Connect("localhost");
			configConn = connection.GetDatabase(15);
			REDIS_CONNECTIONS["config"] = configConn;
		}

		var key = $"config:redis:{component}";
		CONFIGS.TryGetValue(key,out var oldConfig);
		oldConfig ??= new();
		var config = GetConfig(configConn, "redis", component);

		var configsAreEqual = config.Count == oldConfig.Count && !config.Except(oldConfig).Any();
		if (!configsAreEqual) {
			var conn = ConnectionMultiplexer.Connect("localhost");
			var db = conn.GetDatabase();
			if (config.ContainsKey("db")) {
				var dnNo = int.Parse(config["db"].ToString() ?? throw new InvalidOperationException());
				db = conn.GetDatabase(dnNo);
			}

			REDIS_CONNECTIONS[key] = db;
		}

		return REDIS_CONNECTIONS[key];
	}
}
