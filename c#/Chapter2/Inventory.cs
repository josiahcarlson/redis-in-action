// ReSharper disable NotAccessedPositionalProperty.Global
// Disabled since this is just for demo purposes
namespace Chapter2;

public record Inventory(string Id, string Data, long Time) {
	public Inventory(string id) : this(id, "data to cache...", DateTimeOffset.UtcNow.ToUnixTimeSeconds()) { }
}
