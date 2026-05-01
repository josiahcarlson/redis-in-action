using System.Text.Json;
using StackExchange.Redis;

namespace Chapter5.IpLookup;

public static class IpLookupOperations {
	public static void ImportIpsToRedis(IDatabase conn, string path) {
		var blocks = IpLookupCsvHelper.LoadCityBlocks(path);
		Parallel.ForEach(blocks, block => {
			var startIp = block.StartIp;
			if (startIp.ToLower().IndexOf('i') != -1) {
				return;
			}

			var score = 0L;
			if (startIp.IndexOf('.') != -1) {
				score = ipToScore(startIp);
			} else {
				try {
					score = Int64.Parse(startIp);
				} catch (FormatException) {
					return;
				}
			}

			var cityId = block.LocationId + '_' + Guid.NewGuid().ToString("N");
			conn.SortedSetAdd("ip2cityid:", cityId, score);
		});
	}

	public static void ImportCitiesToRedis(IDatabase conn, string path) {
		var locations = IpLookupCsvHelper.LoadCityLocations(path)
			.Where(c=>
					 !string.IsNullOrEmpty(c.City)
						&& !string.IsNullOrEmpty(c.CityId)
						&& !string.IsNullOrEmpty(c.Region)
						&& !string.IsNullOrEmpty(c.Country)
						&& char.IsDigit(c.CityId[0])
				);

		Parallel.ForEach(locations, location => {
			var json = JsonSerializer.Serialize(new[] { location.City, location.Region, location.Country });
			conn.HashSet("cityid2city:", location.CityId, json);
		});
	}

	public static string[]? FindCityByIp(IDatabase conn, string ipAddress) {
		var score = ipToScore(ipAddress);
		var results = conn.SortedSetRangeByScore("ip2cityid:", score, 0, 0, Order.Descending);
		if (results.Length == 0) {
			return null;
		}

		var cityId = results[0].ToString();
		cityId = cityId.Substring(0, cityId.IndexOf('_'));
		var value = conn.HashGet("cityid2city:", cityId).ToString();
		if (string.IsNullOrEmpty(value)) {
			return null;
		}
		var result = JsonSerializer.Deserialize<string[]>(value);
		return result;
	}

	public static  string RandomOctet(int max) {
		var rand = new Random();
		return rand.Next(0, max + 1).ToString();
	}

	private static long ipToScore(string ipAddress) {
		var score = 0L;
		foreach (var v in ipAddress.Split(@".")) {
			score = score * 256 + int.Parse(v);
		}

		return score;
	}
}
