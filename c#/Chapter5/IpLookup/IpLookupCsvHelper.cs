using System.Text;
using TinyCsvParser;
using TinyCsvParser.Mapping;

namespace Chapter5.IpLookup;

public static class IpLookupCsvHelper {
	public class CityBlock {
		public string StartIp { get; set; }
		public string EndIp { get; set; }
		public string LocationId { get; set; }
	}

	public class CityLocation {
		public string CityId { get; set; }
		public string Country { get; set; }
		public string Region { get; set; }
		public string City { get; set; }
	}

	private class CsvCityBlockMapping : CsvMapping<CityBlock> {
		public CsvCityBlockMapping() {
			MapProperty(0, x => x.StartIp);
			MapProperty(1, x => x.EndIp);
			MapProperty(2, x => x.LocationId);
		}
	}

	private class CsvCityLocationMapping : CsvMapping<CityLocation> {
		public CsvCityLocationMapping() {
			MapProperty(0, x => x.CityId);
			MapProperty(1, x => x.Country);
			MapProperty(2, x => x.Region);
			MapProperty(3, x => x.City);
		}
	}

	public static List<CityBlock> LoadCityBlocks(string path) {
		var mapper = new CsvCityBlockMapping();
		var option = new CsvParserOptions(true, ',');
		var parser = new CsvParser<CityBlock>(option, mapper);

		var result = parser
			.ReadFromFile(path, Encoding.ASCII)
			.Select(c => c.Result)
			.Where(c => !c.StartIp.Equals("startIpNum", StringComparison.InvariantCultureIgnoreCase))
			.ToList();

		return result;
	}

	public static List<CityLocation> LoadCityLocations(string path) {
		var mapper = new CsvCityLocationMapping();
		var option = new CsvParserOptions(false, ',');
		var parser = new CsvParser<CityLocation>(option, mapper);

		var result = parser
			.ReadFromFile(path, Encoding.ASCII)
			.Select(c => c.Result)
			.ToList();

		return result;
	}
}
