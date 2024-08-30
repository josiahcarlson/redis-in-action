using System.Globalization;

namespace Chapter5;

public static class DateTimeExtensions {
	public static string ToIsoFormat(this DateTime dateTime) {
		return dateTime.ToUniversalTime().ToString("s");
	}
	public static string ToTimestampFormat(this DateTime dateTime) {
		return dateTime.ToString("ddd MMM HH:00:00 yyyy",CultureInfo.InvariantCulture);
	}
}
