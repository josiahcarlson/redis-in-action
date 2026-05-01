using System.Text;
using StackExchange.Redis;

namespace Chapter6.FileDistribution;

public class RedisStream : Stream {

	private readonly IDatabase _conn;
	private readonly string _key;
	private int _pos;

	public RedisStream(IDatabase conn, string key){
		_conn = conn;
		_key = key;
	}

	public override void Flush() {
		// no op
	}

	public override int Read(byte[] buffer, int offset, int count) {
		var end = _pos + (count - offset - 1);
		var keys = Encoding.UTF8.GetBytes(_key);
		byte[] block = _conn.StringGetRange(keys, _pos, end);

		if (block == null || block.Length == 0){
			return 0;
		}
		Array.Copy(block, 0, buffer, offset, block.Length);
		_pos += block.Length;
		return block.Length;
	}

	public override long Seek(long offset, SeekOrigin origin) {
		throw new NotImplementedException();
	}

	public override void SetLength(long value) {
		throw new NotImplementedException();
	}

	public override void Write(byte[] buffer, int offset, int count) {
		throw new NotImplementedException();
	}

	public override bool CanRead { get; } = true;
	public override bool CanSeek { get; } = false;
	public override bool CanWrite { get; } = false;
	public override long Length { get; }
	public override long Position { get; set; }
}
