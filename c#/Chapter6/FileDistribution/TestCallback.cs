namespace Chapter6.FileDistribution;

public class TestCallback : ICallback {
	private int _index;
	public readonly List<int> Counts = new();

	public void Callback(string? line){
		if (line is null){
			_index++;
			return;
		}
		while (Counts.Count == _index){
			Counts.Add(0);
		}

		Counts[_index] += 1;
	}
}
