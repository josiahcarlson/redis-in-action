using System.Collections;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Chapter6.FileDistribution;
// Array + two unmasked indices approach explained in the following page is used.
//   https://www.snellman.net/blog/archive/2016-12-13-ring-buffers/

[DebuggerTypeProxy(typeof(DequeDebugView<>))]
[DebuggerDisplay("Count = {Count}")]
public sealed class Deque<T> : IList<T>, IReadOnlyList<T>, IList
{
	private const int DefaultCapacity = 4;

	private T[] _buf = Array.Empty<T>();
	private int _read;
	private int _version;
	private int _write;

	public Deque()
	{
	}

	public Deque(int capacity)
	{
		EnsureCapacity(capacity);
	}

	public Deque(IEnumerable<T> items)
	{
		AddRange(items);
	}

	public int Capacity => _buf.Length;

	public int Count => _write - _read;

	bool IList.IsFixedSize => false;

	bool ICollection<T>.IsReadOnly => false;

	bool IList.IsReadOnly => false;

	bool ICollection.IsSynchronized => false;

	object ICollection.SyncRoot => this;

	public T this[int index]
	{
		get
		{
			if ((uint)index >= (uint)Count) {
				throw new IndexOutOfRangeException();
			}

			return _buf[WrapIndex(_read + index)];
		}
		set
		{
			if ((uint)index >= (uint)Count) {
				throw new IndexOutOfRangeException();
			}

			_buf[WrapIndex(_read + index)] = value;
			_version++;
		}
	}

	object? IList.this[int index]
	{
		get => this[index];
		set
		{
			if (IsNullAndNullsAreIllegal(value)) {
				throw new ArgumentNullException(nameof(value));
			}

			try
			{
				this[index] = (T)value!;
			}
			catch (InvalidCastException)
			{
				throw new ArgumentException(null, nameof(value));
			}
		}
	}

	private bool IsEmpty => _read == _write;

	private bool IsFull => Count == Capacity;

	public void Add(T item)
	{
		AddLast(item);
	}

	int IList.Add(object? value)
	{
		if (IsNullAndNullsAreIllegal(value)) {
			throw new ArgumentNullException(nameof(value));
		}

		try
		{
			Add((T)value!);
		}
		catch (InvalidCastException)
		{
			throw new ArgumentException(null, nameof(value));
		}

		return Count - 1;
	}

	public void AddFirst(T item)
	{
		if (IsFull) {
			Grow();
		}

		_read--;
		this[0] = item;
	}

	public void AddLast(T item)
	{
		if (IsFull) {
			Grow();
		}

		_write++;
		this[Count - 1] = item;
	}

	public void AddRange(IEnumerable<T> items)
	{
		InsertRange(Count, items);
	}

	public ReadOnlyCollection<T> AsReadOnly()
	{
		return new ReadOnlyCollection<T>(this);
	}

	public int BinarySearch(T item)
	{
		return BinarySearch(item, null);
	}

	public int BinarySearch(T item, IComparer<T>? comparer)
	{
		return BinarySearch(0, Count, item, comparer);
	}

	public int BinarySearch(int index, int count, T item, IComparer<T>? comparer)
	{
		if (index < 0) {
			throw new ArgumentOutOfRangeException(nameof(index));
		}

		if (count < 0 || count > Count - index) {
			throw new ArgumentOutOfRangeException(nameof(count));
		}

		var read = WrapIndex(_read);

		if (IsContiguous(index, count))
		{
			// Do not use WrapIndex here. Instead, we must do the wrapping ourselves.
			// Here is a problematic example:
			//
			//                   _buf    : 'a' : 'b' : 'c' : 'd' :
			//                           ^     ^     ^     ^     ^
			//                      i   ~0    ~1    ~2    ~3    ~4
			//              ~i - read    0     1     2     3     4
			//   WrapIndex(~i - read)    0     1     2     3     0

			if (read + index < Capacity)
			{
				var i = Array.BinarySearch(_buf, read + index, count, item, comparer);
				return i < 0 ? ~(~i - read) : i - read;
			}
			else
			{
				var i = Array.BinarySearch(_buf, read + index - Capacity, count, item, comparer);
				return i < 0 ? ~(~i - read + Capacity) : i - read + Capacity;
			}
		}
		else
		{
			var leftLen = Capacity - read;
			var leftCount = leftLen - index;
			var rightCount = count - leftCount;

			var i = Array.BinarySearch(_buf, read + index, leftCount, item, comparer);
			if (i >= 0) {
				return i - read;
			}

			if (~i < Capacity) {
				return ~(~i - read);
			}

			var j = Array.BinarySearch(_buf, 0, rightCount, item, comparer);
			if (j >= 0) {
				return j + leftLen;
			}

			return ~(~j + leftLen);
		}
	}

	public void Clear()
	{
		RemoveRange(0, Count);
	}

	public bool Contains(T item)
	{
		return IndexOf(item) != -1;
	}

	bool IList.Contains(object? value)
	{
		return IsCompatibleObject(value) && Contains((T)value!);
	}

	public Deque<TOutput> ConvertAll<TOutput>(Converter<T, TOutput> converter)
	{
		if (converter == null) {
			throw new ArgumentNullException(nameof(converter));
		}

		var count = Count;

		var deque = new Deque<TOutput>(count);
		for (var i = 0; i < count; i++) {
			deque._buf[i] = converter(this[i]);
		}

		deque._write = count;
		return deque;
	}

	public void CopyTo(T[] array)
	{
		CopyTo(array, 0);
	}

	public void CopyTo(T[] array, int arrayIndex)
	{
		CopyTo(0, array, arrayIndex, Count);
	}

	public void CopyTo(int index, T[] array, int arrayIndex, int count)
	{
		if (array == null) {
			throw new ArgumentNullException(nameof(array));
		}

		if (index < 0) {
			throw new ArgumentOutOfRangeException(nameof(index));
		}

		if (arrayIndex < 0) {
			throw new ArgumentOutOfRangeException(nameof(arrayIndex));
		}

		if (count < 0 || index + count > Count || arrayIndex + count > array.Length) {
			throw new ArgumentOutOfRangeException(nameof(count));
		}

		var read = WrapIndex(_read);

		if (IsContiguous(index, count))
		{
			Array.Copy(_buf, WrapIndex(read + index), array, arrayIndex, count);
		}
		else
		{
			var leftLen = Capacity - read;
			var leftCount = leftLen - index;
			var rightCount = count - leftCount;

			Array.Copy(_buf, read + index, array, arrayIndex, leftCount);
			Array.Copy(_buf, 0, array, arrayIndex + leftCount, rightCount);
		}
	}

	void ICollection.CopyTo(Array array, int index)
	{
		if (array == null) {
			throw new ArgumentNullException(nameof(array));
		}

		if (array.Rank != 1 || array.GetLowerBound(0) != 0) {
			throw new ArgumentException(null, nameof(array));
		}

		if (index < 0 || index + Count > array.Length) {
			throw new ArgumentOutOfRangeException(nameof(index));
		}

		try
		{
			var read = WrapIndex(_read);

			if (IsContiguous(0, Count))
			{
				Array.Copy(_buf, read, array!, index, Count);
			}
			else
			{
				var leftCount = Capacity - read;
				var rightCount = Count - leftCount;

				Array.Copy(_buf, read, array!, index, leftCount);
				Array.Copy(_buf, 0, array!, index + leftCount, rightCount);
			}
		}
		catch (ArrayTypeMismatchException)
		{
			throw new ArgumentException(null, nameof(array));
		}
	}

	public int EnsureCapacity(int capacity)
	{
		if (capacity < 0) {
			throw new ArgumentOutOfRangeException(nameof(capacity));
		}

		if (capacity <= Capacity) {
			return Capacity;
		}

		try
		{
			Relocate(NextCapacity(capacity));
		}
		catch (OverflowException)
		{
			throw new OutOfMemoryException();
		}

		_version++;
		return Capacity;
	}

	public bool Exists(Predicate<T> match)
	{
		return FindIndex(match) != -1;
	}

	public T? Find(Predicate<T> match)
	{
		if (match == null) {
			throw new ArgumentNullException(nameof(match));
		}

		var count = Count;

		for (var i = 0; i < count; i++) {
			if (match(this[i])) {
				return this[i];
			}
		}

		return default;
	}

	public int FindIndex(Predicate<T> match)
	{
		return FindIndex(0, match);
	}

	public int FindIndex(int startIndex, Predicate<T> match)
	{
		return FindIndex(startIndex, Count - startIndex, match);
	}

	public int FindIndex(int startIndex, int count, Predicate<T> match)
	{
		if (startIndex < 0 || startIndex > Count) {
			throw new ArgumentOutOfRangeException(nameof(startIndex));
		}

		if (count < 0 || count > Count - startIndex) {
			throw new ArgumentOutOfRangeException(nameof(count));
		}

		if (match == null) {
			throw new ArgumentNullException(nameof(match));
		}

		for (var i = startIndex; i < startIndex + count; i++) {
			if (match(this[i])) {
				return i;
			}
		}

		return -1;
	}

	public T? FindLast(Predicate<T> match)
	{
		if (match == null) {
			throw new ArgumentNullException(nameof(match));
		}

		for (var i = Count - 1; i >= 0; i--) {
			if (match(this[i])) {
				return this[i];
			}
		}

		return default;
	}

	public int FindLastIndex(Predicate<T> match)
	{
		return FindLastIndex(Count - 1, match);
	}

	public int FindLastIndex(int startIndex, Predicate<T> match)
	{
		return FindLastIndex(startIndex, startIndex + 1, match);
	}

	public int FindLastIndex(int startIndex, int count, Predicate<T> match)
	{
		if (match == null) {
			throw new ArgumentNullException(nameof(match));
		}

		if (Count == 0) {
			return -1;
		}

		if (startIndex < 0 || startIndex >= Count) {
			throw new ArgumentOutOfRangeException(nameof(startIndex));
		}

		if (count < 0 || startIndex < count - 1) {
			throw new ArgumentOutOfRangeException(nameof(count));
		}

		for (var i = startIndex; i > startIndex - count; i--) {
			if (match(this[i])) {
				return i;
			}
		}

		return -1;
	}

	public void ForEach(Action<T> action)
	{
		if (action == null) {
			throw new ArgumentNullException(nameof(action));
		}

		var count = Count;

		var version = _version;
		for (var i = 0; i < count; i++)
		{
			action(this[i]);
			if (version != _version) {
				throw new InvalidOperationException();
			}
		}
	}

	// Use the concrete type as the return type for performance.
	public Enumerator GetEnumerator()
	{
		return new Enumerator(this);
	}

	IEnumerator<T> IEnumerable<T>.GetEnumerator()
	{
		return GetEnumerator();
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
		return GetEnumerator();
	}

	public Deque<T> GetRange(int index, int count)
	{
		if (index < 0) {
			throw new ArgumentOutOfRangeException(nameof(index));
		}

		if (count < 0 || count > Count - index) {
			throw new ArgumentOutOfRangeException(nameof(count));
		}

		var deque = new Deque<T>(count);
		CopyTo(index, deque._buf, 0, count);
		deque._write = count;
		return deque;
	}

	public int IndexOf(T item)
	{
		return IndexOf(item, 0);
	}

	public int IndexOf(T item, int index)
	{
		return IndexOf(item, index, Count - index);
	}

	public int IndexOf(T item, int index, int count)
	{
		if (index < 0 || index > Count) {
			throw new ArgumentOutOfRangeException(nameof(index));
		}

		if (count < 0 || count > Count - index) {
			throw new ArgumentOutOfRangeException(nameof(count));
		}

		var read = WrapIndex(_read);

		if (IsContiguous(index, count))
		{
			var i = Array.IndexOf(_buf, item, WrapIndex(read + index), count);
			return i == -1 ? i : WrapIndex(i - read);
		}
		else
		{
			var leftLen = Capacity - read;
			var leftCount = leftLen - index;
			var rightCount = count - leftCount;

			var i = Array.IndexOf(_buf, item, read + index, leftCount);
			if (i != -1) {
				return i - read;
			}

			var j = Array.IndexOf(_buf, item, 0, rightCount);
			if (j != -1) {
				return j + leftLen;
			}

			return -1;
		}
	}

	int IList.IndexOf(object? value)
	{
		if (IsCompatibleObject(value)) {
			return IndexOf((T)value!);
		}

		return -1;
	}

	public void Insert(int index, T item)
	{
		if (index < 0 || index > Count) {
			throw new ArgumentOutOfRangeException(nameof(index));
		}

		if (IsFull) {
			Grow();
		}

		if (index <= Count - index)
		{
			// Closer to r.
			WrapCopy(_read, _read - 1, index);
			_read--;
		}
		else
		{
			// Closer to w.
			WrapCopy(_read + index, _read + index + 1, Count - index);
			_write++;
		}

		this[index] = item;
	}

	void IList.Insert(int index, object? value)
	{
		if (IsNullAndNullsAreIllegal(value)) {
			throw new ArgumentNullException(nameof(value));
		}

		try
		{
			Insert(index, (T)value!);
		}
		catch (InvalidCastException)
		{
			throw new ArgumentException(null, nameof(value));
		}
	}

	public void InsertRange(int index, IEnumerable<T> items)
	{
		if (items == null) {
			throw new ArgumentNullException(nameof(items));
		}

		if (index < 0 || index > Count) {
			throw new ArgumentOutOfRangeException(nameof(index));
		}

		if (items is ICollection<T> collection)
		{
			var count = collection.Count;
			EnsureCapacity(Count + count);

			if (index <= Count - index)
			{
				// Closer to r.
				WrapCopy(_read, _read - count, index);
				_read -= count;
			}
			else
			{
				// Closer to w.
				WrapCopy(_read + index, _read + index + count, Count - index);
				_write += count;
			}

			var read = WrapIndex(_read);

			if (ReferenceEquals(items, this))
			{
				WrapCopy(read, read + index, index);
				WrapCopy(read + index + count, read + 2 * index, count - index);
			}
			else if (collection is T[] array)
			{
				if (IsContiguous(index, count))
				{
					Array.Copy(array, 0, _buf, WrapIndex(read + index), count);
				}
				else
				{
					var leftLen = Capacity - read;
					var leftCount = leftLen - index;
					var rightCount = count - leftCount;

					Array.Copy(array, 0, _buf, read + index, leftCount);
					Array.Copy(array, leftCount, _buf, 0, rightCount);
				}
			}
			else if (collection is List<T> list)
			{
				if (IsContiguous(index, count))
				{
					list.CopyTo(0, _buf, WrapIndex(read + index), count);
				}
				else
				{
					var leftLen = Capacity - read;
					var leftCount = leftLen - index;
					var rightCount = count - leftCount;

					list.CopyTo(0, _buf, read + index, leftCount);
					list.CopyTo(leftCount, _buf, 0, rightCount);
				}
			}
			else if (collection is Deque<T> deque)
			{
				if (IsContiguous(index, count))
				{
					deque.CopyTo(0, _buf, WrapIndex(read + index), count);
				}
				else
				{
					var leftLen = Capacity - read;
					var leftCount = leftLen - index;
					var rightCount = count - leftCount;

					deque.CopyTo(0, _buf, read + index, leftCount);
					deque.CopyTo(leftCount, _buf, 0, rightCount);
				}
			}
			else
			{
				foreach (var item in collection) {
					this[index++] = item;
				}
			}
		}
		else
		{
			foreach (var item in items) {
				Insert(index++, item);
			}
		}

		_version++;
	}

	public int LastIndexOf(T item)
	{
		return LastIndexOf(item, Count - 1);
	}

	public int LastIndexOf(T item, int index)
	{
		return LastIndexOf(item, index, index + 1);
	}

	public int LastIndexOf(T item, int index, int count)
	{
		if (Count == 0) {
			return -1;
		}

		if (index < 0 || index >= Count) {
			throw new ArgumentOutOfRangeException(nameof(index));
		}

		if (count < 0 || index < count - 1) {
			throw new ArgumentOutOfRangeException(nameof(count));
		}

		int ToLeftIndex(int index, int count)
		{
			return index - count + 1;
		}

		int ToRightIndex(int index, int count)
		{
			return index + count - 1;
		}

		var read = WrapIndex(_read);

		index = ToLeftIndex(index, count);

		if (IsContiguous(index, count))
		{
			var i = Array.LastIndexOf(_buf, item, WrapIndex(read + ToRightIndex(index, count)), count);
			return i == -1 ? i : WrapIndex(i - read);
		}
		else
		{
			var leftLen = Capacity - read;
			var leftCount = leftLen - index;
			var rightCount = count - leftCount;

			var j = Array.LastIndexOf(_buf, item, ToRightIndex(0, rightCount), rightCount);
			if (j != -1) {
				return j + leftLen;
			}

			var i = Array.LastIndexOf(_buf, item, read + ToRightIndex(index, leftCount), leftCount);
			if (i != -1) {
				return i - read;
			}

			return -1;
		}
	}

	public T PeekFirst()
	{
		if (IsEmpty) {
			throw new InvalidOperationException();
		}

		return this[0];
	}

	public T PeekLast()
	{
		if (IsEmpty) {
			throw new InvalidOperationException();
		}

		return this[Count - 1];
	}

	public T PopFirst()
	{
		var item = PeekFirst();
		RemoveFirst();
		return item;
	}

	public T PopLast()
	{
		var item = PeekLast();
		RemoveLast();
		return item;
	}

	public bool Remove(T item)
	{
		var i = IndexOf(item);
		if (i == -1) {
			return false;
		}

		RemoveAt(i);
		return true;
	}

	void IList.Remove(object? value)
	{
		if (IsCompatibleObject(value)) {
			Remove((T)value!);
		}
	}

	public int RemoveAll(Predicate<T> match)
	{
		if (match == null) {
			throw new ArgumentNullException(nameof(match));
		}

		var count = Count;

		var i = 0;
		for (; i < count; i++) {
			if (match(this[i])) {
				break;
			}
		}

		if (i == count) {
			return 0;
		}

		var j = i + 1;
		while (true)
		{
			for (; j < count; j++) {
				if (!match(this[j])) {
					break;
				}
			}

			if (j == count) {
				break;
			}

			this[i++] = this[j++];
		}

		if (RuntimeHelpers.IsReferenceOrContainsReferences<T>()) {
			WrapClear(_read + i, count - i);
		}

		var result = count - i;
		_write -= result;
		_version++;
		return result;
	}

	public void RemoveAt(int index)
	{
		RemoveRange(index, 1);
	}

	public void RemoveFirst()
	{
		if (IsEmpty) {
			throw new InvalidOperationException();
		}

		if (RuntimeHelpers.IsReferenceOrContainsReferences<T>()) {
			this[0] = default!;
		}

		_read++;
		_version++;
	}

	public void RemoveLast()
	{
		if (IsEmpty) {
			throw new InvalidOperationException();
		}

		if (RuntimeHelpers.IsReferenceOrContainsReferences<T>()) {
			this[Count - 1] = default!;
		}

		_write--;
		_version++;
	}

	public void RemoveRange(int index, int count)
	{
		if (index < 0) {
			throw new ArgumentOutOfRangeException(nameof(index));
		}

		if (count < 0 || count > Count - index) {
			throw new ArgumentOutOfRangeException(nameof(count));
		}

		if (index <= Count - (index + count))
		{
			// Closer to r.
			WrapCopy(_read, _read + count, index);
			if (RuntimeHelpers.IsReferenceOrContainsReferences<T>()) {
				WrapClear(_read, count);
			}

			_read += count;
		}
		else
		{
			// Closer to w.
			WrapCopy(_read + index + count, _read + index, Count - (index + count));
			if (RuntimeHelpers.IsReferenceOrContainsReferences<T>()) {
				WrapClear(_read + Count - count, count);
			}

			_write -= count;
		}

		_version++;
	}

	public void Reverse()
	{
		Reverse(0, Count);
	}

	public void Reverse(int index, int count)
	{
		if (index < 0) {
			throw new ArgumentOutOfRangeException(nameof(index));
		}

		if (count < 0 || count > Count - index) {
			throw new ArgumentOutOfRangeException(nameof(count));
		}

		if (IsContiguous(index, count))
		{
			Array.Reverse(_buf, WrapIndex(_read + index), count);
		}
		else
		{
			MakeContiguous();
			Array.Reverse(_buf, index, count);
		}

		_version++;
	}

	public void Sort()
	{
		Sort(0, Count, null);
	}

	public void Sort(IComparer<T>? comparer)
	{
		Sort(0, Count, comparer);
	}

	public void Sort(int index, int count, IComparer<T>? comparer)
	{
		if (index < 0) {
			throw new ArgumentOutOfRangeException(nameof(index));
		}

		if (count < 0 || count > Count - index) {
			throw new ArgumentOutOfRangeException(nameof(count));
		}

		if (IsContiguous(index, count))
		{
			Array.Sort(_buf, WrapIndex(_read + index), count, comparer);
		}
		else
		{
			MakeContiguous();
			Array.Sort(_buf, index, count, comparer);
		}

		_version++;
	}

	public void Sort(Comparison<T> comparison)
	{
		if (comparison == null) {
			throw new ArgumentNullException(nameof(comparison));
		}

		if (!IsContiguous(0, Count)) {
			MakeContiguous();
		}

		new Span<T>(_buf, WrapIndex(_read), Count).Sort(comparison);

		_version++;
	}

	public T[] ToArray()
	{
		var array = new T[Count];
		CopyTo(array);
		return array;
	}

	public void TrimExcess()
	{
		var newCapacity = NextCapacity(Count);
		if (newCapacity == Capacity) {
			return;
		}

		Relocate(newCapacity);
		_version++;
	}

	public bool TrueForAll(Predicate<T> match)
	{
		if (match == null) {
			throw new ArgumentNullException(nameof(match));
		}

		var count = Count;

		for (var i = 0; i < count; i++) {
			if (!match(this[i])) {
				return false;
			}
		}

		return true;
	}

	public bool TryPeekFirst(out T item)
	{
		if (IsEmpty)
		{
			item = default!;
			return false;
		}

		item = this[0];
		return true;
	}

	public bool TryPeekLast(out T item)
	{
		if (IsEmpty)
		{
			item = default!;
			return false;
		}

		item = this[Count - 1];
		return true;
	}

	public bool TryPopFirst(out T item)
	{
		if (!TryPeekFirst(out item)) {
			return false;
		}

		RemoveFirst();
		return true;
	}

	public bool TryPopLast(out T item)
	{
		if (!TryPeekLast(out item)) {
			return false;
		}

		RemoveLast();
		return true;
	}

	private static bool IsCompatibleObject(object? value)
	{
		return value is T || (value == null && default(T) == null);
	}

	private static bool IsNullAndNullsAreIllegal(object? value)
	{
		return value == null && default(T) != null;
	}

	private static int NextCapacity(int n)
	{
		if (n == 0) {
			return 0;
		}

		return (int)((uint)n).NextPowerOfTwo();
	}

	private void Grow()
	{
		try
		{
			if (Capacity == 0) {
				Relocate(DefaultCapacity);
			} else {
				Relocate(2 * Capacity);
			}
		}
		catch (OverflowException)
		{
			throw new OutOfMemoryException();
		}
	}

	private bool IsContiguous(int index, int count)
	{
		return count == 0 || WrapIndex(_read + index) <= WrapIndex(_read + index + count - 1);
	}

	private void MakeContiguous()
	{
		Relocate(Capacity);
	}

	private void Relocate(int capacity)
	{
		var newBuf = new T[capacity];
		CopyTo(newBuf);
		_buf = newBuf;
		_write = Count;
		_read = 0;
	}

	private void WrapClear(int index, int count)
	{
		index = WrapIndex(index);

		if (index + count <= Capacity)
		{
			Array.Clear(_buf, index, count);
		}
		else
		{
			Array.Clear(_buf, index, Capacity - index);
			Array.Clear(_buf, 0, count - (Capacity - index));
		}
	}

	private void WrapCopy(int srcIndex, int dstIndex, int count)
	{
		Debug.Assert(count <= Capacity / 2); // (*)

		srcIndex = WrapIndex(srcIndex);
		dstIndex = WrapIndex(dstIndex);

		if (srcIndex <= dstIndex)
		{
			var a = Math.Min(Capacity - dstIndex, count);
			var c = Math.Max(srcIndex + count - Capacity, 0);
			var b = count - (a + c);

			if (srcIndex + count <= dstIndex)
			{
				//  s                               |
				//  +---------------+               |
				//  |       A       |               |
				//  +---------------+               |
				//  |               +---------------+
				//  |               |       A'      |
				//  |               +---------------+
				//  |               d               |

				//  |  s                            |
				//  |  +---------+-----+            |
				//  |  |    A    |  B  |            |
				//  |  +---------+-----+            |
				//  |                     +---------+-----+
				//  |                     |    A'   |  B' |
				//  |                     +---------+-----+
				//  |                     d         |

				// In this case, A' ∩ B = ∅ holds.
				Array.Copy(_buf, srcIndex, _buf, dstIndex, a);
				if (b > 0) {
					Array.Copy(_buf, srcIndex + a, _buf, 0, b);
				}
			}
			else
			{
				// By (*), d + count ≤ s + Capacity.

				//  |   s                           |   s + Capacity
				//  |   +---------------+           |   +---
				//  |   |       A       |           |   |
				//  |   +---------------+           |   +---
				//  |           +---------------+   |
				//  |           |       A'      |   |
				//  |           +---------------+   |
				//  |           d                   |

				//  |           s                   |           s + Capacity
				//  |           +-----------+---+   |           +---
				//  |           |     A     | B |   |           |
				//  |           +-----------+---+   |           +---
				//  |                   +-----------+---+
				//  |                   |     A'    | B'|
				//  |                   +-----------+---+
				//  |                   d           |

				//  |                   s           |                   s + Capacity
				//  |                   +---+-------+---+               +---
				//  |                   | A |   B   | C |               |
				//  |                   +---+-------+---+               +---
				//  |                           +---+-------+---+
				//  |                           | A'|   B'  | C'|
				//  |                           +---+-------+---+
				//  |                           d   |

				// In this case, C' ∩ (A ∪ B) = ∅ and B' ∩ A = ∅ hold.
				if (c > 0) {
					Array.Copy(_buf, 0, _buf, b, c);
				}

				if (b > 0) {
					Array.Copy(_buf, srcIndex + a, _buf, 0, b);
				}

				Array.Copy(_buf, srcIndex, _buf, dstIndex, a);
			}
		}
		else
		{
			var a = Math.Min(Capacity - srcIndex, count);
			var c = Math.Max(dstIndex + count - Capacity, 0);
			var b = count - (a + c);

			if (dstIndex + count <= srcIndex)
			{
				//  |               s               |
				//  |               +---------------+
				//  |               |       A       |
				//  |               +---------------+
				//  +---------------+               |
				//  |       A'      |               |
				//  +---------------+               |
				//  d                               |

				//  |                     s         |
				//  |                     +---------+-----+
				//  |                     |    A    |  B  |
				//  |                     +---------+-----+
				//  |  +---------+-----+            |
				//  |  |    A'   |  B' |            |
				//  |  +---------+-----+            |
				//  |  d                            |

				// In this case, B' ∩ A = ∅ holds.
				if (b > 0) {
					Array.Copy(_buf, 0, _buf, dstIndex + a, b);
				}

				Array.Copy(_buf, srcIndex, _buf, dstIndex, a);
			}
			else
			{
				// By (*), s + count ≤ d + Capacity.

				//  |           s                   |
				//  |           +---------------+   |
				//  |           |       A       |   |
				//  |           +---------------+   |
				//  |   +---------------+           |   +---
				//  |   |       A'      |           |   |
				//  |   +---------------+           |   +---
				//  |   d                           |   d + Capacity

				//  |                   s           |
				//  |                   +-----------+---+
				//  |                   |     A     | B |
				//  |                   +-----------+---+
				//  |           +-----------+---+   |           +---
				//  |           |     A'    | B'|   |           |
				//  |           +-----------+---+   |           +---
				//  |           d                   |           d + Capacity

				//  |                           s   |
				//  |                           +---+-------+---+
				//  |                           | A |   B   | C |
				//  |                           +---+-------+---+
				//  |                   +---+-------+---+               +---
				//  |                   | A'|   B'  | C'|               |
				//  |                   +---+-------+---+               +---
				//  |                   d           |                   d + Capacity

				// In this case, A' ∩ (B ∪ C) = ∅ and B' ∩ C = ∅ hold.
				Array.Copy(_buf, srcIndex, _buf, dstIndex, a);
				if (b > 0) {
					Array.Copy(_buf, 0, _buf, dstIndex + a, b);
				}

				if (c > 0) {
					Array.Copy(_buf, b, _buf, 0, c);
				}
			}
		}
	}

	private int WrapIndex(int index)
	{
		return index & (Capacity - 1);
	}

	public struct Enumerator : IEnumerator<T>
	{
		private readonly int _count;
		private readonly Deque<T> _deque;
		private readonly int _version;
		private T? _current = default;
		private int _next = 0;

		internal Enumerator(Deque<T> deque)
		{
			_count = deque.Count;
			_deque = deque;
			_version = deque._version;
		}

		public T Current => _current!;

		object IEnumerator.Current => _current!;

		public void Dispose()
		{
			// Nothing to do.
		}

		public bool MoveNext()
		{
			if (_version == _deque._version && _next < _count)
			{
				_current = _deque[_next];
				_next++;
				return true;
			}

			// Split the rare cases to reduce the code size.
			return MoveNextRare();
		}

		public void Reset()
		{
			if (_version != _deque._version) {
				throw new InvalidOperationException();
			}

			_next = 0;
			_current = default;
		}

		private bool MoveNextRare()
		{
			if (_version != _deque._version) {
				throw new InvalidOperationException();
			}

			_next = _count + 1;
			_current = default;
			return false;
		}
	}
}

internal static class UInt32Extensions
{
	public static uint NextPowerOfTwo(this uint x)
	{
		if (x == 0)
			return 1;

		x--;
		x |= x >> 1;
		x |= x >> 2;
		x |= x >> 4;
		x |= x >> 8;
		x |= x >> 16;
		x++;
		return x;
	}
}

internal sealed class DequeDebugView<T>
{
	private readonly Deque<T> _deque;

	public DequeDebugView(Deque<T> deque)
	{
		_deque = deque ?? throw new ArgumentNullException(nameof(deque));
	}

	[DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
	public T[] Items => _deque.ToArray();
}
