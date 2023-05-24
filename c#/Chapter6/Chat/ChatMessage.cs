namespace Chapter6.Chat;

public class ChatMessage : IEquatable<ChatMessage> {
	public long Id { get; }
	public long Ts { get; }
	public string Sender { get; }
	public string? Message { get; }

	public ChatMessage(long id,long ts, string sender, string message) {
		Id = id;
		Ts = ts;
		Sender = sender;
		Message = message;
	}

	public bool Equals(ChatMessage? other) {
		if (ReferenceEquals(null, other)) {
			return false;
		}

		if (ReferenceEquals(this, other)) {
			return true;
		}

		return Id == other.Id && Ts == other.Ts && Sender == other.Sender && Message == other.Message;
	}

	public override bool Equals(object? obj) {
		if (ReferenceEquals(null, obj)) {
			return false;
		}

		if (ReferenceEquals(this, obj)) {
			return true;
		}

		if (obj.GetType() != GetType()) {
			return false;
		}

		return Equals((ChatMessage)obj);
	}

	public override int GetHashCode() {
		return HashCode.Combine(Id, Ts, Sender, Message);
	}
}
