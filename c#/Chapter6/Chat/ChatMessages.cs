namespace Chapter6.Chat;

public class ChatMessages : IEquatable<ChatMessages>
{
	public readonly string ChatId;
	public readonly List<ChatMessage> Messages;

	public ChatMessages(string chatId, List<ChatMessage> messages){
		ChatId = chatId;
		Messages = messages;
	}

	public override bool Equals(object? other){
		if (other is not ChatMessages){
			return false;
		}
		var otherCm = (ChatMessages)other;
		return ChatId.Equals(otherCm.ChatId) &&
				Messages.Equals(otherCm.Messages);
	}

	public bool Equals(ChatMessages? other) {
		if (ReferenceEquals(null, other)) {
			return false;
		}

		if (ReferenceEquals(this, other)) {
			return true;
		}

		return ChatId == other.ChatId && Messages.All(other.Messages.Contains);
	}

	public override int GetHashCode() {
		return HashCode.Combine(ChatId, Messages);
	}
}
