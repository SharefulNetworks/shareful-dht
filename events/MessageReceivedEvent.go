package events

import (
	"time"

	"github.com/SharefulNetworks/shareful-dht/commons"
)

type MessageReceivedEvent struct {
	SenderAddress        string
	SenderPlaintextId    string
	RecipientPlaintextId string
	Message              commons.MessageLike
	EventTime            time.Time
}

// NewMessageReceivedEvent - Creates and returns a new instance of a MessageReceivedEvent.
func NewMessageReceivedEvent(senderAddress, senderPlaintextId, recipientPlaintextId string, message commons.MessageLike) MessageReceivedEvent {
	return MessageReceivedEvent{
		SenderAddress:        senderAddress,
		SenderPlaintextId:    senderPlaintextId,
		RecipientPlaintextId: recipientPlaintextId,
		Message:              message,
		EventTime:            time.Now(),
	}
}
