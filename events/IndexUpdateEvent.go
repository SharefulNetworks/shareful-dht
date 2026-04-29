package events

import (
	"time"

	"github.com/SharefulNetworks/shareful-dht/commons"
)

type IndexUpdateEvent struct {
	key                  string
	entries              []commons.RecordIndexEntryLike
	publisherId          string
	publisherAddress     string
	isDeletion           bool
	publisherPlaintextId string
	eventTime            time.Time
}

// NewIndexUpdateEvent - Creates and returns a new instance of an IndexUpdateEvent.
func NewIndexUpdateEvent(
	key string,
	entries []commons.RecordIndexEntryLike,
	publisherId string,
	publisherAddress string,
	isDeletion bool,
	publisherPlaintextId string,
	eventTime time.Time,
) IndexUpdateEvent {
	return IndexUpdateEvent{
		key:                  key,
		entries:              entries,
		publisherId:          publisherId,
		publisherAddress:     publisherAddress,
		isDeletion:           isDeletion,
		publisherPlaintextId: publisherPlaintextId,
		eventTime:            eventTime,
	}
}

func (e IndexUpdateEvent) GetKey() string {
	return e.key
}

func (e IndexUpdateEvent) GetEntries() []commons.RecordIndexEntryLike {
	return e.entries
}

func (e IndexUpdateEvent) GetPublisherId() string {
	return e.publisherId
}

func (e IndexUpdateEvent) GetPublisherAddress() string {
	return e.publisherAddress
}

func (e IndexUpdateEvent) GetEventTime() time.Time {
	return e.eventTime
}

func (e IndexUpdateEvent) IsDeletion() bool {
	return e.isDeletion
}
