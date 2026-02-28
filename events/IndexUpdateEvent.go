package events

import (
	"time"

	"github.com/SharefulNetworks/shareful-dht/commons"
)

type IndexUpdateEvent[T commons.RecordIndexEntryLike] struct {
	key              string
	entries          []T
	publisherId      string
	publisherAddress string
	eventTime        time.Time
}

// NewIndexUpdateEvent - Creates and returns a new instance of an IndexUpdateEvent.
func NewIndexUpdateEvent[T commons.RecordIndexEntryLike](
	key string,
	entries []T,
	publisherId string,
	publisherAddress string,
	eventTime time.Time,
) IndexUpdateEvent[T] {
	return IndexUpdateEvent[T]{
		key:              key,
		entries:          entries,
		publisherId:      publisherId,
		publisherAddress: publisherAddress,
		eventTime:        eventTime,
	}
}

func (e IndexUpdateEvent[T]) GetKey() string {
	return e.key
}

func (e IndexUpdateEvent[T]) GetEntries() []T {
	return e.entries
}

func (e IndexUpdateEvent[T]) GetPublisherId() string {
	return e.publisherId
}

func (e IndexUpdateEvent[T]) GetPublisherAddress() string {
	return e.publisherAddress
}

func (e IndexUpdateEvent[T]) GetEventTime() time.Time {
	return e.eventTime
}
