package dht

import "github.com/SharefulNetworks/shareful-dht/types"

type RecordIndexEntry struct {
	Source                  string       `json:"source"`
	Target                  string       `json:"target"`
	Meta                    []byte       `json:"meta"`
	UpdatedUnix             int64        `json:"updated_unix"`
	CreatedUnix             int64        `json:"created_unix"`
	Publisher               types.NodeID `json:"publisher"`
	PublisherAddr           string       `json:"publisher_addr"`
	TTL                     int64        `json:"ttl"`
	LocalRefreshCount       uint64       `json:"local_refresh_count"`        //record level refresh count doesn't make sense for IndexEntry as each entry in the record will be maintained by different nodes, each of which will have their own respective refresh intervals.
	EnableIndexUpdateEvents bool         `json:"enable_index_update_events"` //where true, any updates to the record will trigger the production of a SyncIndexUpdatedEvent which the parent applicaton can use to be promptly notified of any changes and undertake any application level operations, as necessary.

}

// create setter functions
func (entry RecordIndexEntry) SetSource(source string) {
	entry.Source = source
}

func (entry RecordIndexEntry) SetTarget(target string) {
	entry.Target = target
}

func (entry RecordIndexEntry) SetMeta(meta []byte) {
	entry.Meta = meta
}

func (entry RecordIndexEntry) SetUpdatedUnix(updatedUnix int64) {
	entry.UpdatedUnix = updatedUnix
}

func (entry RecordIndexEntry) SetCreatedUnix(createdUnix int64) {
	entry.CreatedUnix = createdUnix
}

func (entry RecordIndexEntry) SetPublisher(publisher types.NodeID) {
	entry.Publisher = publisher
}

func (entry RecordIndexEntry) SetPublisherAddr(publisherAddr string) {
	entry.PublisherAddr = publisherAddr
}

func (entry RecordIndexEntry) SetTTL(ttl int64) {
	entry.TTL = ttl
}

func (entry RecordIndexEntry) SetLocalRefreshCount(localRefreshCount uint64) {
	entry.LocalRefreshCount = localRefreshCount
}

func (entry RecordIndexEntry) SetEnableIndexUpdateEvents(enableIndexUpdateEvents bool) {
	entry.EnableIndexUpdateEvents = enableIndexUpdateEvents
}

// create getter functions
func (entry RecordIndexEntry) GetSource() string {
	return entry.Source
}

func (entry RecordIndexEntry) GetTarget() string {
	return entry.Target
}

func (entry RecordIndexEntry) GetMeta() []byte {
	return entry.Meta
}

func (entry RecordIndexEntry) GetUpdatedUnix() int64 {
	return entry.UpdatedUnix
}

func (entry RecordIndexEntry) GetCreatedUnix() int64 {
	return entry.CreatedUnix
}

func (entry RecordIndexEntry) GetPublisher() types.NodeID {
	return entry.Publisher
}

func (entry RecordIndexEntry) GetPublisherAddr() string {
	return entry.PublisherAddr
}

func (entry RecordIndexEntry) GetTTL() int64 {
	return entry.TTL
}

func (entry RecordIndexEntry) GetLocalRefreshCount() uint64 {
	return entry.LocalRefreshCount
}

func (entry RecordIndexEntry) GetEnableIndexUpdateEvents() bool {
	return entry.EnableIndexUpdateEvents
}
