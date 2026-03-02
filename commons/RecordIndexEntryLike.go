package commons

import "github.com/SharefulNetworks/shareful-dht/types"

// RecordIndexEntryLike defines the subset of the RecordIndexEntry interface that is applicable
// to the provision of IndexUpdateEvents. This is necessary so as to prevent the
// cyclic import scenario, that would otherwise occur.
type RecordIndexEntryLike interface {
	SetSource(source string)
	SetTarget(target string)
	SetMeta(meta []byte)
	SetUpdatedUnix(updatedUnix int64)
	SetCreatedUnix(createdUnix int64)
	SetPublisher(publisher types.NodeID)
	SetPublisherAddr(publisherAddr string)
	GetSource() string
	GetTarget() string
	GetMeta() []byte
	GetUpdatedUnix() int64
	GetCreatedUnix() int64
	GetPublisher() types.NodeID
	GetPublisherAddr() string
}
